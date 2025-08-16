"""Caching decorator implementation for Polars DataFrames and LazyFrames."""

from __future__ import annotations

import functools
import hashlib
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, overload

import diskcache
import polars as pl

from ._args import normalise_args
from ._debugging import snoop
from ._dummy import _DummyCache
from ._parse_sizes import _parse_size
from .path_manager import CachePathManager
from .symlink_manager import SymlinkManager

if TYPE_CHECKING:
    from .types import CacheKeyCallback, DecoratedFn, EntryDirCallback, FilenameCallback


def _DEFAULT_CACHE_IDENT(func: DecoratedFn, bound_args: dict[str, Any]) -> str:
    """Default cache key (ident function, the value that gets hashed).

    Args:
        func: The decorated function to generate a cache key for.
        bound_args: Bound arguments passed to the function in the cached call.

    Returns:
        str: Conjoined function module, qualname, and positional/named arguments.
    """
    return f"{func.__module__}.{func.__qualname__}({bound_args})"


class PolarsCache:
    """A diskcache wrapper for Polars DataFrames and LazyFrames with configurable readable cache structure."""

    def __init__(
        self,
        cache_dir: str | None = None,
        use_tmp: bool = False,
        hidden: bool = True,
        size_limit: int | str = "1GB",
        symlinks_dir: str = "functions",
        nested: bool = True,
        trim_arg: int = 50,
        symlink_name: str | FilenameCallback | None = None,
        cache_key: CacheKeyCallback | None = None,
        entry_dir: EntryDirCallback | None = None,
    ):
        """Initialise the cache.

        Args:
            cache_dir: Directory for cache storage. If None, uses current working directory.
            use_tmp: If True and cache_dir is None, put cache dir in system temp directory.
            hidden: If True, prefix directory name with dot (e.g. '.polars_cache').
            size_limit: Maximum cache size in bytes (int) or as a string. Default: "1GB".
            symlinks_dir: Name of the readable directory. Default: "functions".
            nested: If True, split module.function into module/function dirs.
                    If False, use percent-encoded function qualname as single dir.
            trim_arg: Maximum length for argument values in directory names.
            symlink_name: Custom name for symlink files. Can be a string or a callable
                          which will receive the function being cached, its bound args,
                          the result, and cache key. If None, uses default of "output.parquet".
            cache_key: Optional callback to set the cache key, otherwise made from the
                       decorated function `{__module__}.{__qualname__}({bound_args})`.
            entry_dir: Optional callback to set the directory name for a cache item. Not
                       used for hashing but should be unique to avoid overwriting symlinks
                       to cached results (the actual data blobs are preserved separately).
        """
        if cache_dir is None:
            cache_dir_name = ".polars_cache" if hidden else "polars_cache"
            if use_tmp:
                cache_dir = Path(tempfile.gettempdir()) / cache_dir_name
            else:
                cache_dir = Path.cwd() / cache_dir_name

        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True, parents=True)

        # Initialise managers
        self.path_manager = CachePathManager(
            cache_dir=self.cache_dir,
            symlinks_dir_name=symlinks_dir,
            nested=nested,
            trim_arg=trim_arg,
            entry_dir_callback=entry_dir,
        )
        self.symlink_manager = SymlinkManager(symlink_name=symlink_name)

        # Cache key generation
        self.cache_ident = _DEFAULT_CACHE_IDENT if cache_key is None else cache_key

        # Use diskcache for metadata (function calls -> parquet file paths)
        self.cache = diskcache.Cache(
            str(self.cache_dir / "metadata"), size_limit=_parse_size(size_limit)
        )

    def _get_cache_key(self, func: DecoratedFn, bound_args: dict[str, Any]) -> str:
        """Generate a cache key from function name and arguments.

        Creates a unique hash-based key by combining the function's module path,
        qualname, and all arguments to ensure cache hits only occur for identical calls.

        Args:
            func: The function being cached.
            bound_args: Bound arguments passed to the function.

        Returns:
            A SHA256 hash string representing the unique cache key.
        """
        ident = self.cache_ident(func, bound_args)
        return hashlib.sha256(ident.encode()).hexdigest()

    def _save_polars_result(
        self, result: pl.DataFrame | pl.LazyFrame, cache_key: str
    ) -> str:
        """Save a Polars DataFrame or LazyFrame to parquet and return the path.

        Args:
            result: The Polars DataFrame or LazyFrame to save.
            cache_key: The unique cache key for this result.

        Returns:
            String path to the saved parquet file.

        Raises:
            TypeError: If result is not a DataFrame or LazyFrame.
        """
        parquet_path = self.path_manager.get_parquet_path(cache_key)

        if isinstance(result, pl.DataFrame):
            result.write_parquet(parquet_path)
        elif isinstance(result, pl.LazyFrame):
            result.sink_parquet(parquet_path)
        else:
            raise TypeError(f"Expected DataFrame or LazyFrame, got {type(result)}")

        return str(parquet_path)

    @overload
    def _load_polars_result(
        self, parquet_path: str, lazy: bool = True
    ) -> pl.LazyFrame: ...

    @overload
    def _load_polars_result(
        self, parquet_path: str, lazy: bool = False
    ) -> pl.DataFrame: ...

    def _load_polars_result(
        self, parquet_path: str, lazy: bool = False
    ) -> pl.DataFrame | pl.LazyFrame:
        """Load a Polars DataFrame or LazyFrame from parquet.

        Args:
            parquet_path: Path to the parquet file to load.
            lazy: If True, return a LazyFrame; if False, return a DataFrame.

        Returns:
            A Polars DataFrame if lazy=False, or LazyFrame if lazy=True.
        """
        if lazy:
            return pl.scan_parquet(parquet_path)
        else:
            return pl.read_parquet(parquet_path)

    def cache_polars(
        self,
        symlinks_dir: str | None = None,
        nested: bool | None = None,
        trim_arg: int | None = None,
        symlink_name: str | FilenameCallback | None = None,
    ):
        """Decorator for caching Polars DataFrames and LazyFrames.

        This decorator will cache function results that return Polars DataFrames or
        LazyFrames. The cache uses function signatures (module, name, bound_args)
        to determine cache hits. Results are stored as parquet files with metadata
        tracked via diskcache, and readable symlink structures are created for
        easy file system navigation.

        Args:
            symlinks_dir: Override instance setting for readable directory name.
            nested: Override instance setting for module path splitting.
            trim_arg: Override instance setting for max argument length.
            symlink_name: Override instance setting for symlink filename.

        Returns:
            A decorator function that can be applied to functions returning
            Polars DataFrames or LazyFrames.
        """

        def decorator(func: DecoratedFn) -> DecoratedFn:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # Generate cache key
                bound_args = normalise_args(func, args, kwargs)
                cache_key = self._get_cache_key(func, bound_args)

                # Check if result is cached
                if cache_key in self.cache:
                    cached_data = self.cache[cache_key]
                    parquet_path = cached_data["path"]
                    is_lazy = cached_data["is_lazy"]

                    # Verify the parquet file still exists
                    if os.path.exists(parquet_path):
                        return self._load_polars_result(parquet_path, is_lazy)
                    else:
                        # File was deleted, remove from cache
                        del self.cache[cache_key]

                # Execute function and cache result
                result = func(*args, **kwargs)

                # Only cache if result is a DataFrame or LazyFrame
                if isinstance(result, (pl.DataFrame, pl.LazyFrame)):
                    is_lazy = isinstance(result, pl.LazyFrame)

                    # Save to parquet
                    parquet_path = self._save_polars_result(result, cache_key)

                    # Store path and type info in cache
                    self.cache[cache_key] = {"path": parquet_path, "is_lazy": is_lazy}

                    # Create readable symlink using temporary manager if overrides provided
                    if any(
                        x is not None
                        for x in [symlinks_dir, nested, trim_arg, symlink_name]
                    ):
                        # Create temporary managers with overridden settings
                        temp_path_manager = CachePathManager(
                            cache_dir=self.cache_dir,
                            symlinks_dir_name=symlinks_dir
                            or self.path_manager.symlinks_dir_name,
                            nested=nested
                            if nested is not None
                            else self.path_manager.nested,
                            trim_arg=trim_arg
                            if trim_arg is not None
                            else self.path_manager.trim_arg,
                            entry_dir_callback=self.path_manager.entry_dir_callback,
                        )
                        temp_symlink_manager = SymlinkManager(
                            symlink_name=symlink_name
                            if symlink_name is not None
                            else self.symlink_manager.symlink_name
                        )

                        readable_dir = temp_path_manager.get_readable_path(
                            func, bound_args
                        )
                        blob_path = temp_path_manager.get_parquet_path(cache_key)
                        temp_symlink_manager.create_symlink(
                            func, bound_args, cache_key, result, readable_dir, blob_path
                        )
                    else:
                        # Use instance managers
                        readable_dir = self.path_manager.get_readable_path(
                            func, bound_args
                        )
                        blob_path = self.path_manager.get_parquet_path(cache_key)
                        self.symlink_manager.create_symlink(
                            func, bound_args, cache_key, result, readable_dir, blob_path
                        )

                return result

            return wrapper

        return decorator

    def clear(self):
        """Clear all cached data.

        Removes all cached metadata, parquet files, and the readable symlink
        structure. This completely resets the cache to an empty state while
        preserving the cache directory structure for future use.
        """
        self.cache.clear()
        # Remove parquet files
        for parquet_file in self.path_manager.parquet_dir.glob("*.parquet"):
            parquet_file.unlink()
        # Remove readable structure
        if self.path_manager.readable_dir.exists():
            import shutil

            shutil.rmtree(self.path_manager.readable_dir, ignore_errors=True)
            self.path_manager.readable_dir.mkdir(exist_ok=True)


# Convenience function for creating a global cache instance. Initialise with dummy cache
_global_cache: PolarsCache | _DummyCache = _DummyCache()


@snoop()
def cache(
    cache_dir: str | None = None,
    use_tmp: bool = False,
    hidden: bool = True,
    size_limit: int | str = "1GB",
    symlinks_dir: str = "functions",
    nested: bool = True,
    trim_arg: int = 50,
    symlink_name: str | None = None,
):
    """Convenience decorator for caching Polars DataFrames and LazyFrames.

    This function provides a simple interface to create and use a global cache
    instance for decorating functions that return Polars DataFrames or LazyFrames.
    On first call, it initialises the global cache with the provided settings.
    Subsequent calls will reuse the existing cache unless a different cache_dir
    is specified.

    Args:
        cache_dir: Directory for cache storage. If None, uses current working directory
                   or system temp directory if use_tmp is True.
        use_tmp: If True and cache_dir is None, put cache dir in system temp directory.
        hidden: If True, prefix directory name with dot (e.g. '.polars_cache').
        size_limit: Maximum cache size in bytes (int) or as a string. Default: "1GB".
        symlinks_dir: Name of the readable directory ("functions", "cache", etc.).
        nested: If True, split module.function into module/function dirs.
                If False, use encoded full qualname as single dir.
        trim_arg: Maximum length for argument values in directory names.
        symlink_name: Custom name for symlink files. If None, uses default.

    Returns:
        A decorator function that can be applied to functions returning
        Polars DataFrames or LazyFrames.

    Example:
        ```python
        from plcache import cache

        @cache(cache_dir="./my_cache", size_limit="500MB")
        def load_data() -> pl.DataFrame:
            return pl.read_csv("large_file.csv")
        ```
    """
    global _global_cache
    uncached = isinstance(_global_cache, _DummyCache)

    # Create new cache if we're still using the dummy (first call to `cache()`)
    if uncached or (
        cache_dir is not None and Path(_global_cache.cache_dir) != Path(cache_dir)
    ):
        _global_cache = PolarsCache(
            cache_dir=cache_dir,
            use_tmp=use_tmp,
            hidden=hidden,
            size_limit=_parse_size(size_limit),
            symlinks_dir=symlinks_dir,
            nested=nested,
            symlink_name=symlink_name,
            trim_arg=trim_arg,
        )

    return _global_cache.cache_polars(
        symlinks_dir=symlinks_dir,
        nested=nested,
        trim_arg=trim_arg,
        symlink_name=symlink_name,
    )
