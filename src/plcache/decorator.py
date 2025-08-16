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
from .config_dataclasses import PathConfig, SymlinkConfig
from .paths import get_parquet_path, get_readable_path
from .symlinks import create_symlink

if TYPE_CHECKING:
    from .types import CacheKeyCallback, DecoratedFn, EntryDirCallback, FilenameCallback


def _DEFAULT_CACHE_IDENT(func: DecoratedFn, bound_args: dict[str, Any]) -> str:
    """Default cache key (ident function, the value that gets hashed)."""
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
        """Initialise the cache."""
        if cache_dir is None:
            cache_dir_name = ".polars_cache" if hidden else "polars_cache"
            if use_tmp:
                cache_dir = Path(tempfile.gettempdir()) / cache_dir_name
            else:
                cache_dir = Path.cwd() / cache_dir_name

        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True, parents=True)

        # Configuration objects
        self.path_config = PathConfig(
            cache_dir=self.cache_dir,
            symlinks_dir_name=symlinks_dir,
            nested=nested,
            trim_arg=trim_arg,
            entry_dir_callback=entry_dir,
        )
        self.symlink_config = SymlinkConfig(symlink_name=symlink_name)
        self.cache_ident = _DEFAULT_CACHE_IDENT if cache_key is None else cache_key

        # Use diskcache for metadata
        self.cache = diskcache.Cache(
            str(self.cache_dir / "metadata"), size_limit=_parse_size(size_limit)
        )

    def _get_cache_key(self, func: DecoratedFn, bound_args: dict[str, Any]) -> str:
        """Generate a cache key from function name and arguments."""
        ident = self.cache_ident(func, bound_args)
        return hashlib.sha256(ident.encode()).hexdigest()

    def _save_polars_result(
        self, result: pl.DataFrame | pl.LazyFrame, cache_key: str
    ) -> str:
        """Save a Polars DataFrame or LazyFrame to parquet and return the path."""
        parquet_path = get_parquet_path(self.path_config, cache_key)

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
        """Load a Polars DataFrame or LazyFrame from parquet."""
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
        """Decorator for caching Polars DataFrames and LazyFrames."""

        def decorator(func: DecoratedFn) -> DecoratedFn:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                bound_args = normalise_args(func, args, kwargs)
                cache_key = self._get_cache_key(func, bound_args)

                # Check if result is cached
                if cache_key in self.cache:
                    cached_data = self.cache[cache_key]
                    parquet_path = cached_data["path"]
                    is_lazy = cached_data["is_lazy"]

                    if os.path.exists(parquet_path):
                        return self._load_polars_result(parquet_path, is_lazy)
                    else:
                        del self.cache[cache_key]

                # Execute function and cache result
                result = func(*args, **kwargs)

                if isinstance(result, (pl.DataFrame, pl.LazyFrame)):
                    is_lazy = isinstance(result, pl.LazyFrame)
                    parquet_path = self._save_polars_result(result, cache_key)
                    self.cache[cache_key] = {"path": parquet_path, "is_lazy": is_lazy}

                    # Create symlink with override configs if provided
                    path_config = self.path_config
                    symlink_config = self.symlink_config

                    if any(x is not None for x in [symlinks_dir, nested, trim_arg]):
                        from dataclasses import replace

                        path_config = replace(
                            path_config,
                            symlinks_dir_name=symlinks_dir
                            or path_config.symlinks_dir_name,
                            nested=nested if nested is not None else path_config.nested,
                            trim_arg=trim_arg
                            if trim_arg is not None
                            else path_config.trim_arg,
                        )

                    if symlink_name is not None:
                        from dataclasses import replace

                        symlink_config = replace(
                            symlink_config, symlink_name=symlink_name
                        )

                    readable_dir = get_readable_path(path_config, func, bound_args)
                    blob_path = get_parquet_path(path_config, cache_key)
                    create_symlink(
                        symlink_config,
                        func,
                        bound_args,
                        cache_key,
                        result,
                        readable_dir,
                        blob_path,
                    )

                return result

            return wrapper

        return decorator

    def clear(self):
        """Clear all cached data."""
        self.cache.clear()
        for parquet_file in (self.cache_dir / "blobs").glob("*.parquet"):
            parquet_file.unlink()
        if (self.cache_dir / self.path_config.symlinks_dir_name).exists():
            import shutil

            shutil.rmtree(
                self.cache_dir / self.path_config.symlinks_dir_name, ignore_errors=True
            )
            (self.cache_dir / self.path_config.symlinks_dir_name).mkdir(exist_ok=True)


# Global cache instance
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
    """Convenience decorator for caching Polars DataFrames and LazyFrames."""
    global _global_cache
    uncached = isinstance(_global_cache, _DummyCache)

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
