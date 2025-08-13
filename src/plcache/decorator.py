from __future__ import annotations

import functools
import hashlib
import os
import tempfile
import urllib.parse
from pathlib import Path
from typing import TYPE_CHECKING, overload

import diskcache
import polars as pl

from ._debugging import snoop

if TYPE_CHECKING:
    from .types import CallableFn

_GB = 2**30

_DEFAULT_SYMLINK_NAME = "output.parquet"


class PolarsCache:
    """A diskcache wrapper for Polars DataFrames and LazyFrames with configurable readable cache structure."""

    def __init__(
        self,
        cache_dir: str | None = None,
        use_tmp: bool = False,
        hidden: bool = True,
        size_limit: int = 1 * _GB,
        readable_dir_name: str = "functions",
        split_module_path: bool = True,
        max_arg_length: int = 50,
        symlink_name: str | None = None,
    ):
        """
        Initialise the cache.

        Args:
            cache_dir: Directory for cache storage. If None, uses current working directory.
            use_tmp: If True and cache_dir is None, put cache dir in system temp directory.
            hidden: If True, prefix directory name with dot (e.g. '.polars_cache').
            size_limit: Maximum cache size in bytes. Default: 1GB (`2**30`).
            readable_dir_name: Name of the readable directory. Default: "functions".
            split_module_path: If True, split module.function into module/function dirs.
                               If False, use percent-encoded function qualname as single dir.
            max_arg_length: Maximum length for argument values in directory names.
        """
        if cache_dir is None:
            dir_name = ".polars_cache" if hidden else "polars_cache"
            if use_tmp:
                cache_dir = Path(tempfile.gettempdir()) / dir_name
            else:
                cache_dir = Path.cwd() / dir_name

        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True, parents=True)

        # Configuration
        self.symlink_name = symlink_name
        self.readable_dir_name = readable_dir_name
        self.split_module_path = split_module_path
        self.max_arg_length = max_arg_length

        # Use diskcache for metadata (function calls -> parquet file paths)
        self.cache = diskcache.Cache(
            str(self.cache_dir / "metadata"), size_limit=size_limit
        )

        # Directory for parquet files (blobs)
        self.parquet_dir = self.cache_dir / "blobs"
        self.parquet_dir.mkdir(exist_ok=True)

        # Directory for readable structure
        self.readable_dir = self.cache_dir / self.readable_dir_name
        self.readable_dir.mkdir(exist_ok=True)

    def _get_cache_key(
        self,
        func: CallableFn[..., pl.DataFrame | pl.LazyFrame],
        args: tuple,
        kwargs: dict,
    ) -> str:
        """Generate a cache key from function name and arguments."""
        # Create a string representation of the function call
        ident = f"{func.__module__}.{func.__qualname__}({args}, {kwargs})"
        # Hash it to get a consistent key
        cache_key = hashlib.sha256(ident.encode()).hexdigest()
        return cache_key

    def _get_parquet_path(self, cache_key: str) -> Path:
        """Get the parquet file path for a cache key (in blobs directory)."""
        return self.parquet_dir / f"{cache_key}.parquet"

    def _save_polars_result(
        self, result: pl.DataFrame | pl.LazyFrame, cache_key: str
    ) -> str:
        """Save a Polars DataFrame or LazyFrame to parquet and return the path."""
        parquet_path = self._get_parquet_path(cache_key)

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
        readable_dir_name: str | None = None,
        split_module_path: bool | None = None,
        max_arg_length: int | None = None,
        symlink_name: str | None = None,
    ):
        """
        Decorator for caching Polars DataFrames and LazyFrames.

        Args:
            readable_dir_name: Override instance setting for readable directory name.
            split_module_path: Override instance setting for module path splitting.
            max_arg_length: Override instance setting for max argument length.
        """
        # Use instance defaults if not overridden
        use_dir_name = (
            readable_dir_name
            if readable_dir_name is not None
            else self.readable_dir_name
        )
        use_split_module = (
            split_module_path
            if split_module_path is not None
            else self.split_module_path
        )
        use_max_arg_len = (
            max_arg_length if max_arg_length is not None else self.max_arg_length
        )
        use_symlink_name = (
            symlink_name if symlink_name is not None else self.symlink_name
        )

        def decorator(
            func: CallableFn[..., pl.DataFrame | pl.LazyFrame],
        ) -> CallableFn[..., pl.DataFrame | pl.LazyFrame]:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # Generate cache key
                cache_key = self._get_cache_key(func, args, kwargs)

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

                    # Create readable symlink
                    # Temporarily override instance settings for this call
                    old_dir_name = self.readable_dir_name
                    old_split = self.split_module_path
                    old_max_arg = self.max_arg_length
                    old_symlink_name = self.symlink_name

                    self.readable_dir_name = use_dir_name
                    self.split_module_path = use_split_module
                    self.max_arg_length = use_max_arg_len
                    self.symlink_name = use_symlink_name

                    try:
                        self._create_readable_symlink(
                            func, args, kwargs, cache_key, result
                        )
                    finally:
                        # Restore instance settings
                        self.readable_dir_name = old_dir_name
                        self.split_module_path = old_split
                        self.max_arg_length = old_max_arg
                        self.symlink_name = old_symlink_name

                    return result

                return result

            return wrapper

        return decorator

    def _create_readable_symlink(
        self,
        func: CallableFn[..., pl.DataFrame | pl.LazyFrame],
        args: tuple,
        kwargs: dict,
        cache_key: str,
        result: pl.DataFrame | pl.LazyFrame,
    ):
        """Create a readable symlink structure pointing to the blob."""
        # Get module and function info
        module_name = func.__module__
        func_qualname = func.__qualname__

        # Build the readable path structure
        if self.split_module_path:
            # Split: readable_dir/module_name/func_qualname/args/
            encoded_module = urllib.parse.quote(module_name, safe="")
            readable_path = self.readable_dir / encoded_module / func_qualname
        else:
            # Flat: readable_dir/full_qualname/args/
            full_qualname = f"{module_name}.{func_qualname}"
            encoded_qualname = urllib.parse.quote(full_qualname, safe="")
            readable_path = self.readable_dir / encoded_qualname

        # Create args directory name
        args_parts = []
        for i, arg in enumerate(args):
            arg_str = str(arg)[: self.max_arg_length]
            encoded_arg = urllib.parse.quote(arg_str, safe="")
            args_parts.append(f"arg{i}={encoded_arg}")

        for key, value in kwargs.items():
            value_str = str(value)[: self.max_arg_length]
            encoded_value = urllib.parse.quote(value_str, safe="")
            args_parts.append(f"{key}={encoded_value}")

        args_dir_name = "_".join(args_parts) if args_parts else "no_args"
        final_readable_dir = readable_path / args_dir_name
        final_readable_dir.mkdir(parents=True, exist_ok=True)

        # Determine filename based on result type
        symlink_name = self.symlink_name or _DEFAULT_SYMLINK_NAME

        # Create symlink
        symlink_path = final_readable_dir / symlink_name
        blob_path = self.parquet_dir / f"{cache_key}.parquet"

        # Create relative path for symlink
        try:
            relative_blob = os.path.relpath(blob_path, final_readable_dir)
            if not symlink_path.exists():
                symlink_path.symlink_to(relative_blob)
        except (OSError, FileExistsError):
            # Symlink creation failed, but that's okay - cache still works
            pass

    def clear(self):
        """Clear all cached data."""
        self.cache.clear()
        # Remove parquet files
        for parquet_file in self.parquet_dir.glob("*.parquet"):
            parquet_file.unlink()
        # Remove readable structure
        if self.readable_dir.exists():
            import shutil

            shutil.rmtree(self.readable_dir, ignore_errors=True)
            self.readable_dir.mkdir(exist_ok=True)


class _DummyCache:
    """A dummy cache that does nothing - just executes functions normally."""

    cache_dir = None

    def cache_polars(self, **kwargs):
        def decorator(func):
            return func  # Just return the original function unchanged

        return decorator


# Convenience function for creating a global cache instance. Initialise with dummy cache
_global_cache: PolarsCache | _DummyCache = _DummyCache()


@snoop()
def cache(
    cache_dir: str | None = None,
    use_tmp: bool = False,
    hidden: bool = True,
    size_limit: int = 1 * _GB,
    readable_dir_name: str = "functions",
    split_module_path: bool = True,
    max_arg_length: int = 50,
    symlink_name: str | None = None,
):
    """
    Convenience decorator for caching Polars DataFrames and LazyFrames.

    Args:
        cache_dir: Directory for cache storage. If None, uses system temp directory.
        size_limit: Maximum cache size in bytes. Default: 1GB (`2 ** 30`).
        readable_dir_name: Name of the readable directory ("functions", "cache", etc.).
        split_module_path: If True, split module.function into module/function dirs.
                          If False, use encoded full qualname as single dir.
        max_arg_length: Maximum length for argument values in directory names.
    """
    global _global_cache
    uncached = isinstance(_global_cache, _DummyCache)

    # Create new cache if we're still using the dummy (first call to `cache()`)
    if uncached or (
        cache_dir is not None and Path(_global_cache.cache_dir) != Path(cache_dir)
    ):
        _global_cache = PolarsCache(
            cache_dir=cache_dir,
            size_limit=size_limit,
            readable_dir_name=readable_dir_name,
            split_module_path=split_module_path,
            symlink_name=symlink_name,
            max_arg_length=max_arg_length,
        )

    return _global_cache.cache_polars(
        readable_dir_name=readable_dir_name,
        split_module_path=split_module_path,
        max_arg_length=max_arg_length,
        symlink_name=symlink_name,
    )


# Example usage
if __name__ == "__main__":
    # Example 1: Using the class directly with custom settings
    pc = PolarsCache(
        cache_dir="/tmp/my_polars_cache",
        readable_dir_name="cached_functions",
        split_module_path=True,
        symlink_name="result.parquet",
    )

    @pc.cache_polars()
    def load_data(rows: int = 5) -> pl.DataFrame:
        print(f"Loading {rows} rows (expensive operation)...")
        return pl.DataFrame(
            {"a": list(range(rows)), "b": [i * 10 for i in range(rows)]}
        )

    @pc.cache_polars()
    def process_data(multiplier: int = 2) -> pl.LazyFrame:
        print(f"Processing data with multiplier {multiplier}...")
        return (
            pl.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
            .lazy()
            .with_columns(pl.col("x") * multiplier)
        )

    # Example 2: Using convenience decorator with different settings
    @cache(
        cache_dir="/tmp/another_cache",
        readable_dir_name="by_function",
        split_module_path=False,  # Flat structure
        symlink_name="cached_output.parquet",
    )
    def get_sample_data(size: int = 3, prefix: str = "test") -> pl.LazyFrame:
        print(f"Generating sample data: size={size}, prefix={prefix}")
        return pl.DataFrame({f"{prefix}_col": [i for i in range(size)]}).lazy()

    # Test the functions
    print("=== Testing load_data ===")
    df1 = load_data(rows=10)
    print(f"First call result type: {type(df1)}")

    print("\nSecond call (should be cached):")
    df2 = load_data(rows=10)
    print(f"Second call result type: {type(df2)}")

    print("\n=== Testing process_data ===")
    lazy_result = process_data(multiplier=3)
    print(f"Lazy result type: {type(lazy_result)}")
    print(lazy_result.collect())

    print("\n=== Testing get_sample_data ===")
    sample = get_sample_data(size=5, prefix="demo")
    print(f"Sample result type: {type(sample)}")
    print(sample)

    print("\n=== Cache directory structures ===")
    print("Cache 1 structure:")
    print(
        "  /tmp/my_polars_cache/cached_functions/__main__/load_data/arg0=10/result.parquet"
    )
    print("Cache 2 structure:")
    print(
        "  /tmp/another_cache/by_function/__main__.get_sample_data/arg0=5_prefix=demo/cached_output.parquet"
    )
