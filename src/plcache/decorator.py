from __future__ import annotations

import functools
import hashlib
import os
import tempfile
from pathlib import Path
from types import FunctionType
from typing import TYPE_CHECKING, Callable, Union, overload

import diskcache
import polars as pl

if TYPE_CHECKING:
    from ty_extensions import Intersection

    CallableFn = Intersection[FunctionType, Callable[[], None]]

# PolarsType = TypeVar("PolarsType", bound=Union[pl.DataFrame, pl.LazyFrame])


class PolarsCache:
    """A diskcache wrapper for Polars DataFrames and LazyFrames."""

    def __init__(
        self, cache_dir: str | None = None, size_limit: int = 2**30
    ):  # 1GB default
        """
        Initialize the cache.

        Args:
            cache_dir: Directory for cache storage. If None, uses system temp directory.
            size_limit: Maximum cache size in bytes.
        """
        if cache_dir is None:
            cache_dir = os.path.join(tempfile.gettempdir(), "polars_cache")

        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True, parents=True)

        # Use diskcache for metadata (function calls -> parquet file paths)
        self.cache = diskcache.Cache(
            str(self.cache_dir / "metadata"), size_limit=size_limit
        )

        # Directory for parquet files
        self.parquet_dir = self.cache_dir / "parquet_files"
        self.parquet_dir.mkdir(exist_ok=True)

    def _get_cache_key(self, func_name: str, args: tuple, kwargs: dict) -> str:
        """Generate a cache key from function name and arguments."""
        # Create a string representation of the function call
        call_str = f"{func_name}({args}, {kwargs})"

        # Hash it to get a consistent key
        return hashlib.sha256(call_str.encode()).hexdigest()

    def _get_parquet_path(self, cache_key: str) -> Path:
        """Get the parquet file path for a cache key."""
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

    def cache_polars(self, lazy: bool | None = None):
        """
        Decorator for caching Polars DataFrames and LazyFrames.

        Args:
            lazy: If True, always return LazyFrame. If False, always return DataFrame.
                 If None, return the same type as the original result.
        """

        def decorator(
            func: CallableFn[..., Union[pl.DataFrame, pl.LazyFrame]],
        ) -> CallableFn[..., Union[pl.DataFrame, pl.LazyFrame]]:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # Generate cache key
                cache_key = self._get_cache_key(func.__name__, args, kwargs)

                # Check if result is cached
                if cache_key in self.cache:
                    parquet_path = self.cache[cache_key]

                    # Verify the parquet file still exists
                    if os.path.exists(parquet_path):
                        # Determine return type
                        if lazy is None:
                            # We need to execute the function once to determine the original type
                            # This is a limitation - we'll default to DataFrame for None case
                            return_lazy = False
                        else:
                            return_lazy = lazy

                        return self._load_polars_result(parquet_path, return_lazy)
                    else:
                        # File was deleted, remove from cache
                        del self.cache[cache_key]

                # Execute function and cache result
                result = func(*args, **kwargs)

                # Only cache if result is a DataFrame or LazyFrame
                if isinstance(result, (pl.DataFrame, pl.LazyFrame)):
                    original_is_lazy = isinstance(result, pl.LazyFrame)

                    # Save to parquet
                    parquet_path = self._save_polars_result(result, cache_key)

                    # Store path in cache
                    self.cache[cache_key] = parquet_path

                    # Return appropriate type
                    if lazy is None:
                        return result  # Return original type
                    elif lazy and not original_is_lazy:
                        # Convert DataFrame to LazyFrame
                        return pl.scan_parquet(parquet_path)
                    elif not lazy and original_is_lazy:
                        # Convert LazyFrame to DataFrame
                        return pl.read_parquet(parquet_path)
                    else:
                        return result

                return result

            return wrapper

        return decorator

    def clear(self):
        """Clear all cached data."""
        self.cache.clear()
        # Also remove parquet files
        for parquet_file in self.parquet_dir.glob("*.parquet"):
            parquet_file.unlink()


# Convenience function for creating a global cache instance
_global_cache: PolarsCache | None = None


def cache(
    cache_dir: str | None = None,
    lazy: bool | None = None,
    size_limit: int = 2**30,
):
    """
    Convenience decorator for caching Polars DataFrames and LazyFrames.

    Args:
        cache_dir: Directory for cache storage. If None, uses system temp directory.
        lazy: If True, always return LazyFrame. If False, always return DataFrame.
             If None, return the same type as the original result.
        size_limit: Maximum cache size in bytes.
    """
    global _global_cache

    if _global_cache is None or (
        _global_cache.cache_dir
        != Path(cache_dir or tempfile.gettempdir()) / "polars_cache"
    ):
        _global_cache = PolarsCache(cache_dir, size_limit)

    return _global_cache.cache_polars(lazy=lazy)


# Example usage
if __name__ == "__main__":
    # Example 1: Using the class directly
    pc = PolarsCache()

    @pc.cache_polars()
    def load_data() -> pl.DataFrame:
        print("Loading data (expensive operation)...")
        return pl.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})

    @pc.cache_polars(lazy=True)  # Always return LazyFrame
    def process_data() -> pl.LazyFrame:
        print("Processing data (expensive operation)...")
        return (
            pl.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
            .lazy()
            .with_columns(pl.col("x") * 2)
        )

    # Example 2: Using the convenience decorator
    @cache(lazy=False)  # Always return DataFrame
    def get_lazy_data() -> pl.LazyFrame:
        print("Getting lazy data...")
        return pl.DataFrame({"col1": [1, 2, 3]}).lazy()

    # Test the functions
    print("First call:")
    df1 = load_data()
    print(f"Result type: {type(df1)}")
    print(df1)

    print("\nSecond call (should be cached):")
    df2 = load_data()
    print(f"Result type: {type(df2)}")
    print(df2)

    print("\nLazy example:")
    lazy_result = process_data()
    print(f"Result type: {type(lazy_result)}")
    print(lazy_result.collect())
