# tests/custom_callbacks_test.py
"""Tests for custom cache_key and entry_dir callback functionality."""

import hashlib
from pathlib import Path

import polars as pl
import pytest

from plcache import PolarsCache, cache


def test_custom_cache_key_callback(tmp_path):
    """Test that custom cache_key callback affects cache behavior."""

    def custom_cache_key(func, bound_args):
        # Create a custom cache key that ignores function name
        # This means different functions with same args will share cache
        return f"shared_cache({bound_args})"

    pc = PolarsCache(cache_dir=tmp_path, cache_key=custom_cache_key)

    @pc.cache_polars()
    def func_a(value: int) -> pl.DataFrame:
        return pl.DataFrame({"a": [value]})

    @pc.cache_polars()
    def func_b(value: int) -> pl.DataFrame:
        return pl.DataFrame({"b": [value * 2]})  # Different computation

    # First call to func_a - should cache
    result_a1 = func_a(10)

    # Call func_b with same args - should get func_a's cached result!
    # because our custom cache key ignores function name
    result_b = func_b(10)

    # They should be identical (both are func_a's result)
    assert result_a1.equals(result_b)

    # Verify only one blob was created (shared cache)
    blob_dir = Path(tmp_path) / "blobs"
    parquet_files = list(blob_dir.glob("*.parquet"))
    assert len(parquet_files) == 1


def test_custom_entry_dir_callback(tmp_path):
    """Test that custom entry_dir callback affects directory structure."""

    def custom_entry_dir(func, bound_args):
        # Create directory name based only on 'a' parameter
        if "a" in bound_args:
            return f"arg_a_{bound_args['a']}"
        return "no_a_arg"

    pc = PolarsCache(cache_dir=tmp_path, entry_dir=custom_entry_dir)

    @pc.cache_polars()
    def test_func(a: int, b: str = "default") -> pl.DataFrame:
        return pl.DataFrame({"a": [a], "b": [b]})

    # Call with different b values but same a
    _ = test_func(42, b="hello")
    _ = test_func(42, b="world")  # Different b, same a

    cache_path = Path(tmp_path)
    symlinks = list(cache_path.rglob("output.parquet"))

    # Should have 1 symlink (second overwrites first due to same directory)
    assert len(symlinks) == 1
    assert symlinks[0].parent.name == "arg_a_42"

    # Verify we have 2 different blob files (different cache entries)
    blob_dir = cache_path / "blobs"
    parquet_files = list(blob_dir.glob("*.parquet"))
    assert len(parquet_files) == 2


def test_cache_key_vs_entry_dir_independence(tmp_path):
    """Test that cache_key and entry_dir work independently."""

    def same_cache_key(func, bound_args):
        # Always return the same cache key
        return "always_same"

    def unique_entry_dir(func, bound_args):
        # Create unique directory for each call
        import time

        return f"call_{int(time.time() * 1000000)}"

    pc = PolarsCache(
        cache_dir=tmp_path, cache_key=same_cache_key, entry_dir=unique_entry_dir
    )

    @pc.cache_polars()
    def test_func(value: int) -> pl.DataFrame:
        return pl.DataFrame({"value": [value]})

    # First call - creates cache and symlink
    result1 = test_func(100)

    # Second call with different args - gets cached result, NO new symlink
    result2 = test_func(200)

    # Should be identical (cached)
    assert result1.equals(result2)

    cache_path = Path(tmp_path)
    symlinks = list(cache_path.rglob("output.parquet"))

    # Should have only 1 symlink (only first call creates symlink)
    assert len(symlinks) == 1

    # But only one blob file (shared cache)
    blob_dir = cache_path / "blobs"
    parquet_files = list(blob_dir.glob("*.parquet"))
    assert len(parquet_files) == 1


def test_custom_cache_key_with_normalised_args(tmp_path):
    """Test that custom cache_key receives properly normalised arguments."""
    captured_calls = []

    def capture_cache_key(func, bound_args):
        # Capture what we receive for inspection
        captured_calls.append((func.__name__, bound_args))
        return f"test_cache_{len(captured_calls)}"

    pc = PolarsCache(cache_dir=tmp_path, cache_key=capture_cache_key)

    @pc.cache_polars()
    def test_func(a: int, b: str = "default", **extra) -> pl.DataFrame:
        return pl.DataFrame({"a": [a], "b": [b]})

    # Call with positional and keyword args
    _ = test_func(10, "hello", extra1="x", extra2="y")

    # Check what the cache_key callback received
    assert len(captured_calls) == 1
    func_name, received_bound_args = captured_calls[0]

    # Should receive normalized args
    assert func_name == "test_func"
    expected = {"a": 10, "b": "hello", "extra1": "x", "extra2": "y"}
    assert received_bound_args == expected


def test_custom_entry_dir_with_normalised_args(tmp_path):
    """Test that custom entry_dir receives properly normalised arguments."""
    captured_calls = []

    def capture_entry_dir(func, bound_args):
        # Capture normalised args for inspection
        captured_calls.append(bound_args)
        return "captured_dir"

    pc = PolarsCache(cache_dir=tmp_path, entry_dir=capture_entry_dir)

    @pc.cache_polars()
    def test_func(a: int, b: str = "default", **extra) -> pl.DataFrame:
        return pl.DataFrame({"a": [a], "b": [b]})

    # Call with positional and keyword args
    _ = test_func(10, "hello", extra2="y", extra1="x")  # kwargs in different order

    # Check what the entry_dir callback received
    assert len(captured_calls) == 1
    normalised_args = captured_calls[0]

    # Should have normalised args with signature order + sorted kwargs
    expected = {"a": 10, "b": "hello", "extra1": "x", "extra2": "y"}
    assert normalised_args == expected


def test_convenience_decorator_with_custom_callbacks(tmp_path):
    """Test that the convenience @cache decorator doesn't support custom callbacks."""

    def custom_cache_key(func, args, kwargs):
        return "custom"

    def custom_entry_dir(func, args, kwargs):
        return "custom_dir"

    # The @cache convenience function doesn't accept these parameters
    # So this should work without using the callbacks
    @cache(cache_dir=tmp_path)
    def test_func(value: int) -> pl.DataFrame:
        return pl.DataFrame({"value": [value]})

    _ = test_func(123)

    # Should use default behavior (no custom callbacks)
    cache_path = Path(tmp_path)
    symlinks = list(cache_path.rglob("output.parquet"))
    assert len(symlinks) == 1

    # Directory should follow default naming (not "custom_dir")
    assert symlinks[0].parent.name != "custom_dir"


def test_default_cache_ident_function(tmp_path):
    """Test the default cache identifier function behavior."""
    from plcache.decorator import _DEFAULT_CACHE_IDENT, normalise_args

    def test_func(a: int, b: str = "test") -> pl.DataFrame:
        return pl.DataFrame()

    # Test the default cache ident function with normalized args
    bound_args = normalise_args(test_func, (10,), {"b": "hello"})
    ident = _DEFAULT_CACHE_IDENT(test_func, bound_args)

    expected = f"{test_func.__module__}.{test_func.__qualname__}({bound_args})"
    assert ident == expected


def test_cache_key_fallback_on_callback_error(tmp_path):
    """Test behavior when cache_key callback raises an exception."""

    def broken_cache_key(func, bound_args):
        raise ValueError("Cache key callback failed!")

    pc = PolarsCache(cache_dir=tmp_path, cache_key=broken_cache_key)

    @pc.cache_polars()
    def test_func(value: int) -> pl.DataFrame:
        return pl.DataFrame({"value": [value]})

    # This should raise an exception because cache key generation fails
    with pytest.raises(ValueError, match="Cache key callback failed!"):
        test_func(123)


def test_entry_dir_fallback_on_callback_error(tmp_path):
    """Test behavior when entry_dir callback raises an exception."""

    def broken_entry_dir(func, bound_args):
        raise ValueError("Entry dir callback failed!")

    pc = PolarsCache(cache_dir=tmp_path, entry_dir=broken_entry_dir)

    @pc.cache_polars()
    def test_func(value: int) -> pl.DataFrame:
        return pl.DataFrame({"value": [value]})

    # This should raise an exception because entry dir generation fails
    with pytest.raises(ValueError, match="Entry dir callback failed!"):
        test_func(123)


def test_cache_key_affects_cache_hits_and_misses(tmp_path):
    """Test that custom cache_key properly affects cache hit/miss behavior."""
    call_count = 0

    def ignore_first_arg_cache_key(func, bound_args):
        # Cache key that ignores the 'ignored_arg' parameter
        filtered_args = {k: v for k, v in bound_args.items() if k != "ignored_arg"}
        return f"{func.__module__}.{func.__qualname__}({filtered_args})"

    pc = PolarsCache(cache_dir=tmp_path, cache_key=ignore_first_arg_cache_key)

    @pc.cache_polars()
    def counting_func(ignored_arg: int, important_arg: int) -> pl.DataFrame:
        nonlocal call_count
        call_count += 1
        return pl.DataFrame({"count": [call_count], "important": [important_arg]})

    # First call
    result1 = counting_func(1, 100)
    assert call_count == 1

    # Second call with different first arg but same second arg
    # Should hit cache because first arg is ignored
    result2 = counting_func(999, 100)  # Different first arg
    assert call_count == 1  # Should not increment (cache hit)
    assert result1.equals(result2)

    # Third call with different second arg
    # Should miss cache because second arg matters
    result3 = counting_func(1, 200)  # Different second arg
    assert call_count == 2  # Should increment (cache miss)
    assert not result1.equals(result3)


def test_sort_args_function():
    """Test the sort_args utility function directly."""
    import inspect

    from plcache.decorator import sort_args

    def example_func(b: int, a: str, **kwargs):
        pass

    sig = inspect.signature(example_func)
    bound_args = {"a": "hello", "b": 42, "kwargs": {"z": "last", "c": "first"}}

    result = sort_args(sig, bound_args)

    # Should have signature params first (b, a) then sorted kwargs (c, z)
    expected = {"b": 42, "a": "hello", "c": "first", "z": "last"}
    assert result == expected


def test_normalise_args_function():
    """Test the normalise_args utility function directly."""
    from plcache.decorator import normalise_args

    def example_func(a: int, b: str = "default", **kwargs):
        pass

    # Test with missing default
    result = normalise_args(example_func, (10,), {"extra": "value"})
    expected = {"a": 10, "b": "default", "extra": "value"}
    assert result == expected

    # Test with all args provided - should be same whether called positionally or by name
    result1 = normalise_args(example_func, (20, "custom"), {"z": "end", "c": "start"})
    result2 = normalise_args(
        example_func, (), {"a": 20, "b": "custom", "z": "end", "c": "start"}
    )
    expected = {"a": 20, "b": "custom", "c": "start", "z": "end"}
    assert result1 == expected
    assert result2 == expected
