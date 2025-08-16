"""Test for callable symlink_name functionality."""

from pathlib import Path

import polars as pl

from plcache import cache


def test_callable_symlink_name(tmp_path):
    """Test that callable symlink_name works correctly."""

    def filename_callback(func, bound_args, result, cache_key):
        # Create filename based on function name, value arg, and result shape
        value = bound_args.get("value", "novalue")
        if isinstance(result, pl.DataFrame):
            rows = result.shape[0]
            return f"{func.__name__}_{value}_{rows}rows.parquet"
        else:
            return f"{func.__name__}_{value}_lazy.parquet"

    @cache(cache_dir=tmp_path, symlink_name=filename_callback)
    def callback_test(value: int) -> pl.DataFrame:
        return pl.DataFrame({"value": list(range(value))})

    # Create cached result
    original_result = callback_test(5)

    # Find the symlink - should be named according to callback
    cache_path = Path(tmp_path)
    expected_filename = "callback_test_5_5rows.parquet"
    symlinks = list(cache_path.rglob(expected_filename))
    assert len(symlinks) == 1

    symlink = symlinks[0]

    # Read data through symlink
    symlink_result = pl.read_parquet(symlink)

    # Should be identical to original
    assert symlink_result.equals(original_result)


def test_callable_symlink_fallback_on_error(tmp_path):
    """Test that bad callbacks fall back to default filename."""

    def bad_callback(func, args, kwargs, result, cache_key):
        # This callback will raise an exception
        raise ValueError("Callback failed!")

    @cache(cache_dir=tmp_path, symlink_name=bad_callback)
    def fallback_test(value: int) -> pl.DataFrame:
        return pl.DataFrame({"value": [value]})

    # Should not crash, should fall back to default
    original_result = fallback_test(123)

    # Should find default filename
    cache_path = Path(tmp_path)
    symlinks = list(cache_path.rglob("output.parquet"))  # default filename
    assert len(symlinks) == 1

    symlink_result = pl.read_parquet(symlinks[0])
    assert symlink_result.equals(original_result)
