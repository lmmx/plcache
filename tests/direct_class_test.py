# tests/direct_class_test.py
"""Direct class usage tests for the Polars caching system."""

from pathlib import Path

import polars as pl

from plcache import PolarsCache


def test_polars_cache_class_direct_usage(tmp_path):
    """Test using PolarsCache class directly with custom settings."""
    pc = PolarsCache(
        cache_dir=tmp_path,
        symlinks_dir="cached_functions",
        nested=True,
        symlink_name="result.parquet",
        trim_arg=20,
    )

    @pc.cache_polars()
    def class_test_func(long_string: str) -> pl.DataFrame:
        return pl.DataFrame({"data": [long_string]})

    # Test with a long argument that should be truncated
    long_arg = "this_is_a_very_long_string_that_should_be_truncated"
    _ = class_test_func(long_arg)

    cache_path = Path(tmp_path)

    # Find the symlink (arg should be truncated to 20 chars)
    symlinks = list(cache_path.rglob("result.parquet"))
    assert len(symlinks) == 1

    # Verify the directory name contains truncated argument
    symlink_parent = symlinks[0].parent.name
    assert "this_is_a_very_long_" in symlink_parent  # 20 chars
