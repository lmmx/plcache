# tests/symlinks_test.py
"""Symlink functionality tests for the Polars caching system."""

from pathlib import Path

import polars as pl

from plcache import cache


def test_symlink_points_to_correct_blob(tmp_path):
    """Test that symlinks point to the correct blob files."""

    @cache(cache_dir=tmp_path, symlink_name="test_result.parquet")
    def symlink_test(value: int) -> pl.DataFrame:
        return pl.DataFrame({"value": [value]})

    # Create cached result
    original_result = symlink_test(123)

    # Find the symlink
    cache_path = Path(tmp_path)
    symlinks = list(cache_path.rglob("test_result.parquet"))
    assert len(symlinks) == 1

    symlink = symlinks[0]

    # Read data through symlink
    symlink_result = pl.read_parquet(symlink)

    # Should be identical to original
    assert symlink_result.equals(original_result)
