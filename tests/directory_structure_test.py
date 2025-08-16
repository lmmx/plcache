# tests/directory_structure_test.py
"""Directory structure tests for the Polars caching system."""

from pathlib import Path

import polars as pl

from plcache import cache


def test_nested(tmp_path):
    """Test cache with split module path structure."""

    @cache(
        cache_dir=tmp_path,
        symlinks_dir="functions",
        nested=True,
        symlink_name="result.parquet",
    )
    def test_func(n: int) -> pl.DataFrame:
        return pl.DataFrame({"data": [n]})

    _ = test_func(42)

    # Get the actual module and qualname from the function
    module_name = test_func.__module__  # Will be "tests.advanced_test" or similar
    func_qualname = test_func.__qualname__  # Will be "test_nested.<locals>.test_func"

    # Apply the same encoding as the implementation
    import urllib.parse

    encoded_module = urllib.parse.quote(module_name, safe="")
    encoded_qualname = urllib.parse.quote(func_qualname, safe="")

    # Check structure: cache_dir/functions/encoded_module/encoded_qualname/args/result.parquet
    cache_path = Path(tmp_path)
    expected_symlink = (
        cache_path
        / "functions"
        / encoded_module
        / encoded_qualname
        / "n=42"
        / "result.parquet"
    )

    assert expected_symlink.exists()
    assert expected_symlink.is_symlink()

    # Verify symlink points to blob
    blob_dir = cache_path / "blobs"
    assert blob_dir.exists()
    assert len(list(blob_dir.glob("*.parquet"))) == 1


def test_flat_module_path(tmp_path):
    """Test cache with flat module path structure."""

    @cache(
        cache_dir=tmp_path,
        nested=False,
        symlink_name="cached_data.parquet",
    )
    def another_func(value: str) -> pl.DataFrame:
        return pl.DataFrame({"text": [value]})

    _ = another_func("hello")

    # Get the actual full qualname
    module_name = another_func.__module__
    func_qualname = another_func.__qualname__
    full_qualname = f"{module_name}.{func_qualname}"

    # URL encode the full qualname (same as urllib.parse.quote(full_qualname, safe=""))
    import urllib.parse

    encoded_qualname = urllib.parse.quote(full_qualname, safe="")

    # Check structure: cache_dir/functions/encoded_full_qualname/args/cached_data.parquet
    cache_path = Path(tmp_path)
    expected_symlink = (
        cache_path
        / "functions"
        / encoded_qualname
        / "value=hello"
        / "cached_data.parquet"
    )

    assert expected_symlink.exists()
    assert expected_symlink.is_symlink()


def test_multiple_functions_separate_directories(tmp_path):
    """Test that different functions create separate directories."""

    @cache(cache_dir=tmp_path)
    def func_a() -> pl.DataFrame:
        return pl.DataFrame({"a": [1]})

    @cache(cache_dir=tmp_path)
    def func_b() -> pl.DataFrame:
        return pl.DataFrame({"b": [2]})

    _ = func_a()
    _ = func_b()

    # Get actual module and qualnames
    module_name_a = func_a.__module__
    func_qualname_a = func_a.__qualname__
    module_name_b = func_b.__module__
    func_qualname_b = func_b.__qualname__

    # Apply the same encoding as the implementation
    import urllib.parse

    encoded_module_a = urllib.parse.quote(module_name_a, safe="")
    encoded_qualname_a = urllib.parse.quote(func_qualname_a, safe="")
    encoded_module_b = urllib.parse.quote(module_name_b, safe="")
    encoded_qualname_b = urllib.parse.quote(func_qualname_b, safe="")

    # Check separate directories were created
    cache_path = Path(tmp_path)
    func_a_dir = cache_path / "functions" / encoded_module_a / encoded_qualname_a
    func_b_dir = cache_path / "functions" / encoded_module_b / encoded_qualname_b

    assert func_a_dir.exists()
    assert func_b_dir.exists()
    assert func_a_dir != func_b_dir
