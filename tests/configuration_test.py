"""Configuration options tests for the Polars caching system."""

from pathlib import Path

import polars as pl

from plcache import cache


def test_cache_custom_dir_name(tmp_path):
    """Test cache with custom directory name."""

    @cache(
        cache_dir=tmp_path,
        symlinks_dir="my_cache",
        symlink_name="output.parquet",
    )
    def custom_func() -> pl.DataFrame:
        return pl.DataFrame({"col": [1, 2, 3]})

    _ = custom_func()

    # Get actual module and qualname
    module_name = custom_func.__module__
    func_qualname = custom_func.__qualname__

    # Apply the same encoding as the implementation
    import urllib.parse

    encoded_module = urllib.parse.quote(module_name, safe="")
    encoded_qualname = urllib.parse.quote(func_qualname, safe="")

    # Check custom directory name
    cache_path = Path(tmp_path)
    expected_symlink = (
        cache_path
        / "my_cache"
        / encoded_module
        / encoded_qualname
        / "no_args"
        / "output.parquet"
    )

    assert expected_symlink.exists()


def test_trim_arg_truncation(tmp_path):
    """Test that long arguments get truncated in directory names."""

    @cache(cache_dir=tmp_path, trim_arg=10)
    def truncate_test(very_long_argument: str) -> pl.DataFrame:
        return pl.DataFrame({"result": [len(very_long_argument)]})

    _ = truncate_test("this_argument_is_very_long_and_should_be_truncated")

    # Find the created directory (default symlink name is "output.parquet")
    cache_path = Path(tmp_path)
    symlinks = list(cache_path.rglob("output.parquet"))
    assert len(symlinks) == 1

    # Check that argument was truncated to 10 characters
    symlink_parent = symlinks[0].parent.name
    assert "this_argum" in symlink_parent  # 10 chars max


def test_cache_with_kwargs(tmp_path):
    """Test cache with function kwargs."""

    @cache(cache_dir=tmp_path, symlink_name="data.parquet")
    def func_with_kwargs(a: int, b: str = "default") -> pl.DataFrame:
        return pl.DataFrame({"a": [a], "b": [b]})

    _ = func_with_kwargs(10, b="test")

    # Get actual module and qualname
    module_name = func_with_kwargs.__module__
    func_qualname = func_with_kwargs.__qualname__

    # Apply the same encoding as the implementation
    import urllib.parse

    encoded_module = urllib.parse.quote(module_name, safe="")
    encoded_qualname = urllib.parse.quote(func_qualname, safe="")

    # Check args directory includes kwargs
    cache_path = Path(tmp_path)
    expected_symlink = (
        cache_path
        / "functions"
        / encoded_module
        / encoded_qualname
        / "a=10_b=test"
        / "data.parquet"
    )

    assert expected_symlink.exists()
