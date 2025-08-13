from pathlib import Path

import polars as pl

from plcache import PolarsCache, cache


def test_split_module_path(tmp_path):
    """Test cache with split module path structure."""

    @cache(
        cache_dir=tmp_path,
        readable_dir_name="functions",
        split_module_path=True,
        symlink_name="result.parquet",
    )
    def test_func(n: int) -> pl.DataFrame:
        return pl.DataFrame({"data": [n]})

    result = test_func(42)

    # Check structure: cache_dir/functions/module_name/function_name/args/result.parquet
    cache_path = Path(tmp_path)
    expected_symlink = (
        cache_path
        / "functions"
        / "__main__"
        / "test_func"
        / "arg0=42"
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
        split_module_path=False,
        symlink_name="cached_data.parquet",
    )
    def another_func(value: str) -> pl.DataFrame:
        return pl.DataFrame({"text": [value]})

    result = another_func("hello")

    # Check structure: cache_dir/functions/full_qualname/args/cached_data.parquet
    cache_path = Path(tmp_path)
    expected_symlink = (
        cache_path
        / "functions"
        / "__main__.another_func"
        / "arg0=hello"
        / "cached_data.parquet"
    )

    assert expected_symlink.exists()
    assert expected_symlink.is_symlink()


def test_che_custom_dir_name(tmp_path):
    """Test cache with custom directory name."""

    @cache(
        cache_dir=tmp_path,
        readable_dir_name="my_cache",
        symlink_name="output.parquet",
    )
    def custom_func() -> pl.DataFrame:
        return pl.DataFrame({"col": [1, 2, 3]})

    result = custom_func()

    # Check custom directory name
    cache_path = Path(tmp_path)
    expected_symlink = (
        cache_path
        / "my_cache"
        / "__main__"
        / "custom_func"
        / "no_args"
        / "output.parquet"
    )

    assert expected_symlink.exists()


def test_cache_with_kwargs(tmp_path):
    """Test cache with function kwargs."""

    @cache(cache_dir=tmp_path, symlink_name="data.parquet")
    def func_with_kwargs(a: int, b: str = "default") -> pl.DataFrame:
        return pl.DataFrame({"a": [a], "b": [b]})

    result = func_with_kwargs(10, b="test")

    # Check args directory includes kwargs
    cache_path = Path(tmp_path)
    expected_symlink = (
        cache_path
        / "functions"
        / "__main__"
        / "func_with_kwargs"
        / "arg0=10_b=test"
        / "data.parquet"
    )

    assert expected_symlink.exists()


def test_polars_cache_class_direct_usage(tmp_path):
    """Test using PolarsCache class directly with custom settings."""

    pc = PolarsCache(
        cache_dir=tmp_path,
        readable_dir_name="cached_functions",
        split_module_path=True,
        symlink_name="result.parquet",
        max_arg_length=20,
    )

    @pc.cache_polars()
    def class_test_func(long_string: str) -> pl.DataFrame:
        return pl.DataFrame({"data": [long_string]})

    # Test with a long argument that should be truncated
    long_arg = "this_is_a_very_long_string_that_should_be_truncated"
    result = class_test_func(long_arg)

    cache_path = Path(tmp_path)

    # Find the symlink (arg should be truncated to 20 chars)
    symlinks = list(cache_path.rglob("result.parquet"))
    assert len(symlinks) == 1

    # Verify the directory name contains truncated argument
    symlink_parent = symlinks[0].parent.name
    assert "this_is_a_very_long_" in symlink_parent  # 20 chars


def test_max_arg_length_truncation(tmp_path):
    """Test that long arguments get truncated in directory names."""

    @cache(cache_dir=tmp_path, max_arg_length=10)
    def truncate_test(very_long_argument: str) -> pl.DataFrame:
        return pl.DataFrame({"result": [len(very_long_argument)]})

    result = truncate_test("this_argument_is_very_long_and_should_be_truncated")

    # Find the created directory
    cache_path = Path(tmp_path)
    symlinks = list(cache_path.rglob("data.parquet"))
    assert len(symlinks) == 1

    # Check that argument was truncated to 10 characters
    symlink_parent = symlinks[0].parent.name
    assert "this_argum" in symlink_parent  # 10 chars max


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


def test_multiple_functions_separate_directories(tmp_path):
    """Test that different functions create separate directories."""

    @cache(cache_dir=tmp_path)
    def func_a() -> pl.DataFrame:
        return pl.DataFrame({"a": [1]})

    @cache(cache_dir=tmp_path)
    def func_b() -> pl.DataFrame:
        return pl.DataFrame({"b": [2]})

    result_a = func_a()
    result_b = func_b()

    # Check separate directories were created
    cache_path = Path(tmp_path)
    func_a_dir = cache_path / "functions" / "__main__" / "func_a"
    func_b_dir = cache_path / "functions" / "__main__" / "func_b"

    assert func_a_dir.exists()
    assert func_b_dir.exists()
    assert func_a_dir != func_b_dir
