import shutil
import tempfile
import time
from contextlib import contextmanager

import polars as pl
from polars.testing import assert_frame_equal, assert_frame_not_equal
from pytest import fixture, mark

from plcache import cache

BRIEF_WAIT = 0.01  # Cannot go lower without actual computation being longer


@fixture
def temp_cache_dir():
    """Create a temporary cache directory that gets cleaned up after each test."""
    temp_dir = tempfile.mkdtemp(prefix="plcache_test_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@contextmanager
def timer():
    start = time.time()
    yield lambda: time.time() - start


@mark.parametrize("wait", [0.01])
def test_cache_performance_and_equality(temp_cache_dir, wait: float):
    """Will be equal because `lazy=True` matches `LazyFrame` return type."""

    @cache(cache_dir=temp_cache_dir, lazy=True)
    def expensive_computation(n: int = 10) -> pl.LazyFrame:
        time.sleep(wait)
        return pl.LazyFrame().with_columns(
            pl.repeat(pl.lit(1), n=n).alias("x"),
            pl.repeat(pl.lit(2), n=n).alias("y"),
            pl.repeat(pl.lit(3), n=n).alias("z"),
        )

    # First call: slow
    with timer() as elapsed:
        df1 = expensive_computation(10)
    assert elapsed() >= wait

    # Second call: fast
    with timer() as elapsed:
        df2 = expensive_computation(10)
    assert elapsed() < wait

    assert_frame_equal(df1, df2)


@mark.parametrize("wait", [0.01])
def test_different_args_different_cache(temp_cache_dir, wait: float):
    """Different arguments create separate cache entries."""

    @cache(cache_dir=temp_cache_dir)
    def compute(n: int) -> pl.DataFrame:
        time.sleep(wait)
        return pl.DataFrame({"value": [i for i in range(n)]})

    # Different n values should both be slow
    with timer() as elapsed:
        df1 = compute(5)
    assert elapsed() >= wait

    with timer() as elapsed:
        df2 = compute(3)  # Different argument
    assert elapsed() >= wait

    assert_frame_not_equal(df1, df2)
