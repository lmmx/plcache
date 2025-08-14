"""Basic functionality tests for the Polars caching system."""

import time
from contextlib import contextmanager

import polars as pl
from polars.testing import assert_frame_equal, assert_frame_not_equal
from pytest import mark

from plcache import cache

BRIEF_WAIT = 0.1  # Cannot go below 0.01 without actual computation being longer


@contextmanager
def timer():
    """Context manager to measure elapsed time."""
    start = time.time()
    yield lambda: time.time() - start


@mark.parametrize("wait", [BRIEF_WAIT])
def test_cache_performance_and_equality(tmp_path, wait: float):
    """Will be equal because `lazy=True` matches `LazyFrame` return type."""

    @cache(cache_dir=tmp_path)
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


@mark.parametrize("wait", [BRIEF_WAIT])
def test_different_args_different_cache(tmp_path, wait: float):
    """Different arguments create separate cache entries."""

    @cache(cache_dir=tmp_path)
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
