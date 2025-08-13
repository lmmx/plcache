import shutil
import tempfile
import time
from contextlib import contextmanager

import polars as pl
import pytest
from plcache import cache


@pytest.fixture
def temp_cache_dir():
    """Create a temporary cache directory that gets cleaned up after each test."""
    temp_dir = tempfile.mkdtemp(prefix="plcache_test_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@contextmanager
def timer():
    start = time.time()
    yield lambda: time.time() - start


def test_cache_performance_and_equality(temp_cache_dir):
    @cache(cache_dir=temp_cache_dir)
    def expensive_computation(n: int) -> pl.DataFrame:
        time.sleep(2)
        return pl.DataFrame(
            {
                "x": [i + 1 for i in range(n)],
                "y": [i + 2 for i in range(n)],
                "z": [i + 3 for i in range(n)],
            }
        )

    # First call: slow
    with timer() as elapsed:
        df1 = expensive_computation(10)
    assert elapsed() >= 2.0

    # Second call: fast
    with timer() as elapsed:
        df2 = expensive_computation(10)
    assert elapsed() < 0.5

    assert df1.equals(df2)


def test_different_args_different_cache(temp_cache_dir):
    """Test different arguments create separate cache entries."""

    @cache(cache_dir=temp_cache_dir)
    def compute(n: int) -> pl.DataFrame:
        time.sleep(1)
        return pl.DataFrame({"value": [i for i in range(n)]})

    # Different n values should both be slow
    with timer() as elapsed:
        df1 = compute(5)
    assert elapsed() >= 1.0

    with timer() as elapsed:
        df2 = compute(3)  # Different argument
    assert elapsed() >= 1.0

    assert not df1.equals(df2)
