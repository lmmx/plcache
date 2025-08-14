"""Performance comparison examples for the Polars Cache library.

This example demonstrates the performance benefits of caching and compares
cached vs uncached function execution times.
"""

import time
from collections.abc import Callable, Generator
from contextlib import contextmanager

import polars as pl
from polars.testing import assert_frame_equal

from plcache import cache


@contextmanager
def timer() -> Generator[Callable]:
    """Context manager to measure execution time."""
    start = time.time()
    yield lambda: time.time() - start


def expensive_data_operation(n_rows: int, delay: float = 0.1) -> pl.DataFrame:
    """Simulate an expensive data operation without caching."""
    print(f"💰 Expensive operation: {n_rows} rows (simulated delay: {delay}s)")
    time.sleep(delay)  # Simulate expensive computation

    return pl.DataFrame(
        {
            "id": range(n_rows),
            "value": [i * 2 for i in range(n_rows)],
            "squared": [i**2 for i in range(n_rows)],
            "category": [f"cat_{i % 5}" for i in range(n_rows)],
        }
    )


@cache(cache_dir="./perf_cache", symlinks_dir="performance_tests")
def cached_data_operation(n_rows: int, delay: float = 0.1) -> pl.DataFrame:
    """Same expensive operation but with caching enabled."""
    print(f"⚡ Cached operation: {n_rows} rows (simulated delay: {delay}s)")
    time.sleep(delay)  # Simulate expensive computation

    return pl.DataFrame(
        {
            "id": range(n_rows),
            "value": [i * 2 for i in range(n_rows)],
            "squared": [i**2 for i in range(n_rows)],
            "category": [f"cat_{i % 5}" for i in range(n_rows)],
        }
    )


@cache(cache_dir="./perf_cache")
def expensive_aggregation(data_size: int, group_operations: int = 3) -> pl.LazyFrame:
    """Simulate expensive aggregation operations."""
    print(f"🔄 Running {group_operations} aggregation operations on {data_size} rows")

    # Simulate computation time
    time.sleep(0.05 * group_operations)

    # Create base data
    df = pl.DataFrame(
        {
            "group": [f"group_{i % 10}" for i in range(data_size)],
            "value1": [i * 1.5 for i in range(data_size)],
            "value2": [i**1.2 for i in range(data_size)],
            "timestamp": [f"2024-01-{(i % 30) + 1:02d}" for i in range(data_size)],
        }
    )

    # Build complex aggregation with deterministic ordering
    result = (
        df.lazy()
        .group_by("group", maintain_order=True)
        .agg(
            [
                pl.col("value1").mean().alias("avg_value1"),
                pl.col("value2").sum().alias("sum_value2"),
                pl.col("value1").max().alias("max_value1"),
            ]
        )
    )

    # Add more operations based on group_operations parameter
    for i in range(group_operations - 1):
        result = result.with_columns(
            (pl.col("avg_value1") * (i + 2)).alias(f"derived_{i}")
        )

    return result


def compare_basic_performance():
    """Compare basic cached vs uncached performance."""
    print("📊 Basic Performance Comparison")
    print("=" * 60)

    n_rows = 1000
    delay = 0.2

    # Test uncached function multiple times
    print("🐌 Testing UNCACHED function:")
    uncached_times = []
    for i in range(3):
        with timer() as elapsed:
            result = expensive_data_operation(n_rows, delay)
        time_taken = elapsed()
        uncached_times.append(time_taken)
        print(f"   Run {i + 1}: {time_taken:.3f}s ({len(result)} rows)")

    avg_uncached = sum(uncached_times) / len(uncached_times)
    print(f"   Average uncached time: {avg_uncached:.3f}s")

    # Test cached function
    print("\n⚡ Testing CACHED function:")
    cached_times = []

    # First call (cache miss)
    with timer() as elapsed:
        result1 = cached_data_operation(n_rows, delay)
    first_cached_time = elapsed()
    cached_times.append(first_cached_time)
    print(f"   First call (miss): {first_cached_time:.3f}s ({len(result1)} rows)")

    # Subsequent calls (cache hits)
    for i in range(2):
        with timer() as elapsed:
            result = cached_data_operation(n_rows, delay)
        time_taken = elapsed()
        cached_times.append(time_taken)
        print(f"   Call {i + 2} (hit): {time_taken:.3f}s ({len(result)} rows)")

    # Calculate speedup
    cache_hit_avg = sum(cached_times[1:]) / len(cached_times[1:])
    speedup = avg_uncached / cache_hit_avg

    print("\n📈 Performance Results:")
    print(f"   Average uncached time: {avg_uncached:.3f}s")
    print(f"   Cache hit time: {cache_hit_avg:.3f}s")
    print(f"   Speedup: {speedup:.1f}x faster with cache!")

    # Verify results are identical
    result2 = cached_data_operation(n_rows, delay)
    assert_frame_equal(result1, result2)
    print("   ✅ Cache integrity verified")


def compare_complex_operations():
    """Compare performance with more complex operations."""
    print("\n🔧 Complex Operations Performance")
    print("=" * 60)

    data_sizes = [500, 1000, 2000]
    operations = 5

    for size in data_sizes:
        print(f"\n📊 Testing with {size} rows, {operations} operations:")

        # First call (cache miss)
        with timer() as elapsed:
            result1 = expensive_aggregation(size, operations)
            collected1 = result1.collect()  # Force evaluation
        miss_time = elapsed()
        print(f"   Cache miss: {miss_time:.3f}s ({len(collected1)} groups)")

        # Second call (cache hit)
        with timer() as elapsed:
            result2 = expensive_aggregation(size, operations)
            collected2 = result2.collect()  # Force evaluation
        hit_time = elapsed()
        print(f"   Cache hit:  {hit_time:.3f}s ({len(collected2)} groups)")

        speedup = miss_time / hit_time if hit_time > 0 else float("inf")
        print(f"   Speedup: {speedup:.1f}x")

        # Verify results
        assert_frame_equal(collected1, collected2)


def memory_usage_demo():
    """Demonstrate memory efficiency of the cache."""
    print("\n💾 Memory Efficiency Demo")
    print("=" * 60)

    @cache(cache_dir="./memory_demo")
    def create_large_dataset(n_rows: int) -> pl.DataFrame:
        """Create a relatively large dataset."""
        print(f"🗄️  Creating dataset with {n_rows:,} rows")
        time.sleep(0.1)  # Simulate processing time

        return pl.DataFrame(
            {
                "id": range(n_rows),
                "data1": [f"value_{i}" for i in range(n_rows)],
                "data2": [i * 1.5 for i in range(n_rows)],
                "data3": [i**0.5 for i in range(n_rows)],
            }
        )

    large_size = 10000

    print(f"Creating dataset with {large_size:,} rows...")

    # First creation
    with timer() as elapsed:
        df1 = create_large_dataset(large_size)
    creation_time = elapsed()
    print(f"   Creation time: {creation_time:.3f}s")
    print(f"   Dataset size: {len(df1):,} rows, {len(df1.columns)} columns")

    # Cached retrieval
    with timer() as elapsed:
        df2 = create_large_dataset(large_size)
    retrieval_time = elapsed()
    print(f"   Retrieval time: {retrieval_time:.3f}s")

    speedup = creation_time / retrieval_time if retrieval_time > 0 else float("inf")
    print(f"   Speedup: {speedup:.1f}x")
    print("   ✅ Memory: Data loaded from disk cache, not recreated in memory")

    # Verify datasets are identical
    assert_frame_equal(df1, df2)


def main():
    """Run all performance comparison examples."""
    print("🚀 Polars Cache Performance Examples\n")

    compare_basic_performance()
    compare_complex_operations()
    memory_usage_demo()

    print("\n🎉 Performance comparison completed!")
    print("\n📝 Key Takeaways:")
    print("   • Cache hits are significantly faster than recomputation")
    print("   • Larger/more complex operations show bigger speedups")
    print("   • Cache stores results on disk, saving memory")
    print("   • Cache maintains data integrity across calls")
    print(
        "\n💡 Best used for: expensive computations, large datasets, repeated analysis"
    )


if __name__ == "__main__":
    main()
