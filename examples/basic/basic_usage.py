"""
Basic usage examples for the Polars Cache library.

This example demonstrates the most common use cases for caching Polars DataFrames and LazyFrames.
"""

import polars as pl

from plcache import cache


# Example 1: Simple caching with default settings
@cache()
def load_customer_data(n_customers: int = 1000) -> pl.DataFrame:
    """Simulate loading customer data - expensive operation."""
    print(f"🔄 Loading {n_customers} customer records (expensive operation)...")

    # Simulate expensive data loading
    import time

    time.sleep(0.5)  # Pretend this takes time

    return pl.DataFrame(
        {
            "customer_id": range(n_customers),
            "name": [f"Customer_{i}" for i in range(n_customers)],
            "age": [20 + (i % 60) for i in range(n_customers)],
            "spend": [100 + (i * 10) % 1000 for i in range(n_customers)],
        }
    )


# Example 2: Caching LazyFrames with custom cache directory
@cache(cache_dir="./my_cache", symlinks_dir="analytics")
def process_sales_data(multiplier: float = 1.5) -> pl.LazyFrame:
    """Process sales data and return a LazyFrame."""
    print(f"📊 Processing sales data with multiplier {multiplier}...")

    # Simulate expensive processing
    import time

    time.sleep(0.3)

    return (
        pl.DataFrame(
            {
                "product": ["A", "B", "C", "D"],
                "sales": [100, 200, 150, 300],
                "profit": [20, 40, 30, 60],
            }
        )
        .lazy()
        .with_columns(
            (pl.col("sales") * multiplier).alias("projected_sales"),
            (pl.col("profit") * multiplier).alias("projected_profit"),
        )
    )


# Example 3: Function with multiple arguments
@cache(cache_dir="./demo_cache", symlink_name="filtered_data.parquet")
def filter_data(
    min_age: int, max_spend: int, category: str = "premium"
) -> pl.DataFrame:
    """Filter customer data based on criteria."""
    print(f"🔍 Filtering data: age>={min_age}, spend<={max_spend}, category={category}")

    # Generate some data to filter
    data = pl.DataFrame(
        {
            "customer_id": range(100),
            "age": [20 + (i % 60) for i in range(100)],
            "spend": [50 + (i * 15) % 500 for i in range(100)],
            "category": ["premium" if i % 3 == 0 else "standard" for i in range(100)],
        }
    )

    return data.filter(
        (pl.col("age") >= min_age)
        & (pl.col("spend") <= max_spend)
        & (pl.col("category") == category)
    )


def main():
    """Demonstrate the caching behavior."""
    print("🚀 Polars Cache Basic Usage Examples")
    print("=" * 50)

    # Example 1: First call is slow, second is fast
    print("Example 1: Basic caching")
    print("=" * 50)

    import time

    start = time.time()
    df1 = load_customer_data(500)
    first_call_time = time.time() - start
    print(f"✅ First call completed in {first_call_time:.2f}s")
    print(f"   Result: {len(df1)} rows, type: {type(df1).__name__}")

    start = time.time()
    df2 = load_customer_data(500)  # Same arguments - should be cached
    second_call_time = time.time() - start
    print(f"⚡ Second call completed in {second_call_time:.2f}s (cached!)")
    print(f"   Result: {len(df2)} rows, type: {type(df2).__name__}")
    print(f"   Speedup: {first_call_time / second_call_time:.1f}x faster")

    # Verify they're the same
    assert df1.equals(df2), "Cached result should be identical"
    print("✅ Results are identical")

    # Example 2: LazyFrame caching
    print("\n" + "=" * 50)
    print("Example 2: LazyFrame caching")
    print("=" * 50)

    lazy_result = process_sales_data(2.0)
    print(f"✅ LazyFrame cached, type: {type(lazy_result).__name__}")

    # Collect to see the data
    collected = lazy_result.collect()
    print("📋 Processed data:")
    print(collected)

    # Second call should be instant
    start = time.time()
    _lazy_result2 = process_sales_data(2.0)
    cached_time = time.time() - start
    print(f"⚡ Cached LazyFrame retrieved in {cached_time:.4f}s")

    # Example 3: Multiple arguments
    print("\n" + "=" * 50)
    print("Example 3: Multiple arguments")
    print("=" * 50)

    filtered = filter_data(min_age=25, max_spend=200, category="premium")
    print(f"✅ Filtered data: {len(filtered)} rows")

    # Different arguments create different cache entries
    filtered2 = filter_data(min_age=30, max_spend=300, category="standard")
    print(f"✅ Different filter: {len(filtered2)} rows")

    print("\n🎉 All examples completed successfully!")
    print("\n📁 Check the cache directories:")
    print("   - Default cache: ~/.polars_cache/ or temp directory")
    print("   - Custom cache: ./my_cache/")
    print("   - Demo cache: ./demo_cache/")


if __name__ == "__main__":
    main()
