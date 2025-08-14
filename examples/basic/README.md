🚀 Polars Cache Basic Usage Examples
==================================================

Example 1: Basic caching
==================================================
✅ First call completed in 0.00s
   Result: 500 rows, type: DataFrame
⚡ Second call completed in 0.00s (cached!)
   Result: 500 rows, type: DataFrame
   Speedup: 4.0x faster
✅ Results are identical

==================================================
Example 2: LazyFrame caching
==================================================
✅ LazyFrame cached, type: LazyFrame
📋 Processed data:
shape: (4, 5)
┌─────────┬───────┬────────┬─────────────────┬──────────────────┐
│ product ┆ sales ┆ profit ┆ projected_sales ┆ projected_profit │
│ ---     ┆ ---   ┆ ---    ┆ ---             ┆ ---              │
│ str     ┆ i64   ┆ i64    ┆ f64             ┆ f64              │
╞═════════╪═══════╪════════╪═════════════════╪══════════════════╡
│ A       ┆ 100   ┆ 20     ┆ 200.0           ┆ 40.0             │
│ B       ┆ 200   ┆ 40     ┆ 400.0           ┆ 80.0             │
│ C       ┆ 150   ┆ 30     ┆ 300.0           ┆ 60.0             │
│ D       ┆ 300   ┆ 60     ┆ 600.0           ┆ 120.0            │
└─────────┴───────┴────────┴─────────────────┴──────────────────┘
⚡ Cached LazyFrame retrieved in 0.0001s

==================================================
Example 3: Multiple arguments
==================================================
✅ Filtered data: 8 rows
✅ Different filter: 27 rows

🎉 All examples completed successfully!

📁 Check the cache directories:
   - Default cache: ~/.polars_cache/ or temp directory
   - Custom cache: ./my_cache/
   - Demo cache: ./demo_cache/
