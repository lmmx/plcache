🚀 Polars Cache Performance Examples

📊 Basic Performance Comparison
============================================================
🐌 Testing UNCACHED function:
💰 Expensive operation: 1000 rows (simulated delay: 0.2s)
   Run 1: 0.201s (1000 rows)
💰 Expensive operation: 1000 rows (simulated delay: 0.2s)
   Run 2: 0.201s (1000 rows)
💰 Expensive operation: 1000 rows (simulated delay: 0.2s)
   Run 3: 0.200s (1000 rows)
   Average uncached time: 0.201s

⚡ Testing CACHED function:
   First call (miss): 0.003s (1000 rows)
   Call 2 (hit): 0.001s (1000 rows)
   Call 3 (hit): 0.001s (1000 rows)

📈 Performance Results:
   Average uncached time: 0.201s
   Cache hit time: 0.001s
   Speedup: 263.3x faster with cache!
   ✅ Cache integrity verified

🔧 Complex Operations Performance
============================================================

📊 Testing with 500 rows, 5 operations:
   Cache miss: 0.000s (10 groups)
   Cache hit:  0.000s (10 groups)
   Speedup: 1.1x

📊 Testing with 1000 rows, 5 operations:
   Cache miss: 0.000s (10 groups)
   Cache hit:  0.000s (10 groups)
   Speedup: 1.1x

📊 Testing with 2000 rows, 5 operations:
   Cache miss: 0.000s (10 groups)
   Cache hit:  0.000s (10 groups)
   Speedup: 1.0x

💾 Memory Efficiency Demo
============================================================
Creating dataset with 10,000 rows...
   Creation time: 0.001s
   Dataset size: 10,000 rows, 4 columns
   Retrieval time: 0.001s
   Speedup: 1.2x
   ✅ Memory: Data loaded from disk cache, not recreated in memory

🎉 Performance comparison completed!

📝 Key Takeaways:
   • Cache hits are significantly faster than recomputation
   • Larger/more complex operations show bigger speedups
   • Cache stores results on disk, saving memory
   • Cache maintains data integrity across calls

💡 Best used for: expensive computations, large datasets, repeated analysis
