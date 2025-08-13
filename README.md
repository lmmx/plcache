# polarscache

A disk cache decorator for Polars DataFrames and LazyFrames that saves results as Parquet files.

## What it does

Caches expensive Polars operations to disk using Parquet format.

When you call a decorated function with the same arguments, it loads the cached Parquet file instead of recomputing.

## Installation

```bash
uv pip install polarscache
```

## Requirements

- Python 3.13+
- polars
- diskcache

## Features

- **Automatic type detection**: Caches and restores DataFrames/LazyFrames with their original types
- **Parquet storage**: Preserves column datatypes and metadata in the Parquet format
- **Human-readable cache structure**: Symlinked directory structure organized by module, function, and arguments for easy browsing
- **Flexible organization**: Choose between nested module/function directories or flat structure with encoded names
- **SQLite-backed tracking**: Uses `diskcache` with SQLite to track Parquet blob files
- **Type-safe**: Full type hints and `ty` type checker compatibility

## Quick Start

```python
import polars as pl
from plcache import cache

# Simple caching - just add the decorator
@cache()
def expensive_computation(n: int) -> pl.DataFrame:
    # Simulate expensive work
    return pl.DataFrame({
        "values": range(n),
        "squared": [i**2 for i in range(n)]
    })

# First call: executes function and caches result
df1 = expensive_computation(1000)

# Second call: loads from cache (much faster!)
df2 = expensive_computation(1000)

assert df1.equals(df2)  # Identical results
```

## How it works

We hash the function name and arguments to create a unique cache key:

```python
call_str = f"{func_name}({args}, {kwargs})"
cache_key = hashlib.sha256(call_str.encode()).hexdigest()
```

The Parquet file is saved to `cache_dir/blobs/{hash}.parquet` and the cache key plus file path are stored in a SQLite database at `cache_dir/metadata/`. 

Human-readable symlinks are created at `cache_dir/functions/module/function/args/` that point back to the blob files, so you can browse your cached results easily.

We hash the function name and arguments to create a unique cache key.
The Parquet file is saved to `cache_dir/blobs/{hash}.parquet` and the cache key + file path are stored in a SQLite database at `cache_dir/metadata/`. Human-readable symlinks are created at `cache_dir/functions/module/function/args/` that point back to the blob files.

## Cache Structure

plcache creates an organized, browsable cache structure:

```
cache_dir/
├── metadata/            # diskcache metadata
├── blobs/               # actual parquet files (by hash)
│   ├── a1b2c3d4.parquet
│   └── e5f6g7h8.parquet
└── functions/           # human-readable symlinks
    └── mymodule/
        └── expensive_computation/
            ├── arg0=1000/
            │   └── data.parquet -> ../../../blobs/a1b2c3d4.parquet
            └── arg0=5000/
                └── data.parquet -> ../../../blobs/e5f6g7h8.parquet
```

## Advanced Usage

### Custom Cache Configuration

```python
from plcache import PolarsCache

# Create custom cache instance
cache_instance = PolarsCache(
    cache_dir="/path/to/my/cache",
    size_limit=5 * 2**30,  # 5GB limit
    readable_dir_name="cached_functions",
    split_module_path=True,  # organize by module/function
    max_arg_length=100,     # longer argument names in dirs
)

@cache_instance.cache_polars()
def my_function(data_path: str, multiplier: float = 1.0) -> pl.LazyFrame:
    return pl.scan_csv(data_path).with_columns(
        pl.col("value") * multiplier
    )
```

### Global Cache with Custom Settings

```python
from plcache import cache

@cache(
    cache_dir="/tmp/my_polars_cache",
    readable_dir_name="analysis_cache", 
    split_module_path=False,  # flat structure
    max_arg_length=50
)
def load_and_process(file_path: str, filters: dict) -> pl.DataFrame:
    return (
        pl.read_csv(file_path)
        .filter(pl.col("status").is_in(filters["allowed_statuses"]))
        .group_by("category")
        .agg(pl.col("amount").sum())
    )
```

### Lazy vs Eager Handling

plcache automatically preserves the return type of your functions:

```python
@cache()
def get_lazy_data(n: int) -> pl.LazyFrame:
    return pl.DataFrame({"x": range(n)}).lazy()

@cache()  
def get_eager_data(n: int) -> pl.DataFrame:
    return pl.DataFrame({"x": range(n)})

# Returns LazyFrame (cached as lazy.parquet)
lazy_result = get_lazy_data(100)

# Returns DataFrame (cached as data.parquet)
eager_result = get_eager_data(100)
```

## Configuration Options

### Cache Function Parameters

- `cache_dir`: Directory for cache storage (default: system temp)
- `size_limit`: Maximum cache size in bytes (default: 1GB)
- `readable_cache`: Create human-readable symlink structure (default: True)
- `readable_dir_name`: Name for readable directory (default: "functions")
- `split_module_path`: Split into module/function subdirs vs flat (default: True)
- `max_arg_length`: Max length for argument values in directory names (default: 50)

### PolarsCache Class Parameters

Same as above, plus:
- `symlink_filename`: Custom filename for symlinks (default: auto-detect)

## Cache Management

```python
# Clear all cached data
cache_instance.clear()

# Or with global cache - create instance to clear
from plcache import PolarsCache
import tempfile

# Clear default cache location
default_cache = PolarsCache(cache_dir=None)  # Uses temp dir
default_cache.clear()
```

## Real-World Example

```python
import polars as pl
from plcache import cache
import requests

@cache(cache_dir="./api_cache", readable_dir_name="datasets")
def load_stock_data(symbol: str, start_date: str, end_date: str) -> pl.LazyFrame:
    """Load stock data - expensive API call, perfect for caching."""
    # Expensive API call
    url = f"https://api.example.com/stocks/{symbol}"
    params = {"start": start_date, "end": end_date}
    response = requests.get(url, params=params)
    
    # Process into LazyFrame
    return pl.from_dicts(response.json()["data"]).lazy()

@cache(cache_dir="./analysis_cache")  
def technical_analysis(stock_data: pl.LazyFrame, window: int = 20) -> pl.DataFrame:
    """Compute technical indicators - expensive computation."""
    return (
        stock_data
        .with_columns([
            pl.col("close").rolling_mean(window).alias("sma"),
            pl.col("close").rolling_std(window).alias("volatility")
        ])
        .collect()
    )

# Usage - only hits API and computes once per unique combination
aapl_data = load_stock_data("AAPL", "2024-01-01", "2024-12-31")
aapl_analysis = technical_analysis(aapl_data, window=20)

# Cache structure created:
# ./api_cache/datasets/__main__/load_stock_data/arg0=AAPL_arg1=2024-01-01_arg2=2024-12-31/lazy.parquet
# ./analysis_cache/functions/__main__/technical_analysis/arg0=<LazyFrame>_arg1=20/data.parquet
```

## Usage Tips

1. **Use appropriate return types**: Return `LazyFrame` for large datasets you'll filter later
2. **Cache at the right level**: Cache expensive I/O operations, not cheap transformations  
3. **Monitor cache size**: Set reasonable `size_limit` to avoid disk space issues (default: 1GB, 2<sup>30</sup>)
4. **Organise with `readable_dir_name`**: Use descriptive names for different cache types

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions welcome! Please see CONTRIBUTING.md for guidelines.
