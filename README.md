# 🐻‍❄️💿 polars-diskcache

A diskcache decorator for Polars DataFrames and LazyFrames that saves results as Parquet files.

## What it does

Caches results of Polars operations to disk using Parquet format.
When you call a decorated function with the same arguments, it loads the cached Parquet file instead of recomputing.

It aims to provide a low-effort outlet for the DataFrames you store in memory without reinventing the wheel.
Feel free to use it as a convenient way to dump DataFrames to disk even when not caching!

## Installation

```bash
uv pip install polars-diskcache
```

## Requirements

- Python 3.13+
- polars
- diskcache

## Features

- **Automatic type detection**: Caches and restores DataFrames/LazyFrames with their original types
- **Parquet storage**: Preserves column datatypes and metadata in the Parquet format
- **Human-readable cache structure**: Symlinked directory structure organised by module, function, and arguments for easy browsing
- **Flexible organisation**: Choose between nested module/function directories or flat structure
- **Filesystem-safe encoding**: Automatically handles special characters in module/function names
- **Configurable symlink names**: Customize output filenames for different use cases
- **Argument handling**: Supports complex argument types with configurable truncation
- **SQLite-backed tracking**: Uses `diskcache` with SQLite to track Parquet blob files
- **Type-safe**: Full type hints and `ty` type checker compatibility

## Quick Start

```python
import polars as pl
from plcache import cache

# Simple caching - just add the decorator
@cache()
def expensive_computation(n: int) -> pl.DataFrame:
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

We hash the function name and arguments (with sorted `**kwargs`) to create a unique cache key:

```python
call_str = f"{func_name}({bound_args})"
cache_key = hashlib.sha256(call_str.encode()).hexdigest()
```

The Parquet file is saved to `{cache_dir}/blobs/{hash}.parquet` and the cache key plus file path are stored in a SQLite database at `{cache_dir}/metadata/`. 

Human-readable symlinks are created at `{cache_dir}/functions/module/function/args/` that point back to the blob files, so you can browse your cached results easily. If no args are passed, the directory is given the name `no_args/` rather than the empty string.

## Cache Structure

plcache creates an organised, browsable cache structure with two layout options:

**Nested** module path (default: `nested=True`) organises cache by separate module and function directories:

```
.polars_cache/
├── metadata/            # diskcache SQLite database
├── blobs/               # actual parquet files (by hash)
│   ├── a1b2c3d4.parquet
│   └── e5f6g7h8.parquet
└── functions/           # human-readable symlinks
    └── __main__/        # module name
        └── expensive_computation/  # function name
            ├── arg0=1000/
            │   └── output.parquet -> ../../../blobs/a1b2c3d4.parquet
            └── arg0=5000/
                └── output.parquet -> ../../../blobs/e5f6g7h8.parquet
```

**Flat** module path (`nested=False`) uses encoded full module.function names in a single directory level:

```
.polars_cache/
├── metadata/
├── blobs/
│   ├── a1b2c3d4.parquet
│   └── e5f6g7h8.parquet
└── functions/
    └── __main__.expensive_computation/  # encoded module.function
        ├── arg0=1000/
        │   └── output.parquet -> ../../blobs/a1b2c3d4.parquet
        └── arg0=5000/
            └── output.parquet -> ../../blobs/e5f6g7h8.parquet
```

## Configurable Options

### Cache Directory Location

By default, plcache creates a hidden `.polars_cache` directory:

```python
# Default: creates .polars_cache in current working directory
@cache()
def my_function(): ...

# Custom location
@cache(cache_dir="/path/to/my/cache")
def my_function(): ...

# Use system temp directory
@cache(use_tmp=True)
def my_function(): ...

# Non-hidden directory name
@cache(hidden=False)  # creates "polars_cache" instead of ".polars_cache"
def my_function(): ...
```

### Directory Structure Options

```python
# Custom readable directory name (great for organising different cache types)
@cache(symlinks_dir="analytics")  # creates cache/analytics/ instead of cache/functions/
def analytics_function(): ...

@cache(symlinks_dir="data_loading")
def load_data(): ...

# Choose layout style
@cache(nested=True)   # module/function/ (default)
def split_example(): ...

@cache(nested=False)  # module.function/ (flat)
def flat_example(): ...
```

### Argument Handling

```python
# Control argument length in directory names
@cache(trim_arg=20)  # truncate long argument values
def function_with_long_args(very_long_argument_name: str): ...

# Custom symlink filename
@cache(symlink_name="results.parquet")
def custom_output(): ...

@cache(symlink_name="processed_data.parquet")  
def data_processor(): ...
```

### All Parameters

```python
@cache(
    cache_dir="/custom/path",           # Cache directory location
    use_tmp=False,                      # Use system temp directory
    hidden=True,                        # Prefix with dot (default)
    size_limit=2**30,                   # Max cache size (1GB default)
    symlinks_dir="functions",           # Readable directory name
    nested=True,                        # Module/function vs flat layout
    trim_arg=50,                        # Max argument length in paths
    symlink_name="output.parquet"       # Custom symlink filename (or a callback)
    cache_key=my_cache_function,        # Custom cache key generation
    entry_dir=my_dir_function,          # Custom directory naming
)
def fully_configured(): ...
```

### Custom Cache Keys

Control what gets cached together by customizing the cache key generation:

```python
def preprocessing_cache_key(func, bound_args):
    """Cache key that ignores debug flags but includes data params."""
    # Remove debug/logging flags from cache consideration
    cache_params = {k: v for k, v in bound_args.items() 
                   if k not in ['debug', 'verbose', 'log_level']}
    return f"{func.__name__}({cache_params})"

cache = PolarsCache(
    cache_dir="./preprocessing_cache",
    cache_key=preprocessing_cache_key
)

@cache.cache_polars()
def preprocess_data(raw_data, normalize=True, remove_outliers=False, debug=False):
    # Expensive data preprocessing that shouldn't re-run for debug flag changes
    return expensive_preprocessing(raw_data, normalize, remove_outliers)

# These calls will hit the same cache entry:
clean_data1 = preprocess_data(df, normalize=True, debug=False)
clean_data2 = preprocess_data(df, normalize=True, debug=True)  # Cache hit!
```

The debug flag doesn't affect the actual processing, so you want the same cached result
whether the debug parameter is on or off.

### Custom Directory Names

Organize your cache directories with meaningful names:

```python
def experiment_dir_name(func, bound_args):
    """Create experiment-specific directory names."""
    model = bound_args['model_type']
    dataset_size = len(bound_args['data'])
    return f"{model}_experiment_{dataset_size}samples"

cache = PolarsCache(
    cache_dir="./experiments",
    entry_dir=experiment_dir_name,
    symlink_name="model_output.parquet"
)

@cache.cache_polars()
def run_experiment(data, model_type, learning_rate=0.01):
    return train_and_evaluate(data, model_type, learning_rate)

# Creates: experiments/functions/.../xgboost_experiment_1000samples/model_output.parquet
result = run_experiment(large_dataset, "xgboost", 0.001)
```

### Callback Signatures

All callbacks receive normalized arguments where positional and keyword arguments are unified:

```python
# These calls produce identical bound_args:
func(10, name="test")           # bound_args = {'value': 10, 'name': 'test'}
func(value=10, name="test")     # bound_args = {'value': 10, 'name': 'test'} 
func(name="test", value=10)     # bound_args = {'value': 10, 'name': 'test'} (sorted)
```

**Cache Key Callback**: `(func, bound_args) -> str`
- Controls which function calls share cache entries
- Return same string to share cache, different strings for separate entries

**Entry Directory Callback**: `(func, bound_args) -> str`  
- Controls symlink directory names for organization
- Should return filesystem-safe directory names
- Used for browsing, not for cache hits/misses

**Symlink Name Callback**: `(func, bound_args, result, cache_key) -> str`
- Controls the symlink filename within each directory
- Receives the actual result and cache key for context

## Advanced Usage

### Using the PolarsCache Class

For more control, use the `PolarsCache` class directly:

```python
from plcache import PolarsCache

# Create custom cache instance
my_cache = PolarsCache(
    cache_dir="./analysis_cache",
    symlinks_dir="experiments",
    nested=True,
    symlink_name="experiment_result.parquet"
)

@my_cache.cache_polars()
def run_experiment(params: dict) -> pl.DataFrame:
    # Expensive experiment
    return pl.DataFrame({"result": [1, 2, 3]})
```

### Complex Arguments

plcache handles various argument types intelligently:

```python
@cache(trim_arg=20)
def complex_function(
    data_list: list[int],
    config: dict,
    enabled: bool = True,
    mode: str = "advanced_processing"
) -> pl.DataFrame:
    # Arguments are safely encoded in directory structure
    # Long values are truncated to trim_arg
    return pl.DataFrame({"processed": [len(data_list)]})

# Creates: functions/__main__/complex_function/arg0=[1, 2, 3]_config={'key': 'val'}_enabled=True_mode=super_long_mode_name/
result = complex_function([1, 2, 3], {"key": "val"}, False, "super_long_mode_name_that_gets_truncated")
```

### Lazy vs Eager Handling

plcache automatically preserves the return type:

```python
@cache()
def get_lazy_data(n: int) -> pl.LazyFrame:
    return pl.LazyFrame({"x": range(n)})

@cache()  
def get_eager_data(n: int) -> pl.DataFrame:
    return pl.DataFrame({"x": range(n)})

# Returns LazyFrame (cached with lazy semantics)
lazy_result = get_lazy_data(100)

# Returns DataFrame (cached as computed result)
eager_result = get_eager_data(100)
```

## Cache Management

```python
# Clear all cached data
from plcache import PolarsCache

cache_instance = PolarsCache(cache_dir="./my_cache")
cache_instance.clear()

# Or clear default cache
default_cache = PolarsCache()  # Uses default location
default_cache.clear()
```

## Real-World Example

```python
import polars as pl
from plcache import cache

@cache(
    cache_dir="./data_cache", 
    symlinks_dir="datasets",
    symlink_name="raw_data.parquet"
)
def load_stock_data(symbol: str, start_date: str, end_date: str) -> pl.LazyFrame:
    """Load stock data - expensive API call, perfect for caching."""
    # Expensive API call or file I/O
    return pl.scan_csv(f"data/{symbol}.csv").filter(
        pl.col("date").is_between(start_date, end_date)
    )

@cache(
    cache_dir="./analysis_cache",
    symlinks_dir="technical_analysis", 
    symlink_name="indicators.parquet"
)
def technical_analysis(symbol: str, window: int = 20) -> pl.DataFrame:
    """Compute technical indicators - expensive computation."""
    stock_data = load_stock_data(symbol, "2024-01-01", "2024-12-31")
    
    return (
        stock_data
        .with_columns([
            pl.col("close").rolling_mean(window).alias("sma"),
            pl.col("close").rolling_std(window).alias("volatility")
        ])
        .collect()
    )

# Usage - only computes once per unique combination
aapl_analysis = technical_analysis("AAPL", window=20)

# Cache structure created:
# ./data_cache/datasets/__main__/load_stock_data/arg0=AAPL_arg1=2024-01-01_arg2=2024-12-31/raw_data.parquet
# ./analysis_cache/technical_analysis/__main__/technical_analysis/arg0=AAPL_arg1=20/indicators.parquet
```

## Examples

See the `examples/` directory for comprehensive usage examples:

- `examples/basic/` - Simple usage patterns and getting started
- `examples/advanced/` - Configuration options and advanced features  
- `examples/perf/` - Performance comparisons and benchmarks

## Usage Tips

1. **Use appropriate return types**: Return `LazyFrame` for large datasets you'll filter later
2. **Cache at the right level**: Cache expensive I/O operations, not cheap transformations  
3. **Monitor cache size**: Set reasonable `size_limit` to avoid disk space issues
4. **organise with `symlinks_dir`**: Use descriptive names like "experiments", "datasets", "analysis" for different cache types
5. **Custom symlink names**: Use descriptive filenames like "raw_data.parquet", "results.parquet" to identify cache contents

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions welcome! Please see CONTRIBUTING.md for guidelines.
