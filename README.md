# polarscache

A disk cache decorator for Polars DataFrames and LazyFrames that saves results as Parquet files.

## What it does

Caches expensive Polars operations to disk using Parquet format.

When you call a decorated function with the same arguments, it loads the cached Parquet file instead of recomputing.

## Basic usage

```python
import polars as pl
from plcache import cache

@cache()
def expensive_computation(n: int) -> pl.DataFrame:
    import time
    time.sleep(2)  # Expensive operation
    
    return pl.DataFrame({
        "x": [i + 1 for i in range(n)],
        "y": [i + 2 for i in range(n)],
        "z": [i + 3 for i in range(n)],
    })

# First call: runs the function and saves result to Parquet cache
df1 = expensive_computation(1000)  # Takes 2+ seconds

# Second call: loads from cached Parquet file  
df2 = expensive_computation(1000)  # Nearly instant!
```

## Control return types

```python
# Always return LazyFrame (good for chaining operations)
@cache(lazy=True)
def compute_features(rows: int) -> pl.LazyFrame:
    # Expensive feature engineering
    return pl.DataFrame({
        "feature_1": [complex_calc_1(i) for i in range(rows)],
        "feature_2": [complex_calc_2(i) for i in range(rows)]
    }).lazy()

# Always return DataFrame (good when you need immediate results)
@cache(lazy=False) 
def generate_report(params: dict) -> pl.DataFrame:
    # Expensive aggregations and transformations
    return create_complex_dataframe(params)

# Preserve original return type (default behavior)
@cache()  # or cache(lazy=None)  
def model_predictions(data_size: int) -> pl.DataFrame:
    # Expensive ML predictions
    return run_model_inference(data_size)
```

## Installation

```bash
uv pip install polarscache
```

## Requirements

- Python 3.13+
- polars
- diskcache
