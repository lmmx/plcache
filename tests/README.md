# Test Coverage Documentation

The core behaviour has been tested, test contributions welcome!

## Test Structure

The test suite is organised into 5 modules, each focusing on different aspects of the caching functionality:

### `basic_test.py` - Core Functionality
Tests the fundamental caching behavior and performance.

- **`test_cache_performance_and_equality`**: Verifies that cached results are returned quickly on subsequent calls and that cached DataFrames/LazyFrames are identical to original results
- **`test_different_args_different_cache`**: Ensures that functions called with different arguments create separate cache entries

### `configuration_test.py` - Configuration Options
Tests various configuration parameters and their effects on cache behavior.

- **`test_cache_custom_dir_name`**: Verifies that custom `symlinks_dir` parameter works correctly
- **`test_trim_arg_truncation`**: Tests that long function arguments are properly truncated in directory names according to `trim_arg` setting
- **`test_cache_with_kwargs`**: Ensures that functions with keyword arguments create proper directory structures including both positional and keyword arguments

### `directory_structure_test.py` - Directory Layout Patterns
Tests how the cache organises files and directories in different structural modes.

- **`test_nested`**: Verifies `nested=True` creates structure like `functions/module_name/function_name/args/`
- **`test_flat_module_path`**: Verifies `nested=False` creates structure like `functions/full_qualified_name/args/`
- **`test_multiple_functions_separate_directories`**: Ensures different functions create separate directory hierarchies

### `symlinks_test.py` - Symlink Functionality
Tests the symbolic link creation and integrity.

- **`test_symlink_points_to_correct_blob`**: Verifies that readable symlinks correctly point to the actual parquet files in the blobs directory and that data can be read through the symlinks

### `direct_class_test.py` - Class Interface
Tests using the `PolarsCache` class directly instead of the convenience decorator.

- **`test_polars_cache_class_direct_usage`**: Verifies that creating a `PolarsCache` instance directly and using its `cache_polars()` decorator works correctly with custom settings

## What's Covered

### ✅ Tested Features

- **Basic caching functionality** (performance, correctness)
- **Cache isolation** (different args = different cache entries)
- **Directory structure modes** (split vs flat module paths)
- **Configuration options** (custom dir names, argument truncation)
- **Argument handling** (positional args, keyword args, long args)
- **Symlink creation and integrity**
- **Direct class usage** (alternative to decorator)
- **URL encoding consistency** (filesystem-safe directory names)

### ⚠️ Not Yet Tested

- **Cache persistence** across Python sessions
- **Cache size limits** and eviction behavior
- **Error handling** (invalid paths, permission issues, disk full)
- **Concurrent access** (multiple processes/threads)
- **Cache clearing** functionality
- **Different data types** beyond DataFrames/LazyFrames
- **Large datasets** and performance under load
- **Symlink failure fallback** behavior
- **Cross-platform compatibility** (Windows, macOS, Linux)

## Test Data Patterns

All tests use:
- **Temporary directories** via pytest's `tmp_path` fixture
- **Small synthetic datasets** for fast execution
- **Deterministic data** for reliable assertions
- **URL-encoded path expectations** for filesystem safety
