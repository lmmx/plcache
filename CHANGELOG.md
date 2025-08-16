# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2025-08-16

### Added
- **Configurable cache key callbacks**: Custom cache key generation via `cache_key` parameter
- **Configurable directory naming**: Custom directory names via `entry_dir` parameter  
- **MIT License**: Added MIT license for the project
- **Enhanced documentation**: Added detailed examples for custom cache keys and directory naming

### Changed
- **Package name**: Changed from original name to `polars-diskcache` (PyPI compatibility)
- **Improved callback system**: Better handling of custom cache key and directory naming functions

### Fixed
- **Test coverage**: Added comprehensive tests for callback functionality
- **Documentation**: Enhanced README with callback examples and usage patterns

## [0.1.0] - 2025-08-15

### Added
- **Core caching functionality**: Decorator for caching Polars DataFrames and LazyFrames
- **Parquet storage**: Results saved as Parquet files with metadata tracking
- **Human-readable cache structure**: Symlinked directory structure organized by module/function/arguments
- **Flexible organization options**:
  - `nested` parameter: Choose between nested module/function directories or flat structure
  - `symlinks_dir` parameter: Customizable readable directory name
  - `trim_arg` parameter: Configurable argument length truncation in directory names
- **Multiple cache directory options**:
  - Custom cache directory via `cache_dir`
  - System temp directory support via `use_tmp`
  - Hidden directory support via `hidden` parameter
- **Size management**: Configurable cache size limits with string parsing ("1GB", "500MB", etc.)
- **Type preservation**: Automatic detection and restoration of DataFrame vs LazyFrame types
- **Symlink customization**: Custom symlink filenames via `symlink_name` parameter
- **Argument handling**: Support for complex argument types with filesystem-safe encoding
- **SQLite-backed tracking**: Uses diskcache with SQLite for metadata management
- **Development tooling**:
  - Pre-commit hooks setup
  - Comprehensive test suite with pytest
  - Code formatting with ruff
  - Type checking with ty
  - CI-ready stub generation
- **Performance optimizations**: Efficient file size reduction (43MB → 16MB stubs)
- **Comprehensive documentation**:
  - Detailed README with examples
  - Usage patterns and best practices
  - Real-world examples with stock data analysis
  - Development documentation with Justfile recipes

### Technical Features
- **PolarsCache class**: Direct class usage for advanced configurations
- **Global cache instance**: Convenient `cache()` decorator for simple usage
- **Argument normalization**: Consistent handling of positional and keyword arguments
- **Cache invalidation**: Automatic cleanup when underlying files are missing
- **Cross-platform compatibility**: URL encoding for filesystem safety
- **Memory efficiency**: Lazy loading support with proper type preservation
