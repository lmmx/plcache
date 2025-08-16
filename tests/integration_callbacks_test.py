# tests/integration_callbacks_test.py
"""Integration tests for custom callbacks with real-world scenarios."""

import hashlib
from pathlib import Path

import polars as pl

from plcache import PolarsCache


def test_machine_learning_pipeline_cache_strategy(tmp_path):
    """Test a realistic ML pipeline caching strategy with custom callbacks."""

    def ml_cache_key(func, bound_args):
        """Cache key that ignores random seeds and focuses on data/model params."""
        # For ML functions, we often want to cache based on data characteristics
        # and model parameters, but ignore random seeds
        filtered_args = {k: v for k, v in bound_args.items() if k != "random_seed"}
        data_shape = bound_args["data"].shape if "data" in bound_args else "none"
        return f"{func.__name__}(data_shape={data_shape}, params={filtered_args})"

    def ml_entry_dir(func, bound_args):
        """Create readable ML experiment directories."""
        if "data" in bound_args and hasattr(bound_args["data"], "shape"):
            rows, cols = bound_args["data"].shape
            model_type = bound_args.get("model_type", "unknown")
            return f"{func.__name__}_{model_type}_{rows}x{cols}"
        return f"{func.__name__}_no_data"

    pc = PolarsCache(
        cache_dir=tmp_path,
        cache_key=ml_cache_key,
        entry_dir=ml_entry_dir,
        symlink_name=lambda func,
        bound_args,
        result,
        cache_key: f"{func.__name__}_result.parquet",
    )

    @pc.cache_polars()
    def train_model(
        data: pl.DataFrame, model_type: str = "linear", random_seed: int = 42
    ) -> pl.DataFrame:
        """Simulate expensive model training."""
        # Simulate some computation that should be cached
        feature_means = data.select(pl.all().mean())
        return feature_means.with_columns(
            pl.lit(model_type).alias("model_type"),
            pl.lit(f"trained_with_seed_{random_seed}").alias("training_info"),
        )

    # Create test data
    train_data = pl.DataFrame(
        {"feature1": [1, 2, 3, 4, 5], "feature2": [10, 20, 30, 40, 50]}
    )

    # First training run
    result1 = train_model(train_data, model_type="random_forest", random_seed=123)

    # Second training with different random seed - should hit cache
    result2 = train_model(train_data, model_type="random_forest", random_seed=999)

    # Should be identical because random_seed is ignored in cache key
    assert result1.equals(result2)

    # Third training with different model type - should miss cache
    result3 = train_model(train_data, model_type="xgboost", random_seed=123)

    # Should be different because model_type affects cache key
    assert not result1.equals(result3)

    # Check directory structure
    cache_path = Path(tmp_path)
    symlinks = list(cache_path.rglob("train_model_result.parquet"))

    # Should have 2 cached results (random_forest and xgboost)
    assert len(symlinks) == 2

    # Check directory names follow our custom pattern
    dir_names = {symlink.parent.name for symlink in symlinks}
    expected_dirs = {"train_model_random_forest_5x2", "train_model_xgboost_5x2"}
    assert dir_names == expected_dirs


def test_data_processing_versioning_strategy(tmp_path):
    """Test a data processing pipeline with version-aware caching."""

    def versioned_cache_key(func, bound_args):
        """Include data version in cache key for data processing functions."""
        data_version = bound_args.get("version", "v1.0")
        core_params = {k: v for k, v in bound_args.items() if k != "debug"}
        return f"{func.__name__}_version_{data_version}({core_params})"

    def processing_entry_dir(func, bound_args):
        """Organize by processing type and version."""
        version = bound_args.get("version", "v1.0")
        process_type = bound_args.get("process_type", "standard")
        return f"{process_type}_processing_{version}"

    pc = PolarsCache(
        cache_dir=tmp_path,
        cache_key=versioned_cache_key,
        entry_dir=processing_entry_dir,
        symlinks_dir="data_processing",
        symlink_name="processed_data.parquet",
    )

    @pc.cache_polars()
    def process_data(
        raw_data: pl.DataFrame,
        process_type: str = "standard",
        version: str = "v1.0",
        debug: bool = False,
    ) -> pl.DataFrame:
        """Simulate data processing that depends on version."""
        if version == "v1.0":
            return raw_data.with_columns(pl.col("value") * 2)
        elif version == "v2.0":
            return raw_data.with_columns(pl.col("value") * 3)
        else:
            return raw_data

    # Test data
    raw_data = pl.DataFrame({"value": [1, 2, 3]})

    # Process with v1.0
    result_v1_1 = process_data(
        raw_data, process_type="advanced", version="v1.0", debug=True
    )
    result_v1_2 = process_data(
        raw_data, process_type="advanced", version="v1.0", debug=False
    )

    # Should be identical (debug flag ignored in cache key)
    assert result_v1_1.equals(result_v1_2)

    # Process with v2.0
    result_v2 = process_data(
        raw_data, process_type="advanced", version="v2.0", debug=True
    )

    # Should be different (different version)
    assert not result_v1_1.equals(result_v2)

    # Check directory structure
    cache_path = Path(tmp_path)
    symlinks = list(cache_path.rglob("processed_data.parquet"))

    # Should have 2 results (v1.0 and v2.0)
    assert len(symlinks) == 2

    # Check directory organization
    dir_names = {symlink.parent.name for symlink in symlinks}
    expected_dirs = {"advanced_processing_v1.0", "advanced_processing_v2.0"}
    assert dir_names == expected_dirs

    # Verify they're in the custom symlinks directory
    for symlink in symlinks:
        assert "data_processing" in str(symlink)


def test_callback_error_handling_in_real_workflow(tmp_path):
    """Test that callback errors don't break the entire caching workflow."""

    def sometimes_broken_cache_key(func, bound_args):
        """Cache key that fails for certain inputs."""
        if (
            "input_value" in bound_args
            and bound_args["input_value"] == "break_cache_key"
        ):
            raise RuntimeError("Cache key generation failed!")
        return f"working_cache_key_{bound_args}"

    def sometimes_broken_entry_dir(func, bound_args):
        """Entry dir that fails for certain inputs."""
        if (
            "input_value" in bound_args
            and bound_args["input_value"] == "break_entry_dir"
        ):
            raise RuntimeError("Entry dir generation failed!")
        return f"working_dir_{len(bound_args)}_args"

    pc = PolarsCache(
        cache_dir=tmp_path,
        cache_key=sometimes_broken_cache_key,
        entry_dir=sometimes_broken_entry_dir,
    )

    @pc.cache_polars()
    def robust_function(input_value: str) -> pl.DataFrame:
        return pl.DataFrame(
            {"input": [input_value], "processed": [input_value.upper()]}
        )

    # This should work fine
    result1 = robust_function("normal_input")
    assert result1["processed"].to_list() == ["NORMAL_INPUT"]

    # This should raise an error due to cache key callback failure
    try:
        robust_function("break_cache_key")
        assert False, "Should have raised an error"
    except RuntimeError as e:
        assert "Cache key generation failed!" in str(e)

    # This should raise an error due to entry dir callback failure
    try:
        robust_function("break_entry_dir")
        assert False, "Should have raised an error"
    except RuntimeError as e:
        assert "Entry dir generation failed!" in str(e)

    # Verify that working calls still create proper cache structure
    cache_path = Path(tmp_path)
    symlinks = list(cache_path.rglob("output.parquet"))
    assert len(symlinks) == 1  # Only the successful call

    # Verify the working directory name
    working_symlink = symlinks[0]
    assert working_symlink.parent.name == "working_dir_1_args"
