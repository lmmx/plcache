"""
Advanced configuration examples for the Polars Cache library.

This example demonstrates various configuration options and the PolarsCache class usage.
"""

import polars as pl
from plcache import PolarsCache, cache


def demonstrate_split_vs_flat():
    """Show the difference between split and flat module path structures."""
    print("📁 Directory Structure Examples")
    print("=" * 60)

    # Split module path (default) - creates: cache/functions/module/function/args/
    @cache(
        cache_dir="./example_cache_split",
        readable_dir_name="functions",
        split_module_path=True,
        symlink_name="result.parquet",
    )
    def split_example(value: int) -> pl.DataFrame:
        return pl.DataFrame({"value": [value]})

    # Flat module path - creates: cache/functions/module.function/args/
    @cache(
        cache_dir="./example_cache_flat",
        readable_dir_name="functions",
        split_module_path=False,
        symlink_name="result.parquet",
    )
    def flat_example(value: int) -> pl.DataFrame:
        return pl.DataFrame({"value": [value]})

    # Create some cached data
    split_result = split_example(42)
    flat_result = flat_example(42)

    print("✅ Split structure created at:")
    print(
        "   ./example_cache_split/functions/[module]/split_example/arg0=42/result.parquet"
    )
    print("✅ Flat structure created at:")
    print(
        "   ./example_cache_flat/functions/[module.flat_example]/arg0=42/result.parquet"
    )

    return split_result, flat_result


def demonstrate_class_usage():
    """Show how to use the PolarsCache class directly for more control."""
    print("\n🏗️  Direct Class Usage")
    print("=" * 60)

    # Create a custom cache instance
    my_cache = PolarsCache(
        cache_dir="./my_custom_cache",
        readable_dir_name="analytics_cache",
        split_module_path=True,
        max_arg_length=20,  # Truncate long arguments
        symlink_name="data.parquet",
    )

    @my_cache.cache_polars()
    def analyze_with_long_params(
        data_source: str, analysis_type: str = "comprehensive_statistical_analysis"
    ) -> pl.DataFrame:
        """Function with potentially long parameter names."""
        print(f"🔬 Analyzing {data_source} with {analysis_type}")

        return pl.DataFrame(
            {
                "source": [data_source],
                "analysis": [analysis_type],
                "result": [f"Analysis of {data_source}"],
            }
        )

    # Test with long parameters
    result = analyze_with_long_params(
        "very_long_data_source_name_that_exceeds_normal_limits",
        "comprehensive_statistical_analysis_with_machine_learning",
    )

    print("✅ Class-based cache created with argument truncation")
    print("   Long arguments are truncated to 20 characters in directory names")

    return result


def demonstrate_custom_configurations():
    """Show various configuration combinations."""
    print("\n⚙️  Custom Configuration Examples")
    print("=" * 60)

    # Configuration 1: Scientific data cache
    @cache(
        cache_dir="./science_cache",
        readable_dir_name="experiments",
        split_module_path=True,
        max_arg_length=15,
        symlink_name="experiment_data.parquet",
    )
    def run_simulation(
        particles: int, temperature: float, simulation_type: str = "molecular_dynamics"
    ) -> pl.DataFrame:
        """Simulate scientific experiment."""
        print(
            f"🧪 Running {simulation_type} with {particles} particles at {temperature}K"
        )

        return pl.DataFrame(
            {
                "time": range(particles),
                "position": [i * 0.1 for i in range(particles)],
                "velocity": [i * 0.05 for i in range(particles)],
                "temperature": [temperature] * particles,
            }
        )

    # Configuration 2: Business analytics cache
    @cache(
        cache_dir="./analytics_cache",
        readable_dir_name="reports",
        split_module_path=False,  # Flat structure for simpler browsing
        max_arg_length=30,
        symlink_name="report.parquet",
    )
    def generate_report(
        start_date: str, end_date: str, metrics: str = "revenue_and_growth"
    ) -> pl.LazyFrame:
        """Generate business report."""
        print(f"📈 Generating {metrics} report from {start_date} to {end_date}")

        return pl.DataFrame(
            {
                "date": [start_date, end_date],
                "metric_type": [metrics, metrics],
                "value": [1000, 1500],
            }
        ).lazy()

    # Test the configurations
    sim_result = run_simulation(particles=100, temperature=298.15)
    report_result = generate_report("2024-01-01", "2024-12-31", "quarterly_performance")

    print("✅ Science cache: ./science_cache/experiments/[module]/run_simulation/")
    print("✅ Analytics cache: ./analytics_cache/reports/[flat_module_path]/")

    return sim_result, report_result


def demonstrate_argument_handling():
    """Show how different argument types are handled."""
    print("\n🔤 Argument Handling Examples")
    print("=" * 60)

    @cache(
        cache_dir="./args_demo", max_arg_length=25, symlink_name="args_result.parquet"
    )
    def complex_function(
        numbers: list[int],
        config: dict,
        flag: bool = True,
        mode: str = "advanced_processing_mode",
    ) -> pl.DataFrame:
        """Function with various argument types."""
        print(
            f"🔧 Processing with numbers={numbers}, config={config}, flag={flag}, mode={mode}"
        )

        return pl.DataFrame(
            {
                "input_size": [len(numbers)],
                "config_keys": [len(config)],
                "flag_value": [flag],
                "mode": [mode],
            }
        )

    # Test with various argument types
    result = complex_function(
        numbers=[1, 2, 3, 4, 5],
        config={"threshold": 0.5, "iterations": 100},
        flag=False,
        mode="super_advanced_machine_learning_mode_with_cross_validation",
    )

    print("✅ Complex arguments handled and encoded in directory structure")
    print("   Lists, dicts, and long strings are safely encoded")

    return result


def main():
    """Run all advanced configuration examples."""
    print("🚀 Polars Cache Advanced Configuration Examples\n")

    # Run all demonstrations
    demonstrate_split_vs_flat()
    demonstrate_class_usage()
    demonstrate_custom_configurations()
    demonstrate_argument_handling()

    print("\n🎉 All advanced examples completed!")
    print("\n📁 Cache directories created:")
    print("   - ./example_cache_split/")
    print("   - ./example_cache_flat/")
    print("   - ./my_custom_cache/")
    print("   - ./science_cache/")
    print("   - ./analytics_cache/")
    print("   - ./args_demo/")
    print("\n💡 Explore these directories to see the different structures!")


if __name__ == "__main__":
    main()
