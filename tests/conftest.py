# tests/conftest.py
"""Test configuration and fixtures for the Polars caching tests."""

import shutil

import pytest


@pytest.fixture(autouse=True, scope="session")
def cleanup_pytest_current_dir(tmp_path_factory):
    """Session-scoped cleanup that cleans up the pytest-current directory AFTER all tests."""
    yield  # Run ALL tests in the session first

    # Only clean up AFTER the entire test session is done
    try:
        # Get the base temp directory (e.g., /tmp/pytest-of-louis/)
        base_temp = tmp_path_factory.getbasetemp().parent

        # Clean up ONLY the pytest-current symlink directory
        pytest_current = base_temp / "pytest-current"
        if pytest_current.exists():
            # pytest-current is usually a symlink, clean up what it points to
            if pytest_current.is_symlink():
                target = pytest_current.resolve()
                if target.exists():
                    shutil.rmtree(target, ignore_errors=True)
                pytest_current.unlink(missing_ok=True)
            else:
                shutil.rmtree(pytest_current, ignore_errors=True)
    except (OSError, PermissionError):
        # If we can't remove it, that's ok
        pass


@pytest.fixture(autouse=True)
def cleanup_polars_cache():
    """Session-scoped cleanup of PolarsCache global state."""
    yield  # Run all tests first

    # Reset global cache to dummy state AFTER all tests
    try:
        import plcache.decorator
        from plcache.decorator import _DummyCache

        plcache.decorator._global_cache = _DummyCache()
    except ImportError:
        pass
