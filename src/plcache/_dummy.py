"""Dummy cache implementation that provides a no-op decorator interface.

This module contains a placeholder cache that doesn't actually cache anything,
used as a default before the real cache is initialized or when caching is disabled.
"""

from __future__ import annotations


class _DummyCache:
    """A dummy cache that does nothing - just executes functions normally."""

    cache_dir = None

    def cache_polars(self, **kwargs):
        """Return a no-op decorator that doesn't cache anything.

        Args:
            **kwargs: Ignored keyword arguments for compatibility.

        Returns:
            A decorator that returns the original function unchanged.
        """

        def decorator(func):
            return func  # Just return the original function unchanged

        return decorator
