"""Polars caching library for DataFrames and LazyFrames."""

from .decorator import PolarsCache, cache

__all__ = ["cache", "PolarsCache"]
