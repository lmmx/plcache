# types.py
"""Type definitions for plcache."""

from collections.abc import Callable
from types import FunctionType
from typing import Union

import polars as pl
from ty_extensions import Intersection

CallableFn = Intersection[FunctionType, Callable[[], None]]
PolarsFrame = Union[pl.DataFrame, pl.LazyFrame]
DecoratedFn = Callable[..., PolarsFrame]

# Type alias for the callback function
FilenameCallback = Callable[
    [
        DecoratedFn,  # func: the decorated function
        tuple,  # args: passed to the function by position
        dict,  # kwargs: passed to the function by name
        PolarsFrame,  # result: what the function returned
        str,  # cache_key: the uniquely hashed string
    ],
    str,  # returns the filename as a string
]

CacheKeyCallback = Callable[
    [
        DecoratedFn,  # func: the decorated function
        tuple,  # args: positional arguments
        dict,  # kwargs: keyword arguments
    ],
    str,  # returns the cache key identifier (before hashing)
]

EntryDirCallback = Callable[
    [
        DecoratedFn,  # func: the decorated function
        tuple,  # args: positional arguments
        dict,  # kwargs: keyword arguments
    ],
    str,  # returns the directory name for this function call
]
