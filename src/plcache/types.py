# types.py
"""Type definitions for plcache."""

from collections.abc import Callable
from types import FunctionType

import polars as pl
from ty_extensions import Intersection

CallableFn = Intersection[FunctionType, Callable[[], None]]

# Type alias for the callback function
FilenameCallback = Callable[
    [
        Callable[..., pl.DataFrame | pl.LazyFrame],  # func: the decorated function
        tuple,  # args: passed to the function by position
        dict,  # kwargs: passed to the function by name
        pl.DataFrame | pl.LazyFrame,  # result: what the function returned
        str,  # cache_key: the uniquely hashed string
    ],
    str,  # returns the filename as a string
]
