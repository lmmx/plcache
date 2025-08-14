"""Type definitions for plcache."""

from collections.abc import Callable
from types import FunctionType

from ty_extensions import Intersection

CallableFn = Intersection[FunctionType, Callable[[], None]]
