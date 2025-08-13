from types import FunctionType
from collections.abc import Callable

from ty_extensions import Intersection

CallableFn = Intersection[FunctionType, Callable[[], None]]
