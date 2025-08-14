import os
from typing import TYPE_CHECKING

DEBUG = os.getenv("DEBUG_PYSNOOPER", False)

if TYPE_CHECKING or not DEBUG:

    def snoop():
        def decorator(func):
            return func

        return decorator
else:
    pass
