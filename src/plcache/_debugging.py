import os

DEBUG = os.getenv("DEBUG_PYSNOOPER", False)

if DEBUG:
    from pysnooper import snoop
else:

    def snoop():
        def decorator(func):
            return func

        return decorator
