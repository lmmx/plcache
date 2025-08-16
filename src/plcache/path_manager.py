"""Path and directory management for polars cache."""

from __future__ import annotations

import urllib.parse
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .types import DecoratedFn, EntryDirCallback


class CachePathManager:
    """Handles all path generation and directory structure management for the cache."""

    def __init__(
        self,
        cache_dir: Path,
        symlinks_dir_name: str = "functions",
        nested: bool = True,
        trim_arg: int = 50,
        entry_dir_callback: EntryDirCallback | None = None,
    ):
        """Initialise the path manager.

        Args:
            cache_dir: Root cache directory
            symlinks_dir_name: Name of the readable symlinks directory
            nested: Whether to nest module/function in separate directories
            trim_arg: Maximum length for argument values in directory names
            entry_dir_callback: Optional callback to generate entry directory names
        """
        self.cache_dir = cache_dir
        self.symlinks_dir_name = symlinks_dir_name
        self.nested = nested
        self.trim_arg = trim_arg
        self.entry_dir_callback = entry_dir_callback or self._default_entry_dir_name

        # Ensure required directories exist
        self.parquet_dir = cache_dir / "blobs"
        self.parquet_dir.mkdir(exist_ok=True)

        self.readable_dir = cache_dir / symlinks_dir_name
        self.readable_dir.mkdir(exist_ok=True)

    def _default_entry_dir_name(
        self, func: DecoratedFn, bound_args: dict[str, Any]
    ) -> str:
        """Create directory name for function arguments.

        Args:
            func: The decorated function to generate a cache key for.
            bound_args: Bound arguments passed to the function in the cached call.

        Returns:
            str: Conjoined function arguments as directory name.
        """
        args_parts = []
        for key, value in bound_args.items():
            value_str = str(value)[: self.trim_arg]
            encoded_value = urllib.parse.quote(value_str, safe="")
            args_parts.append(f"{key}={encoded_value}")

        return "_".join(args_parts) if args_parts else "no_args"

    def get_parquet_path(self, cache_key: str) -> Path:
        """Get the parquet file path for a cache key (in blobs directory).

        Args:
            cache_key: The unique cache key for the cached result.

        Returns:
            Path object pointing to the parquet file location in the blobs directory.
        """
        return self.parquet_dir / f"{cache_key}.parquet"

    def get_readable_path(self, func: DecoratedFn, bound_args: dict[str, Any]) -> Path:
        """Generate the readable directory path for a function call.

        Args:
            func: The cached function
            bound_args: Bound arguments from the function call

        Returns:
            Path to the readable directory for this function call
        """
        module_name = func.__module__
        func_qualname = func.__qualname__

        # Build the readable path structure
        if self.nested:
            # Split: readable_dir/encoded_module/encoded_qualname/args/
            encoded_module = urllib.parse.quote(module_name, safe="")
            encoded_qualname = urllib.parse.quote(func_qualname, safe="")
            readable_path = self.readable_dir / encoded_module / encoded_qualname
        else:
            # Flat: readable_dir/encoded_full_qualname/args/
            full_qualname = f"{module_name}.{func_qualname}"
            encoded_qualname = urllib.parse.quote(full_qualname, safe="")
            readable_path = self.readable_dir / encoded_qualname

        entry_dir_name = self.entry_dir_callback(func=func, bound_args=bound_args)
        return readable_path / entry_dir_name
