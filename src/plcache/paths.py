"""Pure functions for path generation and directory management."""

from __future__ import annotations

import urllib.parse
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .types import DecoratedFn
    from .config_dataclasses import PathConfig


def create_entry_dir_name(
    func: DecoratedFn, bound_args: dict[str, Any], trim_arg: int = 50
) -> str:
    """Create directory name for function arguments."""
    args_parts = []
    for key, value in bound_args.items():
        value_str = str(value)[:trim_arg]
        encoded_value = urllib.parse.quote(value_str, safe="")
        args_parts.append(f"{key}={encoded_value}")
    return "_".join(args_parts) if args_parts else "no_args"


def get_parquet_path(config: PathConfig, cache_key: str) -> Path:
    """Get the parquet file path for a cache key."""
    return config.cache_dir / "blobs" / f"{cache_key}.parquet"


def get_readable_path(
    config: PathConfig, func: DecoratedFn, bound_args: dict[str, Any]
) -> Path:
    """Generate the readable directory path for a function call."""
    readable_dir = config.cache_dir / config.symlinks_dir_name

    module_name = func.__module__
    func_qualname = func.__qualname__

    if config.nested:
        encoded_module = urllib.parse.quote(module_name, safe="")
        encoded_qualname = urllib.parse.quote(func_qualname, safe="")
        readable_path = readable_dir / encoded_module / encoded_qualname
    else:
        full_qualname = f"{module_name}.{func_qualname}"
        encoded_qualname = urllib.parse.quote(full_qualname, safe="")
        readable_path = readable_dir / encoded_qualname

    entry_dir_name = config.entry_dir_callback(func=func, bound_args=bound_args)
    return readable_path / entry_dir_name
