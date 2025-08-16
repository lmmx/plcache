"""Pure functions for symlink creation and management."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    from .types import DecoratedFn
    from .config_dataclasses import SymlinkConfig

_DEFAULT_SYMLINK_NAME = "output.parquet"


def get_symlink_name(
    config: SymlinkConfig,
    func: DecoratedFn,
    bound_args: dict[str, Any],
    result: pl.DataFrame | pl.LazyFrame,
    cache_key: str,
) -> str:
    """Determine the symlink filename based on configuration."""
    if callable(config.symlink_name):
        symlink_name = config.symlink_name(func, bound_args, result, cache_key)
        if not isinstance(symlink_name, str):
            raise TypeError(
                f"symlink_name callback must return str, got {type(symlink_name).__name__}"
            )
        if not symlink_name.strip():
            raise ValueError(
                "symlink_name callback returned empty/whitespace-only string"
            )
        return symlink_name
    elif isinstance(config.symlink_name, str):
        if not config.symlink_name.strip():
            raise ValueError("symlink_name cannot be empty or whitespace-only")
        return config.symlink_name
    else:
        return _DEFAULT_SYMLINK_NAME


def create_symlink(
    config: SymlinkConfig,
    func: DecoratedFn,
    bound_args: dict[str, Any],
    cache_key: str,
    result: pl.DataFrame | pl.LazyFrame,
    readable_dir: Path,
    blob_path: Path,
) -> None:
    """Create a readable symlink pointing to the blob."""
    readable_dir.mkdir(parents=True, exist_ok=True)

    symlink_name = get_symlink_name(config, func, bound_args, result, cache_key)
    symlink_path = readable_dir / symlink_name

    try:
        relative_blob = os.path.relpath(blob_path, readable_dir)
        if not symlink_path.exists():
            symlink_path.symlink_to(relative_blob)
    except (OSError, FileExistsError):
        # Symlink creation failed, but that's okay - cache still works
        pass
