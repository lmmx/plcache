"""Symlink creation and management for readable cache structure."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    from .types import DecoratedFn, FilenameCallback

_DEFAULT_SYMLINK_NAME = "output.parquet"


class SymlinkManager:
    """Handles creation and management of readable symlink structures."""

    def __init__(self, symlink_name: str | FilenameCallback | None = None):
        """Initialise the symlink manager.

        Args:
            symlink_name: Custom name for symlink files. Can be a string or callable.
        """
        self.symlink_name = symlink_name

    def create_symlink(
        self,
        func: DecoratedFn,
        bound_args: dict[str, Any],
        cache_key: str,
        result: pl.DataFrame | pl.LazyFrame,
        readable_dir: Path,
        blob_path: Path,
    ) -> None:
        """Create a readable symlink pointing to the blob.

        Creates a human-readable symlink that points to the actual parquet file,
        making it easier to browse cached results in the file system.

        Args:
            func: The cached function
            bound_args: Bound arguments from the function call
            cache_key: The unique cache key for this result
            result: The function result (used for determining file type)
            readable_dir: Directory where the symlink should be created
            blob_path: Path to the actual parquet file (blob)
        """
        # Ensure the readable directory exists
        readable_dir.mkdir(parents=True, exist_ok=True)

        # Determine symlink filename
        symlink_name = self._get_symlink_name(func, bound_args, result, cache_key)

        # Create symlink
        symlink_path = readable_dir / symlink_name

        # Create relative path for symlink (more portable than absolute paths)
        try:
            relative_blob = os.path.relpath(blob_path, readable_dir)
            if not symlink_path.exists():
                symlink_path.symlink_to(relative_blob)
        except (OSError, FileExistsError):
            # Symlink creation failed, but that's okay - cache still works
            # This can happen on systems without symlink permissions
            pass

    def _get_symlink_name(
        self,
        func: DecoratedFn,
        bound_args: dict[str, Any],
        result: pl.DataFrame | pl.LazyFrame,
        cache_key: str,
    ) -> str:
        """Determine the symlink filename based on configuration.

        Args:
            func: The cached function
            bound_args: Bound arguments from the function call
            result: The function result
            cache_key: The unique cache key

        Returns:
            The filename to use for the symlink

        Raises:
            TypeError: If callback returns non-string type
            ValueError: If filename is empty or whitespace-only
        """
        if callable(self.symlink_name):
            symlink_name = self.symlink_name(func, bound_args, result, cache_key)
            if not isinstance(symlink_name, str):
                raise TypeError(
                    f"symlink_name callback must return str, got {type(symlink_name).__name__}"
                )
            if not symlink_name.strip():
                raise ValueError(
                    "symlink_name callback returned empty/whitespace-only string"
                )
            return symlink_name
        elif isinstance(self.symlink_name, str):
            if not self.symlink_name.strip():
                raise ValueError("symlink_name cannot be empty or whitespace-only")
            return self.symlink_name
        else:
            return _DEFAULT_SYMLINK_NAME
