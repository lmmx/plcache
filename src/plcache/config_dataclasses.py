"""Configuration dataclasses for cache managers."""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

from .paths import create_entry_dir_name

if TYPE_CHECKING:
    from .types import EntryDirCallback, FilenameCallback


@dataclass
class PathConfig:
    """Configuration for path generation and directory structure."""

    cache_dir: Path
    symlinks_dir_name: str = "functions"
    nested: bool = True
    trim_arg: int = 50
    entry_dir_callback: EntryDirCallback = None

    def __post_init__(self):
        """Set up default callback if none provided."""
        if self.entry_dir_callback is None:
            self.entry_dir_callback = partial(
                create_entry_dir_name, trim_arg=self.trim_arg
            )

        # Ensure required directories exist
        (self.cache_dir / "blobs").mkdir(exist_ok=True)
        (self.cache_dir / self.symlinks_dir_name).mkdir(exist_ok=True)


@dataclass
class SymlinkConfig:
    """Configuration for symlink creation."""

    symlink_name: str | FilenameCallback | None = None
