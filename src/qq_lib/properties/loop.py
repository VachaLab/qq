# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

"""
Loop-job metadata and cycle-tracking utilities.

This module defines `LoopInfo`, a dataclass describing the iteration
parameters of a qq loop job: its cycle range, archive location, archive
naming format, and the current cycle as inferred from existing archived
files.
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from qq_lib.archive.archiver import Archiver
from qq_lib.core.common import logical_resolve
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.transfer_mode import TransferMode

logger = get_logger(__name__)


@dataclass(init=False)
class LoopInfo:
    """
    Dataclass containing information about a qq loop job.
    """

    start: int
    end: int
    archive: Path
    archive_format: str
    current: int
    archive_mode: list[TransferMode]

    def __init__(
        self,
        start: int,
        end: int | None,
        archive: Path,
        archive_format: str,
        current: int | None = None,
        input_dir: Path | None = None,
        archive_mode: list[TransferMode] | None = None,
    ):
        """
        Initialize loop job information with validation checks.

        Args:
            start (int): The starting cycle number.
            end (int | None): The ending cycle number. Must be provided and >= `start`.
            archive (Path): Path to the archive directory.
            input_dir (Path | None): The job submission directory. Used to validate archive.
                If `None`, no validation is performed.
            archive_format (str): File naming pattern used for archived files.
            current (int | None): The current cycle number. Defaults to `start`
                if not provided.
            archive_mode (list[TransferMode] | None): When should the files be archived?
                Defaults to [Success()], meaning that archival should only be performed for
                successfully completed jobs.

        Raises:
            QQError: If `end` is not provided, if `start > end`, if `current > end`,
                or if the archive path is invalid.
        """
        if not end:
            raise QQError("Attribute 'loop-end' is undefined.")

        self.archive = logical_resolve(archive)
        if input_dir and self.archive == logical_resolve(input_dir):
            raise QQError("Input directory cannot be used as the loop job's archive.")

        self.archive_format = archive_format
        self.archive_mode = archive_mode or TransferMode.multiFromStr(
            CFG.transfer_files_options.default_archive_mode
        )

        self.start = start
        self.end = end
        self.current = current or self._getCycle()

        if self.start < 0:
            raise QQError(f"Attribute 'loop-start' ({self.start}) cannot be negative.")

        if self.start > self.end:
            raise QQError(
                f"Attribute 'loop-start' ({self.start}) cannot be higher than 'loop-end' ({self.end})."
            )

        if self.current > self.end:
            raise QQError(
                f"Current cycle number ({self.current}) cannot be higher than 'loop-end' ({self.end})."
            )

    @classmethod
    def fromDict(cls, data: dict[str, object]) -> Self:
        """
        Reconstruct a LoopInfo instance from a dictionary produced by toDict.

        Args:
            data (dict[str, object]): A dictionary as returned by `toDict()`.

        Returns:
            LoopInfo: A new instance with fields populated from the dictionary.
        """
        start = data.get("start")
        end = data.get("end")
        archive = data.get("archive")
        archive_format = data.get("archive_format")
        current = data.get("current")
        archive_mode = data.get("archive_mode", ["success"])

        if not isinstance(start, int):
            raise QQError(f"Field 'start' must be an int, got {type(start).__name__}.")
        if not isinstance(end, int):
            raise QQError(f"Field 'end' must be an int, got {type(end).__name__}.")
        if not isinstance(archive, str):
            raise QQError(
                f"Field 'archive' must be a str, got {type(archive).__name__}."
            )
        if not isinstance(archive_format, str):
            raise QQError(
                f"Field 'archive_format' must be a str, got {type(archive_format).__name__}."
            )
        if not isinstance(current, int):
            raise QQError(
                f"Field 'current' must be an int, got {type(current).__name__}."
            )
        if not isinstance(archive_mode, list) or not all(
            isinstance(m, str) for m in archive_mode
        ):
            raise QQError("Field 'archive_mode' must be a list of strings.")

        return cls(
            start=start,
            end=end,
            archive=Path(archive),
            archive_format=archive_format,
            current=current,
            archive_mode=[TransferMode.fromStr(mode) for mode in archive_mode],  # ty: ignore[invalid-argument-type]
        )

    def toDict(self) -> dict[str, object]:
        """Return all fields as a dict."""
        return {
            "start": self.start,
            "end": self.end,
            "archive": str(self.archive),
            "archive_format": self.archive_format,
            "current": self.current,
            "archive_mode": [mode.toStr() for mode in self.archive_mode],
        }

    def toCommandLine(self) -> list[str]:
        """
        Convert loop job settings into a command-line argument list for `qq submit`.

        Returns:
            list[str]: A list of command-line arguments ready to pass to ``qq submit``.
        """
        return [
            "--loop-start",
            str(self.start),
            "--loop-end",
            str(self.end),
            "--archive",
            self.archive.name,
            "--archive-format",
            self.archive_format,
            "--archive-mode",
            ":".join(mode.toStr() for mode in self.archive_mode),
        ]

    def _getCycle(self) -> int:
        """
        Determine the current cycle number based on files in the archive directory.

        Returns:
            int: The detected maximum cycle number, or `self.start` if no valid cycle
                can be inferred.

        Notes:
            - Only the first sequence of digits found in the stem is considered.
        """

        # if the directory does not exist, use the starting cycle number
        if not self.archive.is_dir():
            logger.debug(
                f"Archive '{self.archive}' does not exist. Setting cycle number to start ({self.start})."
            )
            return self.start

        stem_pattern = Archiver._prepare_regex_pattern(self.archive_format)
        logger.debug(f"Stem pattern: {stem_pattern}.")

        # use start as default
        max_number = self.start
        for f in self.archive.iterdir():
            if not stem_pattern.search(f.stem):
                continue

            match = re.search(r"\d+", f.stem)
            if match:
                number = int(match.group(0))
                max_number = max(max_number, number)

        return max_number
