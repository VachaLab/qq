# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import threading
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from qq_lib.core.common import get_info_files, get_runtime_files
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.info.informer import Informer
from qq_lib.properties.states import RealState

logger = get_logger(__name__)


@dataclass
class _ClearResult:
    """Accumulates results from clearing a single directory."""

    deleted: int = 0
    excluded: int = 0


class Clearer:
    """
    Handles detection and removal of qq runtime files from a directory.
    """

    def __init__(self, directories: list[Path]):
        """
        Initialize a Clearer for one or more directories.

        Args:
            directories (list[Path]): The directories to clear qq runtime files from.
        """
        self._directories = directories

    def clear(self, force: bool = False) -> None:
        """
        Remove all qq runtime files from all directories that are safe to be removed.

        Directories are cleared in parallel. Only qq files that do **not**
        correspond to an active or successfully finished job will be removed,
        unless `force` is set to True. A combined summary is logged at the end.

        Args:
            force (bool): If True, remove all qq runtime files, even if unsafe.
        """
        results: list[_ClearResult] = []
        lock = threading.Lock()

        def clear_single(directory: Path) -> None:
            result = Clearer._clear_directory(directory, force)
            with lock:
                results.append(result)

        with ThreadPoolExecutor(
            max_workers=CFG.parallelization_options.clear_max_threads
        ) as executor:
            for directory in self._directories:
                executor.submit(clear_single, directory)

        total_deleted = sum(r.deleted for r in results)
        total_excluded = sum(r.excluded for r in results)

        if total_deleted == 0 and total_excluded == 0:
            logger.info("Nothing to clear.")
            return

        if total_deleted > 0:
            logger.info(
                f"Removed {total_deleted} qq file{'s' if total_deleted > 1 else ''}."
            )

        if total_excluded > 0:
            logger.info(
                f"{total_excluded} qq file{'s' if total_excluded > 1 else ''} could not be safely cleared. "
                f"Rerun as '{CFG.binary_name} clear --force' to clear them forcibly."
            )

    @staticmethod
    def _clear_directory(directory: Path, force: bool) -> _ClearResult:
        """
        Clear qq runtime files from a single directory and return the result.

        Args:
            directory (Path): The directory to clear.
            force (bool): If True, remove all qq runtime files, even if unsafe.

        Returns:
            _ClearResult: The number of deleted and excluded files.
        """
        files = Clearer._collect_runtime_files(directory)
        logger.debug(f"All qq runtime files in '{directory}': {files}.")
        if not files:
            return _ClearResult()

        excluded = Clearer._collect_excluded_files(directory) if not force else set()
        logger.debug(f"Files excluded from clearing in '{directory}': {excluded}.")

        to_delete = files - excluded
        logger.debug(f"Files to delete in '{directory}': {to_delete}.")

        if to_delete:
            Clearer._delete_files(to_delete)

        return _ClearResult(deleted=len(to_delete), excluded=len(excluded))

    @staticmethod
    def _collect_runtime_files(directory: Path) -> set[Path]:
        """
        Collect all qq runtime files in the directory.

        Returns:
            set[Path]: Paths to all files matching qq-specific suffixes.
        """
        return set(get_runtime_files(directory))

    @staticmethod
    def _collect_excluded_files(directory: Path) -> set[Path]:
        """
        Collect qq runtime files that should **not** be deleted.

        Runtime files corresponding to active or successfully finished jobs are included.

        Returns:
            set[Path]: Paths to qq runtime files that should not be deleted.
        """
        excluded = []

        # iterate through info files
        for file in get_info_files(directory):
            try:
                informer = Informer.from_file(file)
                state = informer.get_real_state()
                logger.debug(f"Job state: {str(state)}.")
            except QQError:
                # ignore the file if it cannot be read
                continue

            if state not in [
                RealState.KILLED,
                RealState.FAILED,
                RealState.IN_AN_INCONSISTENT_STATE,
            ]:
                excluded.append(file)  # qq info file
                excluded.append(directory / informer.info.stdout_file)  # script stdout
                excluded.append(directory / informer.info.stderr_file)  # script stderr
                excluded.append(
                    (directory / informer.info.job_name).with_suffix(
                        CFG.suffixes.qq_out
                    )
                )  # qq out file

        return set(excluded)

    @staticmethod
    def _delete_files(files: Iterable[Path]) -> None:
        """
        Delete all specified files.

        Args:
            files (Iterable[Path]): The list of files to delete.
        """
        for file in files:
            logger.debug(f"Removing file '{file}'.")
            file.unlink()
