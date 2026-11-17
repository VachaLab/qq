# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import os
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from qq_lib.core.common import get_info_files, get_runtime_files
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.info.informer import Informer
from qq_lib.properties.states import RealState

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = get_logger(__name__)


@dataclass
class _ClearResult:
    """Accumulates results from clearing a single directory."""

    detected: int
    deleted: int = 0
    excluded: int = 0

    # files that could not be removed, with the reason
    failed_files: list[tuple[Path, str]] = field(default_factory=list)

    # directories that could not be searched, with the reason
    failed_directories: list[tuple[Path, str]] = field(default_factory=list)


class Clearer:
    """
    Handles detection and removal of qq runtime files from a directory.
    """

    def __init__(self, directories: list[Path], recursive: bool = False):
        """
        Initialize a Clearer for one or more directories.

        Args:
            directories: The directories to clear qq runtime files from.
            recursive: If True, also clear subdirectories, but do not descend
                into subdirectories of directories containing qq runtime files.

        """
        self._directories = directories
        self._recursive = recursive

    def clear(self, force: bool = False) -> bool:
        """
        Remove all qq runtime files from all directories that are safe to be removed.

        Directories are cleared in parallel. Only qq files that do **not**
        correspond to an active or successfully finished job will be removed,
        unless `force` is set to True. A combined summary is logged at the end.

        Files that cannot be removed and directories that cannot be searched
        do not stop the clearing. They are logged together after the summary.

        Args:
            force: If True, remove all qq runtime files, even if unsafe.

        Returns:
            True if all qq runtime files that were to be removed were removed,
            False if some files could not be removed or some directories could not be searched.
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
            futures = [
                executor.submit(clear_single, directory)
                for directory in self._get_directories()
            ]

        # unexpected errors raised in the worker threads are only available through the futures
        for future in futures:
            future.result()

        total_detected = sum(r.detected for r in results)
        total_deleted = sum(r.deleted for r in results)
        total_excluded = sum(r.excluded for r in results)
        failed_files = sorted(f for r in results for f in r.failed_files)
        failed_directories = sorted(d for r in results for d in r.failed_directories)

        if (
            total_deleted == 0
            and total_excluded == 0
            and not failed_files
            and not failed_directories
        ):
            logger.info("Nothing to clear.")
            return True

        if total_deleted > 0:
            logger.info(
                f"Removed {total_deleted} qq file{'s' if total_deleted > 1 else ''}."
            )

        if total_excluded > 0 and total_detected > 0:
            actually_excluded = min(total_excluded, total_detected)

            logger.info(
                f"{actually_excluded} qq file{'s' if actually_excluded > 1 else ''} could not be safely cleared. "
                f"Rerun as '{CFG.binary_name} clear --force' to clear {'them' if actually_excluded > 1 else 'it'} forcibly."
            )

        _log_failures(failed_files, failed_directories)
        return not failed_files and not failed_directories

    def _get_directories(self) -> list[Path]:
        """
        Return the directories to clear.

        Without `recursive`, returns the given directories. With `recursive`,
        walks each given directory top-down and returns every directory that
        contains qq runtime files, without descending into the subdirectories
        of such directories. Symlinked directories are not followed.

        Each directory is returned at most once, even if the given directories
        overlap, so no directory is cleared by two threads at once.

        Returns:
            The directories to clear.
        """
        seen: set[Path] = set()
        result: list[Path] = []

        def mark_seen(directory: Path) -> bool:
            """Return True if the directory has not been seen before."""
            key = directory.resolve()
            if key in seen:
                return False
            seen.add(key)
            return True

        def on_error(error: OSError) -> None:
            logger.warning(f"Could not search '{error.filename}': {error.strerror}")

        for root in self._directories:
            if not self._recursive:
                if mark_seen(root):
                    result.append(root)
                continue

            for dirpath, dirnames, _ in os.walk(
                root,
                onerror=on_error,
                # do not follow links to avoid getting stuck in an infinite loop
                followlinks=False,
            ):
                directory = Path(dirpath)
                if not mark_seen(directory):
                    dirnames.clear()
                    continue

                if Clearer._collect_runtime_files(directory):
                    result.append(directory)
                    dirnames.clear()
                else:
                    dirnames.sort()

        return result

    @staticmethod
    def _clear_directory(directory: Path, force: bool) -> _ClearResult:
        """
        Clear qq runtime files from a single directory and return the result.

        Args:
            directory: The directory to clear.
            force: If True, remove all qq runtime files, even if unsafe.

        Returns:
            The numbers of detected, deleted and excluded files, the files that
            could not be deleted, and the directory if it could not be searched.
        """
        try:
            files = Clearer._collect_runtime_files(directory)
            logger.debug(f"All qq runtime files in '{directory}': {files}.")
            if not files:
                return _ClearResult(detected=0)

            excluded = (
                Clearer._collect_excluded_files(directory) if not force else set()
            )
            logger.debug(f"Files excluded from clearing in '{directory}': {excluded}.")
        except OSError as e:
            return _ClearResult(
                detected=0, failed_directories=[(directory, _os_error_reason(e))]
            )

        to_delete = files - excluded
        logger.debug(f"Files to delete in '{directory}': {to_delete}.")

        failed = Clearer._delete_files(to_delete) if to_delete else []

        return _ClearResult(
            detected=len(files),
            deleted=len(to_delete) - len(failed),
            excluded=len(excluded),
            failed_files=failed,
        )

    @staticmethod
    def _collect_runtime_files(directory: Path) -> set[Path]:
        """
        Collect all qq runtime files in the directory.

        Returns:
            Paths to all files matching qq-specific suffixes.
        """
        return set(get_runtime_files(directory))

    @staticmethod
    def _collect_excluded_files(directory: Path) -> set[Path]:
        """
        Collect qq runtime files that should **not** be deleted.

        Runtime files corresponding to active or successfully finished jobs are included.

        Returns:
            Paths to qq runtime files that should not be deleted.
        """
        excluded = []

        # iterate through info files
        for file in get_info_files(directory):
            try:
                informer = Informer.from_file(file)
                state = informer.get_real_state()
                logger.debug(f"Job state: {state!s}.")
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
    def _delete_files(files: Iterable[Path]) -> list[tuple[Path, str]]:
        """
        Delete all specified files.

        A file that cannot be deleted does not stop the deletion of the other files.
        Files that no longer exist are considered deleted.

        Args:
            files: The list of files to delete.

        Returns:
            The files that could not be deleted, with the reason.
        """
        failed: list[tuple[Path, str]] = []
        for file in files:
            logger.debug(f"Removing file '{file}'.")
            try:
                file.unlink(missing_ok=True)
            except OSError as e:
                logger.debug(f"Could not remove file '{file}': {e}.")
                failed.append((file, _os_error_reason(e)))

        return failed


def _os_error_reason(error: OSError) -> str:
    """
    Return a short, user-facing reason of an OS error.

    Args:
        error: The error to describe.

    Returns:
        The error's description (e.g., 'Permission denied').
    """
    return error.strerror or str(error)


def _log_failures(
    failed_files: list[tuple[Path, str]], failed_directories: list[tuple[Path, str]]
) -> None:
    """
    Log all files that could not be removed and all directories that could not be searched.

    Args:
        failed_files: Files that could not be removed, with the reason.
        failed_directories: Directories that could not be searched, with the reason.
    """
    if failed_files:
        n = len(failed_files)
        lines = "\n".join(f"  {path}: {reason}" for path, reason in failed_files)
        logger.error(f"Could not remove {n} qq file{'s' if n > 1 else ''}:\n{lines}")

    if failed_directories:
        n = len(failed_directories)
        lines = "\n".join(f"  {path}: {reason}" for path, reason in failed_directories)
        logger.error(
            f"Could not search {n} director{'ies' if n > 1 else 'y'}:\n{lines}"
        )
