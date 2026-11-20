# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from typing import TYPE_CHECKING

from qq_lib.archive.archiver import Archiver
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.logger import get_logger
from qq_lib.core.logical_paths import logical_resolve
from qq_lib.core.navigator import Navigator
from qq_lib.properties.states import RealState

if TYPE_CHECKING:
    from pathlib import Path

logger = get_logger(__name__)


class Syncer(Navigator):
    """
    Handle synchronization of job files between a remote working directory
    (on a compute node) and the local input directory.
    """

    def ensure_suitable(self, archive: bool = False):
        """
        Verify that the job is in a state where files can be fetched from its working directory.

        Args:
            archive: Whether the files matching the archive format should be archived.
                This is only possible for loop jobs that failed or were killed.

        Raises:
            QQNotSuitableError: If the working directory is not expected to exist,
                if the working directory is the input directory, or if the files
                should be archived and the job is not a failed or killed loop job.
        """
        if self._work_dir_is_input_dir():
            raise QQNotSuitableError(
                "Working directory of the job is the input directory of the job: implicitly synchronized"
            )

        if self._is_synchronized():
            raise QQNotSuitableError(
                "Job has been completed and was synchronized: working directory no longer exists"
            )

        # killed jobs may not have working directory
        if self._is_killed() and not self.has_destination():
            raise QQNotSuitableError(
                "Job has been killed and no working directory is available"
            )

        # queued jobs do not have working directory
        if self._is_queued():
            raise QQNotSuitableError("Job is queued or booting: nothing to sync")

        if archive:
            if self._informer.info.loop_info is None:
                raise QQNotSuitableError("Job is not a loop job: cannot archive files")

            # a running job could still be writing into the files or into the archive
            if self._state not in {RealState.FAILED, RealState.KILLED}:
                raise QQNotSuitableError(
                    f"Files can only be archived for failed or killed jobs. Job is {self._state!s}"
                )

    def sync(self, files: list[str] | None = None, archive: bool = False) -> None:
        """
        Synchronize files from the remote working directory to the local input directory.

        Args:
            files: Optional list of specific filenames to fetch.
                If omitted, all files are synchronized except those excluded by the batch system.
            archive: Copy the files matching the archive format to the archive
                instead of the input directory. Only for loop jobs.

        Behavior:
            - If `files` is provided, only those specific files are copied.
            - If omitted, the entire working directory is synchronized.
            - If `archive` is set, the files matching the archive format (among `files`,
              if provided) are copied to the archive and the rest to the input directory.
              The files are not removed from the working directory.

        Raises:
            QQError: If the job's destination (host or working directory) cannot be determined
                or if the files cannot be copied.
        """
        if not self.has_destination():
            raise QQError(
                "Host ('main_node') or working directory ('work_dir') are not defined"
            )

        # hint for type checker
        # work_dir and main_node must be set - we check that in self.has_destination
        assert self._work_dir and self._main_node

        archived = self._copy_to_archive(files) if archive else []
        if archive and files:
            # only fetch the selected files which were not archived
            archived_names = {logical_resolve(file) for file in archived}
            files = [
                file
                for file in files
                if logical_resolve(self._work_dir / file) not in archived_names
            ]
            if not files:
                return

        if files:
            logger.info(
                f"Fetching file{'s' if len(files) > 1 else ''} '{' '.join(files)}' from job's working directory to input directory."
            )
            self._batch_system.sync_selected(
                self._work_dir,
                self._informer.info.input_dir,
                self._main_node,
                None,
                [self._work_dir / x for x in files],
            )
        else:
            logger.info(
                f"Fetching all {'remaining ' if archived else ''}files from job's working directory to input directory."
            )
            self._batch_system.sync_with_exclusions(
                self._work_dir,
                self._informer.info.input_dir,
                self._main_node,
                None,
                # files copied to the archive are not fetched to the input directory
                archived,
            )

    def _copy_to_archive(self, files: list[str] | None) -> list[Path]:
        """
        Copy the files matching the archive format from the working directory to the archive.

        Args:
            files: Optional list of specific filenames. If provided, only these files
                are considered for archiving.

        Returns:
            Absolute paths of the archived files in the working directory.

        Raises:
            QQError: If the job is not a loop job or if the files cannot be copied.
        """
        assert self._work_dir and self._main_node

        info = self._informer.info
        if (loop_info := info.loop_info) is None:
            raise QQError("Job is not a loop job: cannot archive files")

        archiver = Archiver(
            archive=loop_info.archive,
            archive_format=loop_info.archive_format,
            input_machine=info.input_machine,
            input_dir=info.input_dir,
            batch_system=self._batch_system,
            included_files=info.included_files,
            excluded_files=info.excluded_files,
            ignored_files=info.ignored_files,
        )

        archived = archiver.copy_to_archive(
            self._work_dir,
            self._main_node,
            None,
            [self._work_dir / file for file in files] if files else None,
        )

        if archived:
            logger.info(
                f"Copied {len(archived)} file{'s' if len(archived) > 1 else ''} from job's working directory to archive '{loop_info.archive}'."
            )
        else:
            logger.info("No files to archive in job's working directory.")

        return archived
