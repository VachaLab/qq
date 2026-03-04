# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.logger import get_logger
from qq_lib.core.navigator import Navigator

logger = get_logger(__name__)


class Wiper(Navigator):
    """
    Class to manage deleting working directory of a job.
    """

    def ensureSuitable(self) -> None:
        """
        Verify that the job is in a state where its working directory can be deleted.

        Raises:
            QQNotSuitableError: If the working directory is not expected to exist
                or if the working directory is the input directory.
        """
        if self._workDirIsInputDir():
            raise QQNotSuitableError(
                "Working directory of the job is the input directory of the job. Cannot delete the input directory."
            )

        if self._isQueued():
            raise QQNotSuitableError(
                f"Job is {str(self._informer.getRealState()).lower()} and does not have a working directory yet."
            )

        if self._isRunning() or self._isSuspended():
            raise QQNotSuitableError(
                f"Job is {str(self._informer.getRealState()).lower()}. It is not safe to delete the working directory."
            )

        if self._isSynchronized():
            raise QQNotSuitableError(
                "Job has been completed and was synchronized: working directory no longer exists."
            )

        if not self.hasDestination():
            raise QQNotSuitableError("Job does not have a working directory.")

    def wipe(self) -> str:
        """
        Delete the working directory on the computing node.

        Returns:
            str: The identifier of the job which working directory was deleted.

        Raises:
            QQError: If the working directory of the job does not exist or cannot be deleted.
        """
        if not self.hasDestination():
            raise QQError(
                "Host ('main_node') or working directory ('work_dir') are not defined."
            )

        # hint for type checker
        # work_dir and main_node must be set - we check that in self.hasDestination
        assert self._work_dir and self._main_node

        # we cannot delete the input directory even if the `--force` flag is used
        if self._workDirIsInputDir():
            raise QQError(
                "Working directory of the job is the input directory of the job. Cannot delete the input directory."
            )

        logger.info(
            f"Deleting working directory '{str(self._work_dir)}' on '{self._main_node}'."
        )
        self._batch_system.deleteRemoteDir(self._main_node, self._work_dir)

        return self._informer.info.job_id
