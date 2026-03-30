# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from time import sleep

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.logger import get_logger
from qq_lib.core.navigator import Navigator

logger = get_logger(__name__)


class Goer(Navigator):
    """
    Handles opening a new shell in the job's working directory on the job's main execution node.
    """

    def ensure_suitable(self) -> None:
        """
        Verify that the job is in a state where its working directory can be visited.

        Raises:
            QQNotSuitableError: If the working directory is not expected to exist.
        """
        if self._is_synchronized() and not self._work_dir_is_input_dir():
            raise QQNotSuitableError(
                "Job has been completed and was synchronized: working directory no longer exists."
            )

        if self._is_killed() and not self.has_destination():
            raise QQNotSuitableError(
                "Job has been killed and no working directory has been created."
            )

    def go(self) -> None:
        """
        Open a shell in the job's working directory on the main execution node (if the node is available).

        Raises:
            QQError: If the working directory or main node is not set and navigation
                    cannot proceed.

        Notes:
            - This method may block while waiting for a queued job to start.
        """
        if self._is_in_work_dir():
            logger.info("You are already in the working directory.")
            return

        if self._is_killed() and not self._work_dir_is_input_dir():
            logger.warning(
                "Job has been killed: working directory may no longer exist."
            )

        elif (
            self._is_failed() or self._is_finished()
        ) and not self._work_dir_is_input_dir():
            logger.warning(
                "Job has been completed: working directory may no longer exist."
            )

        elif self._is_unknown_inconsistent():
            logger.warning("Job is in an unknown, unrecognized, or inconsistent state.")

        elif self._is_queued():
            logger.warning(
                f"Job is {str(self._state)}: cannot visit the working directory. Will retry every {CFG.goer.wait_time} seconds."
            )

            # keep retrying until the job stops being queued
            self._wait_queued()
            if self._is_in_work_dir():
                logger.info("You are already in the working directory.")
                return

        if not self.has_destination():
            raise QQError(
                "Host ('main_node') or working directory ('work_dir') are not defined."
            )

        # hint for type checker
        # work_dir and main_node must be set - we check that in self.hasDestination
        assert self._work_dir and self._main_node
        logger.info(f"Navigating to '{str(self._work_dir)}' on '{self._main_node}'.")
        self._batch_system.navigate_to_destination(self._main_node, self._work_dir)

    def _wait_queued(self):
        """
        Wait until the job is no longer in queued/booting/waiting state.

        Raises:
            QQNotSuitableError: If at any point the job is found to be finished
                                or killed without a working directory.

        Note:
            This is a blocking method and will continue looping until the job
            leaves the queued/booting/waiting state or an exception is raised.
        """
        while self._is_queued():
            sleep(CFG.goer.wait_time)
            self.update()
            self.ensure_suitable()
