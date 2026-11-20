# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

import copy
from typing import TYPE_CHECKING

from qq_lib.archive.archiver import Archiver
from qq_lib.clear import Clearer
from qq_lib.core.common import construct_loop_job_name
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.logger import get_logger
from qq_lib.core.operator import Operator
from qq_lib.properties.depend import filter_dependencies
from qq_lib.properties.states import RealState
from qq_lib.submit import Submitter
from qq_lib.wipe import Wiper

if TYPE_CHECKING:
    from qq_lib.info import Informer

logger = get_logger(__name__)


class Respawner(Operator):
    """
    Respawns a failed or killed job by cleaning up and resubmitting it with the same parameters.

    For loop jobs, the archive directory is checked for consistency before respawning.
    If the archive already contains files for the next cycle (e.g., because the cycle
    finished but the next cycle could not be submitted), the caller must choose
    whether to continue with the next cycle or to respawn the failed cycle.
    """

    def ensure_suitable(self) -> None:
        """
        Verify that the job is in a state where it can be respawned.

        Raises:
            QQNotSuitableError: If the job is in any other state than failed or killed.
        """
        if self._state not in {RealState.FAILED, RealState.KILLED}:
            raise QQNotSuitableError(f"Job cannot be respawned. Job is {self._state!s}")

    def get_next_cycle_in_archive(self) -> int | None:
        """
        Check whether the archive of a loop job already contains files for the next cycle.

        Returns:
            The next cycle if the archive already contains files for it,
            or `None` if the job is not a loop job or the archive corresponds
            to the current cycle.

        Raises:
            QQError: If the archive corresponds to any other cycle.
        """
        if (loop_info := self._informer.info.loop_info) is None:
            return None

        archive_cycle = loop_info.determine_cycle_from_archive()
        if archive_cycle == loop_info.current:
            return None
        if archive_cycle == loop_info.current + 1:
            return archive_cycle

        raise QQError(
            f"Respawning loop job in cycle '{loop_info.current}' but the loop job should continue from cycle '{archive_cycle}' "
            "based on the contents of the archive directory. Canceling job respawn"
        )

    def can_continue(self) -> bool:
        """
        Check whether the job can be respawned in the next cycle instead of the failed one.

        Returns:
            True if the archive already contains files for the next cycle
            and the next cycle is not beyond the last cycle of the loop job.

        Raises:
            QQError: If the archive is inconsistent with the current cycle.
        """
        next_cycle = self.get_next_cycle_in_archive()
        loop_info = self._informer.info.loop_info
        return (
            next_cycle is not None
            and loop_info is not None
            and next_cycle <= loop_info.end
        )

    def respawn(self, force_continue: bool = False, force_rerun: bool = False) -> str:
        """
        Respawn the job by cleaning up and submitting a fresh copy.

        If the archive of a loop job already contains files for the next cycle,
        one of `force_continue` and `force_rerun` must be set. Otherwise,
        both options are ignored.

        Args:
            force_continue: Continue with the next cycle if the archive
                already contains files for it. The qq runtime files of the failed cycle
                are archived and its working directory is kept if it still exists.
            force_rerun: Respawn the failed cycle even if the archive
                already contains files for the next cycle.

        Returns:
            The job ID of the newly submitted job.

        Raises:
            QQError: If both options are set, if the archive contains files for the next
                cycle and neither option is set, if the job cannot continue with
                the next cycle, if the submitter cannot be built, if the qq runtime files
                cannot be archived or removed from the input directory,
                or if the job cannot be submitted.
        """
        if force_continue and force_rerun:
            raise QQError(
                "Cannot both continue with the next cycle and respawn the failed cycle"
            )

        informer = self.get_informer()
        next_cycle = self.get_next_cycle_in_archive()

        continue_loop = False
        if next_cycle is not None:
            continue_loop = self._choose_continue(
                next_cycle, force_continue, force_rerun
            )

        submitter = self._build_submitter(
            informer, next_cycle if continue_loop else None
        )

        input_dir = self._info_file.parent

        if continue_loop:
            # the working directory may contain files that were not copied back
            self._warn_if_work_dir_exists(informer)
            # keep the qq runtime files of the failed cycle like a normal cycle would
            self._archive_runtime_files(informer)
        else:
            self._remove_work_dir(informer)

        # clear files from the input directory
        # (files that could not be removed are logged by the clearer)
        clearer = Clearer([input_dir])
        if not clearer.clear():
            raise QQError(
                f"Could not remove all qq runtime files from '{input_dir}'. Not respawning the job"
            )

        # submit a new job
        return submitter.submit()

    def _choose_continue(
        self, next_cycle: int, force_continue: bool, force_rerun: bool
    ) -> bool:
        """
        Decide whether a loop job whose archive contains files for the next cycle
        should continue with the next cycle.

        Args:
            next_cycle: The next cycle found in the archive.
            force_continue: Continue with the next cycle.
            force_rerun: Respawn the failed cycle.

        Returns:
            True if the job should continue with the next cycle,
            False if the failed cycle should be respawned.

        Raises:
            QQError: If neither option is set or if the next cycle is beyond
                the last cycle of the loop job.
        """
        loop_info = self._informer.info.loop_info
        assert loop_info is not None

        if force_rerun:
            return False

        if not force_continue:
            raise QQError(
                f"Respawning loop job in cycle '{loop_info.current}' but the archive already contains files for cycle '{next_cycle}'. "
                "Choose whether to continue with the next cycle or to respawn the failed one"
            )

        if next_cycle > loop_info.end:
            raise QQError(
                f"Cannot continue with cycle '{next_cycle}'. The last cycle of the loop job is '{loop_info.end}'"
            )

        return True

    def _remove_work_dir(self, informer: Informer) -> None:
        """
        Attempt to remove the working directory of the job.

        Failures are logged as warnings.

        Args:
            informer: The informer instance holding job metadata.
        """
        try:
            wiper = Wiper.from_informer(informer)
            wiper.ensure_suitable()
            wiper.wipe()
        except QQNotSuitableError:
            pass
        except QQError as e:
            logger.warning(f"Failed to remove working directory: {e}")

    def _warn_if_work_dir_exists(self, informer: Informer) -> None:
        """
        Warn the user if the working directory of the job still exists.

        The working directory is not removed, since it may contain files
        that were not copied to the input directory.

        Args:
            informer: The informer instance holding job metadata.
        """
        try:
            Wiper.from_informer(informer).ensure_suitable()
        except QQError:
            # the working directory is expected to not exist
            return

        if (destination := informer.get_destination()) is None:
            return
        main_node, work_dir = destination

        try:
            informer.batch_system.list_remote_dir(main_node, work_dir)
        except QQError:
            # the working directory does not exist or cannot be reached
            return

        logger.warning(
            f"Working directory '{work_dir}' on '{main_node}' still exists and was not removed. "
            "It may contain files that were not copied to the archive or directory. Check it and remove it manually."
        )

    def _archive_runtime_files(self, informer: Informer) -> None:
        """
        Move the qq runtime files of the failed cycle into the archive.

        Args:
            informer: The informer instance holding job metadata.

        Raises:
            QQError: If the files cannot be archived.
        """
        loop_info = informer.info.loop_info
        assert loop_info is not None

        archiver = Archiver(
            archive=loop_info.archive,
            archive_format=loop_info.archive_format,
            input_machine=informer.info.input_machine,
            input_dir=informer.info.input_dir,
            batch_system=informer.batch_system,
            included_files=informer.info.included_files,
            excluded_files=informer.info.excluded_files,
            ignored_files=informer.info.ignored_files,
        )

        logger.info(f"Archiving qq runtime files from cycle '{loop_info.current}'.")
        archiver.archive_runtime_files(
            # we need to escape the '+' character
            construct_loop_job_name(
                informer.info.script_name, loop_info.current
            ).replace("+", "\\+"),
            loop_info.current,
        )

    def _build_submitter(
        self, informer: Informer, cycle: int | None = None
    ) -> Submitter:
        """
        Construct a Submitter configured for respawning.

        All original job parameters are preserved. Dependencies are filtered
        to only include jobs still present in the batch system.

        Args:
            informer: The informer instance holding job metadata.
            cycle: The cycle of the loop job to submit.
                If `None`, the current cycle of the job is used.

        Returns:
            A configured Submitter ready to submit the job.
        """
        loop_info = informer.info.loop_info
        if loop_info is not None and cycle is not None:
            # do not modify the loop info of the failed job
            loop_info = copy.copy(loop_info)
            loop_info.current = cycle

        return Submitter(
            batch_system=informer.batch_system,
            queue=informer.info.queue,
            account=informer.info.account,
            script=self._info_file.parent / informer.info.script_name,
            job_type=informer.info.job_type,
            resources=informer.info.resources,
            loop_info=loop_info,
            exclude=[str(x) for x in informer.info.excluded_files],
            include=[str(x) for x in informer.info.included_files],
            ignore=[str(x) for x in informer.info.ignored_files],
            # we need to remove dependencies that are no longer present in the batch system
            depend=filter_dependencies(informer.batch_system, informer.info.depend),
            transfer_mode=informer.info.transfer_mode,
            server=informer.info.server,
            interpreter=informer.info.interpreter,
            resubmit_from=informer.info.resubmit_from,
        )
