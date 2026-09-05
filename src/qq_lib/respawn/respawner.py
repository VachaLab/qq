# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from qq_lib.clear import Clearer
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.logger import get_logger
from qq_lib.core.operator import Operator
from qq_lib.info import Informer
from qq_lib.properties.depend import filter_dependencies
from qq_lib.properties.loop import LoopInfo
from qq_lib.properties.states import RealState
from qq_lib.submit import Submitter
from qq_lib.wipe import Wiper

logger = get_logger(__name__)


class Respawner(Operator):
    """
    Respawns a failed or killed job by cleaning up and resubmitting it with the same parameters.

    For loop jobs, the archive directory is checked for consistency before respawning.
    """

    def ensure_suitable(self) -> None:
        """
        Verify that the job is in a state where it can be respawned.

        Raises:
            QQNotSuitableError: If the job is in any other state than failed or killed.
        """
        if self._state not in {RealState.FAILED, RealState.KILLED}:
            raise QQNotSuitableError(
                f"Job cannot be respawned. Job is {str(self._state)}."
            )

    def respawn(self) -> str:
        """
        Respawn the job by cleaning up and submitting a fresh copy.

        Returns:
            str: The job ID of the newly submitted job.

        Raises:
            QQError: If the submitter cannot be built or the job cannot be submitted.
        """
        informer = self.get_informer()
        submitter = self._build_submitter(informer)

        input_dir = self._info_file.parent

        # attempt to remove the working directory
        try:
            wiper = Wiper.from_informer(informer)
            wiper.ensure_suitable()
            wiper.wipe()
        except QQNotSuitableError:
            pass
        except QQError as e:
            logger.warning(f"Failed to remove working directory: {e}")

        # clear files from the input directory
        clearer = Clearer([input_dir])
        clearer.clear()

        # submit a new job
        return submitter.submit()

    def _build_submitter(self, informer: Informer) -> Submitter:
        """
        Construct a Submitter configured for respawning.

        All original job parameters are preserved. Dependencies are filtered
        to only include jobs still present in the batch system. For loop jobs,
        the archive directory is checked for consistency before proceeding.

        Args:
            informer (Informer): The informer instance holding job metadata.

        Returns:
            Submitter: A configured Submitter ready to submit the job.

        Raises:
            QQError: If the loop job archive is inconsistent with the current cycle.
        """
        if (loop_info := informer.info.loop_info) is not None:
            self._ensure_archive_consistent(loop_info)

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

    @staticmethod
    def _ensure_archive_consistent(loop_info: LoopInfo) -> None:
        """
        Verify that the current loop cycle matches the archive contents.

        Args:
            loop_info (LoopInfo): Loop job metadata.

        Raises:
            QQError: If the cycle determined from the archive does not match
                the current cycle in the loop metadata.
        """
        if (
            archive_cycle := loop_info.determine_cycle_from_archive()
        ) != loop_info.current:
            raise QQError(
                f"Respawning loop job in cycle '{loop_info.current}' but the loop job should continue from cycle '{archive_cycle}' "
                "based on the contents of the archive directory. Canceling job respawn."
            )
