# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from qq_lib.clear import Clearer
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.logger import get_logger
from qq_lib.core.operator import Operator
from qq_lib.properties.depend import Depend
from qq_lib.properties.loop import LoopInfo
from qq_lib.properties.states import RealState
from qq_lib.submit import Submitter

logger = get_logger(__name__)


class Respawner(Operator):
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
        informer = self.get_informer()
        input_dir = self._info_file.parent

        dependencies = self._handle_dependencies(informer.info.depend)
        if (loop_info := informer.info.loop_info) is not None:
            self._ensure_archive_consistent(loop_info)

        submitter = Submitter(
            batch_system=informer.batch_system,
            queue=informer.info.queue,
            account=informer.info.account,
            script=input_dir / informer.info.script_name,
            job_type=informer.info.job_type,
            resources=informer.info.resources,
            loop_info=informer.info.loop_info,
            exclude=informer.info.excluded_files,
            include=informer.info.included_files,
            depend=dependencies,
            transfer_mode=informer.info.transfer_mode,
            server=informer.info.server,
            interpreter=informer.info.interpreter,
        )

        # clear files from the input directory
        clearer = Clearer(input_dir)
        clearer.clear()

        # respawn the job
        return submitter.submit()

    def _handle_dependencies(self, dependencies: list[Depend]) -> list[Depend]:
        """
        Removes jobs from dependencies that are no longer present in the batch system.

        Without removing these jobs, the respawned job would immediately fail.
        """
        BatchSystem = self._informer.batch_system

        filtered = []
        for depend in dependencies:
            # get jobs that are still present in the batch system
            valid_jobs = [
                job.get_id()
                for job_id in depend.jobs
                if not (job := BatchSystem.get_batch_job(job_id)).is_empty()
            ]
            if valid_jobs:
                filtered.append(Depend(depend.type, valid_jobs))

        logger.debug(f"Filtered dependencies: {filtered}.")
        return filtered

    def _ensure_archive_consistent(self, loop_info: LoopInfo) -> None:
        """
        Ensure that the current loop job cycle matches what we would expect based on the contents of the archive directory.
        """
        if (
            archive_cycle := loop_info.determine_cycle_from_archive()
        ) != loop_info.current:
            raise QQError(
                f"Respawning loop job in cycle '{loop_info.current}' but the loop job should continue from cycle '{archive_cycle}' "
                "based on the contents of the archive directory. Canceling job respawn."
            )
