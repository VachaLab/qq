# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from pathlib import Path

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.core.operator import Operator
from qq_lib.core.retryer import Retryer
from qq_lib.info import Informer
from qq_lib.properties.depend import Depend, DependType
from qq_lib.properties.resubmit_host import ResubmitHost
from qq_lib.submit import Submitter

logger = get_logger(__name__, show_time=True)


class Resubmitter(Operator):
    """
    Handles resubmission of loop and continuous jobs.
    """

    def resubmit(self) -> str:
        """
        Resubmit the job to the next cycle.

        Returns:
            str: The job ID of the newly submitted job.

        Raises:
            QQError: If the main node is not defined or if resubmission fails
                on all candidate hosts.
        """
        informer = self.get_informer()
        input_dir = self._info_file.parent

        # we set the current cycle manually instead of determining it from the archive
        # this means that the job will always be resubmitted to the "correct" (expected) cycle,
        # but may fail after starting if the archive does not contain files for this cycle
        self._advance_loop_cycle(informer)

        submitter = self._build_submitter(informer, input_dir)
        hosts = (
            informer.info.resubmit_from
            # fall back to batch system default
            # this is only needed to accomodate transition from loop jobs submitted using previous versions of qq
            or informer.batch_system.get_default_resubmit_hosts()
        )

        return self._try_resubmit(submitter, informer, hosts)

    @staticmethod
    def _advance_loop_cycle(informer: Informer) -> None:
        """
        Advance the loop cycle counter if the job is a loop job.

        Args:
            informer: The informer instance holding job metadata.
        """
        if informer.info.loop_info:
            informer.info.loop_info.current += 1

    @staticmethod
    def _build_submitter(informer: Informer, input_dir: Path) -> Submitter:
        """
        Construct a Submitter configured for resubmission.

        All original job parameters are preserved, except dependencies are replaced
        with a single dependency on the current job. This is because the previous
        cycle has already run, so its original dependencies must have been satisfied.

        Args:
            informer (Informer): The informer instance holding job metadata.
            input_dir (Path): Path to the directory containing the job's input files.

        Returns:
            Submitter: A configured submitter ready to submit the job.
        """
        return Submitter(
            batch_system=informer.batch_system,
            queue=informer.info.queue,
            account=informer.info.account,
            script=input_dir / informer.info.script_name,
            job_type=informer.info.job_type,
            resources=informer.info.resources,
            loop_info=informer.info.loop_info,
            exclude=informer.info.excluded_files,
            include=informer.info.included_files,
            depend=[Depend(type=DependType.AFTER_SUCCESS, jobs=[informer.info.job_id])],
            transfer_mode=informer.info.transfer_mode,
            server=informer.info.server,
            interpreter=informer.info.interpreter,
            resubmit_from=informer.info.resubmit_from
            # fall back to batch system default
            # this is only needed to accommodate transition from loop jobs submitted using previous versions of qq
            or informer.batch_system.get_default_resubmit_hosts(),
        )

    @staticmethod
    def _try_resubmit(
        submitter: Submitter,
        informer: Informer,
        hosts: list[ResubmitHost],
    ) -> str:
        """
        Attempt resubmission on each candidate host in order.

        Args:
            submitter (Submitter): The configured submitter to use for job submission.
            informer (Informer): The informer instance holding job metadata.
            hosts (list[ResubmitHost]): Ordered list of candidate resubmission hosts to try.

        Returns:
            str: The job ID of the newly submitted job.

        Raises:
            QQError: If the main node is not defined in the job metadata.
            QQError: If the list of resubmission hosts is empty.
            QQError: If resubmission fails on all candidate hosts.
        """
        # get the main node for host resolution
        # since the job should be running, the main node should be defined
        main_node = informer.info.main_node
        if not main_node:
            raise QQError(
                "Job cannot be resubmitted. The 'main_node' of the job is not defined."
            )

        if not hosts:
            raise QQError(
                "Job cannot be resubmitted. No resubmission hosts defined. This is a bug."
            )

        for host in hosts:
            hostname = host.resolve(informer.info.input_machine, main_node)
            logger.info(f"Resubmitting from host '{hostname}'.")
            try:
                return Retryer(
                    submitter.submit,
                    remote=hostname,
                    max_tries=CFG.resubmitter.retry_tries,
                    wait_seconds=CFG.resubmitter.retry_wait,
                ).run()
            except Exception as e:
                logger.warning(f"Failed resubmission from host '{hostname}': {e}")

        raise QQError("Could not resubmit the job.")
