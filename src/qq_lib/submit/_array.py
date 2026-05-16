# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from __future__ import annotations

import getpass
import socket
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import qq_lib
from qq_lib.core.array_spec import ArraySpec
from qq_lib.core.common import get_array_file, get_runtime_files, subset_indices
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.core.logical_paths import logical_resolve
from qq_lib.info.array_informer import ArrayInformer
from qq_lib.properties.info import Info
from qq_lib.properties.job_type import JobType
from qq_lib.properties.states import NaiveState

if TYPE_CHECKING:
    from qq_lib.info import Informer
    from qq_lib.submit import Submitter

logger = get_logger(__name__)


class _ArraySubmitterHelper:
    """
    Resolves array job submission type and computes the array spec.

    This helper validates whether an array job is a loop continuation,
    a respawn of failed tasks, or a new submission, and produces the
    corresponding `ArraySpec`.

    The two validation methods (`continues_array_loop` and
    `respawns_array`) cache the computed spec as a side effect.
    `get_array_spec` returns the cached value when available, or
    determines the correct spec from scratch otherwise.

    Args:
        submitter (Submitter): The submitter providing job metadata.
    """

    def __init__(
        self,
        submitter: Submitter,
    ) -> None:
        self._submitter = submitter
        self._job_type = submitter.get_job_type()
        self._input_dir = submitter.get_input_dir()
        self._array_dirs: list[Path] = submitter.get_array_dirs() or []
        self._loop_info = submitter.get_loop_info()
        self._array_spec: ArraySpec | None = None

    def is_submission_valid(self) -> bool:
        """
        Returns True if the submission of an array job is valid, False otherwise.
        """
        try:
            array_file = get_array_file(self._input_dir)
            array_informer = ArrayInformer.from_file(array_file)
        except QQError:
            # submission is invalid if there are runtime files in the input directory or task directories
            return not (
                get_runtime_files(self._input_dir)
                or any(get_runtime_files(dir) for dir in self._array_dirs)
            )

        # submission is invalid if...
        return not (
            # there are runtime files in the input directory or task directories
            (
                get_runtime_files(self._input_dir)
                or any(get_runtime_files(dir) for dir in self._array_dirs)
            )
            # and the job does not continue a previous loop
            and not self._continues_array_loop(array_informer)
            # and the job does not respawn part of the array
            and not self._respawns_array(array_informer)
        )

    def get_array_spec(self) -> ArraySpec | None:
        """
        Return the array spec for submission.

        Uses the cached spec if a prior call to `continues_array_loop`
        or `respawns_array` already computed it. Otherwise, checks for
        a respawn case first and falls back to a full-range spec.

        Returns:
            ArraySpec | None: The spec to submit, or None for non-array jobs.
        """

        if not self._job_type.is_array():
            return None

        if self._array_spec is not None:
            return self._array_spec

        full_range = ArraySpec([(1, len(self._array_dirs) + 1)])

        try:
            array_file = get_array_file(self._input_dir)
            array_informer = ArrayInformer.from_file(array_file)
            self._array_spec = self._try_respawn(array_informer) or full_range
        except QQError as e:
            logger.debug(f"Could not read qq array file: {e}")
            self._array_spec = full_range

        return self._array_spec

    def _continues_array_loop(self, array_informer: ArrayInformer) -> bool:
        """
        Check whether the submitted array job continues a previous loop.

        Args:
            array_informer (ArrayInformer): The informer for the previous array job.

        Returns:
            bool: True if the job continues a previous loop, False otherwise.
        """
        if not (
            self._loop_array_continues_loop(array_informer)
            or self._continuous_array_continues_loop(array_informer)
        ):
            return False

        self._array_spec = ArraySpec([(1, len(self._array_dirs) + 1)])
        return True

    def _respawns_array(self, array_informer: ArrayInformer) -> bool:
        """
        Check whether the submitted array job is a respawn of a previous job.

        Args:
            array_informer (ArrayInformer): The informer for the previous array job.

        Returns:
            bool: True if the submitted array job is a respawn of a previous job,
                False otherwise.
        """
        respawn_spec = self._try_respawn(array_informer)
        if respawn_spec is not None:
            self._array_spec = respawn_spec
            return True

        return False

    def _loop_array_continues_loop(self, previous: ArrayInformer) -> bool:
        """
        Determine whether the submitted job is a continuation of a a loop array.

        Args:
            previous (ArrayInformer): ArrayInformer associated with the previous array job.

        Returns:
            bool: True if the job is a valid continuation of a previous loop array, False otherwise.
        """

        # we are assuming that the loop information is same for all tasks
        previous_informer = self._get_first_informer(previous)

        return (
            # both the previous job and the current job must be loop arrays
            (previous_loop_info := previous_informer.info.loop_info) is not None
            and (current_loop_info := self._loop_info) is not None
            # previous array must be successfully finished
            and previous.array_info.n_finished_tasks == len(previous.tasks)
            # the cycle of the current array is one more than the cycle of the previous array
            and previous_loop_info.current == current_loop_info.current - 1
            # the same tasks as in the previous cycle must be submitted
            and self._array_dirs_match(previous)
        )

    def _continuous_array_continues_loop(self, previous: ArrayInformer) -> bool:
        """
        Determine whether the submitted job is a continuation of a continuous array.

        Args:
            previous (ArrayInformer): ArrayInformer associated with the previous array job.

        Returns:
            bool: True if the job is a valid continuation of a previous continuous array, False otherwise.
        """
        previous_informer = self._get_first_informer(previous)

        return (
            # both the previous job and the current job must be continuous arrays
            previous_informer.info.job_type == JobType.CONTINUOUS_ARRAY
            and self._job_type == JobType.CONTINUOUS_ARRAY
            # previous array must be successfully finished
            and previous.array_info.n_finished_tasks == len(previous.tasks)
            # the same tasks as in the previous cycle must be submitted
            and self._array_dirs_match(previous)
        )

    def _array_dirs_match(self, previous: ArrayInformer) -> bool:
        """
        Check that current and previous array directories match.

        Args:
            previous (ArrayInformer): ArrayInformer associated with the previous array job.

        Returns:
            bool: True if the resolved directory sets are equal.
        """
        current_dirs = {logical_resolve(dir) for dir in self._array_dirs}
        previous_dirs = set(previous.array_info.task_dirs)

        if current_dirs == previous_dirs:
            logger.debug("Array job submission: current dirs match previous dirs.")
            return True

        logger.debug("Array job submission: current dirs do NOT match previous dirs.")
        return False

    def _try_respawn(self, array_informer: ArrayInformer) -> ArraySpec | None:
        """
        Attempt to build a respawn array spec.

        Args:
            array_informer (ArrayInformer): The informer for the previous array job.

        Returns:
            ArraySpec | None: A spec targeting only the tasks to respawn,
                or None if respawn conditions are not met.
        """
        respawn_task_ids = subset_indices(
            array_informer.array_info.task_dirs, self._array_dirs
        )

        # informers for the tasks to respawn should not exist (info files should already be cleared)
        if not all(
            array_informer.tasks[i] is None
            for i in respawn_task_ids
            # array should not be fully finished (tasks to respawn must have failed or been killed)
            # we do not need to respawn all failed/killed tasks in the array but the number of respawned tasks
            # cannot be higher than the total number of tasks in the array
        ) or array_informer.array_info.n_finished_tasks + len(respawn_task_ids) > len(
            array_informer.tasks
        ):
            return None

        return ArraySpec(respawn_task_ids)

    @staticmethod
    def _get_first_informer(array: ArrayInformer) -> Informer:
        """Return the first existing informer from the array informer, or raise an error if there is no such informer."""
        try:
            return next(x for x in array.tasks if x is not None)
        except StopIteration:
            raise QQError("Previous array is empty.")

    def _make_info_files(self, job_id: str, remote: str | None) -> None:
        if not self._job_type.is_array():
            Info(
                batch_system=self._submitter.get_batch_system(),
                qq_version=qq_lib.__version__,
                username=getpass.getuser(),
                job_id=job_id,
                job_name=self._submitter.get_job_name(),
                script_name=self._submitter.get_script().name,
                queue=self._submitter.get_queue(),
                job_type=self._job_type,
                input_machine=socket.getfqdn(remote or ""),
                input_dir=self._input_dir,
                job_state=NaiveState.QUEUED,
                submission_time=datetime.now(),
                stdout_file=str(
                    Path(self._submitter.get_job_name()).with_suffix(
                        CFG.suffixes.stdout
                    )
                ),
                stderr_file=str(
                    Path(self._submitter.get_job_name()).with_suffix(
                        CFG.suffixes.stderr
                    )
                ),
                resources=self._submitter.get_resources(),
                loop_info=self._loop_info,
                excluded_files=self._submitter.get_exclude(),
                included_files=self._submitter.get_include(),
                depend=self._submitter.get_depend(),
                account=self._submitter.get_account(),
                transfer_mode=self._submitter.get_transfer_mode(),
                server=self._submitter.get_server(),
                interpreter=self._submitter.get_interpreter(),
                resubmit_from=self._submitter.get_resubmit_from(),
            ).to_file(self._submitter.get_info_file())
            return

        if (array_spec := self.get_array_spec()) is None:
            raise QQError("Array spec is undefined for an array job. This is a bug.")

        submission_time = datetime.now()
        for task_number, task_dir in zip(array_spec, self._array_dirs):
            Info(
                batch_system=self._submitter.get_batch_system(),
                qq_version=qq_lib.__version__,
                username=getpass.getuser(),
                job_id=job_id
                + f"[{task_number}]",  # TODO: make this transferable to Slurm
                job_name=self._submitter.get_job_name(),
                script_name=self._submitter.get_script().name,
                queue=self._submitter.get_queue(),
                job_type=self._job_type,
                input_machine=socket.getfqdn(remote or ""),
                input_dir=task_dir,
                job_state=NaiveState.QUEUED,
                submission_time=submission_time,
                stdout_file=str(
                    Path(self._submitter.get_job_name()).with_suffix(
                        CFG.suffixes.stdout
                    )
                ),
                stderr_file=str(
                    Path(self._submitter.get_job_name()).with_suffix(
                        CFG.suffixes.stderr
                    )
                ),
                resources=self._submitter.get_resources(),
                loop_info=self._loop_info,
                excluded_files=self._submitter.get_exclude(),
                included_files=self._submitter.get_include(),
                depend=self._submitter.get_depend(),
                account=self._submitter.get_account(),
                transfer_mode=self._submitter.get_transfer_mode(),
                server=self._submitter.get_server(),
                interpreter=self._submitter.get_interpreter(),
                resubmit_from=self._submitter.get_resubmit_from(),
            ).to_file(self._submitter.get_info_file())
