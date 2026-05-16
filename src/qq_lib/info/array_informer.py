# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from typing import Self

from qq_lib.batch.interface import BatchInterface, BatchJobInterface
from qq_lib.core.common import construct_info_file_path
from qq_lib.core.error import QQError, QQJobMismatchError
from qq_lib.core.logger import get_logger
from qq_lib.info import Informer
from qq_lib.properties.array_info import ArrayInfo
from qq_lib.properties.states import RealState

logger = get_logger(__name__)


class ArrayInformer:
    """
    Provides an interface to access and manipulate array job information.
    """

    def __init__(self, array_info: ArrayInfo):
        self.array_info = array_info
        self._tasks = self._get_informers_for_tasks(array_info)

        # track whether batch info has been loaded for all tasks in bulk
        # this is only set to True after `load_batch_info` is called
        # this variable is not set correctly if you load batch info for tasks individually
        self._batch_info_loaded = False

    @property
    def batch_system(self) -> type[BatchInterface]:
        """
        Return the batch system class used for this qq array.

        Returns:
            type[BatchInterface]: The batch system class used for this qq array.
        """
        return self.array_info.batch_system

    @classmethod
    def from_file(cls, file: Path, host: str | None = None) -> Self:
        """
        Create an ArrayInformer by loading job information from a file.

        If 'host' is provided, the file is read from the remote host; otherwise, it is read locally.

        This method does NOT set `_batch_info` for the individual Informers.

        Args:
            file (Path): Path to a YAML file containing array job information.
            host (str | None): Optional remote host from which to read the file.

        Returns:
            ArrayInformer: An instance initialized with the loaded ArrayInfo.

        Raises:
            QQError: If the file cannot be read, reached, or parsed correctly.
        """
        return cls(ArrayInfo.from_file(file, host))

    @classmethod
    def from_job_id(cls, job_id: str) -> Self:
        """
        Create an ArrayInformer from an array job ID.

        Raises an exception if the job is not a qq array job.

        Even in case the qq array is composed of multiple array jobs,
        the resulting ArrayInformer will contain information about all tasks.

        This method does NOT set `_batch_info` for the individual Informers.

        Args:
            job_id (str): The job ID of the array job.

        Returns:
            ArrayInformer: An instance initialized with the loaded ArrayInfo.

        Raises:
            QQError: If the job does not exist or is not a valid qq array job.
            QQJobMismatchError: If the qq array file does not contain the job's ID.
        """
        BatchSystem = BatchInterface.from_env_var_or_guess()
        batch_job: BatchJobInterface = BatchSystem.get_batch_job(job_id)

        if batch_job.is_empty():
            raise QQError(f"Job '{job_id}' does not exist.")

        return cls.from_batch_job(batch_job)

    @classmethod
    def from_batch_job(cls, batch_job: BatchJobInterface) -> Self:
        """
        Load an `ArrayInformer` from batch job information.

        Raises an exception if the job is not a qq array job.

        Even in case the qq array is composed of multiple array jobs,
        the resulting ArrayInformer will contain information about all tasks.

        This method does NOT set `_batch_info` for the individual Informers.

        Args:
            batch_job (BatchJobInterface): The job info provided by the batch system.

        Returns:
            ArrayInformer: The loaded array informer.

        Raises:
            QQError: If the job is not a valid qq array job.
            QQJobMismatchError: If the qq array file does not contain the job's ID.
        """
        if not batch_job.is_array_job():
            raise QQError(f"Job '{batch_job.get_id()}' is not an array job.")

        # info file for an array job is the qq array file
        if not (path := batch_job.get_info_file()):
            raise QQError(f"Job '{batch_job.get_id()}' is not a valid qq job.")

        array_informer = cls.from_file(path)

        if not array_informer.matches_job(batch_job.get_id()):
            raise QQJobMismatchError(
                f"Array file for job '{batch_job.get_id()}' does not exist or is not reachable."
            )

        return array_informer

    def matches_job(self, job_id: str) -> bool:
        """
        Determine whether this array informer corresponds to the specified job ID.

        Args:
            job_id (str): The job ID to compare against.

        Returns:
            bool: True if this qq array contains an array job with the specified ID.
        """
        return any(
            id.split(".", 1)[0] == job_id.split(".", 1)[0]
            for id in self.array_info.job_ids
        )

    def load_batch_info(self) -> None:
        """
        Load the batch job information from the batch system for all tasks
        if it has not been already loaded in bulk before.

        This is performed in bulk by quering the batch system N times
        where N is the number of job arrays forming the qq array.
        Consequently, this is much faster than loading batch info for each task individually.
        """
        if self._batch_info_loaded:
            logger.debug("Batch info already loaded for all tasks. Skipping.")
            return

        batch_tasks = self._get_batch_tasks()
        numbered_tasks = self._number_batch_tasks(batch_tasks)
        self._match_batch_tasks_to_informers(numbered_tasks)

        # sanity check
        if len(numbered_tasks) > len(self._tasks):
            # some task directories have been removed during the array execution
            # this is serious but not critical since the batch information is matched based on task numbers
            # from the informers which should still be accurate
            logger.warning(
                f"Array inconsistency: batch system reports '{len(numbered_tasks)}' array tasks, but qq sees '{len(self._tasks)}' tasks."
            )

        self._batch_info_loaded = True

    def all_tasks_in_state(self, states: list[RealState]) -> bool:
        """
        Return True if all tasks in the array are in any of the given states.
        """
        if not self._batch_info_loaded:
            self.load_batch_info()

        return all(informer.get_real_state() in states for informer in self._tasks)

    @staticmethod
    def _get_informers_for_tasks(array_info: ArrayInfo) -> list[Informer]:
        """
        Create informers for each task in the array job.
        The informers are returned in the order corresponding to the directory order.

        Batch job info is NOT automatically loaded for the informers.

        Args:
            array_info (ArrayInfo): The array job information.

        Returns:
            list[Informer]: A list of Informer instances for each task.
        """
        # TODO: make this parallel
        informers = []
        for dir in array_info.task_dirs:
            info_path = construct_info_file_path(dir, array_info.job_name)
            informers.append(Informer.from_file(info_path))
        return informers

    def _get_batch_tasks(self) -> list[BatchJobInterface]:
        """
        Get batch info for all tasks of the qq array.

        If the qq array is composed of multiple array jobs, tasks from all jobs are loaded.
        """
        batch_tasks: list[BatchJobInterface] = []
        for id in self.array_info.job_ids:
            array_job = self.batch_system.get_batch_job(id)
            if not array_job.is_empty():
                batch_tasks.extend(array_job.get_tasks())

        logger.debug(f"Found {len(batch_tasks)} batch tasks.")
        return batch_tasks

    @staticmethod
    def _number_batch_tasks(
        batch_tasks: list[BatchJobInterface],
    ) -> dict[int, BatchJobInterface]:
        """
        Number batch tasks by their task number, using the newest task for each number.
        """
        numbered_tasks: dict[int, BatchJobInterface] = {}
        for task in batch_tasks:
            num = task.get_task_number()
            if num is None:
                raise QQError(
                    "Array inconsistency: task number is undefined. This is a bug."
                )

            # get the numeric portion of the current task ID for comparison
            task_id = task.get_id_int()
            if task_id is None:
                raise QQError(
                    f"Could not extract numerical job ID for job '{task.get_id()}'."
                )

            # get a task with the same task number as the current task if it exists
            # there can be multiple tasks with the same task number, e.g. if some tasks
            # from previous job array have failed and been respawned
            # we assume this is the case and use the newest batch information
            # (from the array job with the highest ID)
            existing = numbered_tasks.get(num)
            if existing is not None:
                existing_id = existing.get_id_int()
                if existing_id is None:
                    raise QQError(
                        f"Could not extract numerical job ID for job '{existing.get_id()}'."
                    )

                # if the current task is older than the existing one,
                # do not overwrite the existing task
                if task_id <= existing_id:
                    continue

            # assign the current task to the task number, replacing any existing task
            numbered_tasks[num] = task

        logger.debug(f"Found {len(numbered_tasks)} valid tasks.")

        return numbered_tasks

    def _match_batch_tasks_to_informers(
        self,
        numbered_tasks: dict[int, BatchJobInterface],
    ) -> None:
        """
        Match batch tasks to informers based on task numbers.
        """
        for informer in self._tasks:
            # get task number from the informer to match it with the batch system info
            if (task_info := informer.info.task_info) is None:
                raise QQError(
                    "Task info is undefined for a task in an array. This is a bug."
                )
            task_number = task_info.task_number

            if (batch_info := numbered_tasks.get(task_number)) is None:
                # if the array job is no longer present the batch system,
                # info for some tasks may be missing
                # we set it manually to empty
                logger.debug(
                    f"No batch info for task number '{task_number}'. Setting to empty."
                )
                batch_info = self.batch_system.get_empty_batch_job(informer.info.job_id)

            informer.set_batch_info(batch_info)
