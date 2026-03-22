# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Self

from qq_lib.properties.size import Size
from qq_lib.properties.states import BatchState


class BatchJobInterface(ABC):
    """
    Abstract base class for retrieving and maintaining job information
    from a batch scheduling system.

    Must support situations where the job information no longer exists.

    The implementation of the constructor is arbitrary and should only
    be used inside the corresponding implementation of `BatchInterface.get_batch_job`.
    """

    @abstractmethod
    def is_empty(self) -> bool:
        """
        Check whether the job contains any information.
        This should return True if the job does not exist in the batch system.

        Returns:
            bool: True if the job contains no information.
        """

    @abstractmethod
    def get_id(self) -> str:
        """
        Return the ID of the job.

        Returns:
            str: The ID of the job.
        """

    @abstractmethod
    def get_account(self) -> str | None:
        """
        Return the account under which the job is submitted.

        Returns:
            str | None: Account associated with the job or None if no
            account is defined.
        """

    @abstractmethod
    def update(self) -> None:
        """
        Refresh the stored job information from the batch system.

        Raises:
            QQError: If the job cannot be queried or its info updated.
        """

    @abstractmethod
    def get_state(self) -> BatchState:
        """
        Return the current state of the job as reported by the batch system.

        If the job information is no longer available, return `BatchState.UNKNOWN`.

        Returns:
            BatchState: The job state according to the batch system.
        """

    @abstractmethod
    def get_comment(self) -> str | None:
        """
        Retrieve the batch system-provided comment for the job.

        Returns:
            str | None: The job's comment string if available, or None if the
            batch system has not attached a comment.
        """

    @abstractmethod
    def get_estimated(self) -> tuple[datetime, str] | None:
        """
        Retrieve the batch system's estimated job start time and execution node.

        Returns:
            tuple[datetime, str] | None: A tuple containing:
                - datetime: The estimated start time of the job.
                - str: The name of the node where the job is expected to run.
            Returns None if either estimate is unavailable.
        """

    @abstractmethod
    def get_main_node(self) -> str | None:
        """
        Retrieve the hostname of the main execution node for the job.

        Returns:
            str | None: The hostname of the main execution node, or ``None``
            if unavailable or not applicable.
        """

    @abstractmethod
    def get_nodes(self) -> list[str] | None:
        """
        Retrieve the hostnames of all execution nodes allocated for the job.

        Returns:
            list[str] | None:
                A list of hostnames or node identifiers used by the job,
                or `None` if node information is not available.
        """

    @abstractmethod
    def get_short_nodes(self) -> list[str] | None:
        """
        Retrieve the short hostnames of all execution nodes allocated for the job.

        Returns:
            list[str] | None:
                A list of short hostnames used by the job, or `None` if node information
                is not available.
        """

    @abstractmethod
    def get_user(self) -> str | None:
        """
        Return the username of the job owner.

        Returns:
            str | None: Username of the user who owns the job or `None` if not available.
        """

    @abstractmethod
    def get_n_cpus(self) -> int | None:
        """
        Return the number of CPU cores allocated for the job.

        Returns:
            int | None: Number of CPUs allocated for the job or `None` if not available.
        """

    @abstractmethod
    def get_n_gpus(self) -> int | None:
        """
        Return the number of GPUs allocated for the job.

        Returns:
            int | None: Number of GPUs allocated for the job or `None` if not available.
        """

    @abstractmethod
    def get_n_nodes(self) -> int | None:
        """
        Return the number of compute nodes assigned to the job.

        Returns:
            int | None: Number of nodes used by the job or `None` if not available.
        """

    @abstractmethod
    def get_mem(self) -> Size | None:
        """
        Return the amount of memory allocated for the job.

        Returns:
            Size | None: Amount of memory allocated for the job or `None` if not available.
        """

    @abstractmethod
    def get_name(self) -> str | None:
        """
        Return the name of the job.

        Returns:
            str | None: The name of the submitted job or `None` if not available.
        """

    @abstractmethod
    def get_submission_time(self) -> datetime | None:
        """
        Return the timestamp when the job was submitted.

        Returns:
            datetime | None: Time when the job was submitted to the batch system
            or `None` if not available.
        """

    @abstractmethod
    def get_start_time(self) -> datetime | None:
        """
        Return the timestamp when the job started execution.

        Returns:
            datetime | None: Time when the job began running or
            `None` if the job has not yet started.
        """

    @abstractmethod
    def get_completion_time(self) -> datetime | None:
        """
        Return the timestamp when the job was completed.

        Returns:
            datetime | None: Time when the job completed or
            `None` if the job has not yet completed.
        """

    @abstractmethod
    def get_modification_time(self) -> datetime | None:
        """
        Return the timestamp at which the job was last modified.

        Returns:
            datetime | None: Time when the job was last modified or `None`
            if the information is not available.
        """

    @abstractmethod
    def get_walltime(self) -> timedelta | None:
        """
        Return the walltime limit of the job.

        Returns:
            timedelta | None: Walltime for the job or `None` if not available.
        """

    @abstractmethod
    def get_queue(self) -> str | None:
        """
        Return the submission queue of the job.

        Returns:
            str | None: The queue this job is part of or `None` if not available.
        """

    @abstractmethod
    def get_util_cpu(self) -> int | None:
        """
        Return the utilization of requested CPUs in percents (0-100).

        Returns:
            int | None: Utilization of requested CPUs or `None` if not available.
        """

    @abstractmethod
    def get_util_mem(self) -> int | None:
        """
        Return the utilization of requested memory in percents (0-100).

        Returns:
            int | None: Utilization of requested memory or `None` if not available.
        """

    @abstractmethod
    def get_exit_code(self) -> int | None:
        """
        Return the exit code of the job.

        Returns:
            int | None: Exit code of the job or `None` if exit code is not assigned.
        """

    @abstractmethod
    def get_input_dir(self) -> Path | None:
        """
        Return path to the directory from which the job was submitted.

        Returns:
            Path | None: Path to the submission directory or `None` if not available.
        """

    @abstractmethod
    def get_input_machine(self) -> str | None:
        """
        Return the hostname of the submission machine.

        Returns:
            str | None: Hostname of the submission machine or `None` if not available.
        """

    @abstractmethod
    def get_info_file(self) -> Path | None:
        """
        Return path to the info file associated with this job.

        Returns:
            Path | None: Path to the qq info file or `None` if
            this is not a qq job.
        """

    @abstractmethod
    def to_yaml(self) -> str:
        """
        Return all information about the job from the batch system in YAML format.

        Returns:
            str: YAML-formatted string of job metadata.
        """

    @abstractmethod
    def get_steps(self) -> Sequence[Self]:
        """
        Return a list of steps associated with this job.

        Note that job step is represented by BatchJobInterface, but
        may not contain all the values that a proper BatchJobInterface contains.

        Returns:
            Sequence[BatchJobInterface]: List of job steps. An empty list if there are none.
        """

    @abstractmethod
    def get_step_id(self) -> str | None:
        """
        Return the step index if this job is a job step.

        Returns:
            str | None: Job step index or `None` if this is not a job step.
        """

    @abstractmethod
    def is_array_job(self) -> bool:
        """
        Return `True` if the job is a top-level array job (not a sub-job).

        Returns:
            bool: `True` if the job is a top-level array job, else `False`.
        """
