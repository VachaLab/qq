# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from abc import ABC, abstractmethod
from datetime import timedelta

from qq_lib.properties.resources import Resources


class BatchQueueInterface(ABC):
    """
    Abstract base class for retrieving and maintaining queue information
    from a batch scheduling system.

    The implementation of the constructor is arbitrary and should only
    be used inside the corresponding implementation of `BatchInterface.get_queues`.
    """

    @abstractmethod
    def update(self) -> None:
        """
        Refresh the stored queue information from the batch system.

        Raises:
            QQError: If the queue cannot be queried or its info updated.
        """

    @abstractmethod
    def get_name(self) -> str:
        """
        Retrieve the name of the queue.

        Returns:
            str: The name identifying this queue in the batch system.
        """

    @abstractmethod
    def get_priority(self) -> str | None:
        """
        Retrieve the scheduling priority of the queue.

        Returns:
            str | None: The queue priority, or None if priority information
            is not available.
        """

    @abstractmethod
    def get_total_jobs(self) -> int | None:
        """
        Retrieve the total number of jobs currently in the queue.

        Returns:
            int | None: The total count of jobs, regardless of status
            or `None` if the information is not available.
        """

    @abstractmethod
    def get_running_jobs(self) -> int | None:
        """
        Retrieve the number of jobs currently running in the queue.

        Returns:
            int | None: The number of running jobs or `None`
            if the information is not available.
        """

    @abstractmethod
    def get_queued_jobs(self) -> int | None:
        """
        Retrieve the number of jobs waiting to start in the queue.

        Returns:
            int | None: The number of queued jobs or `None`
            if the information is not available.
        """

    @abstractmethod
    def get_other_jobs(self) -> int | None:
        """
        Retrieve the number of jobs in other states (non-running and non-queued).

        Returns:
            int | None: The number of jobs that are neither running nor queued,
            such as exiting or suspended jobs.
            Returns `None` if the information is not available.
        """

    @abstractmethod
    def get_max_walltime(self) -> timedelta | None:
        """
        Retrieve the maximum walltime allowed for jobs in the queue.

        Returns:
            timedelta | None: The walltime limit, or None if unlimited or unknown.
        """

    @abstractmethod
    def get_max_n_nodes(self) -> int | None:
        """
        Retrieve the maximum number of nodes that can be requested in the queue.

        Returns:
            int | None: The maximum number of nodes that can be requested, or None if unlimited or unknown.
        """

    @abstractmethod
    def get_comment(self) -> str | None:
        """
        Retrieve the comment or description associated with the queue.

        Returns:
            str | None: The human-readable comment or note about the queue
            or `None` if the information is not available.
        """

    @abstractmethod
    def is_available_to_user(self, user: str) -> bool:
        """
        Check whether the specified user has access to this queue.

        Args:
            user (str): The username to check access for.

        Returns:
            bool: True if the user can submit jobs to this queue, False otherwise.
        """

    @abstractmethod
    def get_destinations(self) -> list[str]:
        """
        Retrieve all destinations available for this queue route.

        Returns:
            list[str]: A list of destination queue names associated with the queue.
        """

    @abstractmethod
    def from_route_only(self) -> bool:
        """
        Determine whether this queue can only be accessed via a route.

        Returns:
            bool: True if the queue is accessible exclusively through a route,
            False otherwise.
        """

    @abstractmethod
    def to_yaml(self) -> str:
        """
        Return all information about the queue from the batch system in YAML format.

        Returns:
            str: YAML-formatted string of queue metadata.
        """

    @abstractmethod
    def get_default_resources(self) -> Resources:
        """
        Return the default resource definitions for this queue.

        Returns:
            Resources: Default resources allocated for jobs submitted to this queue.
        """
