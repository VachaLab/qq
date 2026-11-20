# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qq_lib.properties.size import Size


class BatchNodeInterface(ABC):
    """
    Abstract base class for obtaining information about compute nodes.

    The implementation of the constructor is arbitrary and should only
    be used inside the corresponding implementation of `BatchInterface.get_nodes`.
    """

    @abstractmethod
    def update(self) -> None:
        """
        Refresh the stored node information from the batch system.

        Raises:
            QQError: If the node cannot be queried or its info updated.
        """

    @abstractmethod
    def get_name(self) -> str:
        """
        Retrieve the name of the node.

        Returns:
            The name identifying the node in the batch system.
        """

    @abstractmethod
    def get_n_cpus(self) -> int | None:
        """
        Retrieve the total number of CPU cores available on the node.

        Returns:
            Total CPU core count or `None` if not available.
        """

    @abstractmethod
    def get_n_free_cpus(self) -> int | None:
        """
        Retrieve the number of currently available (unallocated) CPU cores.

        Returns:
            Number of free CPU cores or `None` if not available.
        """

    @abstractmethod
    def get_n_gpus(self) -> int | None:
        """
        Retrieve the total number of GPUs available on the node.

        Returns:
            Total GPU count or `None` if not available..
        """

    @abstractmethod
    def get_n_free_gpus(self) -> int | None:
        """
        Retrieve the number of currently available (unallocated) GPUs.

        Returns:
            Number of free GPUs or `None` if not available.
        """

    @abstractmethod
    def get_cpu_memory(self) -> Size | None:
        """
        Retrieve the total CPU memory capacity of the node.

        Returns:
            Total CPU memory available on the node or `None` if not available.
        """

    @abstractmethod
    def get_free_cpu_memory(self) -> Size | None:
        """
        Retrieve the currently available CPU memory.

        Returns:
            Free (unused) CPU memory or `None` if not available.
        """

    @abstractmethod
    def get_gpu_memory(self) -> Size | None:
        """
        Retrieve the total GPU memory capacity of the node.

        Returns:
            Total GPU memory available or `None` if not available.
        """

    @abstractmethod
    def get_free_gpu_memory(self) -> Size | None:
        """
        Retrieve the currently available GPU memory.

        Returns:
            Free (unused) GPU memory or `None` if not available.
        """

    @abstractmethod
    def get_local_scratch(self) -> Size | None:
        """
        Retrieve the total local scratch storage capacity of the node.

        Returns:
            Total size of local scratch space or `None` if not available.
        """

    @abstractmethod
    def get_free_local_scratch(self) -> Size | None:
        """
        Retrieve the available local scratch storage space.

        Returns:
            Free local scratch space or `None` if not available.
        """

    @abstractmethod
    def get_ssd_scratch(self) -> Size | None:
        """
        Retrieve the total SSD-based scratch storage capacity.

        Returns:
            Total SSD scratch capacity or `None` if not available.
        """

    @abstractmethod
    def get_free_ssd_scratch(self) -> Size | None:
        """
        Retrieve the currently available SSD-based scratch storage space.

        Returns:
            Free SSD scratch space or `None` if not available.
        """

    @abstractmethod
    def get_shared_scratch(self) -> Size | None:
        """
        Retrieve the total capacity of shared scratch storage accessible from the node.

        Returns:
            Total shared scratch capacity or `None` if not available.
        """

    @abstractmethod
    def get_free_shared_scratch(self) -> Size | None:
        """
        Retrieve the available space in shared scratch storage.

        Returns:
            Free shared scratch space or `None` if not available.
        """

    @abstractmethod
    def get_properties(self) -> list[str]:
        """
        Get the list of properties or labels assigned to the node.

        Returns:
            List of node property strings.
        """

    @abstractmethod
    def is_available_to_user(self, user: str) -> bool:
        """
        Check if the node is available to the specified user.

        Args:
            user: The username to check access for.

        Returns:
            True if the node is up and schedulable, False otherwise.
        """

    @abstractmethod
    def to_yaml(self) -> str:
        """
        Return all information about the node in YAML format.

        Returns:
            YAML-formatted string of node metadata.
        """
