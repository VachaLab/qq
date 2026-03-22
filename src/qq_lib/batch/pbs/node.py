# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

import subprocess
from typing import Self

import yaml

from qq_lib.batch.interface.node import BatchNodeInterface
from qq_lib.batch.pbs.common import parse_pbs_dump_to_dictionary
from qq_lib.batch.pbs.queue import PBSQueue
from qq_lib.core.common import load_yaml_dumper
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.size import Size

logger = get_logger(__name__)

Dumper: type[yaml.Dumper] = load_yaml_dumper()


class QueuesAvailability:
    """
    Utility class for obtaining and caching the availability status of batch queues.
    """

    _queues: dict[str, dict[str, bool]] = {}

    @staticmethod
    def get_or_init(queue: str, user: str, server: str | None) -> bool:
        """
        Retrieve the availability of a queue for the given user.

        Args:
            queue (str): The name of the queue to check.
            user (str): The username to check access for.
            server (str | None): Server on which the queue is located.

        Returns:
            bool:
                True if the queue is available to the current user,
                False otherwise.
        """
        # check whether the availability is cached
        if (
            avail_dict := QueuesAvailability._queues.get(
                QueuesAvailability._get_full_queue_name(queue, server)
            )
        ) is not None and (available := avail_dict.get(user)) is not None:
            return available

        # get the availability by querying the batch system
        available = PBSQueue(queue, server).is_available_to_user(user)

        # cache the result
        try:
            QueuesAvailability._queues[
                QueuesAvailability._get_full_queue_name(queue, server)
            ][user] = available
        except KeyError:
            QueuesAvailability._queues[
                QueuesAvailability._get_full_queue_name(queue, server)
            ] = {user: available}
        logger.debug(
            f"Initialized availability of '{QueuesAvailability._get_full_queue_name(queue, server)}' for user '{user}'."
        )
        return available

    @staticmethod
    def _get_full_queue_name(queue: str, server: str | None) -> str:
        """
        Format a queue name with an optional server qualifier.

        Args:
            queue (str): The name of the queue.
            server (str | None): The server the queue resides on,
                or `None` to address the queue without a server qualifier.

        Returns:
            str: The full queue name as `queue@server`, or just `queue` if
            `server` is `None`.
        """
        if server:
            return f"{queue}@{server}"

        return queue


class PBSNode(BatchNodeInterface):
    """
    Implementation of BatchNodeInterface for PBS.
    Stores metadata for a single PBS node.
    """

    def __init__(self, name: str, server: str | None = None):
        self._name = name
        self._server = server
        self._info: dict[str, str] = {}

        self.update()

    def update(self) -> None:
        # get node info from PBS
        command = f"pbsnodes -v {self._name}"
        if self._server:
            command += f" -s {self._server}"

        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(f"Node '{self._name}' does not exist.")

        self._info = parse_pbs_dump_to_dictionary(result.stdout)

    def get_name(self) -> str:
        return self._name

    def get_n_cpus(self) -> int | None:
        return self._get_int_resource("resources_available.ncpus")

    def get_n_free_cpus(self) -> int | None:
        return self._get_free_int_resource("ncpus")

    def get_n_gpus(self) -> int | None:
        return self._get_int_resource("resources_available.ngpus")

    def get_n_free_gpus(self) -> int | None:
        return self._get_free_int_resource("ngpus")

    def get_cpu_memory(self) -> Size | None:
        return self._get_size_resource("resources_available.mem")

    def get_free_cpu_memory(self) -> Size | None:
        return self._get_free_size_resource("mem")

    def get_gpu_memory(self) -> Size | None:
        return self._get_size_resource("resources_available.gpu_mem")

    def get_free_gpu_memory(self) -> Size | None:
        return self._get_free_size_resource("gpu_mem")

    def get_local_scratch(self) -> Size | None:
        return self._get_size_resource("resources_available.scratch_local")

    def get_free_local_scratch(self) -> Size | None:
        return self._get_free_size_resource("scratch_local")

    def get_ssd_scratch(self) -> Size | None:
        return self._get_size_resource("resources_available.scratch_ssd")

    def get_free_ssd_scratch(self) -> Size | None:
        return self._get_free_size_resource("scratch_ssd")

    def get_shared_scratch(self) -> Size | None:
        return self._get_size_resource("resources_available.scratch_shared")

    def get_free_shared_scratch(self) -> Size | None:
        return self._get_free_size_resource("scratch_shared")

    def get_properties(self) -> list[str]:
        return [
            key.split(".", 1)[-1]
            for key in self._info
            if "resources_available" in key and self._info[key] == "True"
        ]

    def is_available_to_user(self, user: str) -> bool:
        if not (state := self._info.get("state")):
            logger.debug(f"Could not get state information for node '{self._name}'.")
            return False

        if any(
            disabled_state in state
            for disabled_state in {"down", "unknown", "unresolvable", "resv-exclusive"}
        ):
            return False

        if queue := self._info.get("queue"):
            return QueuesAvailability.get_or_init(queue, user, self._server)

        return True

    @classmethod
    def from_dict(cls, name: str, server: str | None, info: dict[str, str]) -> Self:
        """
        Construct a new instance of PBSNode from node name and a dictionary of node information.

        Args:
            name (str): The unique name of the node.
            server (str | None): Server on which the node is located. If `None`, assumes the current server.
            info (dict[str, str]): A dictionary containing PBS node metadata as key-value pairs.

        Returns:
            Self: A new instance of PBSNode.

        Note:
            This method does not perform any validation or processing of the provided dictionary.
        """
        node = cls.__new__(cls)
        node._name = name
        node._server = server
        node._info = info

        return node

    def to_yaml(self) -> str:
        # we need to add node name to the start of the dictionary
        to_dump = {"Node": self._name} | self._info
        return yaml.dump(
            to_dump, default_flow_style=False, sort_keys=False, Dumper=Dumper
        )

    def _get_int_resource(self, res: str) -> int | None:
        """
        Retrieve an integer-valued resource from the node information.

        Args:
            res (str): The resource key to retrieve (e.g., "resources_available.ncpus").

        Returns:
            int: The integer value of the resource, or `None` if unavailable or invalid.
        """
        if not (val := self._info.get(res)):
            return None
        try:
            return int(val)
        except Exception as e:
            logger.debug(f"Could not parse the value '{val}' of resource '{res}': {e}.")
            return None

    def _get_free_int_resource(self, res: str) -> int | None:
        """
        Compute the number of free units for an integer-valued resource.

        Calculates the difference between the total available (`resources_available.<res>`)
        and the assigned (`resources_assigned.<res>`) quantities. If the computed
        difference is negative, returns 0. If the information is not available, returns None.

        Args:
            res (str): The base resource name (e.g., "ncpus", "ngpus").

        Returns:
            int | None: The number of unallocated (free) resource units, or None if unavailable.
        """
        if not (full := self._get_int_resource(f"resources_available.{res}")):
            return None

        # if the `resources_assigned` property is missing, we assume it means that there are no resources assigned
        assigned = self._get_int_resource(f"resources_assigned.{res}") or 0

        if (diff := full - assigned) >= 0:
            return diff

        return 0

    def _get_size_resource(self, res: str) -> Size | None:
        """
        Retrieve a Size resource from the node information.

        Args:
            res (str): The resource key to retrieve (e.g., "resources_available.mem").

        Returns:
            Size | None: The parsed Size, or `None` if unavailable or invalid.
        """
        if not (val := self._info.get(res)):
            return None

        try:
            return Size.from_string(val)
        except Exception as e:
            logger.debug(f"Could not parse the value '{val}' of resource '{res}': {e}.")
            return None

    def _get_free_size_resource(self, res: str) -> Size | None:
        """
        Compute the amount of free space for a Size resource.

        Calculates the difference between total available (`resources_available.<res>`)
        and assigned (`resources_assigned.<res>`) values. If subtraction results in a negative size,
        returns a zero-size object. If the information are not available, returns None.

        Args:
            res (str): The base resource name (e.g., "mem", "scratch_local").

        Returns:
            Size | None: The available (free) size for the resource, or `None` if unavailable.
        """
        if not (full := self._get_size_resource(f"resources_available.{res}")):
            return None

        # if the `resources_assigned` property is missing, we assume it means that there are no resources assigned
        assigned = self._get_size_resource(f"resources_assigned.{res}") or Size(0, "kb")

        try:
            return full - assigned
        except ValueError as e:
            logger.debug(f"Negative free size of resource '{res}': {e}.")
            return Size(0, "kb")
