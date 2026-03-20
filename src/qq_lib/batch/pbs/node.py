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
    def getOrInit(queue: str, user: str, server: str | None) -> bool:
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
                QueuesAvailability._getFullQueueName(queue, server)
            )
        ) is not None and (available := avail_dict.get(user)) is not None:
            return available

        # get the availability by querying the batch system
        available = PBSQueue(queue, server).isAvailableToUser(user)

        # cache the result
        try:
            QueuesAvailability._queues[
                QueuesAvailability._getFullQueueName(queue, server)
            ][user] = available
        except KeyError:
            QueuesAvailability._queues[
                QueuesAvailability._getFullQueueName(queue, server)
            ] = {user: available}
        logger.debug(
            f"Initialized availability of '{QueuesAvailability._getFullQueueName(queue, server)}' for user '{user}'."
        )
        return available

    @staticmethod
    def _getFullQueueName(queue: str, server: str | None) -> str:
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

    def getName(self) -> str:
        return self._name

    def getNCPUs(self) -> int | None:
        return self._getIntResource("resources_available.ncpus")

    def getNFreeCPUs(self) -> int | None:
        return self._getFreeIntResource("ncpus")

    def getNGPUs(self) -> int | None:
        return self._getIntResource("resources_available.ngpus")

    def getNFreeGPUs(self) -> int | None:
        return self._getFreeIntResource("ngpus")

    def getCPUMemory(self) -> Size | None:
        return self._getSizeResource("resources_available.mem")

    def getFreeCPUMemory(self) -> Size | None:
        return self._getFreeSizeResource("mem")

    def getGPUMemory(self) -> Size | None:
        return self._getSizeResource("resources_available.gpu_mem")

    def getFreeGPUMemory(self) -> Size | None:
        return self._getFreeSizeResource("gpu_mem")

    def getLocalScratch(self) -> Size | None:
        return self._getSizeResource("resources_available.scratch_local")

    def getFreeLocalScratch(self) -> Size | None:
        return self._getFreeSizeResource("scratch_local")

    def getSSDScratch(self) -> Size | None:
        return self._getSizeResource("resources_available.scratch_ssd")

    def getFreeSSDScratch(self) -> Size | None:
        return self._getFreeSizeResource("scratch_ssd")

    def getSharedScratch(self) -> Size | None:
        return self._getSizeResource("resources_available.scratch_shared")

    def getFreeSharedScratch(self) -> Size | None:
        return self._getFreeSizeResource("scratch_shared")

    def getProperties(self) -> list[str]:
        return [
            key.split(".", 1)[-1]
            for key in self._info
            if "resources_available" in key and self._info[key] == "True"
        ]

    def isAvailableToUser(self, user: str) -> bool:
        if not (state := self._info.get("state")):
            logger.debug(f"Could not get state information for node '{self._name}'.")
            return False

        if any(
            disabled_state in state
            for disabled_state in {"down", "unknown", "unresolvable", "resv-exclusive"}
        ):
            return False

        if queue := self._info.get("queue"):
            return QueuesAvailability.getOrInit(queue, user, self._server)

        return True

    @classmethod
    def fromDict(cls, name: str, server: str | None, info: dict[str, str]) -> Self:
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

    def toYaml(self) -> str:
        # we need to add node name to the start of the dictionary
        to_dump = {"Node": self._name} | self._info
        return yaml.dump(
            to_dump, default_flow_style=False, sort_keys=False, Dumper=Dumper
        )

    def _getIntResource(self, res: str) -> int | None:
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

    def _getFreeIntResource(self, res: str) -> int | None:
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
        if not (full := self._getIntResource(f"resources_available.{res}")):
            return None

        # if the `resources_assigned` property is missing, we assume it means that there are no resources assigned
        assigned = self._getIntResource(f"resources_assigned.{res}") or 0

        if (diff := full - assigned) >= 0:
            return diff

        return 0

    def _getSizeResource(self, res: str) -> Size | None:
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
            return Size.fromString(val)
        except Exception as e:
            logger.debug(f"Could not parse the value '{val}' of resource '{res}': {e}.")
            return None

    def _getFreeSizeResource(self, res: str) -> Size | None:
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
        if not (full := self._getSizeResource(f"resources_available.{res}")):
            return None

        # if the `resources_assigned` property is missing, we assume it means that there are no resources assigned
        assigned = self._getSizeResource(f"resources_assigned.{res}") or Size(0, "kb")

        try:
            return full - assigned
        except ValueError as e:
            logger.debug(f"Negative free size of resource '{res}': {e}.")
            return Size(0, "kb")
