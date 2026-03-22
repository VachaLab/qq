# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

import socket
import subprocess
from dataclasses import fields
from datetime import timedelta
from typing import Self

import yaml

from qq_lib.batch.interface.queue import BatchQueueInterface
from qq_lib.batch.pbs.common import parse_pbs_dump_to_dictionary
from qq_lib.core.common import hhmmss_to_duration, load_yaml_dumper
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.resources import Resources

logger = get_logger(__name__)

Dumper: type[yaml.Dumper] = load_yaml_dumper()


class ACLData:
    """
    Utility class for caching and retrieving access control (ACL) context data.

    Improves performance when multiple ACL checks are performed repeatedly during queue evaluations.
    """

    _groups: dict[str, list[str]] = {}
    _host: str | None = None

    @staticmethod
    def get_groups_or_init(user: str) -> list[str]:
        """
        Retrieve the cached group memberships for a user, initializing them if needed.
        Args:
            user (str): The username whose group memberships should be retrieved.

        Returns:
            list[str]: A list of group names the user belongs to.
                       Returns an empty list if the system command fails.
        """
        if groups := ACLData._groups.get(user):
            return groups

        result = subprocess.run(
            ["bash"],
            input=f"id -nG {user}",
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            ACLData._groups[user] = []
            return []

        groups = result.stdout.split()
        ACLData._groups[user] = groups
        logger.debug(f"Initialized ACL groups for user '{user}': {groups}.")
        return groups

    @staticmethod
    def get_host_or_init() -> str:
        """
        Retrieve the cached hostname, initializing it if not already set.

        Returns:
            str: The local machine's hostname.
        """
        if host := ACLData._host:
            return host

        host = socket.getfqdn()
        ACLData._host = host
        logger.debug(f"Initialized ACL host: {host}.")
        return host


class PBSQueue(BatchQueueInterface):
    """
    Implementation of BatchQueueInterface for PBS.
    Stores metadata for a single PBS queue.
    """

    def __init__(self, name: str, server: str | None = None):
        self._name = name
        self._server = server
        self._info: dict[str, str] = {}

        self.update()

    def update(self) -> None:
        # get queue info from PBS
        command = f"qstat -Qfw {self._name}"
        if self._server:
            command += f"@{self._server}"

        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(f"Queue '{self._name}' does not exist.")

        self._info = parse_pbs_dump_to_dictionary(result.stdout)
        self._set_attributes()

    def get_name(self) -> str:
        return self._name

    def get_priority(self) -> str | None:
        return self._info.get("Priority")

    def get_total_jobs(self) -> int | None:
        return PBSQueue._get_int_value(self._info, "total_jobs")

    def get_running_jobs(self) -> int | None:
        return PBSQueue._get_int_value(self._job_numbers, "Running")

    def get_queued_jobs(self) -> int | None:
        # we count held and waiting jobs as queued for consistency with Slurm
        return (
            (PBSQueue._get_int_value(self._job_numbers, "Queued") or 0)
            + (PBSQueue._get_int_value(self._job_numbers, "Held") or 0)
            + (PBSQueue._get_int_value(self._job_numbers, "Waiting") or 0)
        )

    def get_other_jobs(self) -> int | None:
        return (
            (PBSQueue._get_int_value(self._job_numbers, "Transit") or 0)
            + (PBSQueue._get_int_value(self._job_numbers, "Exiting") or 0)
            + (PBSQueue._get_int_value(self._job_numbers, "Begun") or 0)
        )

    def get_max_walltime(self) -> timedelta | None:
        if raw_time := self._info.get("resources_max.walltime"):
            return hhmmss_to_duration(raw_time)

        return None

    def get_max_n_nodes(self) -> int | None:
        return PBSQueue._get_int_value(self._info, "resources_max.nodect")

    def get_comment(self) -> str | None:
        if not (raw_comment := self._info.get("comment")):
            return None

        return raw_comment.split("|", 1)[0]

    def is_available_to_user(self, user: str) -> bool:
        # queues that are not enabled or not started are unavailable to all users
        if self._info.get("enabled") != "True" or self._info.get("started") != "True":
            return False

        # check acl users
        if self._info.get("acl_user_enable") == "True":
            acl_users = self._acl_users
            if user not in acl_users:
                return False

        # check acl groups
        if self._info.get("acl_group_enable") == "True":
            expected_acl_groups = self._acl_groups
            users_acl_groups = ACLData.get_groups_or_init(user)
            if not any(item in expected_acl_groups for item in users_acl_groups):
                return False

        # check acl hosts
        if (host := self._info.get("acl_host_enable")) == "True":
            acl_hosts = self._acl_hosts
            host = ACLData.get_host_or_init()
            if host not in acl_hosts:
                return False

        return True

    def get_destinations(self) -> list[str]:
        if raw_destinations := self._info.get("route_destinations"):
            return raw_destinations.split(",")

        return []

    def from_route_only(self) -> bool:
        return self._info.get("from_route_only") == "True"

    def to_yaml(self) -> str:
        # we need to add queue name to the start of the dictionary
        to_dump = {"Queue": self._name} | self._info
        return yaml.dump(
            to_dump, default_flow_style=False, sort_keys=False, Dumper=Dumper
        )

    def get_default_resources(self) -> Resources:
        default_resources = {}

        for key, value in self._info.items():
            if "resources_default" in key:
                resource = key.split(".")[-1]
                default_resources[resource.strip()] = value.strip()

        # filter resources that are part of Resources
        field_names = {f.name for f in fields(Resources)}
        return Resources(
            **{k: v for k, v in default_resources.items() if k in field_names}
        )

    @staticmethod
    def _get_int_value(dict: dict[str, str], key: str) -> int | None:
        """
        Retrieve an integer value from the provided dictionary.

        Args:
            key (str): The key to look up in the dictionary.

        Returns:
            int | None: The integer value if conversion succeeds, otherwise `None`.
        """
        if not (raw := dict.get(key)):
            return None

        try:
            return int(raw)
        except ValueError as e:
            logger.debug(
                f"Could not parse '{key}' value of '{raw}' as an integer: {e}."
            )
            return None

    def _set_attributes(self) -> None:
        """
        Initialize derived queue attributes to avoid redundant parsing.
        """
        self._set_job_numbers()
        self._acl_users = self._info.get("acl_users", "").split(",")
        self._acl_groups = self._info.get("acl_groups", "").split(",")
        self._acl_hosts = self._info.get("acl_hosts", "").split(",")

    @classmethod
    def from_dict(cls, name: str, server: str | None, info: dict[str, str]) -> Self:
        """
        Construct a new instance of PBSQueue from a queue name and a dictionary of queue information.


        Args:
            name (str): The unique name of the queue.
            server (str | None): Server on which the queue is located. If `None`, assumes the current server.
            info (dict[str, str]): A dictionary containing PBS queue metadata as key-value pairs.

        Returns:
            Self: A new instance of PBSQueue.

        Note:
            This method does not perform any validation or processing of the provided dictionary.
        """
        queue = cls.__new__(cls)
        queue._name = name
        queue._server = server
        queue._info = info
        queue._set_attributes()

        return queue

    def _set_job_numbers(self) -> None:
        """
        Parse and store job counts by state from the 'state_count' field.

        If parsing fails or the field is missing, `_job_numbers` is set to an empty dictionary.
        """
        if not (state_count := self._info.get("state_count")):
            self._job_numbers: dict[str, str] = {}
            return

        try:
            self._job_numbers = dict(p.split(":") for p in state_count.split())
        except Exception as e:
            logger.warning(f"Could not get job counts for queue '{self._name}': {e}.")
            self._job_numbers: dict[str, str] = {}
