# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import re
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class ResubmitHost(ABC):
    """
    A host target for resubmitting a loop or continuous job.

    This abstract base class represents a destination host where a batch job
    can be resubmitted. Concrete implementations resolve to actual hostnames
    via the `convert` method.

    Subclasses:
        InputHost: Resolves to the original input machine.
        WorkHost: Resolves to the current working node.
        CustomHost: Resolves to an explicitly specified hostname.
    """

    @classmethod
    def from_str(cls, s: str) -> "ResubmitHost":
        """
        Parses a single resubmission host from a string.

        Args:
            s (str): String representation of the resubmission host.

        Returns:
            ResubmitHost: The corresponding `ResubmitHost` variant.
        """
        match s.lower().strip():
            case "input":
                return InputHost()
            case "working" | "work":
                return WorkHost()
            case _:
                return ExplicitHost(s.strip())

    @classmethod
    def multi_from_str(cls, raw: str) -> list["ResubmitHost"]:
        """
        Parses multiple resubmission hosts from a delimited string.

        Args:
            raw (str): String containing one or more host specifiers separated by
                colons, commas, or spaces.
                Examples: "input:node132.random.server.org", "work,input", or "node123 node234".

        Returns:
            list[ResubmitHost]: A list of parsed `ResubmitHost` instances.
        """
        host_strings = re.split(r"[:,\s]+", raw.strip())
        host_strings = [hs for hs in host_strings if hs]

        return [ResubmitHost.from_str(host_str) for host_str in host_strings]

    @abstractmethod
    def to_str(self) -> str:
        """
        Convert the resubmission host into its string representation.

        Returns:
            str: String, unresolved representation of the resubmission host.
        """

    @abstractmethod
    def resolve(self, input_host: str, working_node: str) -> str:
        """
        Resolves this resubmission host to a concrete hostname.

        Args:
            input_host (str): The name of the machine from which the job was originally submitted.
            working_node (str): The name of the node on which the job is currently running.
                For multi-node jobs, use the main node.

        Returns:
            str: The resolved hostname string.
        """


@dataclass(frozen=True)
class InputHost(ResubmitHost):
    """
    A resubmission host that resolves to the original input machine.
    """

    def to_str(self) -> str:
        return "input"

    def resolve(self, input_host: str, working_node: str) -> str:
        _ = working_node
        return input_host


@dataclass(frozen=True)
class WorkHost(ResubmitHost):
    """
    A resubmission host that resolves to the current working node.
    """

    def to_str(self) -> str:
        return "working"

    def resolve(self, input_host: str, working_node: str) -> str:
        _ = input_host
        return working_node


@dataclass(frozen=True)
class ExplicitHost(ResubmitHost):
    """
    A resubmission host that stores an explicit hostname.

    Attributes:
        hostname: The explicit hostname to use for resubmission.
    """

    hostname: str

    def to_str(self) -> str:
        return self.hostname

    def resolve(self, input_host: str, working_node: str) -> str:
        _ = input_host, working_node
        return self.to_str()
