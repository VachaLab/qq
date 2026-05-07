# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

import shutil
import socket
from dataclasses import dataclass, field
from typing import Any

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError


@dataclass(frozen=True)
class Interpreter:
    """Configuration for the interpreter used to execute a job script.

    Attributes:
        executable: Name or path of the interpreter executable.
            Defaults to the executable from `CFG.runner.default_interpreter`.
        arguments: Additional command-line arguments passed to the interpreter.
            Defaults to arguments parsed from `CFG.runner.default_interpreter`,
            or an empty list if `executable` is provided explicitly.
    """

    executable: str | None = None
    arguments: list[str] = field(default_factory=list)

    def __post_init__(self):
        """Parse executable and arguments from `CFG.runner.default_interpreter` if no executable was provided."""
        if self.executable is None:
            binary, *arguments = CFG.runner.default_interpreter.split()
            object.__setattr__(self, "executable", binary)
            object.__setattr__(self, "arguments", arguments)

    @classmethod
    def from_str(cls, s: str) -> "Interpreter":
        """
        Create an Interpreter from a string containing the executable and optional arguments.

        The string is split on whitespace. The first token is used as the executable
        and any remaining tokens become arguments.

        Args:
            s (str): Space-separated string of the interpreter executable followed by
                optional arguments. For example: "python3 -u" or "/usr/bin/bash".

        Returns:
            Interpreter:An Interpreter instance with the parsed executable and arguments.
        """
        executable, *arguments = s.split()
        return cls(executable=executable, arguments=arguments)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Interpreter":
        """
        Create an Interpreter from a dictionary.

        Args:
            d (dict[str, Any]): Dictionary with keys matching the dataclass fields
                (`executable` and `arguments`).

        Returns:
            Interpreter: An Interpreter instance constructed from the dictionary values.
        """
        return cls(**d)

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize the Interpreter to a dictionary.

        Returns:
            A dictionary with `executable` and `arguments` keys.
        """
        return {"executable": self.executable, "arguments": self.arguments}

    def to_command_list(self) -> list[str]:
        """
        Resolve the executable to its full path and build the command list.

        Uses `shutil.which` to locate the interpreter on the current node.
        The returned list is suitable for use with `subprocess` calls.

        Returns:
            list[str]: A list containing the resolved absolute path of the executable
            followed by any configured arguments.

        Raises:
            QQError: If the interpreter executable cannot be found on the current node.
        """
        # enforced in __post_init__
        assert self.executable is not None

        if not (full := shutil.which(self.executable)):
            raise QQError(
                f"Interpreter '{self.executable}' is not available on node '{socket.getfqdn()}'."
            )

        return [full] + self.arguments
