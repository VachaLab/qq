# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass

from qq_lib.core.error import QQError


@dataclass(frozen=True)
class TransferMode(ABC):
    """
    Specifies when job data should be transferred from the working directory to
    the input directory or archived from the working directory.
    """

    @classmethod
    def from_str(cls, s: str) -> "TransferMode":
        """
        Convert a string to the corresponding TransferMode.

        Args:
            s (str): String representation of the transfer mode.

        Returns:
            TransferMode variant.

        Raises:
            QQError if the string corresponds to no transfer mode.
        """
        match s.lower().strip():
            case "always":
                return Always()
            case "never":
                return Never()
            case "success":
                return Success()
            case "failure":
                return Failure()
            case _:
                # if the string is a number (positive or negative)
                if bool(re.match(r"^-?\d+$", s.strip())):
                    return ExitCode(int(s))

                raise QQError(f"Could not recognize a transfer mode variant '{s}'.")

    @classmethod
    def multi_from_str(cls, raw: str) -> list["TransferMode"]:
        """
        Parse a string containing multiple transfer modes.

        Args:
            s: String containing transfer mode variants separated by a colon, comma, or space.
                Example: "success:42", "1,2,3", or "failure success".

        Returns:
            list[TransferMode]: A list of parsed transfer modes.

        Raises:
            QQError: If any of the individual mode strings cannot be recognized.
        """
        mode_strings = re.split(r"[:,\s]+", raw.strip())
        mode_strings = [ms for ms in mode_strings if ms]

        return [TransferMode.from_str(mode_str) for mode_str in mode_strings]

    @abstractmethod
    def should_transfer(self, exit_code: int) -> bool:
        """
        Determine whether data should be transferred/archived based on the exit code.

        Args:
            exit_code: The exit code of the completed job.

        Returns:
            True if data should be transferred/archived, False otherwise.
        """

    @abstractmethod
    def to_str(self) -> str:
        """
        Convert the TransferMode variant to its string representation.

        Returns:
            A string representation of the transfer mode variant.
        """


@dataclass(frozen=True)
class Always(TransferMode):
    """
    Data are always transferred/archived regardless the job's exit code.
    """

    def should_transfer(self, exit_code: int) -> bool:
        _ = exit_code
        return True

    def to_str(self) -> str:
        return "always"


@dataclass(frozen=True)
class Never(TransferMode):
    """
    Data are never transferred/archived.
    """

    def should_transfer(self, exit_code: int) -> bool:
        _ = exit_code
        return False

    def to_str(self) -> str:
        return "never"


@dataclass(frozen=True)
class Success(TransferMode):
    """
    Data are transferred/archived only if the job completes successfully.
    """

    def should_transfer(self, exit_code: int) -> bool:
        return exit_code == 0

    def to_str(self) -> str:
        return "success"


@dataclass(frozen=True)
class Failure(TransferMode):
    """
    Data are transferred/archived only if the job fails.
    """

    def should_transfer(self, exit_code: int) -> bool:
        return exit_code != 0

    def to_str(self) -> str:
        return "failure"


@dataclass(frozen=True)
class ExitCode(TransferMode):
    """
    Data are transferred/archived only if the job exits with a specific code.

    Attributes:
        code: The exit code that triggers data transfer.
    """

    code: int

    def should_transfer(self, exit_code: int) -> bool:
        return exit_code == self.code

    def to_str(self) -> str:
        return f"{self.code}"
