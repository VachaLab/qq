# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Self

from qq_lib.core.error import QQError


@dataclass(frozen=True)
class TransferMode(ABC):
    """
    Specifies when job data should be transferred from the working directory to
    the input directory or archived from the working directory.
    """

    @classmethod
    def fromStr(cls, s: str) -> "TransferMode":
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

    @abstractmethod
    def shouldTransfer(self, exit_code: int) -> bool:
        """
        Determine whether data should be transferred/archived based on the exit code.

        Args:
            exit_code: The exit code of the completed job.

        Returns:
            True if data should be transferred/archived, False otherwise.
        """
        pass

    @abstractmethod
    def toStr(self) -> str:
        """
        Convert the TransferMode variant to its string representation.

        Returns:
            A string representation of the transfer mode variant.
        """
        pass


@dataclass(frozen=True)
class Always(TransferMode):
    """
    Data are always transferred/archived regardless of job outcome.
    """

    def shouldTransfer(self, exit_code: int) -> bool:
        _ = exit_code
        return True

    def toStr(self) -> str:
        return "always"


@dataclass(frozen=True)
class Never(TransferMode):
    """
    Data are never transferred/archived.
    """

    def shouldTransfer(self, exit_code: int) -> bool:
        _ = exit_code
        return False

    def toStr(self) -> str:
        return "never"


@dataclass(frozen=True)
class Success(TransferMode):
    """
    Data are transferred/archived only if the job completes successfully.
    """

    def shouldTransfer(self, exit_code: int) -> bool:
        return exit_code == 0

    def toStr(self) -> str:
        return "success"


@dataclass(frozen=True)
class Failure(TransferMode):
    """
    Data are transferred/archived only if the job fails or is killed.
    """

    def shouldTransfer(self, exit_code: int) -> bool:
        return exit_code != 0

    def toStr(self) -> str:
        return "failure"


@dataclass(frozen=True)
class ExitCode(TransferMode):
    """
    Data are transferred/archived only if the job exits with a specific code.

    Attributes:
        code: The exit code that triggers data transfer.
    """

    code: int

    def shouldTransfer(self, exit_code: int) -> bool:
        return exit_code == self.code

    def toStr(self) -> str:
        return f"{self.code}"


@dataclass(frozen=True)
class TransferModesList(TransferMode):
    """
    Collection of multiple TransferMode variants combined with OR logic.

    Represents multiple TransferMode variants where data are transferred/archived if ANY
    of the contained modes would trigger a transfer for the given exit code.
    """

    modes: list[TransferMode]

    @classmethod
    def default(cls) -> Self:
        """
        Create a default TransferModesList with Success mode.

        Returns a TransferModesList configured to transfer/archived data only when the job
        completes successfully (exit code 0). This is the recommended default behavior
        for most use cases.

        Returns:
            A TransferModesList instance containing only the Success transfer mode.
        """
        return cls(modes=[Success()])

    @classmethod
    def fromStr(cls, s: str) -> Self:
        """
        Parse a string containing multiple transfer modes.

        Args:
            s: String containing transfer mode variants separated by a colon, comma, or space.
                Example: "success:42", "1,2,3", or "failure success".

        Returns:
            A TransferModes instance containing the parsed modes.

        Raises:
            QQError: If any of the individual mode strings cannot be recognized.
        """
        mode_strings = re.split(r"[:,\s]+", s.strip())
        mode_strings = [ms for ms in mode_strings if ms]

        modes = [TransferMode.fromStr(mode_str) for mode_str in mode_strings]

        if not modes:
            raise QQError("TransferModesList must contain at least one transfer mode.")

        return cls(modes=modes)

    def shouldTransfer(self, exit_code: int) -> bool:
        """
        Check if ANY contained mode should transfer.

        Data are transferred/archived if at least one of the contained transfer modes would
        return True for the given exit code.

        Args:
            exit_code: The exit code of the completed job.

        Returns:
            True if any contained mode would transfer, False if all would not transfer.
        """
        return any(mode.shouldTransfer(exit_code) for mode in self.modes)

    def toStr(self) -> str:
        """
        Convert to string representation with colon separator.

        Returns:
            String representation with all contained modes converted to strings and
            joined by colons.
        """
        return ":".join(mode.toStr() for mode in self.modes)
