# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from dataclasses import dataclass
from pathlib import Path
from typing import Self

from qq_lib.core.error import QQError


@dataclass
class ArrayInfo:
    """
    Dataclass containing information about a qq array job.
    """

    array_file: Path
    task_number: int
    total_tasks: int

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> Self:
        """
        Reconstruct an ArrayInfo instance from a dictionary produced by to_dict.

        Args:
            data (dict[str, object]): A dictionary as returned by `to_dict()`.

        Returns:
            ArrayInfo: A new instance with fields populated from the dictionary.
        """
        array_file = data.get("array_file")
        task_number = data.get("task_number")
        total_tasks = data.get("total_tasks")

        if not isinstance(array_file, str | Path):
            raise QQError(
                f"Field 'array_file' must be a str or Path, got {type(array_file).__name__}."
            )
        if not isinstance(task_number, int):
            raise QQError(
                f"Field 'task_number' must be an int, got {type(task_number).__name__}."
            )
        if not isinstance(total_tasks, int):
            raise QQError(
                f"Field 'total_tasks' must be an int, got {type(total_tasks).__name__}."
            )

        return cls(Path(array_file), task_number, total_tasks)

    def to_dict(self) -> dict[str, object]:
        """Return all fields as a dict."""
        return {
            "array_file": str(self.array_file),
            "task_number": self.task_number,
            "total_tasks": self.total_tasks,
        }
