# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Self

import yaml

from qq_lib.batch.interface import BatchInterface
from qq_lib.core._yaml_serializable import _YAMLSerializable
from qq_lib.core.common import load_yaml_dumper, load_yaml_loader
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

SafeLoader: type[yaml.SafeLoader] = load_yaml_loader()
Dumper: type[yaml.Dumper] = load_yaml_dumper()

logger = get_logger(__name__)


@dataclass
class ArrayInfo(_YAMLSerializable):
    """
    Dataclass containing information about a qq array job.
    """

    # Label used in error messages
    _file_label: ClassVar[str] = "qq array"

    # Comment used in the YAML header
    _file_comment: ClassVar[str] = (
        "this file contains information about a qq array job; do not remove it manually"
    )

    # IDs of all jobs forming this qq array
    job_ids: list[str]

    # Absolute logical paths to all task directories
    task_dirs: list[Path]

    # Number of tasks that have successfully finished
    n_finished_tasks: int

    @classmethod
    def _from_dict(cls, data: dict[str, object]) -> Self:
        """
        Reconstruct an ArrayInfo instance from a dictionary.

        Args:
            data (dict[str, object]): A dictionary containing the array job information.

        Returns:
            ArrayInfo: A new instance with fields populated from the dictionary.
        """
        raw_ids = data.get("job_ids")
        raw_dirs = data.get("task_dirs")
        n_finished_tasks = data.get("n_finished_tasks")

        if not isinstance(raw_ids, list):
            raise QQError(
                f"Field 'job_ids' must be a list, got {type(raw_ids).__name__}."
            )
        if not isinstance(raw_dirs, list):
            raise QQError(
                f"Field 'task_dirs' must be a list, got {type(raw_dirs).__name__}."
            )
        if not isinstance(n_finished_tasks, int):
            raise QQError(
                f"Field 'n_finished_tasks' must be an int, "
                f"got {type(n_finished_tasks).__name__}."
            )

        job_ids: list[str] = []
        for item in raw_ids:
            if not isinstance(item, str):
                raise QQError(f"Each job_id must be str, got {type(item).__name__}.")
            job_ids.append(item)

        task_dirs: list[Path] = []
        for item in raw_dirs:
            if not isinstance(item, str | Path):
                raise QQError(
                    f"Each task_dir must be str or Path, got {type(item).__name__}."
                )
            task_dirs.append(Path(item))

        return cls(job_ids, task_dirs, n_finished_tasks)

    def _to_dict(self) -> dict[str, object]:
        """Return all fields as a dict."""
        return {
            "job_ids": self.job_ids,
            "task_dirs": self.task_dirs,
            "n_finished_tasks": self.n_finished_tasks,
        }

    @classmethod
    def atomically_increase_n_finished_tasks(
        cls, _file: Path, _host: str | None = None
    ) -> int:
        """
        Atomically increase the n_finished_tasks field in the qq array file.

        Args:
            path (Path): The path to the qq array file.

        Returns:
            int: The updated n_finished_tasks value.
        """
        _BatchSystem = BatchInterface.from_env_var_or_guess()
        raise NotImplementedError()
