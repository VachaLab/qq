# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

import getpass
import os
import shutil
import subprocess
from pathlib import Path

from qq_lib.batch.interface import BatchInterface
from qq_lib.batch.slurm import Slurm
from qq_lib.batch.slurm.queue import SlurmQueue
from qq_lib.core.common import equals_normalized
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.resources import Resources

logger = get_logger(__name__)


class SlurmIT4I(Slurm):
    """
    Implementation of BatchInterface for Slurm on IT4I clusters.
    """

    # all scratch directory types supported by SlurmIT4I
    SUPPORTED_SCRATCHES = ["scratch"]

    @classmethod
    def env_name(cls) -> str:
        return "SlurmIT4I"

    @classmethod
    def is_available(cls) -> bool:
        return shutil.which("it4ifree") is not None

    @classmethod
    def create_work_dir_on_scratch(cls, job_id: str) -> Path:
        if not (account := os.environ.get(CFG.env_vars.slurm_job_account)):
            raise QQError(f"No account is defined for job '{job_id}'.")

        user = getpass.getuser()

        # we attempt to create the scratch directory multiple times in different user directory;
        # if the user directory is already created but the user does not have permissions
        # to write into it, we append a number to the user's name and try creating a new directory
        last_exception = None
        for attempt in range(CFG.slurm_it4i_options.scratch_dir_attempts):
            user_component = (
                user if attempt == 0 else f"{user}{attempt + 1}"
            )  # appended number is 2 for the second attempt

            scratch = Path(
                f"/scratch/project/{account.lower()}/{user_component}/qq-jobs/job_{job_id}"
            )

            try:
                scratch.mkdir(parents=True, exist_ok=True)
                return scratch
            except Exception as e:
                last_exception = e

        # if all attempts failed
        raise QQError(
            f"Could not create a working directory on scratch for job '{job_id}' after {CFG.slurm_it4i_options.scratch_dir_attempts} attempts: {last_exception}"
        ) from last_exception

    @classmethod
    def get_supported_work_dir_types(cls) -> list[str]:
        return cls.SUPPORTED_SCRATCHES + [
            "input_dir",
            "job_dir",  # same as input_dir
        ]

    @classmethod
    def navigate_to_destination(cls, host: str, directory: Path) -> None:
        logger.info(
            f"Host '{host}' is not reachable in this environment. Navigating to '{directory}' on the current machine."
        )
        BatchInterface._navigate_same_host(directory)

    @classmethod
    def read_remote_file(cls, host: str, file: Path) -> str:
        # file is always on shared storage
        _ = host
        try:
            return file.read_text()
        except Exception as e:
            raise QQError(f"Could not read file '{file}': {e}.") from e

    @classmethod
    def write_remote_file(cls, host: str, file: Path, content: str) -> None:
        # file is always on shared storage
        _ = host
        try:
            file.write_text(content)
        except Exception as e:
            raise QQError(f"Could not write file '{file}': {e}.") from e

    @classmethod
    def make_remote_dir(cls, host: str, directory: Path) -> None:
        # directory is always on shared storage
        _ = host
        try:
            directory.mkdir(exist_ok=True)
        except Exception as e:
            raise QQError(f"Could not create a directory '{directory}': {e}.") from e

    @classmethod
    def list_remote_dir(cls, host: str, directory: Path) -> list[Path]:
        # directory is always on shared storage
        _ = host
        try:
            return list(directory.iterdir())
        except Exception as e:
            raise QQError(f"Could not list a directory '{directory}': {e}.") from e

    @classmethod
    def delete_remote_dir(cls, host: str, directory: Path) -> None:
        # directory is always on shared storage
        _ = host
        try:
            shutil.rmtree(directory)
        except Exception as e:
            raise QQError(f"Could not delete directory '{directory}': {e}.") from e

    @classmethod
    def move_remote_files(
        cls, host: str, files: list[Path], moved_files: list[Path]
    ) -> None:
        if len(files) != len(moved_files):
            raise QQError(
                "The provided 'files' and 'moved_files' must have the same length."
            )

        # always on shared storage
        _ = host
        for src, dst in zip(files, moved_files):
            shutil.move(str(src), str(dst))

    @classmethod
    def sync_with_exclusions(
        cls,
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        exclude_files: list[Path] | None = None,
    ) -> None:
        # always on shared storage
        _ = src_host
        _ = dest_host
        BatchInterface.sync_with_exclusions(
            src_dir, dest_dir, None, None, exclude_files
        )

    @classmethod
    def sync_selected(
        cls,
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        include_files: list[Path] | None = None,
    ) -> None:
        # always on shared storage
        _ = src_host
        _ = dest_host
        BatchInterface.sync_selected(src_dir, dest_dir, None, None, include_files)

    @classmethod
    def transform_resources(
        cls, queue: str, server: str | None, provided_resources: Resources
    ) -> Resources:
        # server is unused
        _ = server

        # default resources of the queue
        default_queue_resources = SlurmQueue(queue).get_default_resources()
        # default server or hard-coded resources
        default_batch_resources = cls._get_default_server_resources()

        # fill in default parameters
        resources = Resources.merge_resources(
            provided_resources, default_queue_resources, default_batch_resources
        )
        if not resources.work_dir:
            raise QQError(
                "Work-dir is not set after filling in default attributes. This is a bug."
            )

        if provided_resources.work_size_per_cpu or provided_resources.work_size:
            logger.warning(
                "Setting work-size is not supported in this environment. Working directory has a virtually unlimited capacity."
            )

        if not any(
            equals_normalized(resources.work_dir, dir)
            for dir in cls.get_supported_work_dir_types()
        ):
            raise QQError(
                f"Unknown working directory type specified: work-dir='{resources.work_dir}'. Supported types for {cls.env_name()} are: {' '.join(cls.get_supported_work_dir_types())}."
            )

        return resources

    @classmethod
    def is_shared(cls, directory: Path) -> bool:
        _ = directory
        # always on shared storage
        return True

    @classmethod
    def resubmit(
        cls, input_machine: str, input_dir: Path, command_line: list[str]
    ) -> None:
        # input machine is unused, resubmit from the current machine
        _ = input_machine

        qq_submit_command = f"{CFG.binary_name} submit {' '.join(command_line)}"

        logger.debug(f"Navigating to '{input_dir}' to execute '{qq_submit_command}'.")
        try:
            os.chdir(input_dir)
        except Exception as e:
            raise QQError(
                f"Could not resubmit the job. Could not navigate to '{input_dir}': {e}."
            ) from e

        logger.debug(f"Navigated to {str(input_dir)}.")
        result = subprocess.run(
            ["bash"],
            input=qq_submit_command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(f"Could not resubmit the job: {result.stderr.strip()}.")

    @classmethod
    def _get_default_resources(cls) -> Resources:
        return Resources(
            nnodes=1,
            ncpus_per_node=128,
            mem_per_cpu="1gb",
            work_dir="scratch",
            walltime="1d",
        )
