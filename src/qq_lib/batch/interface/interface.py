# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from __future__ import annotations

import inspect
import os
import socket
import subprocess
from abc import ABC, ABCMeta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from qq_lib.core.common import convert_absolute_to_relative
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.core.logical_paths import logical_resolve

from .job import BatchJobInterface
from .node import BatchNodeInterface
from .queue import BatchQueueInterface

if TYPE_CHECKING:
    from qq_lib.properties.depend import Depend
    from qq_lib.properties.resources import Resources

logger = get_logger(__name__)

"""
Type alias for a batch system class.
"""
type AnyBatchClass = type[BatchInterface[Any, Any, Any]]


class _BatchMeta(ABCMeta):
    """
    Metaclass for batch system classes.

    Provides automatic registration of concrete subclasses and lookup
    methods that are callable directly on BatchInterface (or any class
    using this metaclass).

    Examples:
        BatchSystem = BatchInterface.from_env_var_or_guess()
        BatchSystem = BatchInterface.from_str("PBS")
    """

    _registry: dict[str, AnyBatchClass] = {}

    def __init__(cls, name: str, bases: tuple[type, ...], namespace: dict) -> None:
        super().__init__(name, bases, namespace)
        # register every concrete (non-abstract) subclass automatically
        if any(isinstance(b, _BatchMeta) for b in bases) and not inspect.isabstract(
            cls
        ):
            batch_cls = cast("type[BatchInterface]", cls)
            _BatchMeta._registry[batch_cls.env_name()] = batch_cls

    def __str__(cls: type[BatchInterface]) -> str:
        """
        Get the string representation of the batch system class.
        """
        return cls.env_name()

    def from_str(cls, name: str) -> type[BatchInterface]:
        """
        Return the batch system class registered under the given name.

        Raises:
            QQError: If no class is registered for the given name.
        """
        try:
            return _BatchMeta._registry[name]
        except KeyError as e:
            raise QQError(f"No batch system registered as '{name}'.") from e

    def guess(cls) -> type[BatchInterface]:
        """
        Return the first registered batch system that reports itself
        as available.

        Raises:
            QQError: If no available batch system is found.
        """
        for BatchSystem in _BatchMeta._registry.values():
            if BatchSystem.is_available():
                logger.debug(f"Guessed batch system: {BatchSystem}.")
                return BatchSystem

        raise QQError(
            "Could not guess a batch system. No registered batch system available."
        )

    def from_env_var_or_guess(cls) -> type[BatchInterface]:
        """
        Select a batch system from the `QQ_BATCH_SYSTEM` environment
        variable, falling back to `guess` when the variable is unset.

        Raises:
            QQError: If the variable names an unknown system, or if no
                     available system can be guessed.
        """
        name = os.environ.get(CFG.env_vars.batch_system)
        if name:
            logger.debug(
                f"Using batch system name from an environment variable: {name}."
            )
            return cls.from_str(name)

        return cls.guess()

    def obtain(cls, name: str | None) -> type[BatchInterface]:
        """
        Obtain a batch system class by explicit name, environment
        variable, or automatic detection - in that priority order.

        Args:
            name: Optional batch system name.  When `None`, delegates
                  to `from_env_var_or_guess`.

        Raises:
            QQError: If the name is unknown or no system can be resolved.
        """
        if name:
            return cls.from_str(name)

        return cls.from_env_var_or_guess()


class BatchInterface[
    TBatchJob: BatchJobInterface = BatchJobInterface,
    TBatchQueue: BatchQueueInterface = BatchQueueInterface,
    TBatchNode: BatchNodeInterface = BatchNodeInterface,
](ABC, metaclass=_BatchMeta):
    """
    Abstract base class for batch system integrations.

    Concrete batch system classes must implement these methods to allow
    qq to interact with different batch systems uniformly.

    All functions should raise QQError when encountering an error.
    """

    # magic number indicating unreachable directory when navigating to it
    _CD_FAIL = 94
    # exit code of ssh if connection fails
    _SSH_FAIL = 255

    @classmethod
    def env_name(cls) -> str:
        """
        Return the name of the batch system environment.

        Returns:
            str: The batch system name.
        """
        raise NotImplementedError(
            f"env_name method is not implemented for {cls.__name__}"
        )

    @classmethod
    def is_available(cls) -> bool:
        """
        Determine whether the batch system is available on the current host.

        Implementations typically verify this by checking for the presence
        of required commands or other environment-specific indicators.

        Returns:
            bool: True if the batch system is available, False otherwise.
        """
        raise NotImplementedError(
            f"is_available method is not implemented for {cls.__name__}"
        )

    @classmethod
    def get_job_id(cls) -> str | None:
        """
        Get the id of the current job from the corresponding batch system's environment variable.

        For this method to work, it has to be called from the inside of an active job.

        Returns:
            str | None: Index of the job or None if the collective variable is not set.
        """
        raise NotImplementedError(
            f"get_job_id method is not implemented for {cls.__name__}"
        )

    @classmethod
    def create_work_dir_on_scratch(cls, job_id: str) -> Path:
        """
        Create the working directory on scratch for the given job.

        Args:
            job_id (int): Unique identifier of the job.

        Returns:
            Path: Absolute path to the working directory on scratch.

        Raises:
            QQError: If the working directory could not be created.
        """
        raise NotImplementedError(
            f"create_work_dir_on_scratch method is not implemented for {cls.__name__}"
        )

    @classmethod
    def job_submit(
        cls,
        res: Resources,
        queue: str,
        script: Path,
        job_name: str,
        depend: list[Depend],
        env_vars: dict[str, str],
        account: str | None = None,
        server: str | None = None,
    ) -> str:
        """
        Submit a job to the batch system.

        Can also perform additional validation of the job's resources.

        Args:
            res (Resources): Resources required for the job.
            queue (str): Target queue for the job submission.
            script (Path): Path to the script to execute.
            job_name (str): Name of the job to use.
            depend (list[Depend]): List of job dependencies.
            env_vars (dict[str, str]): Dictionary of environment variables to propagate to the job.
            account (str | None): Optional account name to use for the job.
            server (str | None): Optional name of the server to submit the job to.

        Returns:
            str: Unique ID of the submitted job.

        Raises:
            QQError: If the job submission fails.
        """
        raise NotImplementedError(
            f"job_submit method is not implemented for {cls.__name__}"
        )

    @classmethod
    def job_kill(cls, job_id: str) -> None:
        """
        Terminate a job gracefully. The job should have time for proper cleanup.

        Args:
            job_id (str): Identifier of the job to terminate.

        Raises:
            QQError: If the job could not be killed.
        """
        raise NotImplementedError(
            f"job_kill method is not implemented for {cls.__name__}"
        )

    @classmethod
    def job_kill_force(cls, job_id: str) -> None:
        """
        Forcefully terminate a job. There may be no time for proper cleanup.

        Args:
            job_id (str): Identifier of the job to forcefully terminate.

        Raises:
            QQError: If the job could not be killed.
        """
        raise NotImplementedError(
            f"job_kill_force method is not implemented for {cls.__name__}"
        )

    @classmethod
    def get_batch_job(cls, job_id: str) -> TBatchJob:
        """
        Retrieve information about a job from the batch system.

        The returned object should be fully initialized, even if the job
        no longer exists or its information is unavailable.

        Args:
            job_id (str): Identifier of the job.

        Returns:
            BatchJobInterface: Object containing the job's metadata and state.
        """
        raise NotImplementedError(
            f"get_batch_job method is not implemented for {cls.__name__}"
        )

    @classmethod
    def get_unfinished_batch_jobs(
        cls, user: str, server: str | None = None
    ) -> list[TBatchJob]:
        """
        Retrieve information about all uncompleted jobs submitted by `user`
        on the specified or default batch server.

        The jobs can be returned in arbitrary order.

        Args:
            user (str): Username for which to fetch uncompleted jobs.
            server (str | None): Optional name of the batch server to get jobs from.

        Returns:
            list[BatchJobInterface]: A list of job info objects representing the user's uncompleted jobs.
        """
        raise NotImplementedError(
            f"get_unfinished_batch_jobs method is not implemented for {cls.__name__}"
        )

    @classmethod
    def get_batch_jobs(cls, user: str, server: str | None = None) -> list[TBatchJob]:
        """
        Retrieve information about all jobs submitted by a specific user (including finished jobs)
        on the specified or default batch server.

        The jobs can be returned in arbitrary order.

        Args:
            user (str): Username for which to fetch all jobs.
            server (str | None): Optional name of the batch server to get jobs from.

        Returns:
            list[BatchJobInterface]: A list of job info objects representing all jobs of the user.
        """
        raise NotImplementedError(
            f"get_batch_jobs method is not implemented for {cls.__name__}"
        )

    @classmethod
    def get_all_unfinished_batch_jobs(
        cls, server: str | None = None
    ) -> list[TBatchJob]:
        """
        Retrieve information about uncompleted jobs of all users on the specified or default batch server.

        The jobs can be returned in arbitrary order.

        Args:
            server (str | None): Optional name of the batch server to get jobs from.

        Returns:
            list[BatchJobInterface]: A list of job info objects representing uncompleted jobs of all users.
        """
        raise NotImplementedError(
            f"get_all_unfinished_batch_jobs method is not implemented for {cls.__name__}"
        )

    @classmethod
    def get_all_batch_jobs(cls, server: str | None = None) -> list[TBatchJob]:
        """
        Retrieve information about all jobs of all users on the specified or default batch server.

        The jobs can be returned in arbitrary order.

        Args:
            server (str | None): Optional name of the batch server to get jobs from.

        Returns:
            list[BatchJobInterface]: A list of job info objects representing all jobs of all users.
        """
        raise NotImplementedError(
            f"get_all_batch_jobs method is not implemented for {cls.__name__}"
        )

    @classmethod
    def get_queues(cls, server: str | None = None) -> list[TBatchQueue]:
        """
        Retrieve all queues managed by the batch system on the specified or default batch server.

        Args:
            server (str | None): Optional name of the batch server to get queues from.

        Returns:
            list[BatchQueueInterface]: A list of queue objects existing in the batch system.
        """
        raise NotImplementedError(
            f"get_queues method is not implemented for {cls.__name__}"
        )

    @classmethod
    def get_nodes(cls, server: str | None = None) -> list[TBatchNode]:
        """
        Retrieve all nodes managed by the batch system on the specified or default batch server.

        Args:
            server (str | None): Optional name of the batch server to get nodes from.

        Returns:
            list[BatchNodeInterface]: A list of node objects existing in the batch system.
        """
        raise NotImplementedError(
            f"get_nodes method is not implemented for {cls.__name__}"
        )

    @classmethod
    def get_supported_work_dir_types(cls) -> list[str]:
        """
        Retrieve the list of supported types of working directories
        (i.e., strings that can be used with the `--work-dir` option).

        Returns:
            list[str]: A list of supported types of working directories.
        """
        raise NotImplementedError(
            f"get_supported_work_dir_types method is not implemented for {cls.__name__}"
        )

    @classmethod
    def navigate_to_destination(cls, host: str, directory: Path) -> None:
        """
        Open a new terminal on the specified host and change the working directory
        to the given path, handing control over to the user.

        Default behavior:
            - If the target host is different from the current host, SSH is used
            to connect and `cd` is executed to switch to the directory.
            Note that the timeout for the SSH connection is set to `CFG.timeouts.ssh` seconds.
            - If the target host matches the current host, only `cd` is used.

        A new terminal should always be opened, regardless of the host.

        Args:
            host (str): Hostname where the directory is located.
            directory (Path): Directory path to navigate to.

        Raises:
            QQError: If the navigation fails.
        """
        # if the directory is on the current host, we do not need to use ssh
        if host == socket.getfqdn():
            cls._navigate_same_host(directory)
            return

        # the directory is on an another node
        ssh_command = cls._translate_ssh_command(host, directory)
        logger.debug(f"Using ssh: '{' '.join(ssh_command)}'")
        result = subprocess.run(ssh_command)

        # the subprocess exit code can come from:
        # - SSH itself failing - returns _SSH_FAIL
        # - the explicit exit code we set if 'cd' to the directory fails - returns _CD_FAIL
        # - the exit code of the last command the user runs in the interactive shell
        #
        # we ignore user exit codes entirely and only treat _SSH_FAIL and _CD_FAIL as errors
        if result.returncode == cls._SSH_FAIL:
            raise QQError(
                f"Could not reach '{host}:{str(directory)}': Could not connect to host."
            )
        if result.returncode == cls._CD_FAIL:
            raise QQError(
                f"Could not reach '{host}:{str(directory)}': Could not change directory."
            )

    @classmethod
    def read_remote_file(cls, host: str, file: Path) -> str:
        """
        Read the contents of a file on a remote host and return it as a string.

        The default implementation uses SSH to retrieve the file contents.
        This approach may be inefficient on shared storage or high-latency networks.
        Note that the timeout for the SSH connection is set to `CFG.timeouts.ssh` seconds.

        Subclasses should override this method to provide a more efficient implementation
        if possible.

        Args:
            host (str): The hostname of the remote machine where the file resides.
            file (Path): The path to the file on the remote host.

        Returns:
            str: The contents of the remote file.

        Raises:
            QQError: If the file cannot be read or SSH fails.
        """
        result = subprocess.run(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                "-o GSSAPIAuthentication=yes",
                "-o StrictHostKeyChecking=no",  # allow unknown hosts
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
                "-q",  # suppress some SSH messages
                host,
                f"cat {file}",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not read remote file '{file}' on '{host}': {result.stderr.strip()}."
            )
        return result.stdout

    @classmethod
    def write_remote_file(cls, host: str, file: Path, content: str) -> None:
        """
        Write the given content to a file on a remote host, overwriting it if it exists.

        The default implementation uses SSH to send the content to the remote file.
        This approach may be inefficient on shared storage or high-latency networks.
        Note that the timeout for the SSH connection is set to `CFG.timeouts.ssh` seconds.

        Subclasses should override this method to provide a more efficient implementation
        if possible.

        Args:
            host (str): The hostname of the remote machine where the file resides.
            file (Path): The path to the file on the remote host.
            content (str): The content to write to the remote file.

        Raises:
            QQError: If the file cannot be written or SSH fails.
        """

        result = subprocess.run(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                "-o GSSAPIAuthentication=yes",
                "-o StrictHostKeyChecking=no",  # allow unknown hosts
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
                host,
                f"cat > {file}",
            ],
            input=content,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not write to remote file '{file}' on '{host}': {result.stderr.strip()}."
            )

    @classmethod
    def make_remote_dir(cls, host: str, directory: Path) -> None:
        """
        Create a directory at the specified path on a remote host.

        The default implementation uses SSH to run `mkdir` on the remote host.
        This approach may be inefficient on shared storage or high-latency networks.
        Note that the timeout for the SSH connection is set to `CFG.timeouts.ssh` seconds.

        Subclasses should override this method to provide a more efficient implementation
        if possible.

        Args:
            host (str): The hostname of the remote machine where the directory should be created.
            directory (Path): The path of the directory to create on the remote host.

        Raises:
            QQError: If the directory cannot be created but does not already exist or the SSH command fails.
        """
        result = subprocess.run(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                "-o GSSAPIAuthentication=yes",
                "-o StrictHostKeyChecking=no",  # allow unknown hosts
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
                host,
                # ignore an error if the directory already exists
                f"mkdir {directory} 2>/dev/null || [ -d {directory} ]",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not make remote directory '{directory}' on '{host}': {result.stderr.strip()}."
            )

    @classmethod
    def list_remote_dir(cls, host: str, directory: Path) -> list[Path]:
        """
        List all files and directories (absolute paths) in the specified directory on a remote host.

        The default implementation uses SSH to run `ls -A` on the remote host.
        This approach may be inefficient on shared storage or high-latency networks.
        Note that the timeout for the SSH connection is set to `CFG.timeouts.ssh` seconds.

        Subclasses should override this method to provide a more efficient implementation
        if possible.

        Args:
            host (str): The hostname of the remote machine where the directory resides.
            directory (Path): The remote directory to list.

        Returns:
            list[Path]: A list of `Path` objects representing the entries inside the directory.
                        Entries are relative to the given `directory`.

        Raises:
            QQError: If the directory cannot be listed or the SSH command fails.
        """
        result = subprocess.run(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                "-o GSSAPIAuthentication=yes",
                "-o StrictHostKeyChecking=no",  # allow unknown hosts
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
                host,
                f"ls -A {directory}",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not list remote directory '{directory}' on '{host}': {result.stderr.strip()}."
            )

        # split by newline and filter out empty lines
        return [
            logical_resolve(Path(directory) / line)
            for line in result.stdout.splitlines()
            if line.strip()
        ]

    @classmethod
    def delete_remote_dir(cls, host: str, directory: Path) -> None:
        """
        Delete a directory on a remote host.

        The default implementation uses SSH to run `rm -r` on the remote host.
        This approach may be inefficient on shared storage or high-latency networks.
        Note that the timeout for the SSH connection is set to `CFG.timeouts.ssh` seconds.

        Subclasses should override this method to provide a more efficient implementation
        if possible.

        Args:
            host (str): The hostname of the remote machine where the directory resides.
            directory (Path): The remote directory to delete.

        Raises:
            QQError: If the directory cannot be deleted or the SSH command fails.
        """
        result = subprocess.run(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                "-o GSSAPIAuthentication=yes",
                "-o StrictHostKeyChecking=no",  # allow unknown hosts
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
                host,
                f"yes | rm -r {directory}",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not delete remote directory '{directory}' on '{host}': {result.stderr.strip()}."
            )

    @classmethod
    def move_remote_files(
        cls, host: str, files: list[Path], moved_files: list[Path]
    ) -> None:
        """
        Move files on a remote host from their current paths to new paths.

        The default implementation uses SSH to run a sequence of `mv` commands on the remote host.
        This approach may be inefficient on shared storage or high-latency networks.
        Note that the timeout for the SSH connection is set to `CFG.timeouts.ssh` seconds.

        Subclasses should override this method to provide a more efficient implementation
        if possible.

        Args:
            host (str): The hostname of the remote machine where the files reside.
            files (list[Path]): A list of source file paths on the remote host.
            moved_files (list[Path]): A list of destination file paths on the remote host.
                                    Must be the same length as `files`.

        Raises:
            QQError: If the SSH command fails, the files cannot be moved or
                    the length of `files` does not match the length of `moved_files`.
        """
        mv_command = cls._translate_move_command(files, moved_files)

        result = subprocess.run(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                "-o GSSAPIAuthentication=yes",
                "-o StrictHostKeyChecking=no",  # allow unknown hosts
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
                host,
                mv_command,
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not move files on a remote host '{host}': {result.stderr.strip()}."
            )

    @classmethod
    def sync_with_exclusions(
        cls,
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        exclude_files: list[Path] | None = None,
    ) -> None:
        """
        Synchronize the contents of two directories using rsync, optionally across remote hosts,
        while excluding specified files or subdirectories.

        All files and directories in `src_dir` are copied to `dest_dir` except
        those listed in `exclude_files`. Files are never removed from the destination.

        Args:
            src_dir (Path): Source directory to sync from.
            dest_dir (Path): Destination directory to sync to.
            src_host (str | None): Optional hostname of the source machine if remote;
                None if the source is local.
            dest_host (str | None): Optional hostname of the destination machine if remote;
                None if the destination is local.
            exclude_files (list[Path] | None): Optional list of absolute file paths to exclude from syncing.
                These will be converted to paths relative to `src_dir`.

        Raises:
            QQError: If the rsync command fails for any reason or timeouts.
        """
        # convert absolute paths of files to exclude into relative to src_dir
        relative_excluded = (
            convert_absolute_to_relative(exclude_files, src_dir)
            if exclude_files
            else []
        )

        command = cls._translate_rsync_excluded_command(
            src_dir, dest_dir, src_host, dest_host, relative_excluded
        )
        logger.debug(f"Rsync command: {command}.")

        cls._run_rsync(src_dir, dest_dir, src_host, dest_host, command)

    @classmethod
    def sync_selected(
        cls,
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        include_files: list[Path] | None = None,
    ) -> None:
        """
        Synchronize only the explicitly selected files and directories from the source
        to the destination, optionally across remote hosts.

        Only files listed in `include_files` are copied from `src_dir` to `dest_dir`.
        Files not listed are ignored. Files are never removed from the destination.

        Args:
            src_dir (Path): Source directory to sync from.
            dest_dir (Path): Destination directory to sync to.
            src_host (str | None): Optional hostname of the source machine if remote;
                None if the source is local.
            dest_host (str | None): Optional hostname of the destination machine if remote;
                None if the destination is local.
            include_files (list[Path] | None): Optional list of absolute file paths to include in syncing.
                These paths are converted relative to `src_dir`.
                This argument is optional only for consistency with sync_with_exclusions.

        Raises:
            QQError: If the rsync command fails or times out.
        """
        # convert absolute paths of files to include relative to src_dir
        relative_included = (
            convert_absolute_to_relative(include_files, src_dir)
            if include_files
            else []
        )

        command = cls._translate_rsync_included_command(
            src_dir, dest_dir, src_host, dest_host, relative_included
        )
        logger.debug(f"Rsync command: {command}.")

        cls._run_rsync(src_dir, dest_dir, src_host, dest_host, command)

    @classmethod
    def transform_resources(
        cls, queue: str, server: str | None, provided_resources: Resources
    ) -> Resources:
        """
        Transform user-provided Resources into a batch system-specific Resources instance.

        This method takes the resources provided during submission and returns a new
        Resources object with any necessary modifications or defaults applied for
        the target batch system. The original `provided_resources` object is not modified.

        Args:
            queue (str): The name of the queue for which the resources are being adapted.
            server (str | None): Name of the server on which the queue is located.
                If `None`, the queue is treated as being located on the current server.
            provided_resources (Resources): The raw resources specified by the user.

        Returns:
            Resources: A new Resources instance with batch system-specific adjustments,
                        fully constructed and validated.

        Raises:
            QQError: If any of the provided parameters are invalid or inconsistent.
        """
        raise NotImplementedError(
            f"transform_resources method is not implemented for {cls.__name__}"
        )

    @classmethod
    def is_shared(cls, directory: Path) -> bool:
        """
        Determine whether a given directory resides on a shared filesystem.

        Args:
            directory (Path): The directory to check.

        Returns:
            bool: True if the directory is on a shared filesystem, False if it is local.
        """
        # df -l exits with zero if the filesystem is local; otherwise it exits with a non-zero code
        result = subprocess.run(
            ["df", "-l", directory],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        return result.returncode != 0

    @classmethod
    def resubmit(
        cls, input_machine: str, input_dir: Path, command_line: list[str]
    ) -> None:
        """
        Resubmit a job to the batch system.

        The default implementation connects via SSH to the specified machine,
        changes into the job directory, and re-executes the original job
        submission command (`qq submit ...`).

        If the resubmission fails, a QQError is raised.

        Args:
            input_machine (str): Name of the host from which the job is to be submitted.
            input_dir (Path): Path to the job's input directory.
            command_line (list[str]): Options and arguments to use for submitting.

        Raises:
            QQError: If the resubmission fails (non-zero return code from the
            SSH command).
        """
        qq_submit_command = f"{CFG.binary_name} submit {' '.join(command_line)}"

        logger.debug(
            f"Navigating to '{input_machine}:{str(input_dir)}' to execute '{qq_submit_command}'."
        )
        result = subprocess.run(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                "-o GSSAPIAuthentication=yes",
                "-o StrictHostKeyChecking=no",  # allow unknown hosts
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
                "-q",  # suppress some SSH messages
                input_machine,
                f"cd {str(input_dir)} && {qq_submit_command}",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not resubmit the job on '{input_machine}': {result.stderr.strip()}."
            )

    @classmethod
    def sort_jobs(cls, jobs: list[TBatchJob]) -> None:
        """
        Sort a list of batch system jobs by a defined attribute.

        The default implementation sorts the jobs alphabetically by their job ID,
        as returned by `job.get_id()`. Subclasses may override this method to
        implement custom sorting logic.

        Args:
            jobs (list[BatchJobInterface]): A list of batch job objects to be sorted
                in-place.
        """
        jobs.sort(key=lambda job: job.get_id())

    @classmethod
    def jobs_presenter_columns_to_show(cls) -> set[str]:
        """
        Get a set of columns that should be shown in the output of JobsPresenter (`qq jobs`)
        for this batch system.

        In the default implementation, all columns are shown.

        Note that the 'Exit' column is not shown when printing queued and running jobs,
        even if you specify it here.

        Args:
            set[str]: Set of column titles that should be shown.
        """
        return {
            "S",
            "Job ID",
            "User",
            "Job Name",
            "Queue",
            "NCPUs",
            "NGPUs",
            "NNodes",
            "Times",
            "Node",
            "%CPU",
            "%Mem",
            "Exit",
        }

    @classmethod
    def _translate_ssh_command(cls, host: str, directory: Path) -> list[str]:
        """
        Construct the SSH command to navigate to a remote directory.

        This is an internal method of `BatchInterface`; you typically should not override it.

        Args:
            host (str): The hostname of the remote machine.
            directory (Path): The target directory to navigate to.

        Returns:
            list[str]: SSH command as a list suitable for subprocess execution.
        """
        return [
            "ssh",
            "-o PasswordAuthentication=no",  # never ask for password
            "-o GSSAPIAuthentication=yes",  # allow Kerberos tickets
            "-o StrictHostKeyChecking=no",  # allow unknown hosts
            f"-o ConnectTimeout={CFG.timeouts.ssh}",
            host,
            "-t",
            f"cd {directory} || exit {cls._CD_FAIL} && exec bash -l",
        ]

    @classmethod
    def _navigate_same_host(cls, directory: Path) -> None:
        """
        Navigate to a directory on the current host using a subprocess.

        This is an internal method of `BatchInterface`; you typically should not override it.

        Args:
            directory (Path): Directory to navigate to.
        """
        logger.debug("Current host is the same as target host. Using 'cd'.")
        if not directory.is_dir():
            raise QQError(
                f"Could not reach '{socket.getfqdn()}:{str(directory)}': Could not change directory."
            )

        subprocess.run(["bash"], cwd=directory)

        # if the directory exists, always report success,
        # no matter what the user does inside the terminal

    @classmethod
    def _translate_move_command(cls, files: list[Path], moved_files: list[Path]) -> str:
        """
        Translate lists of source and destination file paths into a single shell
        command string for moving the files.

        This is an internal method of `BatchInterface`; you typically should not override it.

        Args:
            files (list[Path]): A list of source file paths to be moved.
            moved_files (list[Path]): A list of destination file paths of the same
                length as `files`.

        Returns:
            str: A single shell command string consisting of `mv` commands joined
            with `&&`.

        Raises:
            QQError: If `files` and `moved_files` do not have the same length.
        """
        if len(files) != len(moved_files):
            raise QQError(
                "The provided 'files' and 'moved_files' must have the same length."
            )

        mv_commands: list[str] = []
        for src, dst in zip(files, moved_files):
            mv_commands.append(f"mv '{src}' '{dst}'")

        return " && ".join(mv_commands)

    @classmethod
    def _translate_rsync_excluded_command(
        cls,
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        relative_excluded: list[Path],
    ) -> list[str]:
        """
        Build an rsync command to synchronize a directory while excluding specific files.

        Both `src_host` and `dest_host` should not be set simultaneously,
        otherwise the resulting rsync command will be invalid.

        This is an internal method of `BatchInterface`; you typically should not override it.

        Args:
            src_dir (Path): Source directory path.
            dest_dir (Path): Destination directory path.
            src_host (str | None): Hostname of the source machine if remote;
                None if the source is local.
            dest_host (str | None): Hostname of the destination machine if remote;
                None if the destination is local.
            relative_excluded (list[Path]): List of paths relative to `src_dir`
                to exclude from syncing.

        Returns:
            list[str]: List of command arguments for rsync, suitable for `subprocess.run`.
        """
        # syncing recursively (-r), symlinks copied as symlinks (-l),
        # preserving times (-t), preserving device/special files (-D),
        # but not preserving owners and groups
        # not using --checksum nor --ignore-times for performance reasons
        # some files may potentially not be correctly synced if they were
        # modified in both src_dir and dest_dir at the same time and have
        # the same size -> this should be so extremely rare that we do not care
        command = [
            "rsync",
            "-e",
            # allow Kerberos tickets, never ask for password, allow unknown hosts
            "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=no",
            "-rltD",
        ]
        for file in relative_excluded:
            command.extend(["--exclude", str(file)])

        src = src_host + ":" + str(src_dir) + "/" if src_host else str(src_dir) + "/"
        dest = dest_host + ":" + str(dest_dir) if dest_host else str(dest_dir)
        command.extend([src, dest])

        return command

    @classmethod
    def _translate_rsync_included_command(
        cls,
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        relative_included: list[Path],
    ) -> list[str]:
        """
        Build an rsync command to synchronize only the explicitly included files.

        Both `src_host` and `dest_host` should not be set simultaneously,
        otherwise the resulting rsync command will be invalid.

        This is an internal method of `BatchInterface`; you typically should not override it.

        Args:
            src_dir (Path): Source directory path.
            dest_dir (Path): Destination directory path.
            src_host (str | None): Hostname of the source machine if remote;
                None if the source is local.
            dest_host (str | None): Hostname of the destination machine if remote;
                None if the destination is local.
            relative_included (list[Path]): List of paths relative to `src_dir`
                that should be included in the sync.

        Returns:
            list[str]: List of command arguments for rsync, suitable for `subprocess.run`.
        """

        command = [
            "rsync",
            "-e",
            # allow Kerberos tickets, never ask for password, allow unknown hosts
            "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=no",
            "-rltD",
        ]
        for file in relative_included:
            # if `file` is a file
            command.extend(["--include", str(file)])
            # if `file` is a directory
            # it's okay to include both patterns - if it is invalid, it's ignored
            command.extend(["--include", f"{str(file)}/***"])
        # exclude all files not specifically included
        command.extend(["--exclude", "*"])

        src = src_host + ":" + str(src_dir) + "/" if src_host else str(src_dir) + "/"
        dest = dest_host + ":" + str(dest_dir) if dest_host else str(dest_dir)
        command.extend([src, dest])

        return command

    @classmethod
    def _run_rsync(
        cls,
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        command: list[str],
    ) -> None:
        """
        Execute an rsync command to synchronize files between source and destination.

        This is an internal method of `BatchInterface`; you typically should not override it.

        Args:
            src_dir (Path): Source directory path.
            dest_dir (Path): Destination directory path.
            src_host (str | None): Optional hostname of the source machine if remote;
                None if the source is local.
            dest_host (str | None): Optional hostname of the destination machine if remote;
                None if the destination is local.
            command (list[str]): List of command-line arguments for rsync, typically
                generated by `_translate_rsync_excluded_command` or `_translate_rsync_included_command`.

        Raises:
            QQError: If the rsync command fails (non-zero exit code) or
                if the command times out after `CFG.timeouts.rsync` seconds.
        """
        src = f"{src_host}:{str(src_dir)}" if src_host else str(src_dir)
        dest = f"{dest_host}:{str(dest_dir)}" if dest_host else str(dest_dir)

        try:
            result = subprocess.run(
                command, capture_output=True, text=True, timeout=CFG.timeouts.rsync
            )
        except subprocess.TimeoutExpired as e:
            raise QQError(
                f"Could not rsync files between '{src}' and '{dest}': Connection timed out after {CFG.timeouts.rsync} seconds."
            ) from e

        if result.returncode != 0:
            raise QQError(
                f"Could not rsync files between '{src}' and '{dest}': {result.stderr.strip()}."
            )
