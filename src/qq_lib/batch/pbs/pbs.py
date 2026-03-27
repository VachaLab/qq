# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import os
import shutil
import socket
import subprocess
from collections.abc import Callable
from pathlib import Path

from qq_lib.batch.interface import BatchInterface, BatchMeta
from qq_lib.batch.interface.meta import batch_system
from qq_lib.batch.pbs.common import parse_multi_pbs_dump_to_dictionaries
from qq_lib.batch.pbs.node import PBSNode
from qq_lib.batch.pbs.queue import PBSQueue
from qq_lib.core.common import equals_normalized
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.core.logical_paths import logical_resolve
from qq_lib.properties.depend import Depend
from qq_lib.properties.resources import Resources

from .job import PBSJob

logger = get_logger(__name__)


@batch_system
class PBS(BatchInterface[PBSJob, PBSQueue, PBSNode], metaclass=BatchMeta):
    """
    Implementation of BatchInterface for PBS Pro batch system.
    """

    # all standard scratch directory (excl. in RAM scratch) types supported by PBS
    SUPPORTED_SCRATCHES = ["scratch_local", "scratch_ssd", "scratch_shared"]

    @classmethod
    def env_name(cls) -> str:
        return "PBS"

    @classmethod
    def is_available(cls) -> bool:
        return shutil.which("qsub") is not None

    @classmethod
    def get_job_id(cls) -> str | None:
        return os.environ.get("PBS_JOBID")

    @classmethod
    def create_work_dir_on_scratch(cls, job_id: str) -> Path:
        scratch_dir = cls._get_scratch_dir(job_id)

        # create working directory inside the scratch directory allocated by the batch system
        # we create this directory because other processes may write files
        # into the allocated scratch directory and we do not want these files
        # to affect the job execution or be copied back to input_dir
        # this also simplifies deletion of the working directory
        # (the allocated scratch dir cannot be deleted)
        work_dir = logical_resolve(scratch_dir / CFG.pbs_options.scratch_dir_inner)

        logger.debug(f"Creating working directory '{str(work_dir)}'.")
        work_dir.mkdir(exist_ok=True)

        return work_dir

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
        # account unused
        _ = account

        cls._shared_guard(res, env_vars, server)

        # set env vars required for Infinity modules
        # this can be removed once Infinity stops being supported
        env_vars.update(cls._collect_ams_env_vars())

        # if we are submitting to a different server, we need to change the AMS site
        # this can be removed once Infinity stops being supported
        if server:
            cls._modify_ams_env_vars(env_vars, server)

        # get the submission command
        command = cls._translate_submit(
            res,
            queue,
            server,
            script.parent,
            str(script),
            job_name,
            depend,
            env_vars,
        )
        logger.debug(command)

        # submit the script
        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(
                f"Failed to submit script '{str(script)}': {result.stderr.strip()}."
            )

        return result.stdout.strip()

    @classmethod
    def job_kill(cls, job_id: str) -> None:
        command = cls._translate_kill(job_id)
        logger.debug(command)

        # run the kill command
        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(f"Failed to kill job '{job_id}': {result.stderr.strip()}.")

    @classmethod
    def job_kill_force(cls, job_id: str) -> None:
        command = cls._translate_kill_force(job_id)
        logger.debug(command)

        # run the kill command
        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(f"Failed to kill job '{job_id}': {result.stderr.strip()}.")

    @classmethod
    def get_batch_job(cls, job_id: str) -> PBSJob:
        return PBSJob(job_id)

    @classmethod
    def get_unfinished_batch_jobs(
        cls, user: str, server: str | None = None
    ) -> list[PBSJob]:
        command = f"qstat -fwtu {user}"
        if server:
            command += f" @{server}"
        logger.debug(command)
        return cls._get_batch_jobs_using_command(command, include_completed=False)

    @classmethod
    def get_batch_jobs(cls, user: str, server: str | None = None) -> list[PBSJob]:
        command = f"qstat -fwxtu {user}"
        if server:
            command += f" @{server}"
        logger.debug(command)
        return cls._get_batch_jobs_using_command(command, include_completed=True)

    @classmethod
    def get_all_unfinished_batch_jobs(cls, server: str | None = None) -> list[PBSJob]:
        command = "qstat -fwt"
        if server:
            command += f" @{server}"
        logger.debug(command)
        return cls._get_batch_jobs_using_command(command, include_completed=False)

    @classmethod
    def get_all_batch_jobs(cls, server: str | None = None) -> list[PBSJob]:
        command = "qstat -fxwt"
        if server:
            command += f" @{server}"
        logger.debug(command)
        return cls._get_batch_jobs_using_command(command, include_completed=True)

    @classmethod
    def get_queues(cls, server: str | None = None) -> list[PBSQueue]:
        command = "qstat -Qfw"
        if server:
            command += f" @{server}"
        logger.debug(command)

        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not retrieve information about queues: {result.stderr.strip()}."
            )

        queues = []
        for data, name in parse_multi_pbs_dump_to_dictionaries(
            result.stdout.strip(), "Queue"
        ):
            queues.append(PBSQueue.from_dict(name, server, data))

        return queues

    @classmethod
    def get_nodes(cls, server: str | None = None) -> list[PBSNode]:
        command = "pbsnodes -a"
        if server:
            command += f" -s {server}"
        logger.debug(command)

        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not retrieve information about nodes: {result.stderr.strip()}."
            )

        queues = []
        for data, name in parse_multi_pbs_dump_to_dictionaries(
            result.stdout.strip(), None
        ):
            queues.append(PBSNode.from_dict(name, server, data))

        return queues

    @classmethod
    def get_supported_work_dir_types(cls) -> list[str]:
        return cls.SUPPORTED_SCRATCHES + [
            "scratch_shm",
            "input_dir",
            "job_dir",  # same as input_dir
        ]

    @classmethod
    def read_remote_file(cls, host: str, file: Path) -> str:
        if os.environ.get(CFG.env_vars.shared_submit):
            # file is on shared storage, we can read it directly
            # this assumes that this method is only used to read files in input_dir
            logger.debug(f"Reading a file '{file}' from shared storage.")
            try:
                return file.read_text()
            except Exception as e:
                raise QQError(f"Could not read file '{file}': {e}.") from e
        else:
            # otherwise, we fall back to the default implementation
            logger.debug(f"Reading a remote file '{file}' on '{host}'.")
            return super().read_remote_file(host, file)

    @classmethod
    def write_remote_file(cls, host: str, file: Path, content: str) -> None:
        if os.environ.get(CFG.env_vars.shared_submit):
            # file should be written to shared storage
            # this assumes that the method is only used to write files into input_dir
            logger.debug(f"Writing a file '{file}' to shared storage.")
            try:
                file.write_text(content)
            except Exception as e:
                raise QQError(f"Could not write file '{file}': {e}.") from e
        else:
            # otherwise, we fall back to the default implementation
            logger.debug(f"Writing a remote file '{file}' on '{host}'.")
            super().write_remote_file(host, file, content)

    @classmethod
    def make_remote_dir(cls, host: str, directory: Path) -> None:
        if os.environ.get(CFG.env_vars.shared_submit):
            # assuming the directory is created in input_dir
            logger.debug(f"Creating a directory '{directory}' on shared storage.")
            try:
                directory.mkdir(exist_ok=True)
            except Exception as e:
                raise QQError(
                    f"Could not create a directory '{directory}': {e}."
                ) from e
        else:
            # otherwise we fall back to the default implementation
            logger.debug(f"Creating a directory '{directory}' on '{host}'.")
            super().make_remote_dir(host, directory)

    @classmethod
    def list_remote_dir(cls, host: str, directory: Path) -> list[Path]:
        if os.environ.get(CFG.env_vars.shared_submit):
            # assuming we are listing input_dir or another directory on shared storage
            logger.debug(f"Listing a directory '{directory}' on shared storage.")
            try:
                return list(directory.iterdir())
            except Exception as e:
                raise QQError(f"Could not list a directory '{directory}': {e}.") from e
        else:
            # otherwise we fall back to the default implementation
            logger.debug(f"Listing a directory '{directory}' on '{host}'.")
            return super().list_remote_dir(host, directory)

    @classmethod
    def delete_remote_dir(cls, host: str, directory: Path) -> None:
        if host == socket.getfqdn():
            # directory is available on the current host
            logger.debug(f"Deleting a directory '{directory}' on local host.")
            try:
                shutil.rmtree(directory)
            except Exception as e:
                raise QQError(f"Could not delete directory '{directory}': {e}.") from e
        else:
            # otherwise we fall back to the default implementation
            logger.debug(f"Deleting a directory '{directory}' on '{host}'.")
            return super().delete_remote_dir(host, directory)

    @classmethod
    def move_remote_files(
        cls, host: str, files: list[Path], moved_files: list[Path]
    ) -> None:
        if len(files) != len(moved_files):
            raise QQError(
                "The provided 'files' and 'moved_files' must have the same length."
            )

        if os.environ.get(CFG.env_vars.shared_submit):
            # assuming we are moving files inside input_dir or another directory on shared storage
            logger.debug(
                f"Moving files '{files}' -> '{moved_files}' on a shared storage."
            )
            for src, dst in zip(files, moved_files):
                shutil.move(str(src), str(dst))
        else:
            # otherwise we fall back to the default implementation
            logger.debug(f"Moving files '{files}' -> '{moved_files}' on '{host}'.")
            super().move_remote_files(host, files, moved_files)

    @classmethod
    def sync_with_exclusions(
        cls,
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        exclude_files: list[Path] | None = None,
    ) -> None:
        cls._sync_directories(
            src_dir,
            dest_dir,
            src_host,
            dest_host,
            exclude_files,
            super().sync_with_exclusions,
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
        cls._sync_directories(
            src_dir,
            dest_dir,
            src_host,
            dest_host,
            include_files,
            super().sync_selected,
        )

    @classmethod
    def transform_resources(
        cls, queue: str, server: str | None, provided_resources: Resources
    ) -> Resources:
        # default resources of the queue
        default_queue_resources = PBSQueue(queue, server).get_default_resources()
        # default hard-coded resources
        default_batch_resources = cls._get_default_server_resources()

        # fill in default parameters
        resources = Resources.merge_resources(
            provided_resources, default_queue_resources, default_batch_resources
        )
        if not resources.work_dir:
            raise QQError(
                "Work-dir is not set after filling in default attributes. This is a bug."
            )

        # sanity check input_dir
        if equals_normalized(resources.work_dir, "job_dir") or equals_normalized(
            resources.work_dir, "input_dir"
        ):
            # work-size should not be used with job_dir
            if provided_resources.work_size:
                logger.warning(
                    "Setting work-size is not supported for work-dir='job_dir' or 'input_dir'.\n"
                    "Job will run in the submission directory with unlimited capacity.\n"
                    "The work-size attribute will be ignored."
                )

            resources.work_dir = "input_dir"
            resources.work_size = None
            resources.work_size_per_cpu = None
            return resources

        # scratch in RAM (https://docs.metacentrum.cz/en/docs/computing/infrastructure/scratch-storages#scratch-in-ram)
        if equals_normalized(resources.work_dir, "scratch_shm"):
            # work-size should not be used with scratch_shm
            if provided_resources.work_size:
                logger.warning(
                    "Setting work-size is not supported for work-dir='scratch_shm'.\n"
                    "Size of the in-RAM scratch is specified using the --mem property.\n"
                    "The work-size attribute will be ignored."
                )

            resources.work_dir = "scratch_shm"
            resources.work_size = None
            resources.work_size_per_cpu = None
            return resources

        # if work-dir matches any of the "standard" scratches supported by PBS
        if match := next(
            (
                x
                for x in cls.SUPPORTED_SCRATCHES
                if equals_normalized(x, resources.work_dir)
            ),
            None,
        ):
            resources.work_dir = match
            return resources

        # unknown work-dir type
        raise QQError(
            f"Unknown working directory type specified: work-dir='{resources.work_dir}'. Supported types for {cls.env_name()} are: '{' '.join(cls.get_supported_work_dir_types())}'."
        )

    @classmethod
    def sort_jobs(cls, jobs: list[PBSJob]) -> None:
        # jobs with invalid ID get assigned an ID of 0 for sorting => they are sorted to the start
        # and therefore are displayed at the top in the qq jobs / qq stat output
        jobs.sort(key=lambda job: job.get_id_int() or 0)

    @classmethod
    def _get_scratch_dir(cls, job_id: str) -> Path:
        """
        Get the path to the scratch directory allocated by PBS.
        """
        scratch_dir = os.environ.get(CFG.env_vars.pbs_scratch_dir)
        if not scratch_dir:
            raise QQError(f"Scratch directory for job '{job_id}' is undefined")

        return Path(scratch_dir)

    @classmethod
    def _shared_guard(
        cls, res: Resources, env_vars: dict[str, str], server: str | None
    ) -> None:
        """
        Ensure correct handling of shared vs. local submission directories.

        If the current working directory is on shared storage, adds the
        environment variable `SHARED_SUBMIT` to the list of env vars to propagate to the job.
        This environment variable is later used e.g. to select the appropriate data copying method.

        If the job is configured to use the submission directory as a working directory
        (`work-dir=input_dir` or 'job_dir') but that directory is not shared, a `QQError` is raised.

        If the job is to be submitted to a potentially non-local server
        but the directory is not shared, a `QQError` is raised.

        Args:
            res (Resources): The job's resource configuration.
            env_vars (dict[str, str]): Dictionary of environment variables to propagate to the job.
            server (str | None): The target PBS server, or None if submitting to the default server.

        Raises:
            QQError: If the job is set to run directly in the submission
                    directory while submission is from a non-shared filesystem.
            QQError: If the job is set to run on a non-default server while
                submission is from a non-shared filesystem.
        """
        if cls.is_shared(Path()):
            env_vars[CFG.env_vars.shared_submit] = "true"
        elif not res.uses_scratch():
            # if job directory is used as working directory, it must always be shared
            raise QQError(
                "Job was requested to run directly in the submission directory (work-dir='job_dir' or 'input_dir'), but submission is done from a local filesystem."
            )
        elif server is not None:
            # if we are submitting to a different server
            raise QQError(
                f"Job was requested to be submitted to server '{server}' which is potentially non-local, but the submission is done from a local filesystem."
            )

    @classmethod
    def _translate_submit(
        cls,
        res: Resources,
        queue: str,
        server: str | None,
        input_dir: Path,
        script: str,
        job_name: str,
        depend: list[Depend],
        env_vars: dict[str, str],
    ) -> str:
        """
        Generate the PBS submission command for a job.

        Args:
            res (Resources): The resources requested for the job.
            queue (str): The queue name to submit to.
            server (str): Optional name of the server to submit the job to.
            input_dir (Path): The directory from which the job is being submitted.
            script (str): Path to the job script.
            job_name (str): Name of the job.
            depend (list[Depend]): List of dependencies of the job.
            env_vars (dict[str, str]): Dictionary of environment variables to set.

        Returns:
            str: The fully constructed qsub command string.
        """
        command = f"qsub -N {job_name} {cls._translate_queue_server(queue, server)} {cls._translate_output_server(input_dir, job_name, server)} "

        # translate environment variables
        if env_vars:
            command += f"-v {cls._translate_env_vars(env_vars)} "

        # handle per-chunk resources, incl. workdir
        translated = cls._translate_per_chunk_resources(res)

        # handle properties
        if res.props:
            translated.extend([f"{k}={v}" for k, v in res.props.items()])

        if len(translated) > 0 and res.nnodes and res.nnodes > 1:
            # we only use the select syntax when multiple nodes are requested
            command += f"-l select={res.nnodes}:"
            join_char = ":"
        else:
            command += "-l "
            join_char = ","

        command += join_char.join(translated) + " "

        # handle walltime
        if res.walltime:
            command += f"-l walltime={res.walltime} "

        if res.nnodes and res.nnodes > 1:
            # 'place=scatter' causes each chunk to be placed on a different node
            command += "-l place=vscatter "

        # handle dependencies
        if converted_depend := cls._translate_dependencies(depend):
            command += f"-W depend={converted_depend} "

        # add script
        command += script

        return command

    @classmethod
    def _translate_queue_server(cls, queue: str, server: str | None) -> str:
        """
        Build the PBS queue destination argument for qsub.

        Constructs the `-q` string from the queue name and optional server.
        If a server is specified, the destination is formatted as `-q queue@server`,
        otherwise only the queue name is used.

        Args:
            queue (str): The name of the target queue.
            server (str | None): The target PBS server, or None if submitting to the default server.

        Returns:
            str: The PBS queue destination argument, e.g. `-q queue@server` or `-q queue`.
        """
        if server:
            return f"-q {queue}@{server}"

        return f"-q {queue}"

    @classmethod
    def _translate_output_server(
        cls, input_dir: Path, job_name: str, server: str | None
    ) -> str:
        """
        Build the PBS output redirection arguments for qsub.

        Constructs the `-j eo -e` string pointing to the job's `.qqout` file.
        If a server is specified and has a configured output host, that host is
        prepended to the path as `host:path`. Otherwise, PBS will default to
        delivering the output file to the submitting host.

        Args:
            input_dir (Path): Directory in which the output file will be placed.
            job_name (str): Name of the job, used to construct the output filename.
            server (str | None): The target PBS server, or None if submitting to the default server.

        Returns:
            str: The PBS output redirection arguments.
        """
        qq_output = str((input_dir / job_name).with_suffix(CFG.suffixes.qq_out))
        if server:
            if output_host := CFG.batch_servers_options.known_output_hosts.get(server):
                logger.debug(
                    f"Using output host '{output_host}' for server '{server}'."
                )
                return f"-j eo -e {output_host}:{qq_output}"

            logger.warning(
                f"No output host configured for server '{server}'. "
                "PBS will deliver qqout file to the submitting host (this desktop), "
                "which may fail if it is not accessible from the working node."
            )

        return f"-j eo -e {qq_output}"

    @classmethod
    def _translate_env_vars(cls, env_vars: dict[str, str]) -> str:
        """
        Convert a dictionary of environment variables into a formatted string.

        Args:
            env_vars (dict[str, str]): A mapping of environment variable names
                to their corresponding values.

        Returns:
            str: A comma-separated string of environment variable assignments,
                suitable for inclusion in the qsub command.
        """
        converted = []
        for key, value in env_vars.items():
            converted.append(f"\"{key}='{value}'\"")

        return ",".join(converted)

    @classmethod
    def _translate_per_chunk_resources(cls, res: Resources) -> list[str]:
        """
        Convert a Resources object into a list of per-node resource specifications.

        Each resource that can be divided by the number of nodes (nnodes) is split
        accordingly.

        Args:
            res (Resources): The resource specification for the job.

        Returns:
            list[str]: A list of per-node resource strings suitable for inclusion
                    in a PBS submission command.

        Raises:
            QQError: If sanity checks fail or required memory attributes are missing.
        """

        trans_res = []

        # sanity checking per-chunk resources
        if not res.nnodes:
            raise QQError(
                "Attribute 'nnodes' should not be undefined. This is a bug, please report it."
            )
        if res.nnodes == 0:
            raise QQError("Attribute 'nnodes' cannot be 0.")

        if res.ncpus and res.ncpus != 0 and res.ncpus % res.nnodes != 0:
            raise QQError(
                f"Attribute 'ncpus' ({res.ncpus}) must be divisible by 'nnodes' ({res.nnodes})."
            )
        if res.ngpus and res.ngpus != 0 and res.ngpus % res.nnodes != 0:
            raise QQError(
                f"Attribute 'ngpus' ({res.ngpus}) must be divisible by 'nnodes' ({res.nnodes})."
            )

        # translate per-chunk resources
        if res.ncpus:
            trans_res.append(f"ncpus={res.ncpus // res.nnodes}")
            # we need to specify the number of MPI processes so that mpirun uses the correct
            # number of sockets; this does not mean that the run script has to use one MPI
            # process per CPU core, this value can be overriden
            trans_res.append(f"mpiprocs={res.ncpus // res.nnodes}")
        elif res.ncpus_per_node:
            trans_res.append(f"ncpus={res.ncpus_per_node}")
            trans_res.append(f"mpiprocs={res.ncpus_per_node}")

        if res.mem:
            trans_res.append(f"mem={(res.mem // res.nnodes).to_str_exact()}")
        elif res.mem_per_node:
            trans_res.append(f"mem={res.mem_per_node.to_str_exact()}")
        elif res.mem_per_cpu:
            if res.ncpus:
                trans_res.append(
                    f"mem={(res.mem_per_cpu * res.ncpus // res.nnodes).to_str_exact()}"
                )
            elif res.ncpus_per_node:
                trans_res.append(
                    f"mem={(res.mem_per_cpu * res.ncpus_per_node).to_str_exact()}"
                )
            else:
                raise QQError(
                    "Attribute 'mem-per-cpu' requires attributes 'ncpus' or 'ncpus-per-node' to be defined."
                )
        else:
            # memory not set in any way
            raise QQError(
                "None of the attributes 'mem', 'mem-per-node', or 'mem-per-cpu' is defined."
            )

        if res.ngpus:
            trans_res.append(f"ngpus={res.ngpus // res.nnodes}")
        elif res.ngpus_per_node:
            trans_res.append(f"ngpus={res.ngpus_per_node}")

        # translate work-dir
        if workdir := cls._translate_work_dir(res):
            trans_res.append(workdir)

        return trans_res

    @classmethod
    def _translate_work_dir(cls, res: Resources) -> str | None:
        """
        Translate the working directory and its requested size into a PBS resource string.

        Args:
            res (Resources): The resources requested for the job.

        Returns:
            str | None: Resource string specifying the working directory, or None if input_dir is used.
        """
        assert res.nnodes is not None

        if res.work_dir == "job_dir" or res.work_dir == "input_dir":
            return None

        if res.work_dir == "scratch_shm":
            return f"{res.work_dir}=true"

        if res.work_size:
            return f"{res.work_dir}={(res.work_size // res.nnodes).to_str_exact()}"
        if res.work_size_per_node:
            return f"{res.work_dir}={res.work_size_per_node.to_str_exact()}"
        if res.work_size_per_cpu:
            if res.ncpus:
                return f"{res.work_dir}={(res.work_size_per_cpu * res.ncpus // res.nnodes).to_str_exact()}"
            if res.ncpus_per_node:
                return f"{res.work_dir}={(res.work_size_per_cpu * res.ncpus_per_node).to_str_exact()}"

            raise QQError(
                "Attribute 'work-size-per-cpu' requires attributes 'ncpus' or 'ncpus-per-node' to be defined."
            )

        raise QQError(
            "None of the attributes 'work-size', 'work-size-per-node', or 'work-size-per-cpu' is defined."
        )

    @classmethod
    def _translate_dependencies(cls, depend: list[Depend]) -> str | None:
        """
        Convert a list of `Depend` objects into a PBS-compatible dependency string.

        Args:
            depend (list[Depend]): List of dependency objects to translate.

        Returns:
            str | None: PBS-style dependency string (e.g., "after:12345,afterok:1:2:3"),
                        or None if the input list is empty.
        """
        if not depend:
            return None

        return ",".join(Depend.to_str(x).replace("=", ":") for x in depend)

    @classmethod
    def _collect_ams_env_vars(cls) -> dict[str, str]:
        """
        Collect environment variables for Infinity AMS.
        This allows importing Infinity AMS modules in qq jobs.

        Returns:
            dict[str, str]: Dictionary of AMS environment variables and their values.
        """
        ams_vars = {
            key: value
            for key, value in os.environ.items()
            if key
            in {
                "AMS_ACTIVE_MODULES",
                "AMS_SITE",
                "AMS_SITE_SUPPORT",
                "AMS_EXIT_CODE",
                "AMS_USER_CONFIG_DIR",
                "AMS_GROUPNS",
                "AMS_BUNDLE_NAME",
                "AMS_HOST_GROUP",
                "AMS_ROOT",
                "AMS_ROOT_V9",
                "AMS_BUNDLE_PATH",
            }
        }
        logger.debug(f"AMS vars: {ams_vars}")

        return ams_vars

    @classmethod
    def _modify_ams_env_vars(cls, env_vars: dict[str, str], server: str) -> None:
        """
        Modify environment variables for Infinity AMS if the job is submitted to a different server.
        """
        # bleh, seriously can't wait to get rid of having to support AMS...
        # this is so needlessly complicated

        ams_site_converter = {
            "robox-pro.ceitec.muni.cz": "robox",
            "sokar-pbs.ncbr.muni.cz": "sokar",
            "pbs-m1.metacentrum.cz": "metavo24",
        }

        if server not in ams_site_converter:
            logger.warning(
                f"Server '{server}' is not supported by the qq-AMS translation layer. The job will not have access to AMS modules. Please report this issue."
            )

        if "AMS_SITE" in env_vars:
            env_vars["AMS_SITE"] = ams_site_converter[server]

        ams_site_support_converter = {
            "robox-pro.ceitec.muni.cz": "linuxsupport@ics.muni.cz",
            "sokar-pbs.ncbr.muni.cz": "support@lcc.ncbr.muni.cz",
            "pbs-m1.metacentrum.cz": "support@lcc.ncbr.muni.cz",
        }

        if "AMS_SITE_SUPPORT" in env_vars:
            env_vars["AMS_SITE_SUPPORT"] = ams_site_support_converter[server]

        ams_groupns_converter = {
            "robox-pro.ceitec.muni.cz": "uvt",
            "sokar-pbs.ncbr.muni.cz": "ncbr",
            "pbs-m1.metacentrum.cz": "ics",
        }

        if "AMS_GROUPNS" in env_vars:
            env_vars["AMS_GROUPNS"] = ams_groupns_converter[server]

    @classmethod
    def _get_default_server_resources(cls) -> Resources:
        """
        Return a Resources object representing the default resources for a batch job.

        Returns:
            Resources: Default batch job resources with predefined settings.
        """
        return Resources(
            nnodes=1,
            ncpus=1,
            mem_per_cpu="1gb",
            work_dir="scratch_local",
            work_size_per_cpu="1gb",
            walltime="1d",
        )

    @classmethod
    def _translate_kill_force(cls, job_id: str) -> str:
        """
        Generate the PBS force kill command for a job.

        Args:
            job_id (str): The ID of the job to kill.

        Returns:
            str: The qdel command with force flag.
        """
        return f"qdel -W force {job_id}"

    @classmethod
    def _translate_kill(cls, job_id: str) -> str:
        """
        Generate the standard PBS kill command for a job.

        Args:
            job_id (str): The ID of the job to kill.

        Returns:
            str: The qdel command without force flag.
        """
        return f"qdel {job_id}"

    @classmethod
    def _sync_directories(
        cls,
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        files: list[Path] | None,
        sync_function: Callable[
            [Path, Path, str | None, str | None, list[Path] | None], None
        ],
    ) -> None:
        """
        Synchronize directories either locally or across remote hosts, depending on the environment and setup.

        Args:
            src_dir (Path): Source directory to sync from.
            dest_dir (Path): Destination directory to sync to.
            src_host (str | None): Hostname of the source machine if remote; None if local.
            dest_host (str | None): Hostname of the destination machine if remote; None if local.
            files (list[Path] | None): Optional list of file paths to include or exclude, depending on `sync_function`.
            sync_function (Callable): Function to perform the actual synchronization.

        Raises:
            QQError: If both source and destination hosts are remote and cannot be
                accessed simultaneously, or if syncing fails internally.
        """
        if os.environ.get(CFG.env_vars.shared_submit):
            # input_dir is on shared storage -> we can copy files from/to it without connecting to the remote host
            logger.debug("Syncing directories on local and shared filesystem.")
            sync_function(src_dir, dest_dir, None, None, files)
        else:
            # input_dir is not on shared storage -> fall back to the default implementation
            logger.debug("Syncing directories on local filesystems.")

            # convert local hosts to none
            local_hostname = socket.getfqdn()
            src = None if src_host == local_hostname else src_host
            dest = None if dest_host == local_hostname else dest_host

            if src is None or dest is None:
                sync_function(src_dir, dest_dir, src, dest, files)
            else:
                raise QQError(
                    f"The source '{src_host}' and destination '{dest_host}' cannot be both remote."
                )

    @classmethod
    def _get_batch_jobs_using_command(
        cls, command: str, include_completed: bool
    ) -> list[PBSJob]:
        """
        Execute a shell command to retrieve information about PBS jobs and parse it.

        Args:
            command (str): The shell command to execute, typically a PBS query command.
            include_completed (bool): Include both completed and uncompleted jobs.
                If `False`, completed jobs are filtered out from the output.

        Returns:
            list[PBSJob]: A list of `PBSJob` instances corresponding to the jobs
                            returned by the command.

        Raises:
            QQError: If the command fails (non-zero return code) or if the output
                    cannot be parsed into valid job information.
        """
        ...
        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not retrieve information about jobs: {result.stderr.strip()}."
            )

        jobs = []
        for data, job_id in parse_multi_pbs_dump_to_dictionaries(
            result.stdout.strip(), "Job Id"
        ):
            job = PBSJob.from_dict(job_id, data)

            # ignore top-level array jobs
            if job.is_array_job():
                continue

            # unless all jobs are to be shown, filter out completed jobs
            # this is necessary because the output from PBS will always contain
            # completed job tasks from array jobs that are uncompleted
            # we do not want qq to show them
            if not include_completed and job.is_completed():
                continue

            jobs.append(job)

        return jobs
