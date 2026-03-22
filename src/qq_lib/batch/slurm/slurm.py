# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from qq_lib.batch.interface import BatchInterface
from qq_lib.batch.interface.meta import BatchMeta, batch_system
from qq_lib.batch.pbs.pbs import PBS
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.depend import Depend
from qq_lib.properties.resources import Resources

from .common import (
    SACCT_FIELDS,
    default_resources_from_dict,
    parse_slurm_dump_to_dictionary,
)
from .job import SlurmJob
from .node import SlurmNode
from .queue import SlurmQueue

logger = get_logger(__name__)


@batch_system
class Slurm(BatchInterface[SlurmJob, SlurmQueue, SlurmNode], metaclass=BatchMeta):
    """
    Implementation of BatchInterface for Slurm batch system.
    """

    @classmethod
    def env_name(cls) -> str:
        return "Slurm"

    @classmethod
    def is_available(cls) -> bool:
        return (
            shutil.which("sbatch") is not None
            and shutil.which("it4ifree") is None
            and shutil.which("lumi-allocations") is None
        )

    @classmethod
    def get_job_id(cls) -> str | None:
        return os.environ.get("SLURM_JOB_ID")

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
        # server is unused
        _ = server

        # intentionally using PBS
        PBS._shared_guard(res, env_vars, server)

        command = cls._translate_submit(
            res, queue, script.parent, str(script), job_name, depend, env_vars, account
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

        return result.stdout.split()[-1]

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
    def get_batch_job(cls, job_id: str) -> SlurmJob:
        return SlurmJob(job_id)

    @classmethod
    def get_unfinished_batch_jobs(
        cls, user: str, server: str | None = None
    ) -> list[SlurmJob]:
        # server unused
        _ = server

        # get running jobs from sacct (faster than using squeue and scontrol)
        command = f"sacct -u {user} --state RUNNING --allocations --noheader --parsable2 --array --format={SACCT_FIELDS}"
        logger.debug(command)

        sacct_jobs = cls._get_batch_jobs_using_sacct_command(command)

        # get pending jobs using squeue
        command = f'squeue -u {user} --array -t PENDING -h -o "%i"'
        logger.debug(command)

        squeue_jobs = cls._get_batch_jobs_using_squeue_command(command)

        # filter out duplicate jobs
        merged = {job.get_id(): job for job in sacct_jobs + squeue_jobs}
        return list(merged.values())

    @classmethod
    def get_batch_jobs(cls, user: str, server: str | None = None) -> list[SlurmJob]:
        # server unused
        _ = server

        # get all jobs, except pending which are not available from sacct
        command = f"sacct -u {user} --allocations --noheader --parsable2 --array --format={SACCT_FIELDS}"
        logger.debug(command)

        sacct_jobs = cls._get_batch_jobs_using_sacct_command(command)

        # get pending jobs using squeue
        command = f'squeue -u {user} --array -t PENDING -h -o "%i"'
        logger.debug(command)

        squeue_jobs = cls._get_batch_jobs_using_squeue_command(command)

        # filter out duplicate jobs
        merged = {job.get_id(): job for job in sacct_jobs + squeue_jobs}
        return list(merged.values())

    @classmethod
    def get_all_unfinished_batch_jobs(cls, server: str | None = None) -> list[SlurmJob]:
        # server unused
        _ = server

        # get running jobs using sacct (faster than using squeue and scontrol)
        command = f"sacct --state RUNNING --allusers --allocations --noheader --parsable2 --array --format={SACCT_FIELDS}"
        logger.debug(command)

        sacct_jobs = cls._get_batch_jobs_using_sacct_command(command)

        # get pending jobs using squeue
        command = 'squeue --array -t PENDING -h -o "%i"'
        logger.debug(command)

        squeue_jobs = cls._get_batch_jobs_using_squeue_command(command)

        # filter out duplicate jobs
        merged = {job.get_id(): job for job in sacct_jobs + squeue_jobs}
        return list(merged.values())

    @classmethod
    def get_all_batch_jobs(cls, server: str | None = None) -> list[SlurmJob]:
        # server unused
        _ = server

        # get all jobs, except pending which are not available from sacct
        command = f"sacct --allusers --allocations --noheader --parsable2 --array --format={SACCT_FIELDS}"
        logger.debug(command)

        sacct_jobs = cls._get_batch_jobs_using_sacct_command(command)

        # get pending jobs using squeue
        command = 'squeue --array -t PENDING -h -o "%i"'
        logger.debug(command)

        squeue_jobs = cls._get_batch_jobs_using_squeue_command(command)

        # filter out duplicate jobs
        merged = {job.get_id(): job for job in sacct_jobs + squeue_jobs}
        return list(merged.values())

    @classmethod
    def get_queues(cls, server: str | None = None) -> list[SlurmQueue]:
        # server unused
        _ = server

        command = "scontrol show partition -o"
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
        for line in result.stdout.splitlines():
            info = parse_slurm_dump_to_dictionary(line)
            queues.append(SlurmQueue.from_dict(info["PartitionName"], info))

        return queues

    @classmethod
    def get_nodes(cls, server: str | None = None) -> list[SlurmNode]:
        # server unused
        _ = server

        command = "scontrol show node -o"
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

        nodes = []
        for line in result.stdout.splitlines():
            info = parse_slurm_dump_to_dictionary(line)
            nodes.append(SlurmNode.from_dict(info["NodeName"], info))

        return nodes

    @classmethod
    def read_remote_file(cls, host: str, file: Path) -> str:
        return PBS.read_remote_file(host, file)

    @classmethod
    def write_remote_file(cls, host: str, file: Path, content: str) -> None:
        PBS.write_remote_file(host, file, content)

    @classmethod
    def make_remote_dir(cls, host: str, directory: Path) -> None:
        PBS.make_remote_dir(host, directory)

    @classmethod
    def list_remote_dir(cls, host: str, directory: Path) -> list[Path]:
        return PBS.list_remote_dir(host, directory)

    @classmethod
    def delete_remote_dir(cls, host: str, directory: Path) -> None:
        PBS.delete_remote_dir(host, directory)

    @classmethod
    def move_remote_files(
        cls, host: str, files: list[Path], moved_files: list[Path]
    ) -> None:
        PBS.move_remote_files(host, files, moved_files)

    @classmethod
    def sync_with_exclusions(
        cls,
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        exclude_files: list[Path] | None = None,
    ) -> None:
        PBS.sync_with_exclusions(src_dir, dest_dir, src_host, dest_host, exclude_files)

    @classmethod
    def sync_selected(
        cls,
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        include_files: list[Path] | None = None,
    ) -> None:
        PBS.sync_selected(src_dir, dest_dir, src_host, dest_host, include_files)

    @classmethod
    def sort_jobs(cls, jobs: list[SlurmJob]) -> None:
        jobs.sort(key=lambda job: job.get_ids_for_sorting())

    @classmethod
    def jobs_presenter_columns_to_show(cls) -> set[str]:
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
            "Exit",
        }

    @classmethod
    def _translate_kill(cls, job_id: str) -> str:
        """
        Generate the Slurm kill command for a job using SIGTERM.

        Args:
            job_id (str): The ID of the job to kill.

        Returns:
            str: The scancel command sending SIGTERM.
        """
        return f"scancel {job_id}"

    @classmethod
    def _translate_kill_force(cls, job_id: str) -> str:
        """
        Generate the Slurm kill command for a job using SIGKILL.

        Args:
            job_id (str): The ID of the job to kill.

        Returns:
            str: The scancel command sending SIGKILL.
        """
        return f"scancel --signal=KILL {job_id}"

    @classmethod
    def _translate_submit(
        cls,
        res: Resources,
        queue: str,
        input_dir: Path,
        script: str,
        job_name: str,
        depend: list[Depend],
        env_vars: dict[str, str],
        account: str | None,
    ) -> str:
        """
        Generate the Slurm submission command for a job.

        Args:
            res (Resources): The resources requested for the job.
            queue (str): The queue name to submit to.
            input_dir (Path): The directory from which the job is being submitted.
            script (str): Path to the job script.
            job_name (str): Name of the job.
            depend (list[Depend]): List of dependencies of the job.
            env_vars (dict[str, str]): Dictionary of environment variables and their values to propagate to the job's environment.
            account (str | None): Optional name of the account to use for the job.

        Returns:
            str: The fully constructed sbatch command string.
        """
        qq_output = str((input_dir / job_name).with_suffix(CFG.suffixes.qq_out))
        command = f"sbatch -J {job_name} -p {queue} -e {qq_output} -o {qq_output} "

        if account:
            command += f"--account {account} "

        # translate environment variables
        if env_vars:
            command += f"--export ALL,{cls._translate_env_vars(env_vars)} "

        # handle number of nodes
        command += f"--nodes {res.nnodes} "

        # handle per-chunk resources
        translated = cls._translate_per_chunk_resources(res)
        command += " ".join(translated) + " "

        # handle properties
        if res.props:
            constraints = []
            for k, v in res.props.items():
                if v != "true":
                    raise QQError(
                        f"Slurm only supports properties with a value of 'true', not '{k}={v}'."
                    )
                constraints.append(k)

            command += f'--constraint="{"&".join(constraints)}" '

        # handle walltime
        if res.walltime:
            command += f"--time={res.walltime} "

        # handle dependencies
        if converted_depend := cls._translate_dependencies(depend):
            command += f"--dependency={converted_depend} "

        # add script
        command += script

        return command

    @classmethod
    def _translate_env_vars(cls, env_vars: dict[str, str]) -> str:
        """
        Convert a dictionary of environment variables into a formatted string.

        Args:
            env_vars (dict[str, str]): A mapping of environment variable names
                to their corresponding values.

        Returns:
            str: A comma-separated string of environment variable assignments,
                suitable for inclusion in the sbatch command.
        """
        converted = []
        for key, value in env_vars.items():
            converted.append(f'{key}="{value}"')

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
                    in the sbatch command.

        Raises:
            QQError: If sanity checks fail or required memory attributes are missing.
        """

        trans_res = []

        # sanity checking per-chunk resources
        if res.nnodes is None:
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
            # we set MPI ranks and OpenMP threads here, but these can be overriden
            # in the body of the script
            # this setup is here only to allow for better accounting by Slurm
            trans_res.append("--ntasks-per-node=1")
            trans_res.append(f"--cpus-per-task={res.ncpus // res.nnodes}")
        elif res.ncpus_per_node:
            trans_res.append("--ntasks-per-node=1")
            trans_res.append(f"--cpus-per-task={res.ncpus_per_node}")

        if res.mem:
            trans_res.append(f"--mem={(res.mem // res.nnodes).to_str_exact_slurm()}")
        elif res.mem_per_node:
            trans_res.append(f"--mem={res.mem_per_node.to_str_exact_slurm()}")
        elif res.mem_per_cpu:
            trans_res.append(f"--mem-per-cpu={res.mem_per_cpu.to_str_exact_slurm()}")
        else:
            # memory not set in any way
            raise QQError(
                "None of the attributes 'mem', 'mem-per-node', or 'mem-per-cpu' is defined."
            )

        if res.ngpus:
            trans_res.append(f"--gpus-per-node={res.ngpus // res.nnodes}")
        elif res.ngpus_per_node:
            trans_res.append(f"--gpus-per-node={res.ngpus_per_node}")

        return trans_res

    @classmethod
    def _translate_dependencies(cls, depend: list[Depend]) -> str | None:
        """
        Convert a list of `Depend` objects into a Slurm-compatible dependency string.

        Args:
            depend (list[Depend]): List of dependency objects to translate.

        Returns:
            str | None: Slurm-style dependency string (e.g., "after:12345,afterok:1:2:3"),
                        or None if the input list is empty.
        """
        if not depend:
            return None

        return ",".join(Depend.to_str(x).replace("=", ":") for x in depend)

    @classmethod
    def _get_default_server_resources(cls) -> Resources:
        """
        Return a Resources object representing the default resources for a batch job.

        Returns:
            Resources: Default batch job resources obtained from `slurm.conf`.
        """
        command = "scontrol show config"

        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            logger.debug("Could not get server resources. Ignoring.")
            return Resources()

        info = parse_slurm_dump_to_dictionary(result.stdout, "\n")
        server_resources = default_resources_from_dict(info)

        return Resources.merge_resources(server_resources, cls._get_default_resources())

    @classmethod
    def _get_default_resources(cls) -> Resources:
        """
        Return a Resources object representing the default, hard-coded resources for a batch job.
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
    def _get_batch_jobs_using_sacct_command(cls, command: str) -> list[SlurmJob]:
        """
        Execute `sacct` to retrieve information about Slurm jobs and parse it.

        Args:
            command (str): A Slurm command to get the relevant jobs.

        Returns:
            list[SlurmJob]: A list of `SlurmJob` instances corresponding to the jobs
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
        for sacct_string in result.stdout.split("\n"):
            if sacct_string.strip() == "":
                continue

            jobs.append(SlurmJob.from_sacct_string(sacct_string))

        return jobs

    @classmethod
    def _get_batch_jobs_using_squeue_command(cls, command: str) -> list[SlurmJob]:
        """
        Execute `squeue` and `scontrol show job` to retrieve information about Slurm jobs.

        Multiple `scontrol` commands are executed in parallel
        to increase the speed of collecting the information about jobs.

        Note that the jobs are returned in an arbitrary order.

        Args:
            command (str): A Slurm command to get the relevant job IDs.

        Returns:
            list[SlurmJob]: A list of `SlurmJob` instances corresponding to the jobs
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

        ids = [line.strip() for line in result.stdout.split("\n") if line.strip()]

        def get_job(job_id: str) -> SlurmJob:
            return SlurmJob(job_id)

        jobs: list[SlurmJob] = []

        # use ThreadPoolExecutor to get information about jobs in parallel
        with ThreadPoolExecutor(
            max_workers=CFG.slurm_options.jobs_scontrol_nthreads
        ) as executor:
            future_to_id = {executor.submit(get_job, job_id): job_id for job_id in ids}

            for future in as_completed(future_to_id):
                try:
                    jobs.append(future.result())
                except Exception as e:
                    job_id = future_to_id[future]
                    raise QQError(f"Failed to load job {job_id}: {e}.") from e

        return jobs
