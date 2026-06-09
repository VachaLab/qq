# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

import re
from pathlib import Path
from typing import NoReturn

import click
from rich.console import Console

from qq_lib.core.click_format import QQOperatorCommand
from qq_lib.core.command_runner import CommandRunner
from qq_lib.core.config import CFG
from qq_lib.core.error import (
    QQError,
    QQNotSuitableError,
)
from qq_lib.core.error_handlers import (
    handle_general_qq_error,
    handle_not_suitable_error,
)
from qq_lib.core.logger import get_logger
from qq_lib.info.informer import Informer

from .syncer import Syncer

logger = get_logger(__name__)
console = Console()


@click.command(
    short_help="Fetch files from a job's working directory.",
    help=f"""Fetch files from the working directory of the specified qq jobs, or from the
working directories of the jobs submitted from the specified directories.

{click.style("JOB_ID...", fg="green")}   One or more IDs of jobs whose working directory files should be fetched. Optional.

If no JOB_ID and no directory are specified, `{CFG.binary_name} sync` searches for qq jobs in the current directory.
If multiple suitable jobs are provided or found, `{CFG.binary_name} sync` fetches files from each job in turn.
Files fetched from later jobs may overwrite files from earlier jobs in the input directory.

You can combine JOB_IDs with directories. All JOB_IDs must be specified before the `--dir` option.

Files are copied from the job's working directory to its input directory, not to the current directory.
""",
    cls=QQOperatorCommand,
    help_options_color="bright_blue",
)
@click.argument(
    "jobs",
    type=str,
    metavar=click.style("JOB_ID", fg="green"),
    required=False,
    default=None,
    nargs=-1,
)
@click.option(
    "-d",
    "--dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    multiple=True,
    help="One or more directories to search for qq jobs in. Supports globs.",
)
@click.option(
    "-a",
    "--all",
    is_flag=True,
    help="Fetch files for all your unfinished jobs.",
)
@click.option(
    "-s",
    "--server",
    default=None,
    help="Operate on jobs from the specified batch server. If not specified, the current server is used. Only used with --all.",
)
@click.option(
    "-f",
    "--files",
    type=str,
    default=None,
    help="""A colon-, comma-, or space-separated list of files or directories to fetch.
If not specified, the entire content of the working directory is fetched.""",
)
def sync(
    jobs: tuple[str, ...],
    dir: tuple[Path, ...],
    all: bool,
    server: str | None,
    files: str | None,
) -> NoReturn:
    """
    Fetch files from the working directory (directories) of the specified qq job(s)
    or of qq job(s) submitted from this directory.
    """
    CommandRunner(
        jobs,
        dir,
        all,
        server,
        _sync_job,
        logger,
        _split_files(files),
        n_threads=CFG.parallelization_options.job_info_max_threads,
    ).on_exception(QQNotSuitableError, handle_not_suitable_error).on_exception(
        QQError, handle_general_qq_error
    ).run()


def _split_files(files: str | None) -> list[str] | None:
    """
    Split the list of files provided on the command line.
    """
    if not files:
        return None

    return re.split(r"[\s:,]+", files)


def _sync_job(informer: Informer, files: list[str] | None) -> None:
    """
    Perform synchronization of job files from a remote working directory to the local input directory.

    Args:
        informer (Informer): Informer associated with the job.
        files (list[str] | None): Optional list of specific file names to synchronize.
            If not provided, all files are fetched from the job's working directory
            except those excluded by the batch system.

    Raises:
        QQNotSuitableError: If the job is not in a state suitable for synchronization,
            e.g., it has already finished, is exiting successfully, has been killed while queued,
            or is queued/booting.
        QQError: If an error occurs during synchronization setup or execution.
    """

    syncer = Syncer.from_informer(informer)
    syncer.print_info(console)

    # make sure that the job is suitable to be synced
    syncer.ensure_suitable()

    syncer.sync(files)
