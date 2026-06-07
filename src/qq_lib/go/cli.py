# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

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
from qq_lib.go.goer import Goer
from qq_lib.info.informer import Informer

logger = get_logger(__name__)
console = Console()


@click.command(
    short_help="Open a shell in a job's working directory.",
    help=f"""Open a new shell in the working directory of the specified qq job, or in the
working directories of qq jobs found in the specified directories.

{click.style("JOB_ID...", fg="green")}   One or more IDs of jobs whose working directories should be entered. Optional.

If no JOB_ID and no directory are specified, `{CFG.binary_name} go` searches for qq jobs in the current directory.
If multiple suitable jobs are provided or found, `{CFG.binary_name} go` opens a shell for each job in turn.

You can combine JOB_IDs with directories. All JOB_IDs must be specified before the `--dir` option.

Uses `cd` for local directories or `ssh` if the working directory is on a remote host.
Note that this command does not change the working directory of the current shell;
it always opens a new shell at the destination.
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
    help="Open a shell in the working directories of all your unfinished jobs.",
)
@click.option(
    "-s",
    "--server",
    default=None,
    help="Operate on jobs on the specified batch server. If not specified, the current server is used. Only used with --all.",
)
def go(
    jobs: tuple[str, ...], dir: tuple[Path, ...], all: bool, server: str | None
) -> NoReturn:
    """
    Go to the working directory (directories) of the specified qq job(s)
    or qq job(s) submitted from the specified directories.
    """
    CommandRunner(
        jobs,
        dir,
        all,
        server,
        _go_to_job,
        logger,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    ).on_exception(QQNotSuitableError, handle_not_suitable_error).on_exception(
        QQError, handle_general_qq_error
    ).run()


def _go_to_job(informer: Informer) -> None:
    """
    Navigate to the working directory of a qq job if it is accessible.

    Args:
        informer (Informer): Informer associated with the job.

    Raises:
        QQError: If the navigation fails.
    """
    goer = Goer.from_informer(informer)
    goer.print_info(console)

    # make sure that the job is not in a state without a working directory
    goer.ensure_suitable()

    # navigate to the working directory
    goer.go()
