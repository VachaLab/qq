# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from typing import NoReturn

import click
from rich.console import Console

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.command_runner import CommandRunner
from qq_lib.core.common import (
    yes_or_no_prompt,
)
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
from qq_lib.wipe.wiper import Wiper

logger = get_logger(__name__)
console = Console()


@click.command(
    short_help="Delete the working directory of a job.",
    help=f"""Delete the working directories of the specified qq jobs, or of all qq jobs in the current directory.

{click.style("JOB_ID", fg="green")}    One or more IDs of jobs whose working directories should be deleted. Optional.

If no JOB_ID is specified, `{CFG.binary_name} wipe` searches for qq jobs in the current directory.

By default, `{CFG.binary_name} wipe` prompts for confirmation before deleting the working directory.

Without the `--force` flag, `{CFG.binary_name} wipe` will only attempt to delete working directories of jobs that have failed or been killed.
When the `--force` flag is used, `{CFG.binary_name} wipe` attempts to wipe the working directory of any job
regardless of its state, including jobs that are queued, running or successfully finished.
You should be very careful when using this option as it may delete useful data or cause your job to crash!

If the working directory matches the input directory, `{CFG.binary_name} wipe` will never delete it, even if you use the `--force` flag.""",
    cls=GNUHelpColorsCommand,
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
    "-y",
    "--yes",
    is_flag=True,
    help="Delete the working directory without confirmation.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Delete the working directory of the job forcibly, ignoring its current state and without confirmation.",
)
def wipe(jobs: tuple[str, ...], yes: bool = False, force: bool = False) -> NoReturn:
    """
    Delete the working directory of the specified qq job or qq job(s) submitted from the current directory.
    """
    CommandRunner(
        jobs,
        _wipe_work_dir,
        logger,
        force,
        yes,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    ).on_exception(QQNotSuitableError, handle_not_suitable_error).on_exception(
        QQError, handle_general_qq_error
    ).run()


def _wipe_work_dir(informer: Informer, force: bool, yes: bool) -> None:
    """
    Attempt to delete the working directory of the job associated with the specified Informer.

    Args:
        informer (Informer): Informer associated with the job.
        force (bool): Whether to forcibly delete the working directory regardless of the job's state.
        yes (bool): Whether to skip confirmation before deleting.
        job (str | None): Optional job ID for matching the target job.

    Raises:
        QQNotSuitableError: If the job does (or should) not have a working directory.
        QQError: If the working directory cannot be deleted.
    """
    wiper = Wiper.from_informer(informer)
    wiper.print_info(console)

    # make sure that the job is suitable for wiping
    if not force:
        wiper.ensure_suitable()

    if (
        force
        or yes
        or yes_or_no_prompt("Do you want to delete the job's working directory?")
    ):
        job_id = wiper.wipe()
        logger.info(f"Deleted the working directory of the job '{job_id}'.")
    else:
        logger.info("Operation aborted.")
