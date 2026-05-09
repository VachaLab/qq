# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from typing import NoReturn

import click
from rich.console import Console

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.command_runner import CommandRunner
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.error_handlers import handle_general_qq_error
from qq_lib.core.logger import get_logger
from qq_lib.info.informer import Informer
from qq_lib.info.presenter import Presenter

logger = get_logger(__name__)


@click.command(
    short_help="Display information about a job.",
    help=f"""Display information about the state and properties of the specified qq jobs,
or of qq jobs found in the current directory.

{click.style("JOB_ID", fg="green")}   One or more IDs of jobs to display information for. Optional.

If no JOB_ID is specified, `{CFG.binary_name} info` searches for qq jobs in the current directory.""",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.argument(
    "jobs",
    type=str,
    metavar=click.style("JOB_ID", fg="green"),
    nargs=-1,
)
@click.option(
    "-s", "--short", is_flag=True, help="Display only the job ID and current state."
)
def info(jobs: tuple[str, ...], short: bool) -> NoReturn:
    """
    Get information about the specified qq jobs or qq jobs submitted from this directory.
    """
    CommandRunner(
        jobs,
        _info_for_job,
        logger,
        short,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    ).on_exception(QQError, handle_general_qq_error).run()


def _info_for_job(informer: Informer, short: bool) -> None:
    """
    Display information about a qq job associated with the specified Informer.

    Args:
        informer (Informer): Informer associated with the job.
        short (bool): If True, print only the job ID and the current job state.
                      If False, print the full formatted information panel.
    """
    presenter = Presenter(informer)
    console = Console()
    if short:
        console.print(presenter.get_short_info())
    else:
        panel = presenter.create_full_info_panel(console)
        console.print(panel)
