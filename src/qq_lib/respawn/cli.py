# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from typing import NoReturn

import click
from rich.console import Console

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.command_runner import CommandRunner
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.error_handlers import (
    handle_general_qq_error,
    handle_not_suitable_error,
)
from qq_lib.core.logger import get_logger
from qq_lib.info import Informer
from qq_lib.respawn.respawner import Respawner

logger = get_logger(__name__)
console = Console()


@click.command(
    short_help="Respawn a failed/killed job.",
    help=f"""Respawn the specified qq jobs, or all qq jobs in the current directory.

{click.style("JOB_ID", fg="green")}   One or more IDs of jobs to respawn. Optional.

If no JOB_ID is specified, `{CFG.binary_name} respawn` searches for qq jobs in the current directory.

Respawning resubmits a failed or killed job to the batch system with its original parameters.
This is useful when a job fails due to a node failure, an unexpected walltime limit, a random crash,
or various other types of premature termination.""",
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
def respawn(jobs: tuple[str, ...]) -> NoReturn:
    CommandRunner(
        jobs,
        _respawn_job,
        logger,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    ).on_exception(QQNotSuitableError, handle_not_suitable_error).on_exception(
        QQError, handle_general_qq_error
    ).run()


def _respawn_job(informer: Informer) -> None:
    """
    Attempt to respawn a qq job associated with the specified informer.

    Args:
        informer (Informer): Informer associated with the job.

    Raises:
        QQNotSuitableError: If the job is not suitable for respawn.
        QQError: If the job cannot be respawned.
    """
    respawner = Respawner.from_informer(informer)
    respawner.print_info(console)

    # make sure that the job can actually be respawned
    respawner.ensure_suitable()

    job_id = respawner.respawn()

    logger.info(f"Job '{informer.info.job_id}' successfully respawned as '{job_id}'.")
