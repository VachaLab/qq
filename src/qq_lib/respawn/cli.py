# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

import sys
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn

import click
from rich.console import Console

from qq_lib.core.click_format import QQOperatorCommand
from qq_lib.core.command_runner import CommandRunner
from qq_lib.core.common import choice_prompt
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.error_handlers import (
    handle_general_qq_error,
    handle_not_suitable_error,
)
from qq_lib.core.logger import get_logger
from qq_lib.respawn.respawner import Respawner

if TYPE_CHECKING:
    from qq_lib.info import Informer

logger = get_logger(__name__)
console = Console()


@click.command(
    short_help="Respawn a failed/killed job.",
    help=f"""Respawn the specified qq jobs, or all qq jobs in the specified directories.

{click.style("JOB_ID...", fg="green")}   One or more IDs of jobs to respawn. Optional.

If no JOB_ID and no directory are specified, `{CFG.binary_name} respawn` searches for qq jobs in the current directory.

You can combine JOB_IDs with directories. All JOB_IDs must be specified before the `--dir` option.

Respawning resubmits a failed or killed job to the batch system with its original parameters.
This is useful when a job fails due to a node failure, an unexpected walltime limit, a random crash,
or various other types of premature termination.

If the archive of a loop job already contains files for the next cycle (for example, because
the cycle finished but the next cycle could not be submitted), `{CFG.binary_name} respawn` asks whether
to continue with the next cycle or to rerun the failed one. Use `--force-continue` or `--force-rerun`
to choose without being asked.""",
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
    "-r",
    "--recursive",
    is_flag=True,
    help="Also search subdirectories, except those inside a qq job's directory.",
)
@click.option(
    "--force-continue",
    is_flag=True,
    help="If the archive of a loop job already contains files for the next cycle, continue with the next cycle without asking.",
)
@click.option(
    "--force-rerun",
    is_flag=True,
    help="If the archive of a loop job already contains files for the next cycle, rerun the failed cycle without asking.",
)
def respawn(
    jobs: tuple[str, ...],
    dir: tuple[Path, ...],
    recursive: bool,
    force_continue: bool,
    force_rerun: bool,
) -> NoReturn:
    if force_continue and force_rerun:
        raise click.UsageError(
            "Options '--force-continue' and '--force-rerun' cannot be used together."
        )

    CommandRunner(
        jobs,
        dir,
        False,
        None,
        _respawn_job,
        logger,
        force_continue,
        force_rerun,
        recursive=recursive,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    ).on_exception(QQNotSuitableError, handle_not_suitable_error).on_exception(
        QQError, handle_general_qq_error
    ).run()


def _respawn_job(
    informer: Informer, force_continue: bool = False, force_rerun: bool = False
) -> None:
    """
    Attempt to respawn a qq job associated with the specified informer.

    If the archive of a loop job already contains files for the next cycle
    and neither option is set, the user is asked how to proceed.

    Args:
        informer: Informer associated with the job.
        force_continue: Continue with the next cycle without asking.
        force_rerun: Rerun the failed cycle without asking.

    Raises:
        QQNotSuitableError: If the job is not suitable for respawn.
        QQError: If the job cannot be respawned.
    """
    respawner = Respawner.from_informer(informer)
    respawner.print_info(console)

    # make sure that the job can actually be respawned
    respawner.ensure_suitable()

    if (
        not force_continue
        and not force_rerun
        and (next_cycle := respawner.get_next_cycle_in_archive()) is not None
    ):
        match _ask_how_to_respawn(informer, next_cycle, respawner.can_continue()):
            case "c":
                force_continue = True
            case "r":
                force_rerun = True
            case _:
                logger.info("Operation aborted.")
                return

    job_id = respawner.respawn(force_continue=force_continue, force_rerun=force_rerun)

    logger.info(f"Job '{informer.info.job_id}' successfully respawned as '{job_id}'.")


def _ask_how_to_respawn(informer: Informer, next_cycle: int, can_continue: bool) -> str:
    """
    Ask the user whether to continue a loop job with the next cycle or to rerun the failed one.

    Args:
        informer: Informer associated with the job.
        next_cycle: The next cycle for which the archive already contains files.
        can_continue: Whether the job can continue with the next cycle.

    Returns:
        "c" to continue with the next cycle, "r" to rerun the failed cycle,
        or "x" to cancel the operation.

    Raises:
        QQError: If the user cannot be asked because the standard input is not a terminal.
    """
    current = next_cycle - 1
    logger.warning(
        f"Job '{informer.info.job_id}' failed in cycle {current}, but the archive already contains files for cycle {next_cycle}."
    )

    if not sys.stdin.isatty():
        raise QQError(
            "Cannot ask how to respawn the job without a terminal. Use '--force-continue' to continue with "
            f"cycle {next_cycle} or '--force-rerun' to rerun cycle {current}"
        )

    if can_continue:
        logger.warning(
            f"This usually means that cycle {current} finished and only the submission of cycle {next_cycle} failed."
        )
        options = [
            ("c", f"Continue with cycle {next_cycle}"),
            ("r", f"rerun cycle {current}"),
            ("x", "cancel this operation"),
        ]
    else:
        logger.warning(
            f"Cycle {current} is the last cycle of the loop job, so it can only be rerun."
        )
        options = [
            ("r", f"Rerun cycle {current}"),
            ("x", "cancel this operation"),
        ]

    return choice_prompt(options)
