# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from pathlib import Path
from typing import NoReturn

import click
from rich.console import Console

from qq_lib.core.click_format import QQOperatorCommand
from qq_lib.core.command_runner import CommandRunner
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.error_handlers import handle_general_qq_error
from qq_lib.core.logger import get_logger
from qq_lib.info.informer import Informer
from qq_lib.info.presenter import Presenter

logger = get_logger(__name__)


# TODO: remove before version 1.0
# This is to help users to adjust to the breaking change in version 0.12.
class InfoCommand(QQOperatorCommand):
    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        if "-s" in args:
            next_arg_index = args.index("-s") + 1
            next_arg = args[next_arg_index] if next_arg_index < len(args) else None
            if next_arg is None or next_arg.startswith("-"):
                raise click.UsageError(
                    "'-s' now means '--server', not '--short'. "
                    "Use '-s <server>' to specify a batch server. "
                    "Or '--short'/'--brief'/'-b' to display only the job ID and current state."
                )
        return super().parse_args(ctx, args)


@click.command(
    short_help="Display information about a job.",
    help=f"""Display information about the state and properties of the specified qq jobs or of qq jobs found in the specified directories.

{click.style("JOB_ID...", fg="green")}   One or more IDs of jobs to display information for. Optional.

If no JOB_ID and no directory are specified, `{CFG.binary_name} info` searches for qq jobs in the current directory.

You can combine JOB_IDs with directories. All JOB_IDs must be specified before the `--dir` option.""",
    cls=InfoCommand,
    help_options_color="bright_blue",
)
@click.argument(
    "jobs",
    type=str,
    metavar=click.style("JOB_ID", fg="green"),
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
    help="Print info for all your unfinished jobs.",
)
@click.option(
    "-s",
    "--server",
    default=None,
    help="Collect jobs from the specified batch server. If not specified, the current server is used. Only used with --all.",
)
@click.option(
    "-b",
    "--brief",
    "--short",
    is_flag=True,
    help="Display a brief summary of the job.",
)
def info(
    jobs: tuple[str, ...],
    dir: tuple[Path, ...],
    all: bool,
    server: str | None,
    brief: bool,
) -> NoReturn:
    """
    Get information about the specified qq jobs or qq jobs submitted from this directory.
    """
    CommandRunner(
        jobs,
        dir,
        all,
        server,
        _info_for_job,
        logger,
        brief,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    ).on_exception(QQError, handle_general_qq_error).run()


def _info_for_job(informer: Informer, brief: bool) -> None:
    """
    Display information about a qq job associated with the specified Informer.

    Args:
        informer (Informer): Informer associated with the job.
        short (bool): If True, print only the job ID and the current job state.
                      If False, print the full formatted information panel.
    """
    presenter = Presenter(informer)
    console = Console()
    if brief:
        console.print(presenter.get_short_info())
    else:
        panel = presenter.create_full_info_panel(console)
        console.print(panel)
