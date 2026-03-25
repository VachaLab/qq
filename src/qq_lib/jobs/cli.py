# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import getpass
import sys
from typing import NoReturn

import click
from rich.console import Console

from qq_lib.batch.interface import BatchMeta
from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.common import translate_server
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.jobs.presenter import JobsPresenter

logger = get_logger(__name__)


@click.command(
    short_help="Display a summary of a user's jobs.",
    help="Display a summary of your jobs or those of a specified user. By default, only uncompleted jobs are shown.",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.option(
    "-u",
    "--user",
    type=str,
    default=None,
    help="Username whose jobs should be displayed. Defaults to your own username.",
)
@click.option(
    "-e",
    "--extra",
    is_flag=True,
    help="Show additional information about the jobs.",
)
@click.option(
    "-a",
    "--all",
    is_flag=True,
    help="Include both completed and uncompleted jobs in the summary.",
)
@click.option(
    "-s",
    "--server",
    default=None,
    help="Collect jobs from the specified batch server. If not specified, the current server is used.",
)
@click.option("--yaml", is_flag=True, help="Output job metadata in YAML format.")
def jobs(user: str, extra: bool, all: bool, server: str | None, yaml: bool) -> NoReturn:
    try:
        batch_system = BatchMeta.from_env_var_or_guess()
        if not user:
            # use the current user, if `--user` is not specified
            user = getpass.getuser()

        if server:
            server = translate_server(server)

        if all:
            jobs = batch_system.get_batch_jobs(user, server)
        else:
            jobs = batch_system.get_unfinished_batch_jobs(user, server)

        if not jobs:
            logger.info("No jobs found.")
            sys.exit(0)

        batch_system.sort_jobs(jobs)
        presenter = JobsPresenter(batch_system, jobs, extra, all, server)
        if yaml:
            presenter.dump_yaml()
        else:
            console = Console(record=False, markup=False)
            panel = presenter.create_jobs_info_panel(console)
            console.print(panel)

        sys.exit(0)
    except QQError as e:
        logger.error(e)
        print()
        sys.exit(CFG.exit_codes.default)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        print()
        sys.exit(CFG.exit_codes.unexpected_error)
