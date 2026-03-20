# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


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
    short_help="Display a summary of all users' jobs.",
    help="Display a summary of jobs from all users. By default, only unfinished jobs are shown.",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
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
    help="Include both unfinished and finished jobs in the summary.",
)
@click.option(
    "-s",
    "--server",
    default=None,
    help="Collect jobs from the specified batch server. If not specified, the current server is used.",
)
@click.option("--yaml", is_flag=True, help="Output job metadata in YAML format.")
def stat(extra: bool, all: bool, server: str | None, yaml: bool) -> NoReturn:
    try:
        batch_system = BatchMeta.fromEnvVarOrGuess()

        if server:
            server = translate_server(server)

        if all:
            jobs = batch_system.getAllBatchJobs(server)
        else:
            jobs = batch_system.getAllUnfinishedBatchJobs(server)

        if not jobs:
            logger.info("No jobs found.")
            sys.exit(0)

        batch_system.sortJobs(jobs)
        presenter = JobsPresenter(batch_system, jobs, extra, all, server)
        if yaml:
            presenter.dumpYaml()
        else:
            console = Console(record=False, markup=False)
            panel = presenter.createJobsInfoPanel(console)
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
