# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import sys
from typing import NoReturn

import click

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.config import CFG
from qq_lib.core.logger import get_logger

logger = get_logger(__name__)


@click.command(
    short_help="Terminate all your jobs. DEPRECATED. Use `qq kill --all` instead.",
    help=f"Terminate all your submitted qq jobs. {click.style('DEPRECATED', bold=True, fg='red')}. Use `qq kill --all` instead.",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.option(
    "-y",
    "--yes",
    is_flag=True,
    help=f"{click.style('DEPRECATED', bold=True, fg='red')}. Use `qq kill --all` instead.",
)
@click.option(
    "--force",
    is_flag=True,
    help=f"{click.style('DEPRECATED', bold=True, fg='red')}. Use `qq kill --all` instead.",
)
@click.option(
    "-s",
    "--server",
    default=None,
    help=f"{click.style('DEPRECATED', bold=True, fg='red')}. Use `qq kill --all` instead.",
)
def killall(
    yes: bool = False, force: bool = False, server: str | None = None
) -> NoReturn:
    _ = yes, force, server
    logger.error("This command is deprecated. Use `qq kill --all` instead.")
    sys.exit(CFG.exit_codes.default)
