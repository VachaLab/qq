# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import sys
from typing import Any, NoReturn

import click

from qq_lib.core.config import CFG
from qq_lib.core.logger import get_logger

logger = get_logger(__name__)


@click.command(
    context_settings={
        "ignore_unknown_options": True,
        "allow_extra_args": True,
    },
    add_help_option=False,
    hidden=True,
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def killall(args: Any) -> NoReturn:
    _ = args
    logger.error(
        f"This command has been removed. Use `{CFG.binary_name} kill --all` instead."
    )
    sys.exit(CFG.exit_codes.default)
