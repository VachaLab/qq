# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import getpass
import sys
from typing import TYPE_CHECKING, NoReturn

import click
from rich.console import Console

from qq_lib.batch.interface import BatchInterface
from qq_lib.core.common import translate_server
from qq_lib.core.config import CFG

if TYPE_CHECKING:
    from qq_lib.batch.interface.queue import BatchQueueInterface
from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

from .presenter import QueuesPresenter

logger = get_logger(__name__)


@click.command(
    short_help="Display the queues available to you.",
    help="""Display information about the queues available to the current user.

If the `--all` flag is specified, display all queues, including those not available.""",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.option(
    "-a",
    "--all",
    is_flag=True,
    help="Display all queues, including those not available to you.",
)
@click.option(
    "-s",
    "--server",
    default=None,
    help="Collect queues from the specified batch server. If not specified, the current server is used.",
)
@click.option("--yaml", is_flag=True, help="Output queue metadata in YAML format.")
def queues(all: bool, server: str | None, yaml: bool) -> NoReturn:
    try:
        BatchSystem = BatchInterface.from_env_var_or_guess()
        if server:
            server = translate_server(server)

        queues: list[BatchQueueInterface] = BatchSystem.get_queues(server)
        user = getpass.getuser()

        if not all:
            queues = [q for q in queues if q.is_available_to_user(user)]

        presenter = QueuesPresenter(queues, user, all, server)
        if yaml:
            presenter.dump_yaml()
        else:
            console = Console(record=False, markup=False)
            panel = presenter.create_queues_info_panel(console)
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
