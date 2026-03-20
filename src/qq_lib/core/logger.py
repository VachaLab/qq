# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

"""
Unified logging utilities for qq.

This module provides a helper for creating consistently formatted loggers using
Rich-based output. Loggers automatically adapt to qq's debug mode, support
optional timestamps, and apply standardized styling across the codebase.
"""

import logging
import os
import sys

from rich.console import Console
from rich.logging import RichHandler

from .config import CFG


def get_logger(name: str, show_time: bool = False) -> logging.Logger:
    """
    Return a logger with unified formatting.
    If colored=True, use rich's RichHandler with colored levels.
    """
    logger = logging.getLogger(name)

    debug_mode = os.environ.get(CFG.env_vars.debug_mode) is not None
    logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)

    if sys.stderr.isatty():
        console = Console(stderr=True)
        handler = RichHandler(
            console=console,
            rich_tracebacks=True,
            show_path=False,
            show_level=True,
            show_time=show_time or debug_mode,
            log_time_format=CFG.date_formats.standard,
            tracebacks_width=None,
            tracebacks_code_width=None,
        )
    else:
        handler = logging.StreamHandler(sys.stderr)
        fmt = (
            "%(asctime)s %(levelname)-8s %(message)s"
            if (show_time or debug_mode)
            else "%(levelname)-8s %(message)s"
        )
        handler.setFormatter(logging.Formatter(fmt, datefmt=CFG.date_formats.standard))

    handler.setLevel(logging.DEBUG if debug_mode else logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False

    return logger
