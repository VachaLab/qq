# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

"""
Error-handling utilities for qq operations.

This module provides helper functions for processing and reporting errors
encountered during multi-item qq operations. Handlers distinguish between
unsuitable jobs, job-ID mismatches, general failures, and ignorable errors,
and exit with appropriate qq exit codes when necessary.
"""

import sys
from typing import NoReturn

from qq_lib.core.command_runner import CommandRunner

from .config import CFG
from .error import QQNotSuitableError
from .logger import get_logger

logger = get_logger(__name__)


def handle_not_suitable_error(
    exception: Exception,
    runner: CommandRunner,
) -> None:
    """Handle cases where a job is unsuitable for a qq operation."""
    if runner.n_jobs == 1:
        logger.error(exception)
        sys.exit(CFG.exit_codes.default)

    if runner.n_jobs > 1:
        logger.info(exception)

    if (
        sum(
            isinstance(x, QQNotSuitableError)
            for x in runner.encountered_errors.values()
        )
        == runner.n_jobs
    ):
        logger.error("No suitable qq job.\n")
        sys.exit(CFG.exit_codes.default)


def handle_job_mismatch_error(
    exception: Exception,
    _runner: CommandRunner,
) -> NoReturn:
    """Handle cases where the provided job ID does not match the qq info file."""
    logger.error(exception)
    sys.exit(CFG.exit_codes.default)


def handle_general_qq_error(
    exception: Exception,
    runner: CommandRunner,
) -> None:
    """Handle general qq errors that occur during a qq operation."""
    logger.error(exception)

    if runner.n_jobs == len(runner.encountered_errors):
        sys.exit(CFG.exit_codes.default)


def ignore_error(
    _exception: Exception,
    _runner: CommandRunner,
) -> None:
    """Ignore the error."""
    pass
