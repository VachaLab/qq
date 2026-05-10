# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import patch

import pytest

from qq_lib.core.command_runner import CommandRunner
from qq_lib.core.error import QQNotSuitableError
from qq_lib.core.error_handlers import (
    CFG,
    handle_general_qq_error,
    handle_job_mismatch_error,
    handle_not_suitable_error,
)


def test_not_suitable_single_item_logs_error_and_exits():
    metadata = CommandRunner.__new__(CommandRunner)
    metadata.n_jobs = 1
    metadata.encountered_errors = {}
    exc = QQNotSuitableError("not suitable")

    with (
        patch("qq_lib.core.error_handlers.logger") as mock_logger,
        patch("qq_lib.core.error_handlers.sys.exit") as mock_exit,
    ):
        handle_not_suitable_error(exc, metadata)

    mock_logger.error.assert_called_once_with(exc)
    mock_exit.assert_called_once_with(CFG.exit_codes.default)


def test_not_suitable_multiple_items_logs_info():
    metadata = CommandRunner.__new__(CommandRunner)
    metadata.n_jobs = 2
    metadata.encountered_errors = {}
    exc = QQNotSuitableError("not suitable")

    with (
        patch("qq_lib.core.error_handlers.logger") as mock_logger,
        patch("qq_lib.core.error_handlers.sys.exit") as mock_exit,
    ):
        handle_not_suitable_error(exc, metadata)

    mock_logger.info.assert_called_once_with(exc)
    mock_exit.assert_not_called()


def test_not_suitable_multiple_items_multiple_errors_logs_and_exits():
    metadata = CommandRunner.__new__(CommandRunner)
    metadata.n_jobs = 2
    metadata.encountered_errors = {
        0: QQNotSuitableError("not suitable"),
        1: QQNotSuitableError("not suitable"),
    }
    exc = QQNotSuitableError("not suitable")

    with (
        patch("qq_lib.core.error_handlers.logger") as mock_logger,
        patch("qq_lib.core.error_handlers.sys.exit") as mock_exit,
    ):
        handle_not_suitable_error(exc, metadata)

    mock_logger.info.assert_called_once_with(exc)
    mock_logger.error.assert_called_once_with("No suitable qq job.\n")
    mock_exit.assert_called_once_with(CFG.exit_codes.default)


@pytest.mark.parametrize("n_jobs", [1, 2, 3])
def test_job_mismatch_logs_and_exits(n_jobs):
    metadata = CommandRunner.__new__(CommandRunner)
    metadata.n_jobs = n_jobs
    metadata.encountered_errors = {}
    exc = RuntimeError("job mismatch")

    with (
        patch("qq_lib.core.error_handlers.logger") as mock_logger,
        patch("qq_lib.core.error_handlers.sys.exit") as mock_exit,
    ):
        handle_job_mismatch_error(exc, metadata)

    mock_logger.error.assert_called_once_with(exc)
    mock_exit.assert_called_once_with(CFG.exit_codes.default)


def test_job_general_qq_error_single_item_logs_and_exits():
    metadata = CommandRunner.__new__(CommandRunner)
    metadata.n_jobs = 1
    metadata.encountered_errors = {RuntimeError("general error")}
    exc = RuntimeError("general error")

    with (
        patch("qq_lib.core.error_handlers.logger") as mock_logger,
        patch("qq_lib.core.error_handlers.sys.exit") as mock_exit,
    ):
        handle_general_qq_error(exc, metadata)

    mock_logger.error.assert_called_once_with(exc)
    mock_exit.assert_called_once_with(CFG.exit_codes.default)


def test_job_general_qq_error_multiple_items_logs():
    metadata = CommandRunner.__new__(CommandRunner)
    metadata.n_jobs = 2
    metadata.encountered_errors = {0: RuntimeError("general error")}
    exc = RuntimeError("general error")

    with (
        patch("qq_lib.core.error_handlers.logger") as mock_logger,
        patch("qq_lib.core.error_handlers.sys.exit") as mock_exit,
    ):
        handle_general_qq_error(exc, metadata)

    mock_logger.error.assert_called_once_with(exc)
    mock_exit.assert_not_called()


def test_job_general_qq_error_multiple_items_multiple_errors_logs_and_exits():
    metadata = CommandRunner.__new__(CommandRunner)
    metadata.n_jobs = 2
    metadata.encountered_errors = {
        0: RuntimeError("general error"),
        1: QQNotSuitableError("not suitable"),
    }
    exc = RuntimeError("general error")

    with (
        patch("qq_lib.core.error_handlers.logger") as mock_logger,
        patch("qq_lib.core.error_handlers.sys.exit") as mock_exit,
    ):
        handle_general_qq_error(exc, metadata)

    mock_logger.error.assert_called_once_with(exc)
    mock_exit.assert_called_once_with(CFG.exit_codes.default)
