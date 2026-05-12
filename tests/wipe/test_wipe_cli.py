# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.error_handlers import (
    handle_general_qq_error,
    handle_not_suitable_error,
)
from qq_lib.wipe.cli import _wipe_work_dir, logger, wipe


@patch("qq_lib.wipe.cli.logger.info")
@patch("qq_lib.wipe.cli.Wiper.from_informer")
def test_wipe_work_dir_success_with_force(mock_wiper_from_informer, mock_logger_info):
    mock_wiper = MagicMock()
    mock_wiper.wipe.return_value = "job123"
    mock_wiper_from_informer.return_value = mock_wiper

    informer = MagicMock()
    _wipe_work_dir(informer, force=True, yes=False)

    mock_wiper.ensure_suitable.assert_not_called()
    mock_wiper.wipe.assert_called_once()
    mock_logger_info.assert_called_with(
        "Deleted the working directory of the job 'job123'."
    )


@patch("qq_lib.wipe.cli.logger.info")
@patch("qq_lib.wipe.cli.Wiper.from_informer")
@patch("qq_lib.wipe.cli.yes_or_no_prompt", return_value=True)
def test_wipe_work_dir_success_with_prompt(
    mock_prompt, mock_wiper_from_informer, mock_logger_info
):
    mock_wiper = MagicMock()
    mock_wiper.wipe.return_value = "jobXYZ"
    mock_wiper_from_informer.return_value = mock_wiper

    informer = MagicMock()
    _wipe_work_dir(informer, force=False, yes=False)

    mock_wiper.ensure_suitable.assert_called_once()
    mock_wiper.wipe.assert_called_once()
    mock_prompt.assert_called_once()
    mock_logger_info.assert_called_with(
        "Deleted the working directory of the job 'jobXYZ'."
    )


@patch("qq_lib.wipe.cli.logger.info")
@patch("qq_lib.wipe.cli.Wiper.from_informer")
@patch("qq_lib.wipe.cli.yes_or_no_prompt", return_value=False)
def test_wipe_work_dir_aborts_on_negative_prompt(
    mock_prompt, mock_wiper_from_informer, mock_logger_info
):
    mock_wiper = MagicMock()
    mock_wiper_from_informer.return_value = mock_wiper

    informer = MagicMock()
    _wipe_work_dir(informer, force=False, yes=False)

    mock_wiper.ensure_suitable.assert_called_once()
    mock_wiper.wipe.assert_not_called()
    mock_prompt.assert_called_once()
    mock_logger_info.assert_called_with("Operation aborted.")


@patch("qq_lib.wipe.cli.Wiper.from_informer")
def test_wipe_work_dir_raises_not_suitable_error(mock_wiper_from_informer):
    mock_wiper = MagicMock()
    mock_wiper.ensure_suitable.side_effect = QQNotSuitableError("Unsuitable job")
    mock_wiper_from_informer.return_value = mock_wiper

    informer = MagicMock()
    with pytest.raises(QQNotSuitableError, match="Unsuitable job"):
        _wipe_work_dir(informer, force=False, yes=True)


@patch("qq_lib.wipe.cli.Wiper.from_informer")
def test_wipe_work_dir_raises_general_error(mock_wiper_from_informer):
    mock_wiper = MagicMock()
    mock_wiper.wipe.side_effect = QQError("Cannot delete working directory")
    mock_wiper_from_informer.return_value = mock_wiper

    informer = MagicMock()
    with pytest.raises(QQError, match="Cannot delete working directory"):
        _wipe_work_dir(informer, force=True, yes=True)


def test_wipe_creates_command_runner_and_runs():
    runner = CliRunner()

    with patch("qq_lib.wipe.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(wipe, ["111"])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with(
        ("111",),
        _wipe_work_dir,
        logger,
        False,
        False,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    )
    mock_cls.return_value.run.assert_called_once()


def test_wipe_passes_force_and_yes_flags():
    runner = CliRunner()

    with patch("qq_lib.wipe.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        runner.invoke(wipe, ["--force", "--yes", "111"])

    assert mock_cls.call_args[0][3] is True
    assert mock_cls.call_args[0][4] is True


def test_wipe_registers_exception_handlers():
    runner = CliRunner()

    with patch("qq_lib.wipe.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        runner.invoke(wipe, [])

    handlers = {
        c[0][0]: c[0][1] for c in mock_cls.return_value.on_exception.call_args_list
    }
    assert handlers[QQNotSuitableError] is handle_not_suitable_error
    assert handlers[QQError] is handle_general_qq_error
