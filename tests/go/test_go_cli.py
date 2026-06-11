# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.error_handlers import (
    handle_general_qq_error,
    handle_not_suitable_error,
)
from qq_lib.go.cli import _go_to_job, go, logger


def test_go_to_job_calls_printinfo_ensure_suitable_and_go():
    informer = MagicMock()
    goer_mock = MagicMock()

    with (
        patch("qq_lib.go.cli.Goer.from_informer", return_value=goer_mock),
        patch("qq_lib.go.cli.console", new=MagicMock()),
    ):
        _go_to_job(informer)

    goer_mock.print_info.assert_called_once()
    goer_mock.ensure_suitable.assert_called_once()
    goer_mock.go.assert_called_once()


def test_go_creates_command_runner_and_runs():
    runner = CliRunner()

    with patch("qq_lib.go.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(go, ["111", "222"])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with(
        ("111", "222"),
        (),
        False,
        None,
        _go_to_job,
        logger,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    )
    mock_cls.return_value.run.assert_called_once()


def test_go_creates_command_runner_with_dirs_and_runs(tmp_path):
    dir1 = tmp_path / "dir1"
    dir2 = tmp_path / "dir2"
    dir1.mkdir()
    dir2.mkdir()
    runner = CliRunner()

    with patch("qq_lib.go.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(go, ["-d", str(dir1), str(dir2)])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with(
        (),
        (dir1, dir2),
        False,
        None,
        _go_to_job,
        logger,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    )
    mock_cls.return_value.run.assert_called_once()


def test_go_creates_complex_command_runner_and_runs(tmp_path):
    dir1 = tmp_path / "dir1"
    dir2 = tmp_path / "dir2"
    dir1.mkdir()
    dir2.mkdir()
    runner = CliRunner()

    with patch("qq_lib.go.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(
            go,
            ["12345", "12346", "-d", str(dir1), str(dir2), "--all", "-s", "server"],
        )

    assert result.exit_code == 0
    mock_cls.assert_called_once_with(
        ("12345", "12346"),
        (dir1, dir2),
        True,
        "server",
        _go_to_job,
        logger,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    )
    mock_cls.return_value.run.assert_called_once()


def test_go_registers_exception_handlers():
    runner = CliRunner()

    with patch("qq_lib.go.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        runner.invoke(go, [])

    handlers = {
        c[0][0]: c[0][1] for c in mock_cls.return_value.on_exception.call_args_list
    }
    assert handlers[QQNotSuitableError] is handle_not_suitable_error
    assert handlers[QQError] is handle_general_qq_error


def test_go_without_args_passes_empty_tuple():
    runner = CliRunner()

    with patch("qq_lib.go.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        runner.invoke(go, [])

    assert mock_cls.call_args[0][0] == ()
