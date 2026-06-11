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
from qq_lib.sync.cli import _split_files, _sync_job, logger, sync


def test_sync_job_calls_sync():
    informer = MagicMock()
    syncer_mock = MagicMock()
    with (
        patch("qq_lib.sync.cli.Syncer.from_informer", return_value=syncer_mock),
        patch("qq_lib.sync.cli.console"),
    ):
        _sync_job(informer, ["a.txt", "b.txt"])

    syncer_mock.print_info.assert_called_once()
    syncer_mock.ensure_suitable.assert_called_once()
    syncer_mock.sync.assert_called_once_with(["a.txt", "b.txt"])


def test_sync_job_calls_sync_without_files():
    informer = MagicMock()
    syncer_mock = MagicMock()
    with (
        patch("qq_lib.sync.cli.Syncer.from_informer", return_value=syncer_mock),
        patch("qq_lib.sync.cli.console"),
    ):
        _sync_job(informer, None)

    syncer_mock.sync.assert_called_once_with(None)


def test_split_files_returns_none_when_input_none():
    assert _split_files(None) is None


def test_split_files_returns_none_when_input_empty_string():
    assert _split_files("") is None


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("a.txt", ["a.txt"]),
        ("a.txt b.txt", ["a.txt", "b.txt"]),
        ("a.txt,b.txt", ["a.txt", "b.txt"]),
        ("a.txt:b.txt", ["a.txt", "b.txt"]),
        ("a.txt , b.txt:c.txt  d.txt", ["a.txt", "b.txt", "c.txt", "d.txt"]),
    ],
)
def test_split_files_splits_correctly(input_str, expected):
    assert _split_files(input_str) == expected


def test_sync_creates_command_runner_and_runs():
    runner = CliRunner()

    with (
        patch("qq_lib.sync.cli.CommandRunner") as mock_cls,
        patch("qq_lib.sync.cli._split_files", return_value=["a.txt", "b.txt"]),
    ):
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(sync, ["111", "--files", "a.txt:b.txt"])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with(
        ("111",),
        (),
        False,
        None,
        _sync_job,
        logger,
        ["a.txt", "b.txt"],
        n_threads=CFG.parallelization_options.job_info_max_threads,
    )
    mock_cls.return_value.run.assert_called_once()


def test_sync_creates_command_runner_with_dirs_and_runs(tmp_path):
    dir1 = tmp_path / "dir1"
    dir2 = tmp_path / "dir2"
    dir1.mkdir()
    dir2.mkdir()
    runner = CliRunner()

    with patch("qq_lib.sync.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(sync, ["-d", str(dir1), str(dir2)])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with(
        (),
        (dir1, dir2),
        False,
        None,
        _sync_job,
        logger,
        None,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    )
    mock_cls.return_value.run.assert_called_once()


def test_sync_creates_complex_command_runner_and_runs(tmp_path):
    dir1 = tmp_path / "dir1"
    dir2 = tmp_path / "dir2"
    dir1.mkdir()
    dir2.mkdir()
    runner = CliRunner()

    with patch("qq_lib.sync.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(
            sync,
            ["12345", "12346", "-d", str(dir1), str(dir2), "--all", "-s", "server"],
        )

    assert result.exit_code == 0
    mock_cls.assert_called_once_with(
        ("12345", "12346"),
        (dir1, dir2),
        True,
        "server",
        _sync_job,
        logger,
        None,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    )
    mock_cls.return_value.run.assert_called_once()


def test_sync_without_files_passes_none():
    runner = CliRunner()

    with (
        patch("qq_lib.sync.cli.CommandRunner") as mock_cls,
        patch("qq_lib.sync.cli._split_files", return_value=None) as mock_split,
    ):
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        runner.invoke(sync, ["111"])

    mock_split.assert_called_once_with(None)
    assert mock_cls.call_args[0][3] is None


def test_sync_registers_exception_handlers():
    runner = CliRunner()

    with (
        patch("qq_lib.sync.cli.CommandRunner") as mock_cls,
        patch("qq_lib.sync.cli._split_files", return_value=None),
    ):
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        runner.invoke(sync, [])

    handlers = {
        c[0][0]: c[0][1] for c in mock_cls.return_value.on_exception.call_args_list
    }
    assert handlers[QQNotSuitableError] is handle_not_suitable_error
    assert handlers[QQError] is handle_general_qq_error
