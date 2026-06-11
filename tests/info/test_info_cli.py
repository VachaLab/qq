# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.error_handlers import handle_general_qq_error
from qq_lib.info.cli import _info_for_job, info, logger


def test_info_for_job_short_prints_short_info():
    informer_mock = MagicMock()
    presenter_mock = MagicMock()
    short_info_mock = MagicMock()

    presenter_mock.get_brief_info.return_value = short_info_mock

    with (
        patch(
            "qq_lib.info.cli.Presenter", return_value=presenter_mock
        ) as presenter_cls,
        patch("qq_lib.info.cli.Console") as console_cls,
    ):
        console_instance = console_cls.return_value
        _info_for_job(informer_mock, brief=True, dirs_provided=False)

        presenter_cls.assert_called_once_with(informer_mock)
        presenter_mock.get_brief_info.assert_called_once()
        console_instance.print.assert_called_once_with(short_info_mock)


def test_info_for_job_full_prints_full_info_panel():
    informer_mock = MagicMock()
    presenter_mock = MagicMock()
    panel_mock = MagicMock()

    presenter_mock.create_full_info_panel.return_value = panel_mock

    with (
        patch(
            "qq_lib.info.cli.Presenter", return_value=presenter_mock
        ) as presenter_cls,
        patch("qq_lib.info.cli.Console") as console_cls,
    ):
        console_instance = console_cls.return_value
        _info_for_job(informer_mock, brief=False, dirs_provided=True)

        presenter_cls.assert_called_once_with(informer_mock)
        presenter_mock.create_full_info_panel.assert_called_once_with(console_instance)
        console_instance.print.assert_called_once_with(panel_mock)


def test_info_creates_command_runner_and_runs():
    runner = CliRunner()

    with patch("qq_lib.info.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(info, ["111"])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with(
        ("111",),
        (),
        False,
        None,
        _info_for_job,
        logger,
        False,
        False,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    )
    mock_cls.return_value.run.assert_called_once()


def test_info_creates_command_runner_with_dirs_and_runs(tmp_path):
    dir1 = tmp_path / "dir1"
    dir2 = tmp_path / "dir2"
    dir1.mkdir()
    dir2.mkdir()
    runner = CliRunner()

    with patch("qq_lib.info.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(info, ["-d", str(dir1), str(dir2)])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with(
        (),
        (dir1, dir2),
        False,
        None,
        _info_for_job,
        logger,
        False,
        True,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    )
    mock_cls.return_value.run.assert_called_once()


def test_info_creates_complex_command_runner_and_runs(tmp_path):
    dir1 = tmp_path / "dir1"
    dir2 = tmp_path / "dir2"
    dir1.mkdir()
    dir2.mkdir()
    runner = CliRunner()

    with patch("qq_lib.info.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(
            info,
            ["12345", "12346", "-d", str(dir1), str(dir2), "--all", "-s", "server"],
        )

    assert result.exit_code == 0
    mock_cls.assert_called_once_with(
        ("12345", "12346"),
        (dir1, dir2),
        True,
        "server",
        _info_for_job,
        logger,
        False,
        True,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    )
    mock_cls.return_value.run.assert_called_once()


def test_info_passes_short_flag():
    runner = CliRunner()

    with patch("qq_lib.info.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        runner.invoke(info, ["--short", "111"])

    assert mock_cls.call_args[0][-2] is True


def test_info_registers_exception_handlers():
    runner = CliRunner()

    with patch("qq_lib.info.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        runner.invoke(info, [])

    handlers = {
        c[0][0]: c[0][1] for c in mock_cls.return_value.on_exception.call_args_list
    }
    assert handlers[QQError] is handle_general_qq_error
