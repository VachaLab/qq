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
from qq_lib.kill.cli import kill, kill_job, logger


def test_kill_job_force_skips_suitability_and_logs_killed():
    with (
        patch("qq_lib.kill.cli.Killer.from_informer") as mock_killer_ctor,
        patch("qq_lib.kill.cli.logger.info") as mock_logger,
        patch("qq_lib.kill.cli.yes_or_no_prompt") as mock_prompt,
        patch("qq_lib.kill.cli.console"),
    ):
        mock_killer = MagicMock()
        mock_killer.kill.return_value = "1234"
        mock_killer_ctor.return_value = mock_killer

        kill_job(MagicMock(), force=True, yes=False)

        mock_killer.ensure_suitable.assert_not_called()
        mock_killer.kill.assert_called_once_with(True)
        mock_prompt.assert_not_called()
        mock_logger.assert_called_once_with("Killed the job '1234'.")


def test_kill_job_prompts_yes_and_kills():
    with (
        patch("qq_lib.kill.cli.Killer.from_informer") as mock_killer_ctor,
        patch("qq_lib.kill.cli.logger.info") as mock_logger,
        patch("qq_lib.kill.cli.console"),
        patch("qq_lib.kill.cli.yes_or_no_prompt", return_value=True),
    ):
        mock_killer = MagicMock()
        mock_killer.kill.return_value = "5678"
        mock_killer_ctor.return_value = mock_killer

        kill_job(MagicMock(), force=False, yes=False)

        mock_killer.ensure_suitable.assert_called_once()
        mock_killer.kill.assert_called_once_with(False)
        mock_logger.assert_called_once_with("Killed the job '5678'.")


def test_kill_job_prompts_no_and_aborts():
    with (
        patch("qq_lib.kill.cli.Killer.from_informer") as mock_killer_ctor,
        patch("qq_lib.kill.cli.logger.info") as mock_logger,
        patch("qq_lib.kill.cli.console"),
        patch("qq_lib.kill.cli.yes_or_no_prompt", return_value=False),
    ):
        mock_killer = MagicMock()
        mock_killer_ctor.return_value = mock_killer

        kill_job(MagicMock(), force=False, yes=False)

        mock_killer.kill.assert_not_called()
        mock_logger.assert_called_once_with("Operation aborted.")


def test_kill_creates_command_runner_and_runs():
    runner = CliRunner()

    with patch("qq_lib.kill.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(kill, ["111"])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with(
        ("111",),
        kill_job,
        logger,
        False,
        False,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    )
    mock_cls.return_value.run.assert_called_once()


def test_kill_passes_force_and_yes_flags():
    runner = CliRunner()

    with patch("qq_lib.kill.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        runner.invoke(kill, ["--force", "--yes", "111"])

    # force is arg index 3, yes is arg index 4
    assert mock_cls.call_args[0][3] is True
    assert mock_cls.call_args[0][4] is True


def test_kill_registers_exception_handlers():
    runner = CliRunner()

    with patch("qq_lib.kill.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        runner.invoke(kill, [])

    handlers = {
        c[0][0]: c[0][1] for c in mock_cls.return_value.on_exception.call_args_list
    }
    assert handlers[QQNotSuitableError] is handle_not_suitable_error
    assert handlers[QQError] is handle_general_qq_error
