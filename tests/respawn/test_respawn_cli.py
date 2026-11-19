# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.error_handlers import (
    handle_general_qq_error,
    handle_not_suitable_error,
)
from qq_lib.properties.states import RealState
from qq_lib.respawn.cli import _respawn_job, logger, respawn
from qq_lib.respawn.respawner import Respawner


@pytest.fixture
def informer_for_respawn():
    informer = MagicMock()
    informer.info.job_id = "111"
    informer.get_info_file.return_value = Path("/tmp/input/job.qqinfo")
    informer.info.input_machine = "submit-node"
    informer.info.loop_info = None
    return informer


def test_respawn_job_succeeds_when_failed(informer_for_respawn):
    informer_for_respawn.get_real_state.return_value = RealState.FAILED

    with (
        patch.object(Respawner, "respawn", return_value="222"),
        patch.object(Respawner, "print_info"),
        patch("qq_lib.respawn.cli.logger") as mock_logger,
    ):
        _respawn_job(informer_for_respawn)

    mock_logger.info.assert_any_call("Job '111' successfully respawned as '222'.")


def test_respawn_job_succeeds_when_killed(informer_for_respawn):
    informer_for_respawn.get_real_state.return_value = RealState.KILLED

    with (
        patch.object(Respawner, "respawn", return_value="222"),
        patch.object(Respawner, "print_info"),
        patch("qq_lib.respawn.cli.logger") as mock_logger,
    ):
        _respawn_job(informer_for_respawn)

    mock_logger.info.assert_any_call("Job '111' successfully respawned as '222'.")


@pytest.mark.parametrize(
    "state",
    [s for s in RealState if s not in {RealState.FAILED, RealState.KILLED}],
)
def test_respawn_job_raises_when_not_suitable(informer_for_respawn, state):
    informer_for_respawn.get_real_state.return_value = state

    with (
        patch.object(Respawner, "print_info"),
        pytest.raises(QQNotSuitableError, match="cannot be respawned"),
    ):
        _respawn_job(informer_for_respawn)


@pytest.mark.parametrize(
    "state",
    [s for s in RealState if s not in {RealState.FAILED, RealState.KILLED}],
)
def test_respawn_job_does_not_respawn_when_not_suitable(informer_for_respawn, state):
    informer_for_respawn.get_real_state.return_value = state

    with (
        patch.object(Respawner, "respawn") as mock_respawn,
        patch.object(Respawner, "print_info"),
        pytest.raises(QQNotSuitableError),
    ):
        _respawn_job(informer_for_respawn)

    mock_respawn.assert_not_called()


def test_respawn_job_prints_info_before_respawning(informer_for_respawn):
    informer_for_respawn.get_real_state.return_value = RealState.FAILED
    call_order = []

    with (
        patch.object(
            Respawner, "print_info", side_effect=lambda _: call_order.append("print")
        ),
        patch.object(
            Respawner,
            "respawn",
            side_effect=lambda **_: (call_order.append("respawn"), "222")[1],
        ),
    ):
        _respawn_job(informer_for_respawn)

    assert call_order == ["print", "respawn"]


def test_respawn_job_propagates_respawn_error(informer_for_respawn):
    informer_for_respawn.get_real_state.return_value = RealState.FAILED

    with (
        patch.object(Respawner, "print_info"),
        patch.object(Respawner, "respawn", side_effect=QQError("submission failed")),
        pytest.raises(QQError, match="submission failed"),
    ):
        _respawn_job(informer_for_respawn)


def test_respawn_creates_command_runner_and_runs():
    runner = CliRunner()

    with patch("qq_lib.respawn.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(respawn, ["111"])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with(
        ("111",),
        (),
        False,
        None,
        _respawn_job,
        logger,
        False,
        False,
        recursive=False,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    )
    mock_cls.return_value.run.assert_called_once()


def test_respawn_creates_command_runner_with_dirs_and_runs(tmp_path):
    dir1 = tmp_path / "dir1"
    dir2 = tmp_path / "dir2"
    dir1.mkdir()
    dir2.mkdir()
    runner = CliRunner()

    with patch("qq_lib.respawn.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(respawn, ["-d", str(dir1), str(dir2)])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with(
        (),
        (dir1, dir2),
        False,
        None,
        _respawn_job,
        logger,
        False,
        False,
        recursive=False,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    )
    mock_cls.return_value.run.assert_called_once()


def test_respawn_creates_command_runner_with_dirs_and_recursive_and_runs(tmp_path):
    dir1 = tmp_path / "dir1"
    dir2 = tmp_path / "dir2"
    dir1.mkdir()
    dir2.mkdir()
    runner = CliRunner()

    with patch("qq_lib.respawn.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(respawn, ["-d", str(dir1), str(dir2), "-r"])

    assert result.exit_code == 0
    mock_cls.assert_called_once_with(
        (),
        (dir1, dir2),
        False,
        None,
        _respawn_job,
        logger,
        False,
        False,
        recursive=True,
        n_threads=CFG.parallelization_options.job_info_max_threads,
    )
    mock_cls.return_value.run.assert_called_once()


def test_respawn_registers_exception_handlers():
    runner = CliRunner()

    with patch("qq_lib.respawn.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        runner.invoke(respawn, [])

    handlers = {
        c[0][0]: c[0][1] for c in mock_cls.return_value.on_exception.call_args_list
    }
    assert handlers[QQNotSuitableError] is handle_not_suitable_error
    assert handlers[QQError] is handle_general_qq_error


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        ([], (False, False)),
        (["--force-continue"], (True, False)),
        (["--force-rerun"], (False, True)),
    ],
)
def test_respawn_passes_force_options_to_command_runner(flags, expected):
    runner = CliRunner()

    with patch("qq_lib.respawn.cli.CommandRunner") as mock_cls:
        mock_cls.return_value.on_exception.return_value = mock_cls.return_value
        mock_cls.return_value.run.side_effect = SystemExit(0)

        result = runner.invoke(respawn, ["111", *flags])

    assert result.exit_code == 0
    assert mock_cls.call_args.args[6:] == expected


def test_respawn_rejects_both_force_options():
    runner = CliRunner()

    with patch("qq_lib.respawn.cli.CommandRunner") as mock_cls:
        result = runner.invoke(respawn, ["111", "--force-continue", "--force-rerun"])

    assert result.exit_code == 2
    assert "cannot be used together" in result.output
    mock_cls.assert_not_called()


def test_respawn_job_calls_respawn_without_force_options_by_default(
    informer_for_respawn,
):
    informer_for_respawn.get_real_state.return_value = RealState.FAILED

    with (
        patch.object(Respawner, "print_info"),
        patch.object(Respawner, "respawn", return_value="222") as mock_respawn,
        patch("qq_lib.respawn.cli.choice_prompt") as mock_prompt,
    ):
        _respawn_job(informer_for_respawn)

    mock_prompt.assert_not_called()
    mock_respawn.assert_called_once_with(force_continue=False, force_rerun=False)


@pytest.fixture
def mismatched_respawn(informer_for_respawn):
    """Patch Respawner so that the archive already contains files for cycle 6."""
    informer_for_respawn.get_real_state.return_value = RealState.FAILED

    with (
        patch.object(Respawner, "print_info"),
        patch.object(Respawner, "get_next_cycle_in_archive", return_value=6),
        patch.object(Respawner, "can_continue", return_value=True) as mock_can_continue,
        patch.object(Respawner, "respawn", return_value="222") as mock_respawn,
        patch("qq_lib.respawn.cli.choice_prompt") as mock_prompt,
        patch("qq_lib.respawn.cli.sys.stdin") as mock_stdin,
        patch("qq_lib.respawn.cli.logger") as mock_logger,
    ):
        mock_stdin.isatty.return_value = True
        yield {
            "informer": informer_for_respawn,
            "can_continue": mock_can_continue,
            "respawn": mock_respawn,
            "prompt": mock_prompt,
            "stdin": mock_stdin,
            "logger": mock_logger,
        }


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("c", {"force_continue": True, "force_rerun": False}),
        ("r", {"force_continue": False, "force_rerun": True}),
    ],
)
def test_respawn_job_on_mismatch_respawns_as_chosen(mismatched_respawn, key, expected):
    mismatched_respawn["prompt"].return_value = key

    _respawn_job(mismatched_respawn["informer"])

    mismatched_respawn["respawn"].assert_called_once_with(**expected)
    mismatched_respawn["logger"].info.assert_any_call(
        "Job '111' successfully respawned as '222'."
    )


def test_respawn_job_on_mismatch_cancels(mismatched_respawn):
    mismatched_respawn["prompt"].return_value = "x"

    _respawn_job(mismatched_respawn["informer"])

    mismatched_respawn["respawn"].assert_not_called()
    mismatched_respawn["logger"].info.assert_any_call("Operation aborted.")


def test_respawn_job_on_mismatch_offers_all_options(mismatched_respawn):
    mismatched_respawn["prompt"].return_value = "x"

    _respawn_job(mismatched_respawn["informer"])

    mismatched_respawn["prompt"].assert_called_once_with(
        [
            ("c", "Continue with cycle 6"),
            ("r", "rerun cycle 5"),
            ("x", "cancel this operation"),
        ]
    )
    warnings = [c.args[0] for c in mismatched_respawn["logger"].warning.call_args_list]
    assert any("failed in cycle 5" in w and "files for cycle 6" in w for w in warnings)


def test_respawn_job_on_mismatch_in_last_cycle_offers_only_rerun(mismatched_respawn):
    mismatched_respawn["can_continue"].return_value = False
    mismatched_respawn["prompt"].return_value = "r"

    _respawn_job(mismatched_respawn["informer"])

    mismatched_respawn["prompt"].assert_called_once_with(
        [("r", "Rerun cycle 5"), ("x", "cancel this operation")]
    )
    mismatched_respawn["respawn"].assert_called_once_with(
        force_continue=False, force_rerun=True
    )


@pytest.mark.parametrize(
    ("force_continue", "force_rerun"), [(True, False), (False, True)]
)
def test_respawn_job_on_mismatch_with_force_option_does_not_ask(
    mismatched_respawn, force_continue, force_rerun
):
    _respawn_job(
        mismatched_respawn["informer"],
        force_continue=force_continue,
        force_rerun=force_rerun,
    )

    mismatched_respawn["prompt"].assert_not_called()
    mismatched_respawn["respawn"].assert_called_once_with(
        force_continue=force_continue, force_rerun=force_rerun
    )


def test_respawn_job_on_mismatch_without_terminal_raises(mismatched_respawn):
    mismatched_respawn["stdin"].isatty.return_value = False

    with pytest.raises(QQError, match="--force-continue.*--force-rerun"):
        _respawn_job(mismatched_respawn["informer"])

    mismatched_respawn["prompt"].assert_not_called()
    mismatched_respawn["respawn"].assert_not_called()
