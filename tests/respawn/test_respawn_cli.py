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
            side_effect=lambda: (call_order.append("respawn"), "222")[1],
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
        _respawn_job,
        logger,
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
