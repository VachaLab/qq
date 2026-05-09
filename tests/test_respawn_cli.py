# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.properties.states import RealState
from qq_lib.respawn.cli import respawn, respawn_job
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
        respawn_job(informer_for_respawn)

    mock_logger.info.assert_any_call("Job '111' successfully respawned as '222'.")


def test_respawn_job_succeeds_when_killed(informer_for_respawn):
    informer_for_respawn.get_real_state.return_value = RealState.KILLED

    with (
        patch.object(Respawner, "respawn", return_value="222"),
        patch.object(Respawner, "print_info"),
        patch("qq_lib.respawn.cli.logger") as mock_logger,
    ):
        respawn_job(informer_for_respawn)

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
        respawn_job(informer_for_respawn)


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
        respawn_job(informer_for_respawn)

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
        respawn_job(informer_for_respawn)

    assert call_order == ["print", "respawn"]


def test_respawn_job_propagates_respawn_error(informer_for_respawn):
    informer_for_respawn.get_real_state.return_value = RealState.FAILED

    with (
        patch.object(Respawner, "print_info"),
        patch.object(Respawner, "respawn", side_effect=QQError("submission failed")),
        pytest.raises(QQError, match="submission failed"),
    ):
        respawn_job(informer_for_respawn)


def test_respawn_invokes_repeater_and_exits_success(tmp_path):
    dummy_file = tmp_path / "info.qq"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()
    informer_mock = MagicMock()

    with (
        patch("qq_lib.respawn.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.respawn.cli.Informer.from_file", return_value=informer_mock),
        patch("qq_lib.respawn.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.respawn.cli.logger"),
    ):
        result = runner.invoke(respawn, [])

    assert result.exit_code == 0

    calls = [c[0][0] for c in repeater_mock.on_exception.call_args_list]
    assert QQNotSuitableError in calls
    assert QQError in calls

    repeater_mock.run.assert_called_once()


def test_respawn_with_job_id_invokes_repeater():
    runner = CliRunner()
    repeater_mock = MagicMock()
    informer_mock = MagicMock()

    with (
        patch("qq_lib.respawn.cli.Informer.from_job_id", return_value=informer_mock),
        patch("qq_lib.respawn.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.respawn.cli.logger"),
    ):
        result = runner.invoke(respawn, ["12345"])

    assert result.exit_code == 0
    repeater_mock.run.assert_called_once()


def test_respawn_catches_qqerror_and_exits_with_default_code():
    runner = CliRunner()
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = QQError("error occurred")
    informer_mock = MagicMock()

    with (
        patch("qq_lib.respawn.cli.Informer.from_job_id", return_value=informer_mock),
        patch("qq_lib.respawn.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.respawn.cli.logger") as mock_logger,
    ):
        result = runner.invoke(respawn, ["12345"])

    assert result.exit_code == CFG.exit_codes.default
    mock_logger.error.assert_called_once_with(repeater_mock.run.side_effect)


def test_respawn_catches_generic_exception_and_exits_with_unexpected_error_code():
    runner = CliRunner()
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = Exception("critical error")
    informer_mock = MagicMock()

    with (
        patch("qq_lib.respawn.cli.Informer.from_job_id", return_value=informer_mock),
        patch("qq_lib.respawn.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.respawn.cli.logger") as mock_logger,
    ):
        result = runner.invoke(respawn, ["12345"])

    assert result.exit_code == CFG.exit_codes.unexpected_error
    mock_logger.critical.assert_called_once()
