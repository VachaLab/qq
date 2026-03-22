# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.go.goer import Goer
from qq_lib.properties.info import Success
from qq_lib.properties.states import RealState
from qq_lib.properties.transfer_mode import Always, ExitCode, Failure, Never


def test_goer_wait_queued():
    goer = Goer.__new__(Goer)
    goer._is_queued = MagicMock(side_effect=[True, True, False])
    goer.update = MagicMock()
    goer.ensure_suitable = MagicMock()

    with patch("qq_lib.go.goer.sleep") as mock_sleep:
        goer._wait_queued()

        # everything called twice (once per True in side_effect except last)
        assert mock_sleep.call_count == 2
        assert goer.update.call_count == 2
        assert goer.ensure_suitable.call_count == 2


def test_goer_wait_queued_raises_not_suitable_error():
    goer = Goer.__new__(Goer)
    goer._is_queued = MagicMock(return_value=True)
    goer.update = MagicMock()
    goer.ensure_suitable = MagicMock(side_effect=QQNotSuitableError("not suitable"))

    with patch("qq_lib.go.goer.sleep"):
        with pytest.raises(QQNotSuitableError):
            goer._wait_queued()

        # ensure that update is called at least once before exception
        goer.update.assert_called_once()


@pytest.mark.parametrize(
    "state, job_exit_code, transfer_mode, expected_message",
    [
        (
            RealState.FINISHED,
            0,
            [Success()],
            "Job has been completed and was synchronized: working directory no longer exists",
        ),
        (
            RealState.FINISHED,
            0,
            [Always()],
            "Job has been completed and was synchronized: working directory no longer exists",
        ),
        (
            RealState.FINISHED,
            0,
            [ExitCode(0)],
            "Job has been completed and was synchronized: working directory no longer exists",
        ),
        (
            RealState.FAILED,
            1,
            [Failure()],
            "Job has been completed and was synchronized: working directory no longer exists",
        ),
        (
            RealState.FAILED,
            1,
            [Always()],
            "Job has been completed and was synchronized: working directory no longer exists",
        ),
        (
            RealState.FAILED,
            1,
            [ExitCode(1)],
            "Job has been completed and was synchronized: working directory no longer exists",
        ),
        (
            RealState.EXITING,
            0,
            [Always()],
            "Job has been completed and was synchronized: working directory no longer exists",
        ),
        (
            RealState.EXITING,
            0,
            [Success()],
            "Job has been completed and was synchronized: working directory no longer exists",
        ),
        (
            RealState.EXITING,
            1,
            [Always()],
            "Job has been completed and was synchronized: working directory no longer exists",
        ),
        (
            RealState.EXITING,
            1,
            [Failure()],
            "Job has been completed and was synchronized: working directory no longer exists",
        ),
        (
            RealState.EXITING,
            2,
            [ExitCode(2)],
            "Job has been completed and was synchronized: working directory no longer exists",
        ),
    ],
)
def test_goer_ensure_suitable_raises_for_unsuitable_states(
    state, job_exit_code, transfer_mode, expected_message
):
    goer = Goer.__new__(Goer)
    goer._state = state
    goer._informer = MagicMock()
    goer._informer.info.job_exit_code = job_exit_code
    goer._informer.get_real_state.return_value = state
    goer._informer.info.transfer_mode = transfer_mode

    goer._work_dir = Path("/workdir")
    goer._main_node = "node"
    goer._input_machine = "input"
    goer._informer.info.input_dir = Path("/different_input")
    goer._informer.uses_scratch.return_value = True
    goer._batch_system = MagicMock()

    with pytest.raises(QQNotSuitableError, match=expected_message):
        goer.ensure_suitable()


@pytest.mark.parametrize(
    "destination", [(None, "host"), (Path("some/path"), None), (None, None)]
)
def test_goer_ensure_suitable_raises_killed_without_destination(destination):
    goer = Goer.__new__(Goer)
    goer._state = RealState.KILLED
    goer._informer = MagicMock()
    goer._informer.info.job_exit_code = None
    goer._work_dir, goer._main_node = destination

    with pytest.raises(
        QQNotSuitableError,
        match="Job has been killed and no working directory has been created.",
    ):
        goer.ensure_suitable()


@pytest.mark.parametrize(
    "state, job_exit_code, transfer_mode",
    [
        (
            RealState.QUEUED,
            None,
            [Always()],
        ),
        (
            RealState.BOOTING,
            None,
            [Always()],
        ),
        (
            RealState.HELD,
            None,
            [Always()],
        ),
        (
            RealState.WAITING,
            None,
            [Always()],
        ),
        (
            RealState.RUNNING,
            None,
            [Always()],
        ),
        (
            RealState.SUSPENDED,
            None,
            [Always()],
        ),
        (
            RealState.FINISHED,
            0,
            [Failure()],
        ),
        (
            RealState.FINISHED,
            0,
            [Never()],
        ),
        (
            RealState.FAILED,
            1,
            [Success()],
        ),
        (
            RealState.FAILED,
            2,
            [Success(), ExitCode(1)],
        ),
        (
            RealState.FAILED,
            1,
            [Never()],
        ),
        (
            RealState.KILLED,
            None,
            [Failure()],
        ),
        (
            RealState.KILLED,
            None,
            [Always()],
        ),
        (
            RealState.KILLED,
            None,
            [Never()],
        ),
        (
            RealState.KILLED,
            None,
            [Success()],
        ),
        (
            RealState.UNKNOWN,
            None,
            [Success()],
        ),
        (
            RealState.UNKNOWN,
            None,
            [Always()],
        ),
        (
            RealState.UNKNOWN,
            1,
            [Success()],
        ),
        (
            RealState.IN_AN_INCONSISTENT_STATE,
            None,
            [Success()],
        ),
        (
            RealState.IN_AN_INCONSISTENT_STATE,
            None,
            [Always()],
        ),
    ],
)
def test_goer_ensure_suitable_passes_for_allowed_states(
    state, job_exit_code, transfer_mode
):
    goer = Goer.__new__(Goer)
    goer._state = state
    goer._informer = MagicMock()
    goer._informer.info.job_exit_code = job_exit_code
    goer._informer.info.transfer_mode = transfer_mode
    goer._informer.get_real_state.return_value = state
    goer._work_dir = Path("/valid/workdir")
    goer._main_node = "main"
    goer._input_machine = "input"
    goer._informer.info.input_dir = Path("/different")
    goer._informer.uses_scratch.return_value = True
    goer._batch_system = MagicMock()

    goer.ensure_suitable()


@pytest.mark.parametrize(
    "state, job_exit_code, transfer_mode",
    [
        (
            RealState.FINISHED,
            0,
            [Success()],
        ),
        (
            RealState.FINISHED,
            0,
            [Always()],
        ),
        (
            RealState.FINISHED,
            0,
            [ExitCode(0)],
        ),
        (
            RealState.FAILED,
            1,
            [Failure()],
        ),
        (
            RealState.FAILED,
            1,
            [Always()],
        ),
        (
            RealState.FAILED,
            1,
            [ExitCode(1)],
        ),
        (
            RealState.EXITING,
            0,
            [Always()],
        ),
        (
            RealState.EXITING,
            0,
            [Success()],
        ),
        (
            RealState.EXITING,
            1,
            [Always()],
        ),
        (
            RealState.EXITING,
            1,
            [Failure()],
        ),
        (
            RealState.EXITING,
            2,
            [ExitCode(2)],
        ),
    ],
)
def test_goer_ensure_suitable_passes_for_synchronized_states_if_workdir_is_inputdir(
    state, job_exit_code, transfer_mode
):
    goer = Goer.__new__(Goer)
    goer._state = state
    goer._informer = MagicMock()
    goer._informer.info.job_exit_code = job_exit_code
    goer._informer.get_real_state.return_value = state
    goer._informer.info.transfer_mode = transfer_mode
    same_path = Path("/shared/path")

    goer._work_dir = same_path
    goer._main_node = "main"
    goer._input_machine = "main"
    goer._informer.info.input_dir = same_path
    goer._informer.uses_scratch.return_value = False
    goer._batch_system = MagicMock()

    goer.ensure_suitable()


def test_goer_go_already_in_work_dir_logs_info_and_returns():
    goer = Goer.__new__(Goer)
    goer._batch_system = MagicMock()
    goer._is_in_work_dir = MagicMock(return_value=True)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.info.assert_called_once_with(
            "You are already in the working directory."
        )
        goer._batch_system.navigate_to_destination.assert_not_called()


def test_goer_go_killed_state_logs_warning_and_navigates():
    goer = Goer.__new__(Goer)
    goer._state = RealState.KILLED
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._is_in_work_dir = MagicMock(return_value=False)
    goer._work_dir_is_input_dir = MagicMock(return_value=False)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.warning.assert_called_once_with(
            "Job has been killed: working directory may no longer exist."
        )
        goer._batch_system.navigate_to_destination.assert_called_once_with(
            goer._main_node, goer._work_dir
        )


def test_goer_go_killed_state_navigates_without_warning_if_workdir_is_inputdir():
    goer = Goer.__new__(Goer)
    goer._state = RealState.KILLED
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._is_in_work_dir = MagicMock(return_value=False)
    goer._work_dir_is_input_dir = MagicMock(return_value=True)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.warning.assert_not_called()
        goer._batch_system.navigate_to_destination.assert_called_once_with(
            goer._main_node, goer._work_dir
        )


def test_goer_go_failed_state_logs_warning_and_navigates():
    goer = Goer.__new__(Goer)
    goer._state = RealState.FAILED
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._is_in_work_dir = MagicMock(return_value=False)
    goer._work_dir_is_input_dir = MagicMock(return_value=False)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.warning.assert_called_once_with(
            "Job has been completed: working directory may no longer exist."
        )
        goer._batch_system.navigate_to_destination.assert_called_once_with(
            goer._main_node, goer._work_dir
        )


def test_goer_go_failed_state_navigates_without_warning_if_workdir_is_inputdir():
    goer = Goer.__new__(Goer)
    goer._state = RealState.FAILED
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._is_in_work_dir = MagicMock(return_value=False)
    goer._work_dir_is_input_dir = MagicMock(return_value=True)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.warning.assert_not_called()
        goer._batch_system.navigate_to_destination.assert_called_once_with(
            goer._main_node, goer._work_dir
        )


def test_goer_go_finished_state_logs_warning_and_navigates():
    goer = Goer.__new__(Goer)
    goer._state = RealState.FINISHED
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._is_in_work_dir = MagicMock(return_value=False)
    goer._work_dir_is_input_dir = MagicMock(return_value=False)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.warning.assert_called_once_with(
            "Job has been completed: working directory may no longer exist."
        )
        goer._batch_system.navigate_to_destination.assert_called_once_with(
            goer._main_node, goer._work_dir
        )


def test_goer_go_finished_state_navigates_without_warning_if_workdir_is_inputdir():
    goer = Goer.__new__(Goer)
    goer._state = RealState.FINISHED
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._is_in_work_dir = MagicMock(return_value=False)
    goer._work_dir_is_input_dir = MagicMock(return_value=True)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.warning.assert_not_called()
        goer._batch_system.navigate_to_destination.assert_called_once_with(
            goer._main_node, goer._work_dir
        )


@pytest.mark.parametrize(
    "state", [RealState.UNKNOWN, RealState.IN_AN_INCONSISTENT_STATE]
)
def test_goer_go_unknown_inconsistent_logs_warning_and_navigates(state):
    goer = Goer.__new__(Goer)
    goer._state = state
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._is_in_work_dir = MagicMock(return_value=False)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.warning.assert_called_once_with(
            "Job is in an unknown, unrecognized, or inconsistent state."
        )
        goer._batch_system.navigate_to_destination.assert_called_once_with(
            goer._main_node, goer._work_dir
        )


@pytest.mark.parametrize(
    "state", [RealState.QUEUED, RealState.BOOTING, RealState.HELD, RealState.WAITING]
)
def test_goer_go_queued_state_in_work_dir_calls_waitqueued_and_logs_info(state):
    goer = Goer.__new__(Goer)
    goer._state = state
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._is_in_work_dir = MagicMock(side_effect=[False, True])
    goer._wait_queued = MagicMock()

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        goer._wait_queued.assert_called_once()
        mock_logger.info.assert_called_with("You are already in the working directory.")
        goer._batch_system.navigate_to_destination.assert_not_called()


@pytest.mark.parametrize(
    "state", [RealState.QUEUED, RealState.BOOTING, RealState.HELD, RealState.WAITING]
)
def test_goer_go_queued_state_not_in_work_dir_navigates(state):
    goer = Goer.__new__(Goer)
    goer._state = state
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._is_in_work_dir = MagicMock(side_effect=[False, False])
    goer._wait_queued = MagicMock()

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        goer._wait_queued.assert_called_once()
        mock_logger.info.assert_called_with(
            f"Navigating to '{str(goer._work_dir)}' on '{goer._main_node}'."
        )
        goer._batch_system.navigate_to_destination.assert_called_once_with(
            goer._main_node, goer._work_dir
        )


def test_goer_go_no_destination_raises_error():
    goer = Goer.__new__(Goer)
    goer._state = RealState.RUNNING
    goer._work_dir = None
    goer._main_node = None
    goer._batch_system = MagicMock()
    goer._is_in_work_dir = MagicMock(return_value=False)

    with (
        patch("qq_lib.go.goer.logger"),
        pytest.raises(
            QQError,
            match="Host \\('main_node'\\) or working directory \\('work_dir'\\) are not defined.",
        ),
    ):
        goer.go()


def test_goer_go_navigates_when_suitable():
    goer = Goer.__new__(Goer)
    goer._state = RealState.RUNNING
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._is_in_work_dir = MagicMock(return_value=False)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.info.assert_called_with(
            f"Navigating to '{str(goer._work_dir)}' on '{goer._main_node}'."
        )
        goer._batch_system.navigate_to_destination.assert_called_once_with(
            goer._main_node, goer._work_dir
        )
