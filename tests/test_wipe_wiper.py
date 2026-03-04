# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.properties.info import Success
from qq_lib.properties.states import RealState
from qq_lib.properties.transfer_mode import Always, ExitCode, Failure, Never
from qq_lib.wipe.wiper import Wiper


@pytest.mark.parametrize(
    "state, job_exit_code, transfer_mode, expected_message",
    [
        (
            RealState.QUEUED,
            None,
            [Always()],
            "Job is queued and does not have a working directory yet",
        ),
        (
            RealState.BOOTING,
            None,
            [Always()],
            "Job is booting and does not have a working directory yet",
        ),
        (
            RealState.HELD,
            None,
            [Always()],
            "Job is held and does not have a working directory yet",
        ),
        (
            RealState.WAITING,
            None,
            [Always()],
            "Job is waiting and does not have a working directory yet",
        ),
        (
            RealState.RUNNING,
            None,
            [Always()],
            "Job is running. It is not safe to delete the working directory",
        ),
        (
            RealState.SUSPENDED,
            None,
            [Always()],
            "Job is suspended. It is not safe to delete the working directory",
        ),
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
            RealState.FINISHED,
            0,
            [Failure()],
            "It may not be safe to delete a working directory of a successfully finished job",
        ),
        (
            RealState.FINISHED,
            0,
            [Never()],
            "It may not be safe to delete a working directory of a successfully finished job",
        ),
        (
            RealState.FINISHED,
            0,
            [ExitCode(3)],
            "It may not be safe to delete a working directory of a successfully finished job",
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
def test_wiper_ensure_suitable_raises_for_disallowed_states(
    state, job_exit_code, transfer_mode, expected_message
):
    wiper = Wiper.__new__(Wiper)
    wiper._state = state
    wiper._informer = MagicMock()
    wiper._informer.info.job_exit_code = job_exit_code
    wiper._informer.getRealState.return_value = state
    wiper._informer.info.transfer_mode = transfer_mode

    wiper._work_dir = Path("/workdir")
    wiper._main_node = "node"
    wiper._input_machine = "input"
    wiper._informer.info.input_dir = Path("/different_input")
    wiper._informer.usesScratch.return_value = True
    wiper._batch_system = MagicMock()

    with pytest.raises(QQNotSuitableError, match=expected_message):
        wiper.ensureSuitable()


def test_wiper_ensure_suitable_raises_when_destination_missing():
    wiper = Wiper.__new__(Wiper)
    wiper._state = RealState.FAILED
    wiper._informer = MagicMock()
    wiper._informer.info.job_exit_code = 1
    wiper._informer.getRealState.return_value = RealState.FAILED

    wiper._work_dir = None
    wiper._main_node = None
    wiper._input_machine = "input"
    wiper._informer.info.input_dir = Path("/input")
    wiper._informer.usesScratch.return_value = True
    wiper._batch_system = MagicMock()

    with pytest.raises(
        QQNotSuitableError, match="Job does not have a working directory."
    ):
        wiper.ensureSuitable()


def test_wiper_ensure_suitable_raises_when_workdir_is_inputdir():
    wiper = Wiper.__new__(Wiper)
    wiper._state = RealState.FAILED
    wiper._informer = MagicMock()
    wiper._informer.info.job_exit_code = 1
    wiper._informer.getRealState.return_value = RealState.FAILED
    same_path = Path("/shared/path")

    wiper._work_dir = same_path
    wiper._main_node = "main"
    wiper._input_machine = "main"
    wiper._informer.info.input_dir = same_path
    wiper._informer.usesScratch.return_value = False
    wiper._batch_system = MagicMock()

    with pytest.raises(
        QQNotSuitableError, match="Working directory of the job is the input directory"
    ):
        wiper.ensureSuitable()


@pytest.mark.parametrize(
    "state, job_exit_code, transfer_mode",
    [
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
def test_wiper_ensure_suitable_passes_for_allowed_states(
    state, job_exit_code, transfer_mode
):
    wiper = Wiper.__new__(Wiper)
    wiper._state = state
    wiper._informer = MagicMock()
    wiper._informer.info.job_exit_code = job_exit_code
    wiper._informer.info.transfer_mode = transfer_mode
    wiper._informer.getRealState.return_value = state
    wiper._work_dir = Path("/valid/workdir")
    wiper._main_node = "main"
    wiper._input_machine = "input"
    wiper._informer.info.input_dir = Path("/different")
    wiper._informer.usesScratch.return_value = True
    wiper._batch_system = MagicMock()

    wiper.ensureSuitable()


@pytest.mark.parametrize(
    "has_destination, workdir_is_inputdir, expected_exception, expected_message",
    [
        (
            False,
            False,
            QQError,
            "not defined",
        ),
        (
            True,
            True,
            QQError,
            "Working directory of the job is the input directory of the job",
        ),
    ],
)
def test_wiper_wipe_raises_for_invalid_conditions(
    has_destination, workdir_is_inputdir, expected_exception, expected_message
):
    wiper = Wiper.__new__(Wiper)
    wiper.hasDestination = MagicMock(return_value=has_destination)
    wiper._workDirIsInputDir = MagicMock(return_value=workdir_is_inputdir)
    wiper._batch_system = MagicMock()
    wiper._informer = MagicMock()
    wiper._main_node = "node"
    wiper._work_dir = Path("/workdir")

    with pytest.raises(expected_exception, match=expected_message):
        wiper.wipe()


@patch("qq_lib.wipe.wiper.logger.info")
def test_wiper_delete_success_calls_logger_and_deletes(mock_logger_info):
    # Successful deletion should call logger and deleteRemoteDir
    wiper = Wiper.__new__(Wiper)
    wiper.hasDestination = MagicMock(return_value=True)
    wiper._workDirIsInputDir = MagicMock(return_value=False)
    wiper._batch_system = MagicMock()
    wiper._informer = MagicMock()
    wiper._informer.info.job_id = "job123"
    wiper._main_node = "main_node"
    wiper._work_dir = Path("/some/workdir")

    result = wiper.wipe()

    assert result == "job123"
    mock_logger_info.assert_called_once()
    wiper._batch_system.deleteRemoteDir.assert_called_once_with(
        "main_node", Path("/some/workdir")
    )
