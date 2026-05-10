# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.properties.info import Success
from qq_lib.properties.states import RealState
from qq_lib.properties.transfer_mode import Always, ExitCode, Failure, Never
from qq_lib.sync.syncer import Syncer


@pytest.mark.parametrize(
    "state, job_exit_code, transfer_mode, expected_message",
    [
        (
            RealState.QUEUED,
            None,
            [Always()],
            "Job is queued or booting: nothing to sync",
        ),
        (
            RealState.BOOTING,
            None,
            [Always()],
            "Job is queued or booting: nothing to sync",
        ),
        (
            RealState.HELD,
            None,
            [Always()],
            "Job is queued or booting: nothing to sync",
        ),
        (
            RealState.WAITING,
            None,
            [Always()],
            "Job is queued or booting: nothing to sync",
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
def test_syncer_ensure_suitable_raises_for_unsuitable_states(
    state, job_exit_code, transfer_mode, expected_message
):
    syncer = Syncer.__new__(Syncer)
    syncer._state = state
    syncer._informer = MagicMock()
    syncer._informer.info.job_exit_code = job_exit_code
    syncer._informer.get_real_state.return_value = state
    syncer._informer.info.transfer_mode = transfer_mode

    syncer._work_dir = Path("/workdir")
    syncer._main_node = "node"
    syncer._input_machine = "input"
    syncer._informer.info.input_dir = Path("/different_input")
    syncer._informer.uses_scratch.return_value = True
    syncer._batch_system = MagicMock()

    with pytest.raises(QQNotSuitableError, match=expected_message):
        syncer.ensure_suitable()


def test_syncer_ensure_suitable_raises_when_workdir_is_inputdir():
    syncer = Syncer.__new__(Syncer)
    syncer._state = RealState.FAILED
    syncer._informer = MagicMock()
    syncer._informer.info.job_exit_code = 1
    syncer._informer.get_real_state.return_value = RealState.FAILED
    syncer._informer.info.transfer_mode = [Success()]
    same_path = Path("/shared/path")

    syncer._work_dir = same_path
    syncer._main_node = "main"
    syncer._input_machine = "main"
    syncer._informer.info.input_dir = same_path
    syncer._informer.uses_scratch.return_value = False
    syncer._batch_system = MagicMock()

    with pytest.raises(
        QQNotSuitableError,
        match="Working directory of the job is the input directory of the job: implicitly synchronized",
    ):
        syncer.ensure_suitable()


@pytest.mark.parametrize("destination", [(None, "host"), (Path("some/path"), None)])
def test_syncer_ensure_suitable_raises_killed_without_destination(destination):
    syncer = Syncer.__new__(Syncer)
    syncer._state = RealState.KILLED
    syncer._informer = MagicMock()
    syncer._informer.info.job_exit_code = None
    syncer._work_dir, syncer._main_node = destination

    with pytest.raises(
        QQNotSuitableError,
        match="Job has been killed and no working directory is available.",
    ):
        syncer.ensure_suitable()


@pytest.mark.parametrize(
    "state, job_exit_code, transfer_mode",
    [
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
def test_syncer_ensure_suitable_passes_when_suitable(
    state, job_exit_code, transfer_mode
):
    syncer = Syncer.__new__(Syncer)
    syncer._state = state
    syncer._informer = MagicMock()
    syncer._informer.info.job_exit_code = job_exit_code
    syncer._informer.get_real_state.return_value = state
    syncer._informer.info.transfer_mode = transfer_mode

    syncer._work_dir = Path("/workdir")
    syncer._main_node = "node"
    syncer._input_machine = "input"
    syncer._informer.info.input_dir = Path("/different_input")
    syncer._informer.uses_scratch.return_value = True
    syncer._batch_system = MagicMock()

    # should not raise
    syncer.ensure_suitable()


@pytest.mark.parametrize(
    "destination", [(None, "host"), (Path("some/path"), None), (None, None)]
)
def test_syncer_sync_raises_without_destination(destination):
    syncer = Syncer.__new__(Syncer)
    syncer._work_dir, syncer._main_node = destination

    with pytest.raises(
        QQError,
        match=r"Host \('main_node'\) or working directory \('work_dir'\) are not defined\.",
    ):
        syncer.sync()


def test_syncer_sync_calls_sync_selected_with_files():
    syncer = Syncer.__new__(Syncer)
    syncer._work_dir = Path("/work")
    syncer._main_node = "host"
    syncer._batch_system = MagicMock()
    syncer._informer = MagicMock()
    syncer._informer.info.input_dir = Path("/input")
    syncer.has_destination = MagicMock(return_value=True)

    files = ["a.txt", "b.txt"]

    with patch("qq_lib.sync.syncer.logger") as mock_logger:
        syncer.sync(files=files)
        mock_logger.info.assert_called_once_with(
            "Fetching files 'a.txt b.txt' from job's working directory to input directory."
        )
        syncer._batch_system.sync_selected.assert_called_once_with(
            syncer._work_dir,
            syncer._informer.info.input_dir,
            syncer._main_node,
            None,
            [syncer._work_dir / x for x in files],
        )


def test_syncer_sync_calls_sync_with_exclusions_without_files():
    syncer = Syncer.__new__(Syncer)
    syncer._work_dir = Path("/work")
    syncer._main_node = "host"
    syncer._batch_system = MagicMock()
    syncer._informer = MagicMock()
    syncer._informer.info.input_dir = Path("/input")
    syncer.has_destination = MagicMock(return_value=True)

    with patch("qq_lib.sync.syncer.logger") as mock_logger:
        syncer.sync()
        mock_logger.info.assert_called_once_with(
            "Fetching all files from job's working directory to input directory."
        )
        syncer._batch_system.sync_with_exclusions.assert_called_once_with(
            syncer._work_dir, syncer._informer.info.input_dir, syncer._main_node, None
        )
