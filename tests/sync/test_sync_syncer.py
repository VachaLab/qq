# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.properties.info import Success
from qq_lib.properties.loop import LoopInfo
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
        match="Job has been killed and no working directory is available",
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
        match=r"Host \('main_node'\) or working directory \('work_dir'\) are not defined",
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
            syncer._work_dir,
            syncer._informer.info.input_dir,
            syncer._main_node,
            None,
            [],
        )


def make_syncer_for_archive(state: RealState, loop_info=None) -> Syncer:
    syncer = Syncer.__new__(Syncer)
    syncer._state = state
    syncer._informer = MagicMock()
    syncer._informer.info.job_exit_code = None if state == RealState.KILLED else 91
    syncer._informer.get_real_state.return_value = state
    syncer._informer.info.transfer_mode = [Success()]
    syncer._informer.info.loop_info = MagicMock() if loop_info is None else loop_info
    syncer._work_dir = Path("/workdir")
    syncer._main_node = "node"
    syncer._input_machine = "input"
    syncer._informer.info.input_dir = Path("/different_input")
    syncer._informer.uses_scratch.return_value = True
    syncer._batch_system = MagicMock()
    return syncer


@pytest.mark.parametrize("state", [RealState.FAILED, RealState.KILLED])
def test_syncer_ensure_suitable_for_archive_passes(state):
    make_syncer_for_archive(state).ensure_suitable(archive=True)


@pytest.mark.parametrize(
    "state",
    [
        RealState.RUNNING,
        RealState.SUSPENDED,
        RealState.EXITING,
        RealState.UNKNOWN,
        RealState.IN_AN_INCONSISTENT_STATE,
    ],
)
def test_syncer_ensure_suitable_for_archive_raises_for_other_states(state):
    syncer = make_syncer_for_archive(state)
    # the job can still be synced without archiving
    syncer.ensure_suitable()

    with pytest.raises(
        QQNotSuitableError, match="only be archived for failed or killed jobs"
    ):
        syncer.ensure_suitable(archive=True)


def test_syncer_ensure_suitable_for_archive_raises_for_non_loop_job():
    syncer = make_syncer_for_archive(RealState.FAILED)
    syncer._informer.info.loop_info = None

    with pytest.raises(QQNotSuitableError, match="not a loop job"):
        syncer.ensure_suitable(archive=True)


def test_syncer_ensure_suitable_for_archive_raises_when_synchronized():
    syncer = make_syncer_for_archive(RealState.FAILED)
    syncer._informer.info.job_exit_code = 1
    syncer._informer.info.transfer_mode = [Always()]

    with pytest.raises(QQNotSuitableError, match="was synchronized"):
        syncer.ensure_suitable(archive=True)


@pytest.fixture
def syncer_with_archiver():
    syncer = make_syncer_for_archive(RealState.FAILED)
    syncer.has_destination = MagicMock(return_value=True)
    with patch("qq_lib.sync.syncer.Archiver") as mock_archiver_cls:
        yield syncer, mock_archiver_cls


def test_syncer_sync_archive_builds_archiver_from_info(syncer_with_archiver):
    syncer, mock_archiver_cls = syncer_with_archiver
    mock_archiver_cls.return_value.copy_to_archive.return_value = []
    info = syncer._informer.info

    syncer.sync(archive=True)

    mock_archiver_cls.assert_called_once_with(
        archive=info.loop_info.archive,
        archive_format=info.loop_info.archive_format,
        input_machine=info.input_machine,
        input_dir=info.input_dir,
        batch_system=syncer._batch_system,
        included_files=info.included_files,
        excluded_files=info.excluded_files,
        ignored_files=info.ignored_files,
    )


def test_syncer_sync_archive_all_files(syncer_with_archiver):
    syncer, mock_archiver_cls = syncer_with_archiver
    archived = [Path("/workdir/job0005.xtc"), Path("/workdir/job0006.tpr")]
    mock_archiver_cls.return_value.copy_to_archive.return_value = archived

    syncer.sync(archive=True)

    mock_archiver_cls.return_value.copy_to_archive.assert_called_once_with(
        Path("/workdir"), "node", None, None
    )
    syncer._batch_system.sync_with_exclusions.assert_called_once_with(
        Path("/workdir"), Path("/different_input"), "node", None, archived
    )
    syncer._batch_system.sync_selected.assert_not_called()


def test_syncer_sync_archive_selected_files(syncer_with_archiver):
    syncer, mock_archiver_cls = syncer_with_archiver
    mock_archiver_cls.return_value.copy_to_archive.return_value = [
        Path("/workdir/job0005.xtc")
    ]

    syncer.sync(["job0005.xtc", "md.log"], archive=True)

    mock_archiver_cls.return_value.copy_to_archive.assert_called_once_with(
        Path("/workdir"),
        "node",
        None,
        [Path("/workdir/job0005.xtc"), Path("/workdir/md.log")],
    )
    # only the files which were not archived are fetched
    syncer._batch_system.sync_selected.assert_called_once_with(
        Path("/workdir"),
        Path("/different_input"),
        "node",
        None,
        [Path("/workdir/md.log")],
    )
    syncer._batch_system.sync_with_exclusions.assert_not_called()


def test_syncer_sync_archive_selected_files_all_archived(syncer_with_archiver):
    syncer, mock_archiver_cls = syncer_with_archiver
    mock_archiver_cls.return_value.copy_to_archive.return_value = [
        Path("/workdir/job0005.xtc")
    ]

    syncer.sync(["job0005.xtc"], archive=True)

    syncer._batch_system.sync_selected.assert_not_called()
    syncer._batch_system.sync_with_exclusions.assert_not_called()


def test_syncer_sync_archive_does_not_fetch_if_archiving_fails(syncer_with_archiver):
    syncer, mock_archiver_cls = syncer_with_archiver
    mock_archiver_cls.return_value.copy_to_archive.side_effect = QQError("rsync")

    with pytest.raises(QQError, match="rsync"):
        syncer.sync(archive=True)

    syncer._batch_system.sync_selected.assert_not_called()
    syncer._batch_system.sync_with_exclusions.assert_not_called()


def test_syncer_sync_without_archive_does_not_archive(syncer_with_archiver):
    syncer, mock_archiver_cls = syncer_with_archiver

    syncer.sync()

    mock_archiver_cls.assert_not_called()
    syncer._batch_system.sync_with_exclusions.assert_called_once_with(
        Path("/workdir"), Path("/different_input"), "node", None, []
    )


def _copy_files(dest_dir, files):
    dest_dir.mkdir(exist_ok=True)
    for file in files:
        shutil.copy2(file, dest_dir / file.name)


@pytest.mark.parametrize(
    ("files", "expected_input", "expected_archive"),
    [
        (
            None,
            ["md.log", "run+0005.out"],
            ["job0005.xtc", "job0006.tpr"],
        ),
        (["job0005.xtc", "md.log"], ["md.log"], ["job0005.xtc"]),
    ],
)
def test_syncer_sync_archive_with_real_files(
    tmp_path, files, expected_input, expected_archive
):
    work_dir = tmp_path / "work"
    input_dir = tmp_path / "input"
    work_dir.mkdir()
    input_dir.mkdir()
    for name in ("job0005.xtc", "job0006.tpr", "md.log", "run+0005.out"):
        (work_dir / name).write_text(name)

    syncer = make_syncer_for_archive(
        RealState.FAILED,
        LoopInfo(
            start=1,
            end=10,
            archive=input_dir / "storage",
            archive_format="job%04d",
            current=5,
        ),
    )
    syncer._work_dir = work_dir
    syncer._informer.info.input_dir = input_dir
    syncer._informer.info.included_files = []
    syncer._informer.info.excluded_files = []
    syncer._informer.info.ignored_files = []
    syncer.has_destination = MagicMock(return_value=True)

    batch = MagicMock()
    batch.list_remote_dir.side_effect = lambda _host, directory: list(
        directory.iterdir()
    )
    batch.sync_selected.side_effect = (
        lambda _src, dest, _src_host, _dest_host, selected: _copy_files(dest, selected)
    )
    batch.sync_with_exclusions.side_effect = (
        lambda src, dest, _src_host, _dest_host, exclude: _copy_files(
            dest, [f for f in src.iterdir() if f not in exclude]
        )
    )
    syncer._batch_system = batch

    syncer.sync(files, archive=True)

    assert sorted(f.name for f in input_dir.iterdir() if f.is_file()) == expected_input
    assert sorted(f.name for f in (input_dir / "storage").iterdir()) == expected_archive
    # nothing is removed from the working directory
    assert len(list(work_dir.iterdir())) == 4
