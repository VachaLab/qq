# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.properties.depend import Depend, DependType
from qq_lib.properties.loop import LoopInfo
from qq_lib.properties.states import RealState
from qq_lib.respawn.respawner import Respawner


@pytest.fixture
def make_loop_info(tmp_path):
    def _make(current: int, start: int = 1, archive_files: list[str] | None = None):
        archive = tmp_path / "archive"
        archive.mkdir(exist_ok=True)
        for name in archive_files or []:
            (archive / name).touch()
        return LoopInfo(
            current=current,
            start=start,
            end=10,
            archive=archive,
            archive_format="job%04d",
        )

    return _make


def make_respawner_with_loop_info(loop_info: LoopInfo | None) -> Respawner:
    respawner = Respawner.__new__(Respawner)
    respawner._informer = MagicMock()
    respawner._informer.info.loop_info = loop_info
    return respawner


def test_respawner_get_next_cycle_in_archive_returns_none_for_non_loop_job():
    respawner = make_respawner_with_loop_info(None)

    assert respawner.get_next_cycle_in_archive() is None


@pytest.mark.parametrize(
    ("current", "start", "archive_files"),
    [
        (3, 1, ["job0003.init"]),
        (3, 1, ["job0001.dat", "job0002.dat", "job0003.init"]),
        (1, 1, []),
        (8, 8, []),
    ],
)
def test_respawner_get_next_cycle_in_archive_returns_none_when_cycles_match(
    make_loop_info, current, start, archive_files
):
    respawner = make_respawner_with_loop_info(
        make_loop_info(current=current, start=start, archive_files=archive_files)
    )

    assert respawner.get_next_cycle_in_archive() is None


def test_respawner_get_next_cycle_in_archive_returns_none_when_archive_does_not_exist_and_current_is_starting(
    tmp_path,
):
    loop_info = LoopInfo(
        current=3,
        start=3,
        end=10,
        archive=tmp_path / "nonexistent",
        archive_format="job%04d",
    )

    assert make_respawner_with_loop_info(loop_info).get_next_cycle_in_archive() is None


def test_respawner_get_next_cycle_in_archive_returns_next_cycle(make_loop_info):
    respawner = make_respawner_with_loop_info(
        make_loop_info(
            current=2, start=1, archive_files=["job0002.dat", "job0003.init"]
        )
    )

    assert respawner.get_next_cycle_in_archive() == 3


@pytest.mark.parametrize(
    ("current", "start", "archive_files", "match"),
    [
        # the archive is more than one cycle ahead
        (2, 1, ["job0004.init"], "cycle '2'.*cycle '4'"),
        # the archive is behind the current cycle
        (3, 2, [], "cycle '3'.*cycle '2'"),
        (5, 1, ["job0003.init"], "cycle '5'.*cycle '3'"),
    ],
)
def test_respawner_get_next_cycle_in_archive_raises_on_other_mismatch(
    make_loop_info, current, start, archive_files, match
):
    respawner = make_respawner_with_loop_info(
        make_loop_info(current=current, start=start, archive_files=archive_files)
    )

    with pytest.raises(QQError, match=match):
        respawner.get_next_cycle_in_archive()


def test_respawner_get_next_cycle_in_archive_raises_when_archive_does_not_exist_and_current_is_not_starting(
    tmp_path,
):
    loop_info = LoopInfo(
        current=5,
        start=1,
        end=10,
        archive=tmp_path / "nonexistent",
        archive_format="job%04d",
    )

    with pytest.raises(QQError, match="cycle '5'.*cycle '1'"):
        make_respawner_with_loop_info(loop_info).get_next_cycle_in_archive()


@pytest.mark.parametrize(
    ("current", "archive_files", "expected"),
    [
        # the archive corresponds to the current cycle
        (3, ["job0003.init"], False),
        # the archive already contains the next cycle
        (3, ["job0004.init"], True),
        # the next cycle is the last cycle
        (9, ["job0010.init"], True),
        # the failed cycle is the last cycle
        (10, ["job0011.init"], False),
    ],
)
def test_respawner_can_continue(make_loop_info, current, archive_files, expected):
    respawner = make_respawner_with_loop_info(
        make_loop_info(current=current, archive_files=archive_files)
    )

    assert respawner.can_continue() is expected


def test_respawner_can_continue_returns_false_for_non_loop_job():
    assert make_respawner_with_loop_info(None).can_continue() is False


@pytest.fixture
def respawner_with_info_file():
    respawner = Respawner.__new__(Respawner)
    respawner._info_file = Path("/tmp/input/job.qqinfo")
    return respawner


def test_respawner_build_submitter_creates_submitter_with_correct_params(
    respawner_with_info_file,
):
    informer = MagicMock()
    informer.info.loop_info = None
    informer.info.depend = []

    dependencies = [Depend(type=DependType.AFTER_SUCCESS, jobs=["12345"])]

    with (
        patch("qq_lib.respawn.respawner.Submitter") as mock_submitter_cls,
        patch(
            "qq_lib.respawn.respawner.filter_dependencies",
            return_value=dependencies,
        ),
    ):
        respawner_with_info_file._build_submitter(informer)

    mock_submitter_cls.assert_called_once_with(
        batch_system=informer.batch_system,
        queue=informer.info.queue,
        account=informer.info.account,
        script=Path("/tmp/input") / informer.info.script_name,
        job_type=informer.info.job_type,
        resources=informer.info.resources,
        loop_info=None,
        exclude=[str(x) for x in informer.info.excluded_files],
        include=[str(x) for x in informer.info.included_files],
        ignore=[str(x) for x in informer.info.ignored_files],
        depend=dependencies,
        transfer_mode=informer.info.transfer_mode,
        server=informer.info.server,
        interpreter=informer.info.interpreter,
        resubmit_from=informer.info.resubmit_from,
    )


def test_respawner_build_submitter_filters_dependencies(respawner_with_info_file):
    informer = MagicMock()
    informer.info.loop_info = None
    informer.info.depend = [
        Depend(type=DependType.AFTER_SUCCESS, jobs=["111", "222"]),
    ]
    filtered = [Depend(type=DependType.AFTER_SUCCESS, jobs=["111"])]

    with (
        patch("qq_lib.respawn.respawner.Submitter") as mock_submitter_cls,
        patch(
            "qq_lib.respawn.respawner.filter_dependencies", return_value=filtered
        ) as mock_filter,
    ):
        respawner_with_info_file._build_submitter(informer)

    mock_filter.assert_called_once_with(informer.batch_system, informer.info.depend)
    assert mock_submitter_cls.call_args.kwargs["depend"] == filtered


def test_respawner_build_submitter_passes_loop_info_to_submitter(
    respawner_with_info_file,
):
    informer = MagicMock()
    loop_info = MagicMock()
    informer.info.loop_info = loop_info

    with (
        patch("qq_lib.respawn.respawner.Submitter") as mock_submitter_cls,
        patch("qq_lib.respawn.respawner.filter_dependencies", return_value=[]),
    ):
        respawner_with_info_file._build_submitter(informer)

    assert mock_submitter_cls.call_args.kwargs["loop_info"] is loop_info


def test_respawner_build_submitter_uses_requested_cycle(
    respawner_with_info_file, make_loop_info
):
    informer = MagicMock()
    loop_info = make_loop_info(current=3, archive_files=["job0004.init"])
    informer.info.loop_info = loop_info

    with (
        patch("qq_lib.respawn.respawner.Submitter") as mock_submitter_cls,
        patch("qq_lib.respawn.respawner.filter_dependencies", return_value=[]),
    ):
        respawner_with_info_file._build_submitter(informer, 4)

    submitted = mock_submitter_cls.call_args.kwargs["loop_info"]
    assert submitted.current == 4
    assert submitted.end == loop_info.end
    assert submitted.archive == loop_info.archive
    # the loop info of the failed job is not modified
    assert loop_info.current == 3


RESPAWNABLE_STATES = {RealState.FAILED, RealState.KILLED}


@pytest.mark.parametrize("state", RealState)
def test_respawner_ensure_suitable(state):
    respawner = Respawner.__new__(Respawner)
    respawner._state = state

    if state in RESPAWNABLE_STATES:
        respawner.ensure_suitable()
    else:
        with pytest.raises(QQNotSuitableError, match="cannot be respawned"):
            respawner.ensure_suitable()


@pytest.fixture
def respawner_for_respawn():
    respawner = Respawner.__new__(Respawner)
    respawner._info_file = Path("/tmp/input/job.qqinfo")
    respawner._informer = MagicMock()
    respawner._informer.info.loop_info.current = 5
    respawner._informer.info.loop_info.end = 10
    respawner.get_informer = MagicMock(return_value=respawner._informer)
    respawner.get_next_cycle_in_archive = MagicMock(return_value=None)
    respawner._build_submitter = MagicMock()
    respawner._build_submitter.return_value.submit.return_value = "99999"
    respawner._warn_if_work_dir_exists = MagicMock()
    respawner._archive_runtime_files = MagicMock()
    return respawner


def test_respawnerrespawn_returns_job_id(respawner_for_respawn):
    with (
        patch("qq_lib.respawn.respawner.Wiper"),
        patch("qq_lib.respawn.respawner.Clearer"),
    ):
        result = respawner_for_respawn.respawn()

    assert result == "99999"


def test_respawner_respawn_wipes_and_clears_before_submitting(respawner_for_respawn):
    call_order = []

    with (
        patch("qq_lib.respawn.respawner.Wiper") as mock_wiper_cls,
        patch("qq_lib.respawn.respawner.Clearer") as mock_clearer_cls,
    ):
        wiper = mock_wiper_cls.from_informer.return_value
        wiper.wipe.side_effect = lambda: call_order.append("wipe")

        def clear() -> bool:
            call_order.append("clear")
            return True

        mock_clearer_cls.return_value.clear.side_effect = clear
        respawner_for_respawn._build_submitter.return_value.submit.side_effect = (
            lambda: (call_order.append("submit"), "99999")[1]
        )

        respawner_for_respawn.respawn()

    assert call_order == ["wipe", "clear", "submit"]


def test_respawnerrespawn_calls_wiper_from_informer(respawner_for_respawn):
    informer = respawner_for_respawn.get_informer.return_value

    with (
        patch("qq_lib.respawn.respawner.Wiper") as mock_wiper_cls,
        patch("qq_lib.respawn.respawner.Clearer"),
    ):
        respawner_for_respawn.respawn()

    mock_wiper_cls.from_informer.assert_called_once_with(informer)


def test_respawner_respawn_calls_clearer_with_input_dir(respawner_for_respawn):
    with (
        patch("qq_lib.respawn.respawner.Wiper"),
        patch("qq_lib.respawn.respawner.Clearer") as mock_clearer_cls,
    ):
        respawner_for_respawn.respawn()

    mock_clearer_cls.assert_called_once_with([Path("/tmp/input")])
    mock_clearer_cls.return_value.clear.assert_called_once()


def test_respawner_respawn_continues_when_wiper_not_suitable(respawner_for_respawn):
    with (
        patch("qq_lib.respawn.respawner.Wiper") as mock_wiper_cls,
        patch("qq_lib.respawn.respawner.Clearer") as mock_clearer_cls,
    ):
        wiper = mock_wiper_cls.from_informer.return_value
        wiper.ensure_suitable.side_effect = QQNotSuitableError("not suitable")

        result = respawner_for_respawn.respawn()

    assert result == "99999"
    wiper.wipe.assert_not_called()
    mock_clearer_cls.return_value.clear.assert_called_once()


def test_respawner_respawn_continues_when_wipe_fails_with_qq_error(
    respawner_for_respawn,
):
    with (
        patch("qq_lib.respawn.respawner.Wiper") as mock_wiper_cls,
        patch("qq_lib.respawn.respawner.Clearer") as mock_clearer_cls,
        patch("qq_lib.respawn.respawner.logger") as mock_logger,
    ):
        wiper = mock_wiper_cls.from_informer.return_value
        wiper.wipe.side_effect = QQError("permission denied")

        result = respawner_for_respawn.respawn()

    assert result == "99999"
    mock_logger.warning.assert_any_call(
        "Failed to remove working directory: permission denied"
    )
    mock_clearer_cls.return_value.clear.assert_called_once()


def test_respawner_respawn_builds_submitter_from_informer(respawner_for_respawn):
    informer = respawner_for_respawn.get_informer.return_value

    with (
        patch("qq_lib.respawn.respawner.Wiper"),
        patch("qq_lib.respawn.respawner.Clearer"),
    ):
        respawner_for_respawn.respawn()

    respawner_for_respawn._build_submitter.assert_called_once_with(informer, None)


def test_respawner_respawn_raises_if_files_cannot_be_cleared(respawner_for_respawn):
    with (
        patch("qq_lib.respawn.respawner.Wiper"),
        patch("qq_lib.respawn.respawner.Clearer") as mock_clearer_cls,
    ):
        mock_clearer_cls.return_value.clear.return_value = False

        with pytest.raises(QQError, match="Could not remove all qq runtime files"):
            respawner_for_respawn.respawn()

    respawner_for_respawn._build_submitter.return_value.submit.assert_not_called()


@pytest.mark.parametrize("flags", [{}, {"force_continue": True}, {"force_rerun": True}])
def test_respawner_respawn_without_mismatch_ignores_options(
    respawner_for_respawn, flags
):
    informer = respawner_for_respawn.get_informer.return_value

    with (
        patch("qq_lib.respawn.respawner.Wiper") as mock_wiper_cls,
        patch("qq_lib.respawn.respawner.Clearer"),
    ):
        result = respawner_for_respawn.respawn(**flags)

    assert result == "99999"
    respawner_for_respawn._build_submitter.assert_called_once_with(informer, None)
    mock_wiper_cls.from_informer.return_value.wipe.assert_called_once()
    respawner_for_respawn._archive_runtime_files.assert_not_called()


def test_respawner_respawn_raises_when_both_options_set(respawner_for_respawn):
    with (
        patch("qq_lib.respawn.respawner.Wiper") as mock_wiper_cls,
        patch("qq_lib.respawn.respawner.Clearer") as mock_clearer_cls,
        pytest.raises(QQError, match="Cannot both continue"),
    ):
        respawner_for_respawn.respawn(force_continue=True, force_rerun=True)

    mock_wiper_cls.from_informer.assert_not_called()
    mock_clearer_cls.assert_not_called()
    respawner_for_respawn._build_submitter.return_value.submit.assert_not_called()


def test_respawner_respawn_raises_on_mismatch_without_options(respawner_for_respawn):
    respawner_for_respawn.get_next_cycle_in_archive.return_value = 6

    with (
        patch("qq_lib.respawn.respawner.Wiper") as mock_wiper_cls,
        patch("qq_lib.respawn.respawner.Clearer") as mock_clearer_cls,
        pytest.raises(QQError, match="cycle '5'.*cycle '6'"),
    ):
        respawner_for_respawn.respawn()

    mock_wiper_cls.from_informer.assert_not_called()
    mock_clearer_cls.assert_not_called()
    respawner_for_respawn._archive_runtime_files.assert_not_called()
    respawner_for_respawn._build_submitter.return_value.submit.assert_not_called()


def test_respawner_respawn_force_rerun_reruns_failed_cycle(respawner_for_respawn):
    respawner_for_respawn.get_next_cycle_in_archive.return_value = 6
    informer = respawner_for_respawn.get_informer.return_value

    with (
        patch("qq_lib.respawn.respawner.Wiper") as mock_wiper_cls,
        patch("qq_lib.respawn.respawner.Clearer") as mock_clearer_cls,
    ):
        result = respawner_for_respawn.respawn(force_rerun=True)

    assert result == "99999"
    respawner_for_respawn._build_submitter.assert_called_once_with(informer, None)
    mock_wiper_cls.from_informer.return_value.wipe.assert_called_once()
    mock_clearer_cls.return_value.clear.assert_called_once()
    respawner_for_respawn._archive_runtime_files.assert_not_called()
    respawner_for_respawn._warn_if_work_dir_exists.assert_not_called()


def test_respawner_respawn_force_continue_submits_next_cycle(respawner_for_respawn):
    respawner_for_respawn.get_next_cycle_in_archive.return_value = 6
    informer = respawner_for_respawn.get_informer.return_value
    call_order = []
    respawner_for_respawn._warn_if_work_dir_exists.side_effect = lambda _: (
        call_order.append("warn")
    )
    respawner_for_respawn._archive_runtime_files.side_effect = lambda _: (
        call_order.append("archive")
    )
    respawner_for_respawn._build_submitter.return_value.submit.side_effect = lambda: (
        call_order.append("submit"),
        "99999",
    )[1]

    with (
        patch("qq_lib.respawn.respawner.Wiper") as mock_wiper_cls,
        patch("qq_lib.respawn.respawner.Clearer") as mock_clearer_cls,
    ):

        def clear() -> bool:
            call_order.append("clear")
            return True

        mock_clearer_cls.return_value.clear.side_effect = clear
        result = respawner_for_respawn.respawn(force_continue=True)

    assert result == "99999"
    respawner_for_respawn._build_submitter.assert_called_once_with(informer, 6)
    # the working directory is never removed when continuing
    mock_wiper_cls.from_informer.return_value.wipe.assert_not_called()
    respawner_for_respawn._warn_if_work_dir_exists.assert_called_once_with(informer)
    respawner_for_respawn._archive_runtime_files.assert_called_once_with(informer)
    assert call_order == ["warn", "archive", "clear", "submit"]


def test_respawner_respawn_force_continue_raises_after_last_cycle(
    respawner_for_respawn,
):
    respawner_for_respawn._informer.info.loop_info.current = 10
    respawner_for_respawn.get_next_cycle_in_archive.return_value = 11

    with (
        patch("qq_lib.respawn.respawner.Wiper") as mock_wiper_cls,
        patch("qq_lib.respawn.respawner.Clearer") as mock_clearer_cls,
        pytest.raises(QQError, match="Cannot continue with cycle '11'.*'10'"),
    ):
        respawner_for_respawn.respawn(force_continue=True)

    mock_wiper_cls.from_informer.assert_not_called()
    mock_clearer_cls.assert_not_called()
    respawner_for_respawn._archive_runtime_files.assert_not_called()


def test_respawner_respawn_force_continue_does_not_submit_if_archiving_fails(
    respawner_for_respawn,
):
    respawner_for_respawn.get_next_cycle_in_archive.return_value = 6
    respawner_for_respawn._archive_runtime_files.side_effect = QQError("cannot move")

    with (
        patch("qq_lib.respawn.respawner.Wiper"),
        patch("qq_lib.respawn.respawner.Clearer") as mock_clearer_cls,
        pytest.raises(QQError, match="cannot move"),
    ):
        respawner_for_respawn.respawn(force_continue=True)

    mock_clearer_cls.assert_not_called()
    respawner_for_respawn._build_submitter.return_value.submit.assert_not_called()


@pytest.fixture
def respawner_with_work_dir():
    respawner = Respawner.__new__(Respawner)
    informer = MagicMock()
    informer.get_destination.return_value = ("node1", Path("/scratch/job"))
    return respawner, informer


def test_respawner_warn_if_work_dir_exists_warns_when_directory_exists(
    respawner_with_work_dir,
):
    respawner, informer = respawner_with_work_dir

    with (
        patch("qq_lib.respawn.respawner.Wiper"),
        patch("qq_lib.respawn.respawner.logger") as mock_logger,
    ):
        respawner._warn_if_work_dir_exists(informer)

    informer.batch_system.list_remote_dir.assert_called_once_with(
        "node1", Path("/scratch/job")
    )
    mock_logger.warning.assert_called_once()
    message = mock_logger.warning.call_args.args[0]
    assert "'/scratch/job' on 'node1' still exists" in message


def test_respawner_warn_if_work_dir_exists_silent_when_directory_missing(
    respawner_with_work_dir,
):
    respawner, informer = respawner_with_work_dir
    informer.batch_system.list_remote_dir.side_effect = QQError("no such directory")

    with (
        patch("qq_lib.respawn.respawner.Wiper"),
        patch("qq_lib.respawn.respawner.logger") as mock_logger,
    ):
        respawner._warn_if_work_dir_exists(informer)

    mock_logger.warning.assert_not_called()


def test_respawner_warn_if_work_dir_exists_silent_when_not_expected_to_exist(
    respawner_with_work_dir,
):
    respawner, informer = respawner_with_work_dir

    with (
        patch("qq_lib.respawn.respawner.Wiper") as mock_wiper_cls,
        patch("qq_lib.respawn.respawner.logger") as mock_logger,
    ):
        mock_wiper_cls.from_informer.return_value.ensure_suitable.side_effect = (
            QQNotSuitableError("synchronized")
        )
        respawner._warn_if_work_dir_exists(informer)

    informer.batch_system.list_remote_dir.assert_not_called()
    mock_logger.warning.assert_not_called()


def test_respawner_warn_if_work_dir_exists_silent_without_destination(
    respawner_with_work_dir,
):
    respawner, informer = respawner_with_work_dir
    informer.get_destination.return_value = None

    with (
        patch("qq_lib.respawn.respawner.Wiper"),
        patch("qq_lib.respawn.respawner.logger") as mock_logger,
    ):
        respawner._warn_if_work_dir_exists(informer)

    informer.batch_system.list_remote_dir.assert_not_called()
    mock_logger.warning.assert_not_called()


def test_respawner_archive_runtime_files_archives_failed_cycle(make_loop_info):
    respawner = Respawner.__new__(Respawner)
    informer = MagicMock()
    loop_info = make_loop_info(current=5, archive_files=["job0006.init"])
    informer.info.loop_info = loop_info
    informer.info.script_name = "run.sh"

    with patch("qq_lib.respawn.respawner.Archiver") as mock_archiver_cls:
        respawner._archive_runtime_files(informer)

    mock_archiver_cls.assert_called_once_with(
        archive=loop_info.archive,
        archive_format="job%04d",
        input_machine=informer.info.input_machine,
        input_dir=informer.info.input_dir,
        batch_system=informer.batch_system,
        included_files=informer.info.included_files,
        excluded_files=informer.info.excluded_files,
        ignored_files=informer.info.ignored_files,
    )
    mock_archiver_cls.return_value.archive_runtime_files.assert_called_once_with(
        "run\\+0005.sh", 5
    )


def test_respawner_archive_runtime_files_moves_files_into_archive(
    tmp_path, make_loop_info
):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    for suffix in (".qqinfo", ".qqout", ".out", ".err"):
        (input_dir / f"run+0005{suffix}").write_text(suffix)
    (input_dir / "run.sh").write_text("script")

    loop_info = make_loop_info(current=5, archive_files=["job0006.init"])
    informer = MagicMock()
    informer.info.loop_info = loop_info
    informer.info.script_name = "run.sh"
    informer.info.input_dir = input_dir
    informer.info.input_machine = None
    informer.info.included_files = []
    informer.info.excluded_files = []
    informer.info.ignored_files = []

    def move(_host, files, moved_files):
        for src, dst in zip(files, moved_files, strict=True):
            src.rename(dst)

    informer.batch_system.move_remote_files.side_effect = move

    Respawner.__new__(Respawner)._archive_runtime_files(informer)

    assert sorted(p.name for p in input_dir.iterdir()) == ["run.sh"]
    assert sorted(p.name for p in loop_info.archive.iterdir()) == [
        "job0005.err",
        "job0005.out",
        "job0005.qqinfo",
        "job0005.qqout",
        "job0006.init",
    ]
