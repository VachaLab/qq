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


def test_respawner_ensure_archive_consistent_passes_when_cycles_match(make_loop_info):
    loop_info = make_loop_info(current=3, start=1, archive_files=["job0003.init"])
    Respawner._ensure_archive_consistent(loop_info)


def test_respawner_ensure_archive_consistent_raises_when_cycles_differ(make_loop_info):
    loop_info = make_loop_info(current=2, start=1, archive_files=["job0003.init"])

    with pytest.raises(QQError, match="cycle '2'.*cycle '3'"):
        Respawner._ensure_archive_consistent(loop_info)


def test_respawner_ensure_archive_consistent_passes_when_archive_empty_and_current_is_one(
    make_loop_info,
):
    loop_info = make_loop_info(current=1, start=1, archive_files=[])

    Respawner._ensure_archive_consistent(loop_info)


def test_respawner_ensure_archive_consistent_passes_when_archive_empty_and_current_is_starting(
    make_loop_info,
):
    loop_info = make_loop_info(current=8, start=8, archive_files=[])

    Respawner._ensure_archive_consistent(loop_info)


def test_respawner_ensure_archive_consistent_raises_when_archive_empty_and_current_is_not_starting(
    make_loop_info,
):
    loop_info = make_loop_info(current=3, start=2, archive_files=[])

    with pytest.raises(QQError, match="cycle '3'.*cycle '2'"):
        Respawner._ensure_archive_consistent(loop_info)


def test_respawner_ensure_archive_consistent_passes_when_archive_does_not_exist_and_current_is_starting(
    tmp_path,
):
    loop_info = LoopInfo(
        current=3,
        start=3,
        end=10,
        archive=tmp_path / "nonexistent",
        archive_format="job%04d",
    )

    Respawner._ensure_archive_consistent(loop_info)


def test_respawner_ensure_archive_consistent_raises_when_archive_does_not_exist_and_current_is_not_starting(
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
        Respawner._ensure_archive_consistent(loop_info)


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
        exclude=informer.info.excluded_files,
        include=informer.info.included_files,
        depend=dependencies,
        transfer_mode=informer.info.transfer_mode,
        server=informer.info.server,
        interpreter=informer.info.interpreter,
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


def test_respawner_build_submitter_calls_ensure_archive_consistent_for_loop_job(
    respawner_with_info_file,
):
    informer = MagicMock()
    loop_info = MagicMock()
    informer.info.loop_info = loop_info

    with (
        patch("qq_lib.respawn.respawner.Submitter"),
        patch("qq_lib.respawn.respawner.filter_dependencies", return_value=[]),
        patch.object(Respawner, "_ensure_archive_consistent") as mock_ensure,
    ):
        respawner_with_info_file._build_submitter(informer)

    mock_ensure.assert_called_once_with(loop_info)


def test_respawner_build_submitter_does_not_call_ensure_archive_for_non_loop_job(
    respawner_with_info_file,
):
    informer = MagicMock()
    informer.info.loop_info = None

    with (
        patch("qq_lib.respawn.respawner.Submitter"),
        patch("qq_lib.respawn.respawner.filter_dependencies", return_value=[]),
        patch.object(Respawner, "_ensure_archive_consistent") as mock_ensure,
    ):
        respawner_with_info_file._build_submitter(informer)

    mock_ensure.assert_not_called()


def test_respawner_build_submitter_raises_when_archive_inconsistent(
    respawner_with_info_file,
):
    informer = MagicMock()
    informer.info.loop_info = MagicMock()

    with (
        patch("qq_lib.respawn.respawner.Submitter"),
        patch("qq_lib.respawn.respawner.filter_dependencies", return_value=[]),
        patch.object(
            Respawner,
            "_ensure_archive_consistent",
            side_effect=QQError("inconsistent"),
        ),
        pytest.raises(QQError, match="inconsistent"),
    ):
        respawner_with_info_file._build_submitter(informer)


def test_respawner_build_submitter_passes_loop_info_to_submitter(
    respawner_with_info_file,
):
    informer = MagicMock()
    loop_info = MagicMock()
    informer.info.loop_info = loop_info

    with (
        patch("qq_lib.respawn.respawner.Submitter") as mock_submitter_cls,
        patch("qq_lib.respawn.respawner.filter_dependencies", return_value=[]),
        patch.object(Respawner, "_ensure_archive_consistent"),
    ):
        respawner_with_info_file._build_submitter(informer)

    assert mock_submitter_cls.call_args.kwargs["loop_info"] is loop_info


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
    respawner.get_informer = MagicMock()
    respawner._build_submitter = MagicMock()
    respawner._build_submitter.return_value.submit.return_value = "99999"
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
        mock_clearer_cls.return_value.clear.side_effect = lambda: call_order.append(
            "clear"
        )
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

    mock_clearer_cls.assert_called_once_with(Path("/tmp/input"))
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

    respawner_for_respawn._build_submitter.assert_called_once_with(informer)
