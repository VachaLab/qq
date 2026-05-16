# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.interface import BatchInterface
from qq_lib.batch.pbs import PBS
from qq_lib.core.error import QQError
from qq_lib.info import Informer
from qq_lib.info.array_informer import ArrayInformer
from qq_lib.properties.array_info import ArrayInfo
from qq_lib.properties.states import RealState


@pytest.fixture()
def valid_dict() -> dict[str, object]:
    return {
        "batch_system": "PBS",
        "job_name": "test-array",
        "job_ids": ["100", "101", "102"],
        "task_dirs": ["/work/t0", Path("/work/t1"), "/work/t2"],
        "n_finished_tasks": 1,
    }


@pytest.fixture()
def sample_array_info() -> ArrayInfo:
    return ArrayInfo(
        batch_system=PBS,
        job_name="test-array",
        job_ids=["100", "101", "102"],
        task_dirs=[Path("/work/t0"), Path("/work/t1"), Path("/work/t2")],
        n_finished_tasks=1,
    )


def test_from_dict_valid(valid_dict: dict[str, object]) -> None:
    info = ArrayInfo._from_dict(valid_dict)

    assert info.batch_system == PBS
    assert info.job_name == "test-array"
    assert info.job_ids == ["100", "101", "102"]
    assert info.task_dirs == [Path("/work/t0"), Path("/work/t1"), Path("/work/t2")]
    assert info.n_finished_tasks == 1


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("batch_system", 42),
        ("batch_system", None),
        ("job_name", 123),
        ("job_name", None),
        ("job_ids", "not-a-list"),
        ("job_ids", None),
        ("task_dirs", "not-a-list"),
        ("task_dirs", None),
        ("n_finished_tasks", "three"),
        ("n_finished_tasks", None),
    ],
    ids=[
        "batch_system-int",
        "batch_system-none",
        "job_name-int",
        "job_name-none",
        "job_ids-str",
        "job_ids-none",
        "task_dirs-str",
        "task_dirs-none",
        "n_finished-str",
        "n_finished-none",
    ],
)
def test_from_dict_bad_field_type(
    valid_dict: dict[str, object],
    field: str,
    bad_value: object,
) -> None:
    valid_dict[field] = bad_value

    with pytest.raises(QQError, match=field):
        ArrayInfo._from_dict(valid_dict)


@pytest.mark.parametrize(
    "field",
    ["batch_system", "job_name", "job_ids", "task_dirs", "n_finished_tasks"],
)
def test_from_dict_missing_field(
    valid_dict: dict[str, object],
    field: str,
) -> None:
    del valid_dict[field]

    with pytest.raises(QQError):
        ArrayInfo._from_dict(valid_dict)


def test_from_dict_bad_job_id_element(valid_dict: dict[str, object]) -> None:
    valid_dict["job_ids"] = ["100", 101]

    with pytest.raises(QQError, match="job_id"):
        ArrayInfo._from_dict(valid_dict)


def test_from_dict_bad_task_dir_element(valid_dict: dict[str, object]) -> None:
    valid_dict["task_dirs"] = ["/work/t0", 999]

    with pytest.raises(QQError, match="task_dir"):
        ArrayInfo._from_dict(valid_dict)


def test_to_dict_roundtrip(sample_array_info: ArrayInfo) -> None:
    d = sample_array_info._to_dict()

    restored = ArrayInfo._from_dict(d)

    assert restored.batch_system == sample_array_info.batch_system
    assert restored.job_name == sample_array_info.job_name
    assert restored.job_ids == sample_array_info.job_ids
    assert restored.task_dirs == sample_array_info.task_dirs
    assert restored.n_finished_tasks == sample_array_info.n_finished_tasks


_ARRAY_YAML_TEMPLATE = """\
# this file contains information about a qq array job; do not remove or modify it manually
batch_system: PBS
job_name: test-array
job_ids:
- '100'
- '101'
- '102'
task_dirs:
- /work/t0
- /work/t1
- /work/t2
n_finished_tasks: {n}
"""


def _make_modify_side_effect(initial_content: str) -> Any:
    def side_effect(_host: str, _file: Path, modify_fn: Any) -> None:
        modify_fn(initial_content)

    return side_effect


def test_atomically_increment_returns_new_value() -> None:
    content = _ARRAY_YAML_TEMPLATE.format(n=5)

    mock_batch = MagicMock()
    mock_batch.modify_remote_file_with_lock.side_effect = _make_modify_side_effect(
        content
    )

    with patch.object(BatchInterface, "from_env_var_or_guess", return_value=mock_batch):
        result = ArrayInfo.atomically_increment_n_finished_tasks(
            Path("/tmp/array.yaml"), "node01"
        )

    assert result == 6


def test_atomically_increment_preserves_surrounding_content() -> None:
    content = _ARRAY_YAML_TEMPLATE.format(n=9)
    captured: list[str] = []

    def capturing_side_effect(_host: str, _file: Path, modify_fn: Any) -> None:
        captured.append(modify_fn(content))

    mock_batch = MagicMock()
    mock_batch.modify_remote_file_with_lock.side_effect = capturing_side_effect

    with patch.object(BatchInterface, "from_env_var_or_guess", return_value=mock_batch):
        ArrayInfo.atomically_increment_n_finished_tasks(
            Path("/tmp/array.yaml"), "node01"
        )

    assert "n_finished_tasks: 10" in captured[0]
    assert "job_name: test-array" in captured[0]
    assert "batch_system: PBS" in captured[0]
    assert captured[0].startswith(f"# {ArrayInfo._file_comment}")


def test_atomically_increment_missing_field() -> None:
    content = (
        f"# {ArrayInfo._file_comment}\n"
        "batch_system: FakeBatch\n"
        "job_name: test-array\n"
        "job_ids:\n- '100'\n"
        "task_dirs:\n- /work/t0\n"
    )

    mock_batch = MagicMock()
    mock_batch.modify_remote_file_with_lock.side_effect = _make_modify_side_effect(
        content
    )

    with (
        patch.object(BatchInterface, "from_env_var_or_guess", return_value=mock_batch),
        pytest.raises(QQError, match="n_finished_tasks"),
    ):
        ArrayInfo.atomically_increment_n_finished_tasks(
            Path("/tmp/array.yaml"), "node01"
        )


def test_atomically_increment_from_zero() -> None:
    content = _ARRAY_YAML_TEMPLATE.format(n=0)

    mock_batch = MagicMock()
    mock_batch.modify_remote_file_with_lock.side_effect = _make_modify_side_effect(
        content
    )

    with patch.object(BatchInterface, "from_env_var_or_guess", return_value=mock_batch):
        result = ArrayInfo.atomically_increment_n_finished_tasks(
            Path("/tmp/array.yaml"), "node01"
        )

    assert result == 1


def test_to_file_from_file_local_roundtrip(
    tmp_path: Path,
    sample_array_info: ArrayInfo,
) -> None:
    """Writing and reading back locally produces equivalent data."""
    file = tmp_path / "array.yaml"

    sample_array_info.to_file(file)

    restored = ArrayInfo.from_file(file)

    assert restored.batch_system == sample_array_info.batch_system
    assert restored.job_name == sample_array_info.job_name
    assert restored.job_ids == sample_array_info.job_ids
    assert restored.task_dirs == sample_array_info.task_dirs
    assert restored.n_finished_tasks == sample_array_info.n_finished_tasks


def test_to_file_contains_comment(
    tmp_path: Path,
    sample_array_info: ArrayInfo,
) -> None:
    file = tmp_path / "array.yaml"
    sample_array_info.to_file(file)

    first_line = file.read_text().splitlines()[0]
    assert first_line == f"# {ArrayInfo._file_comment}"


def _make_array_informer_with_states(
    states: list[RealState],
    *,
    batch_info_loaded: bool = True,
) -> ArrayInformer:
    mock_array_info = MagicMock(spec=ArrayInfo)
    mock_array_info.task_dirs = [Path(f"/work/t{i}") for i in range(len(states))]
    mock_array_info.job_name = "myjob"
    mock_array_info.batch_system = PBS

    informers = []
    for state in states:
        mock = MagicMock(spec=Informer)
        mock.get_real_state.return_value = state
        informers.append(mock)

    with patch.object(
        ArrayInformer, "_get_informers_for_tasks", return_value=informers
    ):
        ai = ArrayInformer(mock_array_info)

    ai._batch_info_loaded = batch_info_loaded
    return ai


def test_all_tasks_in_state_all_match() -> None:
    ai = _make_array_informer_with_states(
        [RealState.FINISHED, RealState.FINISHED, RealState.FINISHED]
    )

    assert ai.all_tasks_in_state([RealState.FINISHED]) is True


def test_all_tasks_in_state_some_mismatch() -> None:
    ai = _make_array_informer_with_states(
        [RealState.FINISHED, RealState.RUNNING, RealState.FINISHED]
    )

    assert ai.all_tasks_in_state([RealState.FINISHED]) is False


def test_all_tasks_in_state_multiple_allowed_states() -> None:
    ai = _make_array_informer_with_states(
        [RealState.FINISHED, RealState.FAILED, RealState.KILLED]
    )

    assert (
        ai.all_tasks_in_state([RealState.FINISHED, RealState.FAILED, RealState.KILLED])
        is True
    )


def test_all_tasks_in_state_empty_tasks() -> None:
    ai = _make_array_informer_with_states([])

    assert ai.all_tasks_in_state([RealState.RUNNING]) is True


def test_all_tasks_in_state_loads_batch_info_when_needed() -> None:
    ai = _make_array_informer_with_states([RealState.FINISHED], batch_info_loaded=False)

    with patch.object(ai, "load_batch_info") as mock_load:
        ai.all_tasks_in_state([RealState.FINISHED])

    mock_load.assert_called_once()


def test_all_tasks_in_state_skips_load_when_already_loaded() -> None:
    ai = _make_array_informer_with_states([RealState.FINISHED], batch_info_loaded=True)

    with patch.object(ai, "load_batch_info") as mock_load:
        ai.all_tasks_in_state([RealState.FINISHED])

    mock_load.assert_not_called()
