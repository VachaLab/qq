# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from qq_lib.batch.interface import BatchInterface, BatchJobInterface
from qq_lib.batch.pbs import PBS, PBSJob
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQJobMismatchError
from qq_lib.info import Informer
from qq_lib.info.array_informer import ArrayInformer
from qq_lib.properties.array_info import ArrayInfo
from qq_lib.properties.info import Info
from qq_lib.properties.job_type import JobType
from qq_lib.properties.resources import Resources
from qq_lib.properties.states import NaiveState
from qq_lib.properties.task_info import TaskInfo


@pytest.fixture()
def array_info_three_tasks() -> ArrayInfo:
    return ArrayInfo(
        batch_system=PBS,
        job_name="myjob",
        job_ids=["100"],
        task_dirs=[Path("/work/t0"), Path("/work/t1"), Path("/work/t2")],
        n_finished_tasks=0,
    )


@pytest.fixture()
def array_info_no_tasks() -> ArrayInfo:
    return ArrayInfo(
        batch_system=PBS,
        job_name="myjob",
        job_ids=["100"],
        task_dirs=[],
        n_finished_tasks=0,
    )


def test_get_informers_for_tasks_returns_one_per_dir(
    array_info_three_tasks: ArrayInfo,
) -> None:
    with patch.object(Informer, "from_file", return_value=MagicMock(spec=Informer)):
        result = ArrayInformer._get_informers_for_tasks(array_info_three_tasks)

    assert len(result) == 3


def test_get_informers_for_tasks_loads_correct_info_paths(
    array_info_three_tasks: ArrayInfo,
) -> None:
    mock_from_file = MagicMock(return_value=MagicMock(spec=Informer))
    suffix = CFG.suffixes.qq_info

    with patch.object(Informer, "from_file", mock_from_file):
        ArrayInformer._get_informers_for_tasks(array_info_three_tasks)

    mock_from_file.assert_has_calls(
        [
            call(Path(f"/work/t0/myjob{suffix}")),
            call(Path(f"/work/t1/myjob{suffix}")),
            call(Path(f"/work/t2/myjob{suffix}")),
        ]
    )


def test_get_informers_for_tasks_preserves_directory_order(
    array_info_three_tasks: ArrayInfo,
) -> None:
    informers = [MagicMock(spec=Informer, name=f"informer_{i}") for i in range(3)]

    with patch.object(Informer, "from_file", side_effect=informers):
        result = ArrayInformer._get_informers_for_tasks(array_info_three_tasks)

    assert result == informers


def test_get_informers_for_tasks_empty(array_info_no_tasks: ArrayInfo) -> None:
    result = ArrayInformer._get_informers_for_tasks(array_info_no_tasks)

    assert result == []


def test_get_informers_for_tasks_propagates_from_file_error(
    array_info_three_tasks: ArrayInfo,
) -> None:
    with (
        patch.object(
            Informer,
            "from_file",
            side_effect=QQError("info file missing"),
        ),
        pytest.raises(QQError, match="info file missing"),
    ):
        ArrayInformer._get_informers_for_tasks(array_info_three_tasks)


def test_init_stores_array_info(array_info_three_tasks: ArrayInfo) -> None:
    with patch.object(Informer, "from_file", return_value=MagicMock(spec=Informer)):
        informer = ArrayInformer(array_info_three_tasks)

    assert informer.array_info is array_info_three_tasks


def test_init_populates_tasks(array_info_three_tasks: ArrayInfo) -> None:
    """The constructor builds one Informer per task directory."""
    with patch.object(Informer, "from_file", return_value=MagicMock(spec=Informer)):
        informer = ArrayInformer(array_info_three_tasks)

    assert len(informer._tasks) == len(array_info_three_tasks.task_dirs)


def test_init_batch_info_not_loaded(array_info_three_tasks: ArrayInfo) -> None:
    """Batch info is not marked as loaded after construction."""
    with patch.object(Informer, "from_file", return_value=MagicMock(spec=Informer)):
        informer = ArrayInformer(array_info_three_tasks)

    assert informer._batch_info_loaded is False


def test_batch_system_returns_class_from_array_info(
    array_info_three_tasks: ArrayInfo,
) -> None:
    with patch.object(Informer, "from_file", return_value=MagicMock(spec=Informer)):
        informer = ArrayInformer(array_info_three_tasks)

    assert informer.batch_system is PBS


def test_from_file_delegates_to_array_info(tmp_path: Path) -> None:
    mock_array_info = MagicMock(spec=ArrayInfo)
    mock_array_info.task_dirs = []
    mock_array_info.job_name = "myjob"
    file = tmp_path / "array.yaml"

    with patch.object(
        ArrayInfo, "from_file", return_value=mock_array_info
    ) as mock_load:
        result = ArrayInformer.from_file(file)

    mock_load.assert_called_once_with(file, None)
    assert result.array_info is mock_array_info


def test_from_file_passes_host(tmp_path: Path) -> None:
    mock_array_info = MagicMock(spec=ArrayInfo)
    mock_array_info.task_dirs = []
    mock_array_info.job_name = "myjob"
    file = tmp_path / "array.yaml"

    with patch.object(
        ArrayInfo, "from_file", return_value=mock_array_info
    ) as mock_load:
        ArrayInformer.from_file(file, host="node01")

    mock_load.assert_called_once_with(file, "node01")


def test_from_file_propagates_error(tmp_path: Path) -> None:
    file = tmp_path / "missing.yaml"

    with (
        patch.object(ArrayInfo, "from_file", side_effect=QQError("file not found")),
        pytest.raises(QQError, match="file not found"),
    ):
        ArrayInformer.from_file(file)


def test_matches_job_exact_id(array_info_three_tasks: ArrayInfo) -> None:
    with patch.object(Informer, "from_file", return_value=MagicMock(spec=Informer)):
        informer = ArrayInformer(array_info_three_tasks)

    assert informer.matches_job("100") is True


def test_matches_job_prefix_match(array_info_three_tasks: ArrayInfo) -> None:
    array_info_three_tasks.job_ids = ["100.random.server.org"]

    with patch.object(Informer, "from_file", return_value=MagicMock(spec=Informer)):
        informer = ArrayInformer(array_info_three_tasks)

    assert informer.matches_job("100") is True
    assert informer.matches_job("100.random.server.org") is True
    assert informer.matches_job("100.server") is True


def test_matches_job_no_match(array_info_three_tasks: ArrayInfo) -> None:
    with patch.object(Informer, "from_file", return_value=MagicMock(spec=Informer)):
        informer = ArrayInformer(array_info_three_tasks)

    assert informer.matches_job("999") is False


def test_matches_job_empty_job_ids() -> None:
    array_info = ArrayInfo(
        batch_system=PBS,
        job_name="myjob",
        job_ids=[],
        task_dirs=[],
        n_finished_tasks=0,
    )

    informer = ArrayInformer(array_info)

    assert informer.matches_job("100") is False


def test_matches_job_multiple_arrays() -> None:
    array_info = ArrayInfo(
        batch_system=PBS,
        job_name="myjob",
        job_ids=["200", "300"],
        task_dirs=[],
        n_finished_tasks=0,
    )

    informer = ArrayInformer(array_info)

    assert informer.matches_job("200") is True
    assert informer.matches_job("300") is True
    assert informer.matches_job("400") is False


def _make_mock_batch_job(
    *,
    job_id: str = "100",
    is_array: bool = True,
    info_file: Path | None = Path("/work/array.yaml"),
) -> MagicMock:
    mock = MagicMock(spec=BatchJobInterface)
    mock.get_id.return_value = job_id
    mock.is_array_job.return_value = is_array
    mock.get_info_file.return_value = info_file
    return mock


def test_from_batch_job_success() -> None:
    batch_job = _make_mock_batch_job(job_id="100")

    mock_array_info = MagicMock(spec=ArrayInfo)
    mock_array_info.task_dirs = []
    mock_array_info.job_ids = ["100"]
    mock_array_info.job_name = "myjob"

    with patch.object(ArrayInfo, "from_file", return_value=mock_array_info):
        result = ArrayInformer.from_batch_job(batch_job)

    assert result.array_info is mock_array_info


def test_from_batch_job_loads_from_info_file_path() -> None:
    info_path = Path("/work/custom/array.yaml")
    batch_job = _make_mock_batch_job(job_id="100", info_file=info_path)

    mock_array_info = MagicMock(spec=ArrayInfo)
    mock_array_info.task_dirs = []
    mock_array_info.job_ids = ["100"]
    mock_array_info.job_name = "myjob"

    with patch.object(
        ArrayInfo, "from_file", return_value=mock_array_info
    ) as mock_load:
        ArrayInformer.from_batch_job(batch_job)

    mock_load.assert_called_once_with(info_path, None)


def test_from_batch_job_not_array_job() -> None:
    batch_job = _make_mock_batch_job(is_array=False)

    with pytest.raises(QQError, match="not an array job"):
        ArrayInformer.from_batch_job(batch_job)


def test_from_batch_job_no_info_file() -> None:
    batch_job = _make_mock_batch_job(info_file=None)

    with pytest.raises(QQError, match="not a valid qq job"):
        ArrayInformer.from_batch_job(batch_job)


def test_from_batch_job_job_mismatch() -> None:
    batch_job = _make_mock_batch_job(job_id="999")

    mock_array_info = MagicMock(spec=ArrayInfo)
    mock_array_info.task_dirs = []
    mock_array_info.job_ids = ["100"]
    mock_array_info.job_name = "myjob"

    with (
        patch.object(ArrayInfo, "from_file", return_value=mock_array_info),
        pytest.raises(QQJobMismatchError),
    ):
        ArrayInformer.from_batch_job(batch_job)


def test_from_job_id_success() -> None:
    batch_job = _make_mock_batch_job(job_id="100")
    batch_job.is_empty.return_value = False

    mock_array_info = MagicMock(spec=ArrayInfo)
    mock_array_info.task_dirs = []
    mock_array_info.job_ids = ["100"]
    mock_array_info.job_name = "myjob"

    with (
        patch.object(
            BatchInterface,
            "from_env_var_or_guess",
            return_value=PBS,
        ),
        patch.object(PBS, "get_batch_job", return_value=batch_job),
        patch.object(ArrayInfo, "from_file", return_value=mock_array_info),
    ):
        result = ArrayInformer.from_job_id("100")

    assert result.array_info is mock_array_info


def test_from_job_id_queries_batch_system() -> None:
    batch_job = _make_mock_batch_job(job_id="100")
    batch_job.is_empty.return_value = False

    mock_array_info = MagicMock(spec=ArrayInfo)
    mock_array_info.task_dirs = []
    mock_array_info.job_ids = ["100"]
    mock_array_info.job_name = "myjob"

    with (
        patch.object(
            BatchInterface,
            "from_env_var_or_guess",
            return_value=PBS,
        ),
        patch.object(PBS, "get_batch_job", return_value=batch_job) as mock_get,
        patch.object(ArrayInfo, "from_file", return_value=mock_array_info),
    ):
        ArrayInformer.from_job_id("100")

    mock_get.assert_called_once_with("100")


def test_from_job_id_nonexistent_job() -> None:
    batch_job = MagicMock(spec=BatchJobInterface)
    batch_job.is_empty.return_value = True

    with (
        patch.object(
            BatchInterface,
            "from_env_var_or_guess",
            return_value=PBS,
        ),
        patch.object(PBS, "get_batch_job", return_value=batch_job),
        pytest.raises(QQError, match="does not exist"),
    ):
        ArrayInformer.from_job_id("999")


def test_from_job_id_delegates_to_from_batch_job() -> None:
    batch_job = _make_mock_batch_job(job_id="100")
    batch_job.is_empty.return_value = False

    mock_informer = MagicMock(spec=ArrayInformer)

    with (
        patch.object(
            BatchInterface,
            "from_env_var_or_guess",
            return_value=PBS,
        ),
        patch.object(PBS, "get_batch_job", return_value=batch_job),
        patch.object(
            ArrayInformer, "from_batch_job", return_value=mock_informer
        ) as mock_from_batch,
    ):
        result = ArrayInformer.from_job_id("100")

    mock_from_batch.assert_called_once_with(batch_job)
    assert result is mock_informer


def _make_mock_task(
    *,
    task_number: int | None = 0,
    id_int: int | None = 1000,
    job_id: str = "1000",
) -> MagicMock:
    mock = MagicMock(spec=BatchJobInterface)
    mock.get_task_number.return_value = task_number
    mock.get_id_int.return_value = id_int
    mock.get_id.return_value = job_id
    return mock


def _make_mock_array_job(
    tasks: list[MagicMock],
    *,
    empty: bool = False,
) -> MagicMock:
    mock = MagicMock(spec=BatchJobInterface)
    mock.is_empty.return_value = empty
    mock.get_tasks.return_value = tasks
    return mock


def _make_mock_informer(
    *,
    task_number: int,
    job_id: str = "100",
    task_info_none: bool = False,
) -> MagicMock:
    mock = MagicMock(spec=Informer)
    if task_info_none:
        mock.info = MagicMock(spec=Info)
        mock.info.task_info = None
    else:
        mock.info = MagicMock(spec=Info)
        mock.info.task_info = MagicMock(spec=TaskInfo)
        mock.info.task_info.task_number = task_number
    mock.info.job_id = job_id
    return mock


def test_get_batch_tasks_single_job() -> None:
    tasks = [_make_mock_task(task_number=i) for i in range(3)]
    array_job = _make_mock_array_job(tasks)

    ai = MagicMock(spec=ArrayInformer)
    ai.array_info = MagicMock(spec=ArrayInfo)
    ai.array_info.job_ids = ["100"]
    ai.batch_system = PBS

    with patch.object(PBS, "get_batch_job", return_value=array_job):
        result = ArrayInformer._get_batch_tasks(ai)

    assert result == tasks


def test_get_batch_tasks_multiple_jobs() -> None:
    tasks_a = [_make_mock_task(task_number=0)]
    tasks_b = [_make_mock_task(task_number=1), _make_mock_task(task_number=2)]
    job_a = _make_mock_array_job(tasks_a)
    job_b = _make_mock_array_job(tasks_b)

    ai = MagicMock(spec=ArrayInformer)
    ai.array_info = MagicMock(spec=ArrayInfo)
    ai.array_info.job_ids = ["100", "200"]
    ai.batch_system = PBS

    with patch.object(PBS, "get_batch_job", side_effect=[job_a, job_b]):
        result = ArrayInformer._get_batch_tasks(ai)

    assert result == tasks_a + tasks_b


def test_get_batch_tasks_skips_empty_jobs() -> None:
    empty_job = _make_mock_array_job([], empty=True)
    real_tasks = [_make_mock_task(task_number=0)]
    real_job = _make_mock_array_job(real_tasks)

    ai = MagicMock(spec=ArrayInformer)
    ai.array_info = MagicMock(spec=ArrayInfo)
    ai.array_info.job_ids = ["100", "200"]
    ai.batch_system = PBS

    with patch.object(PBS, "get_batch_job", side_effect=[empty_job, real_job]):
        result = ArrayInformer._get_batch_tasks(ai)

    assert result == real_tasks
    empty_job.get_tasks.assert_not_called()


def test_get_batch_tasks_all_empty() -> None:
    ai = MagicMock(spec=ArrayInformer)
    ai.array_info = MagicMock(spec=ArrayInfo)
    ai.array_info.job_ids = ["100"]
    ai.batch_system = PBS

    with patch.object(
        PBS,
        "get_batch_job",
        return_value=_make_mock_array_job([], empty=True),
    ):
        result = ArrayInformer._get_batch_tasks(ai)

    assert result == []


def test_get_batch_tasks_no_job_ids() -> None:
    ai = MagicMock(spec=ArrayInformer)
    ai.array_info = MagicMock(spec=ArrayInfo)
    ai.array_info.job_ids = []
    ai.batch_system = PBS

    result = ArrayInformer._get_batch_tasks(ai)

    assert result == []


def test_get_batch_tasks_queries_each_job_id() -> None:
    ai = MagicMock(spec=ArrayInformer)
    ai.array_info = MagicMock(spec=ArrayInfo)
    ai.array_info.job_ids = ["100", "200", "300"]
    ai.batch_system = PBS

    with patch.object(
        PBS,
        "get_batch_job",
        return_value=_make_mock_array_job([], empty=True),
    ) as mock_get:
        ArrayInformer._get_batch_tasks(ai)

    mock_get.assert_has_calls([call("100"), call("200"), call("300")])


def test_number_batch_tasks_unique_numbers() -> None:
    tasks: list[BatchJobInterface] = [
        _make_mock_task(task_number=0, id_int=1000),
        _make_mock_task(task_number=1, id_int=1001),
        _make_mock_task(task_number=2, id_int=1002),
    ]

    result = ArrayInformer._number_batch_tasks(tasks)

    assert list(result.keys()) == [0, 1, 2]
    assert result[0] is tasks[0]
    assert result[1] is tasks[1]
    assert result[2] is tasks[2]


def test_number_batch_tasks_newer_replaces_older() -> None:
    older = _make_mock_task(task_number=0, id_int=1000)
    newer = _make_mock_task(task_number=0, id_int=2000)

    result = ArrayInformer._number_batch_tasks([older, newer])

    assert result[0] is newer


def test_number_batch_tasks_older_does_not_replace_newer() -> None:
    newer = _make_mock_task(task_number=0, id_int=2000)
    older = _make_mock_task(task_number=0, id_int=1000)

    result = ArrayInformer._number_batch_tasks([newer, older])

    assert result[0] is newer


def test_number_batch_tasks_equal_id_keeps_existing() -> None:
    first = _make_mock_task(task_number=0, id_int=1000)
    second = _make_mock_task(task_number=0, id_int=1000)

    result = ArrayInformer._number_batch_tasks([first, second])

    assert result[0] is first


def test_number_batch_tasks_task_number_none() -> None:
    task = _make_mock_task(task_number=None, id_int=1000)

    with pytest.raises(QQError, match="task number is undefined"):
        ArrayInformer._number_batch_tasks([task])


def test_number_batch_tasks_task_id_int_none() -> None:
    task = _make_mock_task(task_number=0, id_int=None, job_id="bad-id")

    with pytest.raises(QQError, match="Could not extract numerical job ID"):
        ArrayInformer._number_batch_tasks([task])


def test_number_batch_tasks_existing_id_int_none() -> None:
    first = _make_mock_task(task_number=0, id_int=1000)
    second = _make_mock_task(task_number=0, id_int=2000)

    # first call returns 1000 (passes initial check), second call returns None
    first.get_id_int.side_effect = [1000, None]
    first.get_id.return_value = "flaky-id"

    with pytest.raises(QQError, match="Could not extract numerical job ID"):
        ArrayInformer._number_batch_tasks([first, second])


def test_number_batch_tasks_empty() -> None:
    result = ArrayInformer._number_batch_tasks([])

    assert result == {}


def test_number_batch_tasks_multiple_duplicates() -> None:
    oldest = _make_mock_task(task_number=5, id_int=100)
    middle = _make_mock_task(task_number=5, id_int=200)
    newest = _make_mock_task(task_number=5, id_int=300)

    result = ArrayInformer._number_batch_tasks([oldest, middle, newest])

    assert len(result) == 1
    assert result[5] is newest


def _make_array_informer_with_informers(
    informers: list[MagicMock],
) -> ArrayInformer:
    mock_array_info = MagicMock(spec=ArrayInfo)
    mock_array_info.task_dirs = [Path(f"/work/t{i}") for i in range(len(informers))]
    mock_array_info.job_name = "myjob"
    mock_array_info.batch_system = PBS

    with patch.object(
        ArrayInformer, "_get_informers_for_tasks", return_value=informers
    ):
        return ArrayInformer(mock_array_info)


def test_match_batch_tasks_to_informers_all_matched() -> None:
    task_0 = _make_mock_task(task_number=0)
    task_1 = _make_mock_task(task_number=1)
    numbered: dict[int, BatchJobInterface] = {0: task_0, 1: task_1}

    inf_0 = _make_mock_informer(task_number=0)
    inf_1 = _make_mock_informer(task_number=1)
    ai = _make_array_informer_with_informers([inf_0, inf_1])

    ai._match_batch_tasks_to_informers(numbered)

    inf_0.set_batch_info.assert_called_once_with(task_0)
    inf_1.set_batch_info.assert_called_once_with(task_1)


def test_match_batch_tasks_to_informers_missing_gets_empty() -> None:
    empty_job = MagicMock(spec=BatchJobInterface)
    inf = _make_mock_informer(task_number=7, job_id="100")
    ai = _make_array_informer_with_informers([inf])

    with patch.object(PBS, "get_empty_batch_job", return_value=empty_job) as mock_empty:
        ai._match_batch_tasks_to_informers({})

    mock_empty.assert_called_once_with("100")
    inf.set_batch_info.assert_called_once_with(empty_job)


def test_match_batch_tasks_to_informers_task_info_none() -> None:
    inf = _make_mock_informer(task_number=0, task_info_none=True)
    ai = _make_array_informer_with_informers([inf])

    with pytest.raises(QQError, match="Task info is undefined"):
        ai._match_batch_tasks_to_informers({})


def test_match_batch_tasks_to_informers_mixed() -> None:
    task_0 = _make_mock_task(task_number=0)
    empty_job = MagicMock(spec=BatchJobInterface)

    inf_0 = _make_mock_informer(task_number=0)
    inf_1 = _make_mock_informer(task_number=1, job_id="200")
    ai = _make_array_informer_with_informers([inf_0, inf_1])

    with patch.object(PBS, "get_empty_batch_job", return_value=empty_job):
        ai._match_batch_tasks_to_informers({0: task_0})

    inf_0.set_batch_info.assert_called_once_with(task_0)
    inf_1.set_batch_info.assert_called_once_with(empty_job)


def _make_pbs_task(job_id: str, task_number: int) -> PBSJob:
    return PBSJob.from_dict(job_id, {"array_index": str(task_number)})


def _make_pbs_array_job(job_id: str, tasks: Sequence[PBSJob]) -> PBSJob:
    job = PBSJob.from_dict(job_id, {"array": "True"})
    job.get_tasks = lambda: tasks  # type: ignore
    return job


def _make_info(*, task_number: int, job_id: str = "100", total_tasks: int = 3) -> Info:
    return Info(
        batch_system=PBS,
        qq_version="0.12",
        username="testuser",
        job_id=job_id,
        job_name="myjob",
        script_name="run.sh",
        queue="default",
        job_type=JobType.ARRAY,
        input_machine="localhost",
        input_dir=Path("/work"),
        job_state=NaiveState.QUEUED,
        submission_time=datetime.strptime(
            "2065-03-21 12:00:00", CFG.date_formats.standard
        ),
        stdout_file="stdout.log",
        stderr_file="stderr.log",
        resources=MagicMock(spec=Resources),
        task_info=TaskInfo(
            array_file=Path("/work/array.yaml"),
            task_number=task_number,
            total_tasks=total_tasks,
        ),
    )


def _make_real_informers(
    task_numbers: list[int], job_id: str = "100"
) -> list[Informer]:
    return [
        Informer(
            _make_info(task_number=n, job_id=job_id, total_tasks=len(task_numbers))
        )
        for n in task_numbers
    ]


def _make_integration_array_informer(
    *,
    job_ids: list[str],
    informers: list[Informer],
) -> ArrayInformer:
    array_info = ArrayInfo(
        batch_system=PBS,
        job_name="myjob",
        job_ids=job_ids,
        task_dirs=[Path(f"/work/t{i}") for i in range(len(informers))],
        n_finished_tasks=0,
    )

    with patch.object(
        ArrayInformer, "_get_informers_for_tasks", return_value=informers
    ):
        return ArrayInformer(array_info)


def test_load_batch_info_assigns_tasks_to_informers() -> None:
    task_0 = _make_pbs_task("100[0]", task_number=0)
    task_1 = _make_pbs_task("100[1]", task_number=1)
    task_2 = _make_pbs_task("100[2]", task_number=2)

    array_job = _make_pbs_array_job("100[]", [task_0, task_1, task_2])
    informers = _make_real_informers([0, 1, 2])
    ai = _make_integration_array_informer(job_ids=["100[]"], informers=informers)

    with patch.object(PBS, "get_batch_job", return_value=array_job):
        ai.load_batch_info()

    assert informers[0]._batch_info is task_0
    assert informers[1]._batch_info is task_1
    assert informers[2]._batch_info is task_2
    assert ai._batch_info_loaded is True


def test_load_batch_info_multiple_array_jobs() -> None:
    task_0 = _make_pbs_task("100[0]", task_number=0)
    task_1 = _make_pbs_task("200[1]", task_number=1)

    job_a = _make_pbs_array_job("100[]", [task_0])
    job_b = _make_pbs_array_job("200[]", [task_1])

    informers = _make_real_informers([0, 1])
    ai = _make_integration_array_informer(
        job_ids=["100[]", "200[]"], informers=informers
    )

    with patch.object(PBS, "get_batch_job", side_effect=[job_a, job_b]):
        ai.load_batch_info()

    assert informers[0]._batch_info is task_0
    assert informers[1]._batch_info is task_1


def test_load_batch_info_missing_batch_task_gets_empty_job() -> None:
    task_0 = _make_pbs_task("100[0]", task_number=0)
    empty_job = PBSJob.from_dict("100", {})

    array_job = _make_pbs_array_job("100[]", [task_0])
    informers = _make_real_informers([0, 1])
    ai = _make_integration_array_informer(job_ids=["100[]"], informers=informers)

    with (
        patch.object(PBS, "get_batch_job", return_value=array_job),
        patch.object(PBS, "get_empty_batch_job", return_value=empty_job),
    ):
        ai.load_batch_info()

    assert informers[0]._batch_info is task_0
    assert informers[1]._batch_info is empty_job
    assert informers[1]._batch_info.is_empty()


def test_load_batch_info_all_jobs_gone_from_batch_system() -> None:
    gone_job = PBSJob.from_dict("100[]", {})
    empty_job = PBSJob.from_dict("100", {})

    informers = _make_real_informers([0, 1])
    ai = _make_integration_array_informer(job_ids=["100[]"], informers=informers)

    with (
        patch.object(PBS, "get_batch_job", return_value=gone_job),
        patch.object(PBS, "get_empty_batch_job", return_value=empty_job),
    ):
        ai.load_batch_info()

    assert informers[0]._batch_info is not None
    assert informers[0]._batch_info.is_empty()
    assert informers[1]._batch_info is not None
    assert informers[1]._batch_info.is_empty()


def test_load_batch_info_skips_second_call() -> None:
    # a second call to load_batch_info does not re-query PBS
    task_0 = _make_pbs_task("100[0]", task_number=0)
    array_job = _make_pbs_array_job("100[]", [task_0])

    informers = _make_real_informers([0])
    ai = _make_integration_array_informer(job_ids=["100[]"], informers=informers)

    with patch.object(PBS, "get_batch_job", return_value=array_job) as mock_get:
        ai.load_batch_info()
        ai.load_batch_info()

    assert mock_get.call_count == 1


def test_load_batch_info_warns_on_more_batch_tasks_than_informers() -> None:
    """A warning is logged when batch tasks outnumber informers."""
    task_0 = _make_pbs_task("100[0]", task_number=0)
    task_1 = _make_pbs_task("100[1]", task_number=1)
    task_2 = _make_pbs_task("100[2]", task_number=2)

    array_job = _make_pbs_array_job("100[]", [task_0, task_1, task_2])
    informers = _make_real_informers([0])
    ai = _make_integration_array_informer(job_ids=["100[]"], informers=informers)

    with (
        patch.object(PBS, "get_batch_job", return_value=array_job),
        patch("qq_lib.info.array_informer.logger") as mock_logger,
    ):
        ai.load_batch_info()

    mock_logger.warning.assert_called_once()
    assert "Array inconsistency" in mock_logger.warning.call_args[0][0]
    assert informers[0]._batch_info is task_0
    assert ai._batch_info_loaded is True


def test_load_batch_info_task_number_none_raises() -> None:
    bad_task = PBSJob.from_dict("100[0]", {})  # missing array_index

    array_job = _make_pbs_array_job("100[]", [bad_task])
    informers = _make_real_informers([0])
    ai = _make_integration_array_informer(job_ids=["100[]"], informers=informers)

    with (
        patch.object(PBS, "get_batch_job", return_value=array_job),
        pytest.raises(QQError, match="task number is undefined"),
    ):
        ai.load_batch_info()


def test_load_batch_info_task_id_int_none_raises() -> None:
    bad_task = PBSJob.from_dict("bad-id", {"array_index": "0"})  # get_id_int() is None

    array_job = _make_pbs_array_job("100[]", [bad_task])
    informers = _make_real_informers([0])
    ai = _make_integration_array_informer(job_ids=["100[]"], informers=informers)

    with (
        patch.object(PBS, "get_batch_job", return_value=array_job),
        pytest.raises(QQError, match="Could not extract numerical job ID"),
    ):
        ai.load_batch_info()
