# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from typing import cast
from unittest.mock import MagicMock

import pytest

from qq_lib.batch.interface import AnyBatchClass
from qq_lib.core.error import QQError
from qq_lib.properties.depend import Depend, DependType, filter_dependencies


@pytest.mark.parametrize(
    "input_str, expected_type",
    [
        ("after", DependType.AFTER_START),
        ("afterok", DependType.AFTER_SUCCESS),
        ("afternotok", DependType.AFTER_FAILURE),
        ("afterany", DependType.AFTER_COMPLETION),
    ],
)
def test_depend_type_from_str_valid_mappings(input_str, expected_type):
    result = DependType.from_str(input_str)
    assert result == expected_type
    assert isinstance(result, DependType)


@pytest.mark.parametrize(
    "invalid_str",
    [
        "after_fail",
        "after_ok",
        "AFTEROK",
        "after any",
        "unknown",
        "",
        " ",
        None,
        123,
        [],
        {},
    ],
)
def test_depend_type_from_str_invalid_inputs(invalid_str):
    with pytest.raises(QQError, match="Unknown dependency type"):
        DependType.from_str(invalid_str)


@pytest.mark.parametrize(
    "raw_depend, expected_type, expected_jobs",
    [
        ("after=12345", DependType.AFTER_START, ["12345"]),
        ("afterok=12345:67890", DependType.AFTER_SUCCESS, ["12345", "67890"]),
        (
            "afternotok=jobA:jobB:jobC",
            DependType.AFTER_FAILURE,
            ["jobA", "jobB", "jobC"],
        ),
        ("afterany=abcd", DependType.AFTER_COMPLETION, ["abcd"]),
    ],
)
def test_depend_init_valid_inputs(raw_depend, expected_type, expected_jobs):
    dep = Depend.from_str(raw_depend)

    assert dep.type == expected_type
    assert dep.jobs == expected_jobs
    assert isinstance(dep.jobs, list)
    assert all(isinstance(j, str) for j in dep.jobs)


@pytest.mark.parametrize(
    "raw_depend",
    [
        "after=",
        "afterok=:",
        "afternotok=::",
        "afterany=   ",
        "afterok=1234:",
        "afternotok=:1234",
    ],
)
def test_depend_init_empty_or_blank_jobs_raises(raw_depend):
    with pytest.raises(QQError, match="Missing job id"):
        Depend.from_str(raw_depend)


@pytest.mark.parametrize(
    "raw_depend",
    [
        "after",
        "=12345",
        "unknown=12345",
        "afterok",
        "",
        "=",
        "afterok=12345=6789",
    ],
)
def test_depend_init_invalid_inputs_raise(raw_depend):
    with pytest.raises(QQError, match="Could not parse dependency specification"):
        Depend.from_str(raw_depend)


@pytest.mark.parametrize(
    "raw, expected_types, expected_jobs",
    [
        (
            "after=111 afterok=222 afternotok=333\tafterany=444",
            [
                DependType.AFTER_START,
                DependType.AFTER_SUCCESS,
                DependType.AFTER_FAILURE,
                DependType.AFTER_COMPLETION,
            ],
            [["111"], ["222"], ["333"], ["444"]],
        ),
        (
            "after=1,afterok=2:6:8:11,afternotok=3,afterany=4",
            [
                DependType.AFTER_START,
                DependType.AFTER_SUCCESS,
                DependType.AFTER_FAILURE,
                DependType.AFTER_COMPLETION,
            ],
            [["1"], ["2", "6", "8", "11"], ["3"], ["4"]],
        ),
        (
            "after=1, afterok=2 afternotok=3 , afterany=4",
            [
                DependType.AFTER_START,
                DependType.AFTER_SUCCESS,
                DependType.AFTER_FAILURE,
                DependType.AFTER_COMPLETION,
            ],
            [["1"], ["2"], ["3"], ["4"]],
        ),
        (
            "after=1:2,afterok=3:4:5",
            [DependType.AFTER_START, DependType.AFTER_SUCCESS],
            [["1", "2"], ["3", "4", "5"]],
        ),
    ],
)
def test_from_depend_str_valid_inputs(raw, expected_types, expected_jobs):
    depends = Depend.multi_from_str(raw)

    assert len(depends) == len(expected_types)
    for dep, expected_type, expected_joblist in zip(
        depends, expected_types, expected_jobs
    ):
        assert isinstance(dep, Depend)
        assert dep.type == expected_type
        assert dep.jobs == expected_joblist


@pytest.mark.parametrize(
    "raw, expected_count",
    [
        ("after=1   afterok=2 , afternotok=3", 3),
        ("after=1,,,afterok=2", 2),
        (" after=123   ", 1),
        ("   after=123,   ", 1),
    ],
)
def test_from_depend_str_robust_splitting(raw, expected_count):
    depends = Depend.multi_from_str(raw)
    assert len(depends) == expected_count
    assert all(isinstance(dep, Depend) for dep in depends)


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        ",,,",
        "   ,   ,",
    ],
)
def test_from_depend_str_empty_or_whitespace_input(raw):
    assert Depend.multi_from_str(raw) == []


@pytest.mark.parametrize(
    "raw",
    [
        "after=1 unknown=2",
        "after=1,invalid",
        "unknown=12345",
        "after=1,=",
        "afterok",
        "afterok=1234:",
    ],
)
def test_from_depend_str_invalid_entries_raise(raw):
    with pytest.raises(QQError, match="Could not parse dependency specification"):
        Depend.multi_from_str(raw)


@pytest.mark.parametrize(
    "dep_type, expected",
    [
        (DependType.AFTER_START, "after"),
        (DependType.AFTER_SUCCESS, "afterok"),
        (DependType.AFTER_FAILURE, "afternotok"),
        (DependType.AFTER_COMPLETION, "afterany"),
    ],
)
def test_dependtype_to_str_valid(dep_type, expected):
    assert dep_type.to_str() == expected


def test_depend_to_str_single_job():
    dep = Depend.from_str("after=12345")
    assert dep.to_str() == "after=12345"


def test_depend_to_str_multiple_jobs():
    dep = Depend.from_str("afterok=123:456:789")
    assert dep.to_str() == "afterok=123:456:789"


def test_depend_to_str_all_depend_types():
    data = [
        ("after=1", "after=1"),
        ("afterok=2", "afterok=2"),
        ("afternotok=3", "afternotok=3"),
        ("afterany=4:5", "afterany=4:5"),
    ]
    for raw, expected in data:
        dep = Depend.from_str(raw)
        assert dep.to_str() == expected


@pytest.fixture
def batch_system() -> AnyBatchClass:
    return cast("AnyBatchClass", MagicMock())


def test_filter_dependencies_keeps_valid_jobs(batch_system):
    job = MagicMock()
    job.is_empty.return_value = False
    batch_system.get_batch_job.return_value = job

    deps = [Depend(type=DependType.AFTER_SUCCESS, jobs=["123"])]
    result = filter_dependencies(batch_system, deps)

    assert len(result) == 1
    assert result[0].jobs == ["123"]


def test_filter_dependencies_removes_missing_jobs(batch_system):
    job = MagicMock()
    job.is_empty.return_value = True
    batch_system.get_batch_job.return_value = job

    deps = [Depend(type=DependType.AFTER_SUCCESS, jobs=["123"])]
    result = filter_dependencies(batch_system, deps)

    assert result == []


def test_filter_dependencies_removes_missing_jobs_from_multi_job_depend(batch_system):
    valid_job = MagicMock()
    valid_job.is_empty.return_value = False

    missing_job = MagicMock()
    missing_job.is_empty.return_value = True

    batch_system.get_batch_job.side_effect = lambda jid: (
        valid_job if jid == "123" else missing_job
    )

    deps = [Depend(type=DependType.AFTER_SUCCESS, jobs=["123", "456", "789"])]
    result = filter_dependencies(batch_system, deps)

    assert len(result) == 1
    assert result[0].jobs == ["123"]


def test_filter_dependencies_drops_depend_entirely_when_all_jobs_missing(batch_system):
    job = MagicMock()
    job.is_empty.return_value = True
    batch_system.get_batch_job.return_value = job

    deps = [Depend(type=DependType.AFTER_SUCCESS, jobs=["123", "456"])]
    result = filter_dependencies(batch_system, deps)

    assert result == []


def test_filter_dependencies_preserves_depend_type(batch_system):
    job = MagicMock()
    job.is_empty.return_value = False
    job.get_id.return_value = "1"
    batch_system.get_batch_job.return_value = job

    deps = [
        Depend(type=DependType.AFTER_START, jobs=["1"]),
        Depend(type=DependType.AFTER_FAILURE, jobs=["1"]),
        Depend(type=DependType.AFTER_COMPLETION, jobs=["1"]),
    ]
    result = filter_dependencies(batch_system, deps)

    assert [d.type for d in result] == [
        DependType.AFTER_START,
        DependType.AFTER_FAILURE,
        DependType.AFTER_COMPLETION,
    ]


def test_filter_dependencies_handles_multiple_depends(batch_system):
    valid_job = MagicMock()
    valid_job.is_empty.return_value = False

    missing_job = MagicMock()
    missing_job.is_empty.return_value = True

    batch_system.get_batch_job.side_effect = lambda jid: (
        valid_job if jid == "1" else missing_job
    )

    deps = [
        Depend(type=DependType.AFTER_SUCCESS, jobs=["1"]),
        Depend(type=DependType.AFTER_FAILURE, jobs=["2"]),
        Depend(type=DependType.AFTER_COMPLETION, jobs=["1", "3"]),
    ]
    result = filter_dependencies(batch_system, deps)

    assert len(result) == 2
    assert result[0].type == DependType.AFTER_SUCCESS
    assert result[0].jobs == ["1"]
    assert result[1].type == DependType.AFTER_COMPLETION
    assert result[1].jobs == ["1"]


def test_filter_dependencies_empty_input(batch_system):
    result = filter_dependencies(batch_system, [])

    assert result == []
    batch_system.get_batch_job.assert_not_called()
