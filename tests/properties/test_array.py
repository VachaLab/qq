# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path

import pytest

from qq_lib.core.error import QQError
from qq_lib.properties.array import ArrayInfo


def test_array_info_to_dict():
    info = ArrayInfo(array_file=Path("job.qqarray"), task_number=3, total_tasks=10)
    result = info.to_dict()

    assert result == {
        "array_file": "job.qqarray",
        "task_number": 3,
        "total_tasks": 10,
    }


def test_array_info_to_dict_converts_path_to_str():
    info = ArrayInfo(array_file=Path("job.qqarray"), task_number=1, total_tasks=5)

    assert isinstance(info.to_dict()["array_file"], str)


def test_array_info_from_dict():
    data: dict[str, object] = {
        "array_file": "job.qqarray",
        "task_number": 3,
        "total_tasks": 10,
    }
    info = ArrayInfo.from_dict(data)

    assert info.array_file == Path("job.qqarray")
    assert info.task_number == 3
    assert info.total_tasks == 10


def test_array_info_from_dict_converts_str_to_path():
    data: dict[str, object] = {
        "array_file": "job.qqarray",
        "task_number": 1,
        "total_tasks": 5,
    }
    info = ArrayInfo.from_dict(data)

    assert isinstance(info.array_file, Path)


def test_array_info_from_dict_accepts_path_object():
    data: dict[str, object] = {
        "array_file": Path("job.qqarray"),
        "task_number": 1,
        "total_tasks": 5,
    }
    info = ArrayInfo.from_dict(data)

    assert info.array_file == Path("job.qqarray")


def test_array_info_from_dict_raises_on_missing_array_file():
    data: dict[str, object] = {"task_number": 1, "total_tasks": 5}

    with pytest.raises(QQError, match="array_file"):
        ArrayInfo.from_dict(data)


def test_array_info_from_dict_raises_on_missing_task_number():
    data: dict[str, object] = {"array_file": "job.qqarray", "total_tasks": 5}

    with pytest.raises(QQError, match="task_number"):
        ArrayInfo.from_dict(data)


def test_array_info_from_dict_raises_on_missing_total_tasks():
    data: dict[str, object] = {"array_file": "job.qqarray", "task_number": 1}

    with pytest.raises(QQError, match="total_tasks"):
        ArrayInfo.from_dict(data)


def test_array_info_from_dict_raises_on_invalid_array_file_type():
    data: dict[str, object] = {"array_file": 123, "task_number": 1, "total_tasks": 5}

    with pytest.raises(QQError, match="array_file.*int"):
        ArrayInfo.from_dict(data)


def test_array_info_from_dict_raises_on_invalid_task_number_type():
    data: dict[str, object] = {
        "array_file": "job.qqarray",
        "task_number": "3",
        "total_tasks": 5,
    }

    with pytest.raises(QQError, match="task_number.*str"):
        ArrayInfo.from_dict(data)


def test_array_info_from_dict_raises_on_invalid_total_tasks_type():
    data: dict[str, object] = {
        "array_file": "job.qqarray",
        "task_number": 1,
        "total_tasks": "5",
    }

    with pytest.raises(QQError, match="total_tasks.*str"):
        ArrayInfo.from_dict(data)


def test_array_info_round_trip():
    original = ArrayInfo(array_file=Path("job.qqarray"), task_number=3, total_tasks=10)
    restored = ArrayInfo.from_dict(original.to_dict())

    assert restored.array_file == original.array_file
    assert restored.task_number == original.task_number
    assert restored.total_tasks == original.total_tasks
