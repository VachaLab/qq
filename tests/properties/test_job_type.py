# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import pytest

from qq_lib.core.error import QQError
from qq_lib.properties.job_type import JobType


def test_str_method():
    assert str(JobType.STANDARD) == "standard"
    assert str(JobType.LOOP) == "loop"
    assert str(JobType.CONTINUOUS) == "continuous"
    assert str(JobType.ARRAY) == "array"
    assert str(JobType.LOOP_ARRAY) == "loop array"
    assert str(JobType.CONTINUOUS_ARRAY) == "continuous array"


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("standard", JobType.STANDARD),
        ("STANDARD", JobType.STANDARD),
        ("sTaNdArD", JobType.STANDARD),
        ("loop", JobType.LOOP),
        ("LOOP", JobType.LOOP),
        ("LoOp", JobType.LOOP),
        ("continuous", JobType.CONTINUOUS),
        ("CONTINUOUS", JobType.CONTINUOUS),
        ("ConTiNUOus", JobType.CONTINUOUS),
        ("array", JobType.ARRAY),
        ("ARRAY", JobType.ARRAY),
        ("aRrAy", JobType.ARRAY),
        ("loop array", JobType.LOOP_ARRAY),
        ("LOOP-ARRAY", JobType.LOOP_ARRAY),
        ("LoOp_Array", JobType.LOOP_ARRAY),
        ("continuous_array", JobType.CONTINUOUS_ARRAY),
        ("CONTINUOUS ARRAY", JobType.CONTINUOUS_ARRAY),
        ("ConTiNUOus-aRrAy", JobType.CONTINUOUS_ARRAY),
    ],
)
def test_from_str_valid(input_str, expected):
    assert JobType.from_str(input_str) == expected


@pytest.mark.parametrize(
    "invalid_str",
    [
        "",
        "unknown",
        "job",
        "123",
        "standrd",  # intentional typo
        "looping",
        "continous",  # intentional typo
    ],
)
def test_from_str_invalid_raises(invalid_str):
    with pytest.raises(QQError) as excinfo:
        JobType.from_str(invalid_str)
    assert invalid_str in str(excinfo.value)


@pytest.mark.parametrize(
    "job_type,expected",
    [
        (JobType.STANDARD, True),
        (JobType.LOOP, False),
        (JobType.CONTINUOUS, False),
        (JobType.ARRAY, False),
        (JobType.LOOP_ARRAY, False),
        (JobType.CONTINUOUS_ARRAY, False),
    ],
)
def test_is_standard(job_type, expected):
    assert job_type.is_standard() == expected


@pytest.mark.parametrize(
    "job_type,expected",
    [
        (JobType.STANDARD, False),
        (JobType.LOOP, True),
        (JobType.CONTINUOUS, False),
        (JobType.ARRAY, False),
        (JobType.LOOP_ARRAY, True),
        (JobType.CONTINUOUS_ARRAY, False),
    ],
)
def test_is_loop(job_type, expected):
    assert job_type.is_loop() == expected


@pytest.mark.parametrize(
    "job_type,expected",
    [
        (JobType.STANDARD, False),
        (JobType.LOOP, False),
        (JobType.CONTINUOUS, True),
        (JobType.ARRAY, False),
        (JobType.LOOP_ARRAY, False),
        (JobType.CONTINUOUS_ARRAY, True),
    ],
)
def test_is_continuous(job_type, expected):
    assert job_type.is_continuous() == expected


@pytest.mark.parametrize(
    "job_type,expected",
    [
        (JobType.STANDARD, False),
        (JobType.LOOP, True),
        (JobType.CONTINUOUS, True),
        (JobType.ARRAY, False),
        (JobType.LOOP_ARRAY, True),
        (JobType.CONTINUOUS_ARRAY, True),
    ],
)
def test_is_loop_or_continuous(job_type, expected):
    assert job_type.is_loop_or_continuous() == expected


@pytest.mark.parametrize(
    "job_type,expected",
    [
        (JobType.STANDARD, False),
        (JobType.LOOP, False),
        (JobType.CONTINUOUS, False),
        (JobType.ARRAY, True),
        (JobType.LOOP_ARRAY, True),
        (JobType.CONTINUOUS_ARRAY, True),
    ],
)
def test_is_array(job_type, expected):
    assert job_type.is_array() == expected
