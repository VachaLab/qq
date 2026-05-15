# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import pytest

from qq_lib.batch.pbs.array_spec import PBSArraySpec

type ArrayElement = int | tuple[int, int] | tuple[int, int, int]


@pytest.mark.parametrize(
    ("elements", "expected"),
    [
        pytest.param([0], "0", id="single_index"),
        pytest.param([1, 3, 5], "1,3,5", id="multiple_indices"),
        pytest.param([(0, 10)], "0-10", id="range"),
        pytest.param([(0, 10, 2)], "0-10:2", id="strided_range"),
        pytest.param([(5, 5)], "5", id="range_start_equals_stop"),
        pytest.param([(0, 10, 1)], "0-10", id="step_of_one"),
        pytest.param(
            [1, (10, 20), (30, 50, 5)],
            "1,10-20,30-50:5",
            id="mixed_elements",
        ),
        pytest.param(
            [(0, 4), 7, (10, 20, 3)],
            "0-4,7,10-20:3",
            id="mixed_range_first",
        ),
    ],
)
def test_pbs_array_spec_translate(
    elements: list[ArrayElement],
    expected: str,
) -> None:
    assert PBSArraySpec(elements).translate() == expected
