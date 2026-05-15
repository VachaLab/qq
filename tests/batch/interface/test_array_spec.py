# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import pytest

from qq_lib.batch.interface.array_spec import ArraySpec
from qq_lib.core.error import QQError

type ArrayElement = int | tuple[int, int] | tuple[int, int, int]


class _ConcreteArraySpec(ArraySpec):
    """Minimal concrete subclass used only for testing the base constructor."""

    def translate(self) -> str:
        return ""


def test_array_spec_single_index() -> None:
    spec = _ConcreteArraySpec([0])
    assert spec._elements == [0]


def test_array_spec_multiple_indices() -> None:
    spec = _ConcreteArraySpec([0, 5, 100])
    assert spec._elements == [0, 5, 100]


def test_array_spec_range_two_tuple() -> None:
    spec = _ConcreteArraySpec([(0, 10)])
    assert spec._elements == [(0, 10)]


def test_array_spec_range_three_tuple() -> None:
    spec = _ConcreteArraySpec([(0, 10, 2)])
    assert spec._elements == [(0, 10, 2)]


def test_array_spec_mixed_elements() -> None:
    elements: list[ArrayElement] = [1, (10, 20), (30, 50, 5)]
    spec = _ConcreteArraySpec(elements)
    assert spec._elements == elements


def test_array_spec_range_start_equals_stop() -> None:
    spec = _ConcreteArraySpec([(5, 5)])
    assert spec._elements == [5]


def test_array_spec_step_of_one() -> None:
    spec = _ConcreteArraySpec([(0, 10, 1)])
    assert spec._elements == [(0, 10)]


def test_array_spec_zero_index() -> None:
    spec = _ConcreteArraySpec([0])
    assert spec._elements == [0]


def test_array_spec_empty_list_raises() -> None:
    with pytest.raises(QQError, match="elements must not be empty"):
        _ConcreteArraySpec([])


def test_array_spec_negative_bare_index() -> None:
    with pytest.raises(QQError, match="index must be >= 0"):
        _ConcreteArraySpec([-1])


def test_array_spec_negative_start_two_tuple() -> None:
    with pytest.raises(QQError, match="start must be >= 0"):
        _ConcreteArraySpec([(-1, 10)])


def test_array_spec_negative_start_three_tuple() -> None:
    with pytest.raises(QQError, match="start must be >= 0"):
        _ConcreteArraySpec([(-1, 10, 2)])


def test_array_spec_start_greater_than_stop_two_tuple() -> None:
    with pytest.raises(QQError, match="stop must be >= start"):
        _ConcreteArraySpec([(10, 5)])


def test_array_spec_start_greater_than_stop_three_tuple() -> None:
    with pytest.raises(QQError, match="stop must be >= start"):
        _ConcreteArraySpec([(10, 5, 1)])


def test_array_spec_zero_step() -> None:
    with pytest.raises(QQError, match="step must be >= 1"):
        _ConcreteArraySpec([(0, 10, 0)])


def test_array_spec_negative_step() -> None:
    with pytest.raises(QQError, match="step must be >= 1"):
        _ConcreteArraySpec([(0, 10, -1)])


def test_array_spec_string_element() -> None:
    with pytest.raises(QQError, match="expected int or tuple of 2-3 ints"):
        _ConcreteArraySpec(["1-10"])  # type: ignore


def test_array_spec_float_element() -> None:
    with pytest.raises(QQError, match="expected int or tuple of 2-3 ints"):
        _ConcreteArraySpec([1.5])  # type: ignore


def test_array_spec_single_element_tuple() -> None:
    with pytest.raises(QQError, match="expected int or tuple of 2-3 ints"):
        _ConcreteArraySpec([(1,)])  # type: ignore


def test_array_spec_four_element_tuple() -> None:
    with pytest.raises(QQError, match="expected int or tuple of 2-3 ints"):
        _ConcreteArraySpec([(1, 2, 3, 4)])  # type: ignore


def test_array_spec_none_element() -> None:
    with pytest.raises(QQError, match="expected int or tuple of 2-3 ints"):
        _ConcreteArraySpec([None])  # type: ignore


@pytest.mark.parametrize(
    ("elements", "expected"),
    [
        # no merging needed
        pytest.param([5], [5], id="single_index"),
        pytest.param([1, 3, 5], [1, 3, 5], id="disjoint_indices"),
        pytest.param([(0, 10)], [(0, 10)], id="single_range"),
        pytest.param([(0, 10, 3)], [(0, 10, 3)], id="single_strided_range"),
        pytest.param(
            [1, (10, 20), (30, 50, 5)],
            [1, (10, 20), (30, 50, 5)],
            id="disjoint_mixed",
        ),
        # duplicate indices and ranges
        pytest.param([3, 3, 3], [3], id="duplicate_indices"),
        pytest.param([5, 1, 5, 1], [1, 5], id="duplicate_indices_unsorted"),
        pytest.param(
            [(0, 10), (0, 10)],
            [(0, 10)],
            id="duplicate_ranges",
        ),
        # sorting
        pytest.param([10, 2, 7], [2, 7, 10], id="indices_sorted"),
        pytest.param(
            [(20, 30), (0, 5)],
            [(0, 5), (20, 30)],
            id="ranges_sorted",
        ),
        # adjacent indices merging into ranges
        pytest.param([1, 2], [(1, 2)], id="two_adjacent_indices"),
        pytest.param([1, 2, 3], [(1, 3)], id="three_consecutive_indices"),
        pytest.param(
            [5, 6, 7, 8, 9, 10],
            [(5, 10)],
            id="many_consecutive_indices",
        ),
        pytest.param(
            [1, 2, 3, 10, 11, 12],
            [(1, 3), (10, 12)],
            id="two_groups_of_consecutive_indices",
        ),
        pytest.param(
            [1, 2, 3, 5, 10, 11],
            [(1, 3), 5, (10, 11)],
            id="consecutive_groups_with_loner",
        ),
        # adjacent ranges merging
        pytest.param(
            [(1, 5), (6, 10)],
            [(1, 10)],
            id="adjacent_ranges",
        ),
        pytest.param(
            [(1, 10), (20, 30), (11, 19)],
            [(1, 30)],
            id="three_ranges_fill_gap",
        ),
        # overlapping ranges
        pytest.param(
            [(1, 5), (5, 10)],
            [(1, 10)],
            id="overlapping_ranges_shared_boundary",
        ),
        pytest.param(
            [(1, 10), (5, 15)],
            [(1, 15)],
            id="overlapping_ranges",
        ),
        pytest.param(
            [(0, 100), (10, 20)],
            [(0, 100)],
            id="range_fully_contained",
        ),
        pytest.param(
            [(0, 10), (3, 7), (8, 20)],
            [(0, 20)],
            id="multiple_overlapping_ranges",
        ),
        # index absorbed by range
        pytest.param(
            [5, (0, 10)],
            [(0, 10)],
            id="index_inside_range",
        ),
        pytest.param(
            [11, (0, 10)],
            [(0, 11)],
            id="index_adjacent_to_range",
        ),
        pytest.param(
            [0, (1, 5), 6],
            [(0, 6)],
            id="indices_extend_range_both_sides",
        ),
        # merging strided ranges
        pytest.param(
            [(0, 10, 2), (12, 20, 2)],
            [(0, 20, 2)],
            id="adjacent_strided_same_phase",
        ),
        pytest.param(
            [(0, 8, 2), (4, 12, 2)],
            [(0, 12, 2)],
            id="overlapping_strided_same_phase",
        ),
        pytest.param(
            [(0, 10, 5), (15, 30, 5)],
            [(0, 30, 5)],
            id="adjacent_strided_step_5",
        ),
        # strided ranges with different phase are not merged
        pytest.param(
            [(0, 10, 2), (1, 11, 2)],
            [(0, 10, 2), (1, 11, 2)],
            id="strided_different_phase",
        ),
        pytest.param(
            [(0, 10, 3), (1, 10, 3)],
            [(0, 10, 3), (1, 10, 3)],
            id="strided_step_3_different_phase",
        ),
        # strided ranges with different step are not merged
        pytest.param(
            [(0, 10, 2), (0, 10, 3)],
            [(0, 10, 2), (0, 10, 3)],
            id="strided_different_step_same_start",
        ),
        # large ranges
        pytest.param(
            [(0, 1_000_000), (1_000_001, 2_000_000)],
            [(0, 2_000_000)],
            id="large_adjacent_ranges",
        ),
        pytest.param(
            [(0, 1_000_000), (500_000, 1_500_000)],
            [(0, 1_500_000)],
            id="large_overlapping_ranges",
        ),
        pytest.param(
            [999_999, (0, 1_000_000)],
            [(0, 1_000_000)],
            id="index_inside_large_range",
        ),
        # range collapses to a single index
        pytest.param(
            [(5, 5)],
            [5],
            id="degenerate_range_to_index",
        ),
        pytest.param(
            [(5, 5, 3)],
            [5],
            id="degenerate_strided_range_to_index",
        ),
        # step-1 strided range loses step
        pytest.param(
            [(0, 10, 1)],
            [(0, 10)],
            id="step_one_normalized",
        ),
        # complex mixed scenarios
        pytest.param(
            [1, 2, (3, 7), 8, (10, 20, 2), (14, 22, 2)],
            [(1, 8), (10, 22, 2)],
            id="indices_ranges_strided_mixed",
        ),
        pytest.param(
            [(0, 5), 3, (4, 8), 12, 13, (11, 11)],
            [(0, 8), (11, 13)],
            id="many_overlapping_with_indices",
        ),
        pytest.param(
            [0, 2, 4, (10, 20), (25, 30), 21, 22, 23, 24],
            [0, 2, 4, (10, 30)],
            id="indices_bridge_gap_between_ranges",
        ),
        # same index repeated in many forms
        pytest.param(
            [5, (5, 5), (5, 5, 1)],
            [5],
            id="same_index_many_representations",
        ),
        # multiple single element ranges
        pytest.param(
            [(0, 0), (1, 1), (2, 2)],
            [(0, 2)],
            id="degenerate_ranges_merge_consecutive",
        ),
    ],
)
def test_array_spec_merge(
    elements: list[ArrayElement],
    expected: list[ArrayElement],
) -> None:
    spec = _ConcreteArraySpec(elements)
    assert spec._elements == expected
