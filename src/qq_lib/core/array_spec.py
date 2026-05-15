# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from qq_lib.core.error import QQError

type ArrayElement = int | tuple[int, int] | tuple[int, int, int]


class ArraySpec:
    """
    Specification for job-array task indices.

    Args:
        elements (list[ArrayElement]): Non-empty list of indices and ranges.

    Raises:
        QQError: If the list is empty or any element violates constraints.
    """

    def __init__(self, elements: list[ArrayElement]):
        _validate_elements(elements)
        self.elements = _merge_elements(elements)


def _validate_elements(elements: list[ArrayElement]) -> None:
    """
    Validate a list of array elements.

    Args:
        elements (list[ArrayElement]): Non-empty list of indices and ranges.

    Raises:
        QQError: If an element has an unsupported type.
        QQError: If the list is empty or any element violates
            constraints (negative index, start > stop, step < 1).
    """
    if not elements:
        raise QQError("Invalid array specification: no elements provided.")

    for element in elements:
        match element:
            case int(index):
                if index < 0:
                    raise QQError(
                        f"Invalid array specification: index must be >= 0, got {index}."
                    )
            case (int(start), int(stop)):
                if start < 0:
                    raise QQError(
                        f"Invalid array specification: start must be >= 0, got {start}."
                    )
                if stop < start:
                    raise QQError(
                        f"Invalid array specification:stop must be >= start, got start={start}, stop={stop}."
                    )
            case (int(start), int(stop), int(step)):
                if start < 0:
                    raise QQError(
                        f"Invalid array specification: start must be >= 0, got {start}."
                    )
                if stop < start:
                    raise QQError(
                        f"Invalid array specification: stop must be >= start, got start={start}, stop={stop}."
                    )
                if step < 1:
                    raise QQError(
                        f"Invalid array specification: step must be >= 1, got {step}."
                    )
            case _:
                raise QQError(
                    f"Invalid array specification: expected int or tuple of 2-3 ints, got {element!r}."
                )


def _merge_elements(elements: list[ArrayElement]) -> list[ArrayElement]:
    """
    Merge array elements into a shorter equivalent list.

    Groups ranges by step and phase (`start % step`), then merges
    overlapping or adjacent ranges within each group. The algorithm is
    greedy and does not attempt cross-step merging - bare indices `[1, 3, 5]`
    will not be collapsed into `(1, 5, 2)`.

    Args:
        elements (list[ArrayElement]): Non-empty list of indices and ranges.

    Returns:
        list[ArrayElement]: Equivalent list with overlapping and adjacent
            same-step ranges merged.
    """
    if not elements:
        return []

    # expand everything to (start, stop, step) triples
    triples: list[tuple[int, int, int]] = []
    for element in elements:
        match element:
            case int(index):
                triples.append((index, index, 1))
            case (int(start), int(stop)):
                triples.append((start, stop, 1))
            case (int(start), int(stop), int(step)):
                triples.append((start, stop, step))

    # group by (step, phase) so only ranges that are compatible are merged
    groups: dict[tuple[int, int], list[tuple[int, int]]] = {}
    for start, stop, step in triples:
        key = (step, start % step)
        groups.setdefault(key, []).append((start, stop))

    # merge overlapping and adjacent ranges in each group
    merged = []
    for (step, _), ranges in groups.items():
        ranges.sort()
        current_start, current_stop = ranges[0]
        for start, stop in ranges[1:]:
            if start <= current_stop + step:
                current_stop = max(current_stop, stop)
            else:
                merged.append((current_start, current_stop, step))
                current_start, current_stop = start, stop
        merged.append((current_start, current_stop, step))

    merged.sort()

    # convert back to ArrayElement
    result: list[ArrayElement] = []
    for start, stop, step in merged:
        if start == stop:
            result.append(start)
        elif step == 1:
            result.append((start, stop))
        else:
            result.append((start, stop, step))

    return result
