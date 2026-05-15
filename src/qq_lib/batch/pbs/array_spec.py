# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from qq_lib.batch.interface.array_spec import ArraySpec


class PBSArraySpec(ArraySpec):
    """
    Specification of job-array task indices.

    Args:
        elements (list[ArrayElement]): Non-empty list of indices and ranges.

    Raises:
        QQError: If the list is empty or any element violates constraints.
    """

    def translate(self) -> str:
        parts: list[str] = []
        for elem in self._elements:
            match elem:
                case int(index):
                    parts.append(str(index))
                case (int(start), int(stop)):
                    parts.append(f"{start}-{stop}")
                case (int(start), int(stop), int(step)):
                    parts.append(f"{start}-{stop}:{step}")
        return ",".join(parts)
