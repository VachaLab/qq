# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from qq_lib.batch.pbs.array_spec import PBSArraySpec


class SlurmArraySpec(PBSArraySpec):
    """
    Specification of job-array task indices.

    Args:
        elements (list[ArrayElement]): Non-empty list of indices and ranges.

    Raises:
        QQError: If the list is empty or any element violates constraints.
    """

    pass
