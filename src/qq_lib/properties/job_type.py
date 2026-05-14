# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

"""
Enumeration of supported qq job types.

This module defines `JobType`, an enum distinguishing between
standard jobs, loop jobs, array jobs and their combinations.
"""

from enum import Enum
from typing import Self

from qq_lib.core.error import QQError


class JobType(Enum):
    """
    Type of the qq job.
    """

    STANDARD = 1
    LOOP = 2
    CONTINUOUS = 3
    ARRAY = 4
    LOOP_ARRAY = 5
    CONTINUOUS_ARRAY = 6

    def __str__(self) -> str:
        """
        Return the string representation of the job type.
        """
        return self.name.lower().replace("_", " ")

    @classmethod
    def from_str(cls, s: str) -> Self:
        """
        Convert a string to the corresponding JobType enum variant.

        Args:
            s (str): String representation of the job type (case-insensitive).

        Returns:
            JobType variant.

        Raises:
            QQError if the string does not correspond to a valid JobType.
        """
        try:
            return cls[s.upper().replace("-", "_").replace(" ", "_")]
        except KeyError:
            raise QQError(f"Could not recognize a job type '{s}'.")

    def is_standard(self) -> bool:
        """
        True if the job type is a standard job.
        """
        return self == JobType.STANDARD

    def is_loop(self) -> bool:
        """
        True if the job type is a loop job or loop array job.
        """
        return self in [JobType.LOOP, JobType.LOOP_ARRAY]

    def is_continuous(self) -> bool:
        """
        True if the job type is a continuous job or continuous array job.
        """
        return self in [JobType.CONTINUOUS, JobType.CONTINUOUS_ARRAY]

    def is_loop_or_continuous(self) -> bool:
        """
        True if the job type is a loop job, continuous job, loop array job, or continuous array job.
        """
        return self in [
            JobType.LOOP,
            JobType.CONTINUOUS,
            JobType.LOOP_ARRAY,
            JobType.CONTINUOUS_ARRAY,
        ]

    def is_array(self) -> bool:
        """
        True if the job type is an array job, loop array job or a continuous array job.
        """
        return self in [JobType.ARRAY, JobType.LOOP_ARRAY, JobType.CONTINUOUS_ARRAY]
