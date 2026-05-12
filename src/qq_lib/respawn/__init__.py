# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

"""
Respawn utilities for qq jobs.

This module defines the `Respawner` class, which extends `Operator` to validate
whether a failed or killed job can be respawned and to resubmit it with its
original parameters.

Respawning involves cleaning up the working directory, clearing runtime files
from the input directory, and submitting a fresh copy of the job. For loop jobs,
the archive directory is checked for consistency before resubmission.
"""

from .respawner import Respawner

__all__ = [
    "Respawner",
]
