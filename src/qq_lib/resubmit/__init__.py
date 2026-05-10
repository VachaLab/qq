# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

"""
Execution utilities for resubmitting qq continuous and loop jobs inside the batch environment.

This module defines the `Resubmitter` class, which handles resubmission of loop
and continuous jobs by resolving candidate hosts and attempting submission
on each in order until one succeeds.
"""

from .resubmitter import Resubmitter

__all__ = [
    "Resubmitter",
]
