# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

"""
Execution utilities for running qq jobs inside the batch environment.

This module defines the `Runner` class, which prepares the execution
environment, launches the user's job script, updates qq's state tracking,
and performs cleanup on success, failure, or interruption. It handles both
shared and scratch working directories, loop-job archiving, resubmission,
communication with the batch system, and SIGTERM-safe shutdown.

It also defines the `Resubmitter` class, which handles resubmission of loop
and continuous jobs by resolving candidate hosts and attempting submission
on each in order until one succeeds.
"""

from .runner import Runner

__all__ = [
    "Runner",
    "Resubmitter",
]
