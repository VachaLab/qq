# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

from unittest.mock import MagicMock

import pytest

from qq_lib.batch.pbs.job import PBSJob
from qq_lib.batch.slurm.job import SlurmJob
from qq_lib.properties.states import BatchState


@pytest.mark.parametrize("state", [BatchState.FINISHED, BatchState.FAILED])
def test_pbs_job_is_completed_returns_true_if_completed(state):
    job = PBSJob.__new__(PBSJob)
    job.get_state = MagicMock(return_value=state)

    assert job.is_completed()


@pytest.mark.parametrize(
    "state",
    [
        BatchState.RUNNING,
        BatchState.EXITING,
        BatchState.HELD,
        BatchState.MOVING,
        BatchState.QUEUED,
        BatchState.SUSPENDED,
        BatchState.UNKNOWN,
        BatchState.WAITING,
    ],
)
def test_pbs_job_is_completed_returns_false_if_not_completed(state):
    job = PBSJob.__new__(PBSJob)
    job.get_state = MagicMock(return_value=state)

    assert not job.is_completed()


@pytest.mark.parametrize("state", [BatchState.FINISHED, BatchState.FAILED])
def test_slurm_job_is_completed_returns_true_if_completed(state):
    job = SlurmJob.__new__(SlurmJob)
    job.get_state = MagicMock(return_value=state)

    assert job.is_completed()


@pytest.mark.parametrize(
    "state",
    [
        BatchState.RUNNING,
        BatchState.EXITING,
        BatchState.HELD,
        BatchState.MOVING,
        BatchState.QUEUED,
        BatchState.SUSPENDED,
        BatchState.UNKNOWN,
        BatchState.WAITING,
    ],
)
def test_slurm_job_is_completed_returns_false_if_not_completed(state):
    job = SlurmJob.__new__(SlurmJob)
    job.get_state = MagicMock(return_value=state)

    assert not job.is_completed()
