# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import patch

import pytest
from click.testing import CliRunner

from qq_lib.batch.interface import BatchInterface
from qq_lib.batch.pbs import PBS, PBSJob
from qq_lib.batch.pbs.common import parse_multi_pbs_dump_to_dictionaries
from qq_lib.jobs.cli import jobs
from qq_lib.jobs.presenter import JobsPresenter


@pytest.fixture
def sample_pbs_dump():
    return """
Job Id: 123456.fake-cluster.example.com
    Job_Name = example_job_1
    Job_Owner = user1@EXAMPLE
    resources_used.cpupercent = 75
    resources_used.cput = 01:23:45
    resources_used.mem = 51200kb
    resources_used.ncpus = 4
    resources_used.vmem = 51200kb
    resources_used.walltime = 01:00:00
    job_state = R
    queue = gpu
    server = fake-cluster.example.com
    ctime = Sun Sep 21 00:00:00 2025
    mtime = Sun Sep 21 01:00:00 2025
    Resource_List.ncpus = 4
    Resource_List.ngpus = 1
    Resource_List.nodect = 1
    Resource_List.walltime = 02:00:00
    exec_host = nodeA/4*4
    exec_vnode = (nodeA:ncpus=4:ngpus=1:mem=4096mb)
    Output_Path = /fake/path/job_123456.log
    stime = Sun Sep 21 00:00:00 2025
    jobdir = /fake/home/user1

Job Id: 654321.fake-cluster.example.com
    Job_Name = example_job_2
    Job_Owner = user2@EXAMPLE
    resources_used.cpupercent = 150
    resources_used.cput = 02:34:56
    resources_used.mem = 102400kb
    resources_used.ncpus = 8
    resources_used.vmem = 102400kb
    resources_used.walltime = 02:00:00
    job_state = Q
    queue = batch
    server = fake-cluster.example.com
    ctime = Sun Sep 21 00:00:00 2025
    mtime = Sun Sep 21 01:00:00 2025
    Resource_List.ncpus = 8
    Resource_List.ngpus = 0
    Resource_List.nodect = 2
    Resource_List.walltime = 04:00:00
    exec_host = nodeB/8*8
    exec_vnode = (nodeB:ncpus=8:mem=8192mb)
    Output_Path = /fake/path/job_654321.log
    jobdir = /fake/home/user2
""".strip()


@pytest.fixture
def parsed_jobs(sample_pbs_dump):
    jobs = []
    for data, job_id in parse_multi_pbs_dump_to_dictionaries(sample_pbs_dump, "Job Id"):
        jobs.append(PBSJob.from_dict(job_id, data))
    return jobs


def test_jobs_command_unfinished_shows_jobs(parsed_jobs):
    runner = CliRunner()

    with (
        patch.object(BatchInterface, "from_env_var_or_guess", return_value=PBS),
        patch.object(
            PBS, "get_unfinished_batch_jobs", return_value=parsed_jobs
        ) as mock_get_jobs,
        patch.object(
            PBS,
            "get_batch_jobs",
            side_effect=Exception("get_batch_jobs should not be called"),
        ),
        patch.object(PBS, "sort_jobs") as mock_sort,
        patch("getpass.getuser", return_value="user"),
    ):
        result = runner.invoke(jobs, [], catch_exceptions=False)

        assert result.exit_code == 0
        mock_sort.assert_called_once()
        mock_get_jobs.assert_called_once_with("user", None)
        output = result.output

        for job in parsed_jobs:
            assert JobsPresenter._shorten_job_id(job.get_id()) in output
            assert job.get_name() in output
            assert job.get_user() in output


def test_jobs_command_unfinished_shows_jobs_with_server(parsed_jobs):
    runner = CliRunner()

    with (
        patch.object(BatchInterface, "from_env_var_or_guess", return_value=PBS),
        patch.object(
            PBS, "get_unfinished_batch_jobs", return_value=parsed_jobs
        ) as mock_get_jobs,
        patch.object(
            PBS,
            "get_batch_jobs",
            side_effect=Exception("get_batch_jobs should not be called"),
        ),
        patch.object(PBS, "sort_jobs") as mock_sort,
        patch("getpass.getuser", return_value="user"),
    ):
        result = runner.invoke(jobs, ["-s", "server"], catch_exceptions=False)

        assert result.exit_code == 0
        mock_get_jobs.assert_called_once_with("user", "server")
        mock_sort.assert_called_once()
        output = result.output

        for job in parsed_jobs:
            assert job.get_id() in output
            assert job.get_name() in output
            assert job.get_user() in output


def test_jobs_command_all_flag_shows_all_jobs(parsed_jobs):
    runner = CliRunner()

    with (
        patch.object(BatchInterface, "from_env_var_or_guess", return_value=PBS),
        patch.object(PBS, "get_batch_jobs", return_value=parsed_jobs) as mock_get_jobs,
        patch.object(
            PBS,
            "get_unfinished_batch_jobs",
            side_effect=Exception("get_unfinished_batch_jobs should not be called"),
        ),
        patch.object(PBS, "sort_jobs") as mock_sort,
        patch("getpass.getuser", return_value="user"),
    ):
        result = runner.invoke(jobs, ["--all"], catch_exceptions=False)

        assert result.exit_code == 0
        mock_get_jobs.assert_called_once_with("user", None)
        mock_sort.assert_called_once()
        output = result.output

        for job in parsed_jobs:
            assert JobsPresenter._shorten_job_id(job.get_id()) in output
            assert job.get_name() in output
            assert job.get_user() in output


def test_jobs_command_all_flag_shows_all_jobs_with_server(parsed_jobs):
    runner = CliRunner()

    with (
        patch.object(BatchInterface, "from_env_var_or_guess", return_value=PBS),
        patch.object(PBS, "get_batch_jobs", return_value=parsed_jobs) as mock_get_jobs,
        patch.object(
            PBS,
            "get_unfinished_batch_jobs",
            side_effect=Exception("get_unfinished_batch_jobs should not be called"),
        ),
        patch.object(PBS, "sort_jobs") as mock_sort,
        patch("getpass.getuser", return_value="user"),
    ):
        result = runner.invoke(jobs, ["--all", "-s", "server"], catch_exceptions=False)

        assert result.exit_code == 0
        mock_get_jobs.assert_called_once_with("user", "server")
        mock_sort.assert_called_once()
        output = result.output

        for job in parsed_jobs:
            assert job.get_id() in output
            assert job.get_name() in output
            assert job.get_user() in output


def test_jobs_command_yaml_flag_outputs_yaml(parsed_jobs):
    runner = CliRunner()

    with (
        patch.object(BatchInterface, "from_env_var_or_guess", return_value=PBS),
        patch.object(PBS, "get_unfinished_batch_jobs", return_value=parsed_jobs),
        patch.object(
            PBS,
            "get_batch_jobs",
            side_effect=Exception("get_batch_jobs should not be called"),
        ),
        patch.object(PBS, "sort_jobs") as mock_sort,
    ):
        result = runner.invoke(jobs, ["--yaml"], catch_exceptions=False)

        assert result.exit_code == 0
        mock_sort.assert_called_once()
        output = result.output

        for job in parsed_jobs:
            yaml_repr = job.to_yaml()
            assert yaml_repr.strip() in output


def test_jobs_command_no_jobs():
    """
    Test that the command exits cleanly when no jobs are returned.
    """
    runner = CliRunner()

    with (
        patch.object(BatchInterface, "from_env_var_or_guess", return_value=PBS),
        patch.object(PBS, "get_unfinished_batch_jobs", return_value=[]),
        patch.object(
            PBS,
            "get_batch_jobs",
            side_effect=Exception("get_batch_jobs should not be called"),
        ),
        patch.object(PBS, "sort_jobs") as mock_sort,
    ):
        result = runner.invoke(jobs, [], catch_exceptions=False)

        assert result.exit_code == 0
        mock_sort.assert_not_called()
