# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import os
import socket
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.interface import AnyBatchClass
from qq_lib.batch.pbs.pbs import PBS
from qq_lib.batch.slurm import Slurm
from qq_lib.core.error import QQError
from qq_lib.info.informer import Informer
from qq_lib.properties.depend import Depend, DependType
from qq_lib.properties.interpreter import Interpreter
from qq_lib.properties.job_type import JobType
from qq_lib.properties.loop import LoopInfo
from qq_lib.properties.resources import Resources
from qq_lib.properties.resubmit_host import ExplicitHost, InputHost, WorkHost
from qq_lib.properties.states import NaiveState
from qq_lib.properties.transfer_mode import Always, Success
from qq_lib.submit.submitter import CFG, Submitter


def test_submitter_init_sets_all_attributes_correctly(tmp_path):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    with (
        patch.object(Submitter, "_construct_job_name", return_value="job1"),
        patch.object(Submitter, "_has_valid_shebang", return_value=True),
    ):
        submitter = Submitter(
            batch_system=PBS,
            queue="default",
            account=None,
            script=script,
            job_type=JobType.STANDARD,
            resources=Resources(),
            exclude=["exclude", "/tmp/exclude"],
            include=["include", "/tmp/include"],
            ignore=["ignore", "/tmp/ignore"],
            transfer_mode=[Always()],
            server="pbs-m1.metacentrum.cz",
            interpreter=Interpreter(executable="bash"),
            resubmit_from=[InputHost(), WorkHost()],
        )

        assert submitter._batch_system == PBS
        assert submitter._job_type == JobType.STANDARD
        assert submitter._queue == "default"
        assert submitter._account is None
        assert submitter._loop_info is None
        assert submitter._script == script
        assert submitter._input_dir == tmp_path
        assert submitter._job_name == "job1"
        assert submitter._info_file == tmp_path / f"job1{CFG.suffixes.qq_info}"
        assert submitter._resources == Resources()
        assert submitter._exclude == [tmp_path / "exclude", Path("/tmp/exclude")]
        assert submitter._include == [tmp_path / "include", Path("/tmp/include")]
        assert submitter._ignore == [tmp_path / "ignore", Path("/tmp/ignore")]
        assert submitter._depend == []
        assert isinstance(submitter._transfer_mode[0], Always)
        assert submitter._server == "pbs-m1.metacentrum.cz"
        assert submitter._interpreter == Interpreter(executable="bash")
        assert submitter._resubmit_from == [InputHost(), WorkHost()]


def test_submitter_init_raises_error_if_script_does_not_exist(tmp_path):
    script = tmp_path / "nonexistent.sh"

    with pytest.raises(QQError, match="does not exist"):
        Submitter(
            batch_system=PBS,
            queue="default",
            account=None,
            script=script,
            job_type=JobType.STANDARD,
            resources=Resources(),
        )


def test_submitter_init_raises_error_if_invalid_shebang(tmp_path):
    script = tmp_path / "bad_script.sh"
    script.write_text("invalid shebang\n")

    with (
        patch.object(Submitter, "_construct_job_name", return_value="job1"),
        patch.object(Submitter, "_has_valid_shebang", return_value=False),
        pytest.raises(QQError, match="invalid shebang"),
    ):
        Submitter(
            batch_system=PBS,
            queue="default",
            account="fake-account",
            script=script,
            job_type=JobType.STANDARD,
            resources=Resources(),
        )


def test_submitter_init_sets_all_optional_arguments_correctly(tmp_path):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    loop_info = LoopInfo(1, 5, Path("storage"), "job%04d")
    exclude_files = [str(tmp_path / "file1.txt"), str(tmp_path / "file2.txt")]
    include_files = [str(tmp_path / "file3.txt"), str(tmp_path / "file4.txt")]
    ignore_files = [str(tmp_path / "file5.txt"), str(tmp_path / "file6.txt")]
    depend_jobs = [
        Depend(DependType.AFTER_SUCCESS, ["12345"]),
        Depend(DependType.AFTER_START, ["23456"]),
    ]

    with (
        patch.object(Submitter, "_construct_job_name", return_value="job"),
        patch.object(Submitter, "_has_valid_shebang", return_value=True),
    ):
        submitter = Submitter(
            batch_system=PBS,
            queue="long",
            account="fake-account",
            script=script,
            job_type=JobType.LOOP,
            resources=Resources(),
            loop_info=loop_info,
            exclude=exclude_files,
            include=include_files,
            ignore=ignore_files,
            depend=depend_jobs,
            server="fake.server.com",
            resubmit_from=[WorkHost(), ExplicitHost("node01")],
        )

        assert submitter._batch_system == PBS
        assert submitter._job_type == JobType.LOOP
        assert submitter._queue == "long"
        assert submitter._account == "fake-account"
        assert submitter._loop_info == loop_info
        assert submitter._script == script
        assert submitter._input_dir == tmp_path
        assert submitter._script_name == script.name
        assert submitter._job_name == "job"
        assert submitter._include == [Path(x) for x in include_files]
        assert submitter._ignore == [Path(x) for x in ignore_files]
        assert submitter._info_file == tmp_path / f"job{CFG.suffixes.qq_info}"
        assert submitter._resources == Resources()
        assert submitter._exclude == [Path(x) for x in exclude_files]
        assert submitter._depend == depend_jobs
        assert submitter._server == "fake.server.com"
        assert submitter._resubmit_from == [WorkHost(), ExplicitHost("node01")]


def test_submitter_construct_job_name_returns_script_name_for_standard_job(
    tmp_path,
):
    script = tmp_path / "job.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")
    submitter = Submitter.__new__(Submitter)
    submitter._script_name = "job.sh"
    submitter._loop_info = None

    result = submitter._construct_job_name()

    assert result == "job.sh"


def test_submitter_construct_job_name_returns_name_with_cycle_number_for_loop_job(
    tmp_path,
):
    script = tmp_path / "job.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")
    submitter = Submitter.__new__(Submitter)
    submitter._script_name = "job.sh"

    class DummyLoopInfo:
        current = 3

    submitter._loop_info = DummyLoopInfo()

    result = submitter._construct_job_name()

    assert result == f"job{CFG.loop_jobs.pattern % 3}.sh"


def test_submitter_construct_job_name_loop_job_no_extension(
    tmp_path,
):
    script = tmp_path / "job"
    script.write_text("#!/usr/bin/env -S qq run\n")
    submitter = Submitter.__new__(Submitter)
    submitter._script_name = "job"

    class DummyLoopInfo:
        current = 3

    submitter._loop_info = DummyLoopInfo()

    result = submitter._construct_job_name()

    assert result == f"job{CFG.loop_jobs.pattern % 3}"


def test_submitter_has_valid_shebang_returns_true_for_valid_shebang(tmp_path):
    script = tmp_path / "valid_script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    submitter = Submitter.__new__(Submitter)
    result = submitter._has_valid_shebang(script)

    assert result is True


def test_submitter_has_valid_shebang_returns_false_if_not_ending_with_qq_run(
    tmp_path,
):
    script = tmp_path / "wrong_end.sh"
    script.write_text("#!/usr/bin/env python\n")

    submitter = Submitter.__new__(Submitter)
    result = submitter._has_valid_shebang(script)

    assert result is False


def test_submitter__has_valid_shebang_returns_false_when_no_shebang_line(tmp_path):
    script = tmp_path / "random_command.sh"
    script.write_text("echo 'hello world'\n")

    submitter = Submitter.__new__(Submitter)
    result = submitter._has_valid_shebang(script)

    assert result is False


@pytest.mark.parametrize("debug_mode", [True, False])
def test_submitter_create_env_vars_dict_sets_all_required_variables(
    tmp_path, debug_mode
):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    submitter = Submitter.__new__(Submitter)
    submitter._info_file = tmp_path / "job.qqinfo"
    submitter._batch_system = PBS
    submitter._loop_info = None
    submitter._input_dir = tmp_path
    submitter._resources = Resources(nnodes=2, ncpus=8, ngpus=2, walltime="1d")
    submitter._job_type = JobType.STANDARD

    if debug_mode:
        with patch.dict(os.environ, {CFG.env_vars.debug_mode: "true"}):
            env = submitter._create_env_vars_dict()
    else:
        env = submitter._create_env_vars_dict()

    assert env[CFG.env_vars.guard] == "true"
    assert env[CFG.env_vars.info_file] == str(submitter._info_file)
    assert env[CFG.env_vars.input_machine] == socket.getfqdn()
    assert env[CFG.env_vars.batch_system] == str(submitter._batch_system)
    assert env[CFG.env_vars.input_dir] == str(submitter._input_dir)
    assert env[CFG.env_vars.nnodes] == str(submitter._resources.nnodes)
    assert env[CFG.env_vars.ncpus] == str(submitter._resources.ncpus)
    assert env[CFG.env_vars.ngpus] == str(submitter._resources.ngpus)
    assert env[CFG.env_vars.walltime] == "24.0"
    if debug_mode:
        assert env[CFG.env_vars.debug_mode] == "true"
    else:
        assert CFG.env_vars.debug_mode not in env


@pytest.mark.parametrize("debug_mode", [True, False])
def test_submitter_create_env_vars_dict_sets_all_required_variables_with_per_node_properties(
    tmp_path, debug_mode
):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    submitter = Submitter.__new__(Submitter)
    submitter._info_file = tmp_path / "job.qqinfo"
    submitter._batch_system = PBS
    submitter._loop_info = None
    submitter._input_dir = tmp_path
    submitter._resources = Resources(
        nnodes=2, ncpus_per_node=8, ngpus_per_node=2, walltime="1d"
    )
    submitter._job_type = JobType.STANDARD

    if debug_mode:
        with patch.dict(os.environ, {CFG.env_vars.debug_mode: "true"}):
            env = submitter._create_env_vars_dict()
    else:
        env = submitter._create_env_vars_dict()

    assert env[CFG.env_vars.guard] == "true"
    assert env[CFG.env_vars.info_file] == str(submitter._info_file)
    assert env[CFG.env_vars.input_machine] == socket.getfqdn()
    assert env[CFG.env_vars.batch_system] == str(submitter._batch_system)
    assert env[CFG.env_vars.input_dir] == str(submitter._input_dir)
    assert env[CFG.env_vars.nnodes] == str(submitter._resources.nnodes)
    assert submitter._resources.ncpus_per_node is not None
    assert submitter._resources.nnodes is not None
    assert env[CFG.env_vars.ncpus] == str(
        submitter._resources.ncpus_per_node * submitter._resources.nnodes
    )
    assert submitter._resources.ngpus_per_node is not None
    assert env[CFG.env_vars.ngpus] == str(
        submitter._resources.ngpus_per_node * submitter._resources.nnodes
    )
    assert env[CFG.env_vars.walltime] == "24.0"
    if debug_mode:
        assert env[CFG.env_vars.debug_mode] == "true"
    else:
        assert CFG.env_vars.debug_mode not in env


@pytest.mark.parametrize("debug_mode", [True, False])
def test_submitter_create_env_vars_dict_sets_loop_variables(tmp_path, debug_mode):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    class DummyLoop:
        current = 1
        start = 0
        end = 5
        archive_format = "job%02d"

    submitter = Submitter.__new__(Submitter)
    submitter._info_file = tmp_path / "job.qqinfo"
    submitter._batch_system = "BatchSystem"
    submitter._loop_info = DummyLoop()
    submitter._input_dir = tmp_path
    submitter._resources = Resources()
    submitter._job_type = JobType.LOOP

    if debug_mode:
        with patch.dict(os.environ, {CFG.env_vars.debug_mode: "true"}):
            env = submitter._create_env_vars_dict()
    else:
        env = submitter._create_env_vars_dict()

    assert env[CFG.env_vars.guard] == "true"
    assert env[CFG.env_vars.info_file] == str(submitter._info_file)
    assert env[CFG.env_vars.input_machine] == socket.getfqdn()
    assert env[CFG.env_vars.batch_system] == str(submitter._batch_system)
    assert env[CFG.env_vars.input_dir] == str(submitter._input_dir)

    assert env[CFG.env_vars.loop_current] == str(DummyLoop.current)
    assert env[CFG.env_vars.loop_next] == str(DummyLoop.current + 1)
    assert env[CFG.env_vars.loop_start] == str(DummyLoop.start)
    assert env[CFG.env_vars.loop_end] == str(DummyLoop.end)
    assert env[CFG.env_vars.archive_format] == DummyLoop.archive_format
    assert env[CFG.env_vars.no_resubmit] == str(CFG.exit_codes.qq_run_no_resubmit)
    assert env[CFG.env_vars.archive_current] == "job01"
    assert env[CFG.env_vars.archive_next] == "job02"
    if debug_mode:
        assert env[CFG.env_vars.debug_mode] == "true"
    else:
        assert CFG.env_vars.debug_mode not in env


@pytest.mark.parametrize("debug_mode", [True, False])
def test_submitter_create_env_vars_dict_continuous_job(tmp_path, debug_mode):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    submitter = Submitter.__new__(Submitter)
    submitter._info_file = tmp_path / "job.qqinfo"
    submitter._batch_system = "BatchSystem"
    submitter._input_dir = tmp_path
    submitter._resources = Resources()
    submitter._loop_info = None
    submitter._job_type = JobType.CONTINUOUS

    if debug_mode:
        with patch.dict(os.environ, {CFG.env_vars.debug_mode: "true"}):
            env = submitter._create_env_vars_dict()
    else:
        env = submitter._create_env_vars_dict()

    assert env[CFG.env_vars.guard] == "true"
    assert env[CFG.env_vars.info_file] == str(submitter._info_file)
    assert env[CFG.env_vars.input_machine] == socket.getfqdn()
    assert env[CFG.env_vars.batch_system] == str(submitter._batch_system)
    assert env[CFG.env_vars.input_dir] == str(submitter._input_dir)
    assert env[CFG.env_vars.no_resubmit] == str(CFG.exit_codes.qq_run_no_resubmit)
    if debug_mode:
        assert env[CFG.env_vars.debug_mode] == "true"
    else:
        assert CFG.env_vars.debug_mode not in env


def test_submitter_get_input_dir_returns_correct_path(tmp_path):
    submitter = Submitter.__new__(Submitter)
    submitter._input_dir = tmp_path

    result = submitter.get_input_dir()

    assert result == tmp_path


def test_submitter_loop_job_continues_loop_true_for_valid_continuation():
    submitter = Submitter.__new__(Submitter)
    submitter._loop_info = MagicMock(current=2)

    dummy_info = MagicMock()
    dummy_info.loop_info = MagicMock(current=1)
    dummy_info.job_state = NaiveState.FINISHED

    dummy_informer = MagicMock()
    dummy_informer.info = dummy_info

    assert submitter._loop_job_continues_loop(dummy_informer) is True


def test_submitter_loop_job_continues_loop_false_if_previous_not_finished():
    submitter = Submitter.__new__(Submitter)
    submitter._loop_info = MagicMock(current=2)

    dummy_info = MagicMock()
    dummy_info.loop_info = MagicMock(current=1)
    dummy_info.job_state = NaiveState.RUNNING

    dummy_informer = MagicMock()
    dummy_informer.info = dummy_info

    assert submitter._loop_job_continues_loop(dummy_informer) is False


def test_submitter_loop_job_continues_loop_false_if_previous_cycle_mismatch():
    submitter = Submitter.__new__(Submitter)
    submitter._loop_info = MagicMock(current=5)

    dummy_info = MagicMock()
    dummy_info.loop_info = MagicMock(current=3)
    dummy_info.job_state = NaiveState.FINISHED

    dummy_informer = MagicMock()
    dummy_informer.info = dummy_info

    assert submitter._loop_job_continues_loop(dummy_informer) is False


def test_submitter_loop_job_continues_loop_false_if_no_loop_info_in_past():
    submitter = Submitter.__new__(Submitter)
    submitter._loop_info = MagicMock(current=2)

    dummy_info = MagicMock()
    dummy_info.loop_info = None
    dummy_info.job_state = NaiveState.FINISHED

    dummy_informer = MagicMock()
    dummy_informer.info = dummy_info

    assert submitter._loop_job_continues_loop(dummy_informer) is False


def test_submitter_loop_job_continues_loop_false_if_no_loop_info_current():
    submitter = Submitter.__new__(Submitter)
    submitter._loop_info = None

    dummy_info = MagicMock()
    dummy_info.loop_info = MagicMock(current=1)
    dummy_info.job_state = NaiveState.FINISHED

    dummy_informer = MagicMock()
    dummy_informer.info = dummy_info

    assert submitter._loop_job_continues_loop(dummy_informer) is False


def test_submitter_continuous_job_continues_loop_true_for_valid_continuation():
    submitter = Submitter.__new__(Submitter)
    submitter._job_type = JobType.CONTINUOUS

    dummy_info = MagicMock()
    dummy_info.job_type = JobType.CONTINUOUS
    dummy_info.job_state = NaiveState.FINISHED

    dummy_informer = MagicMock()
    dummy_informer.info = dummy_info

    assert submitter._continuous_job_continues_loop(dummy_informer) is True


def test_submitter_continuous_job_continues_loop_false_if_not_finished():
    submitter = Submitter.__new__(Submitter)
    submitter._job_type = JobType.CONTINUOUS

    dummy_info = MagicMock()
    dummy_info.job_type = JobType.CONTINUOUS
    dummy_info.job_state = NaiveState.RUNNING

    dummy_informer = MagicMock()
    dummy_informer.info = dummy_info

    assert submitter._continuous_job_continues_loop(dummy_informer) is False


def test_submitter_continuous_job_continues_loop_false_if_previous_not_continuous():
    submitter = Submitter.__new__(Submitter)
    submitter._job_type = JobType.CONTINUOUS

    dummy_info = MagicMock()
    dummy_info.job_type = JobType.LOOP
    dummy_info.job_state = NaiveState.FINISHED

    dummy_informer = MagicMock()
    dummy_informer.info = dummy_info

    assert submitter._continuous_job_continues_loop(dummy_informer) is False


def test_submitter_continuous_job_continues_loop_false_if_current_not_continuous():
    submitter = Submitter.__new__(Submitter)
    submitter._job_type = JobType.STANDARD

    dummy_info = MagicMock()
    dummy_info.job_type = JobType.CONTINUOUS
    dummy_info.job_state = NaiveState.FINISHED

    dummy_informer = MagicMock()
    dummy_informer.info = dummy_info

    assert submitter._continuous_job_continues_loop(dummy_informer) is False


def test_submitter_continues_loop_returns_true_if_valid_loop(tmp_path):
    submitter = Submitter.__new__(Submitter)
    submitter._input_dir = tmp_path

    dummy_informer = MagicMock()

    with (
        patch(
            "qq_lib.submit.submitter.get_info_file",
            return_value=tmp_path / "job.qqinfo",
        ),
        patch.object(Informer, "from_file", return_value=dummy_informer),
        patch.object(Submitter, "_loop_job_continues_loop", return_value=True),
        patch.object(Submitter, "_continuous_job_continues_loop", return_value=False),
    ):
        assert submitter.continues_loop() is True


def test_submitter_continues_loop_returns_true_if_valid_continuous(tmp_path):
    submitter = Submitter.__new__(Submitter)
    submitter._input_dir = tmp_path

    dummy_informer = MagicMock()

    with (
        patch(
            "qq_lib.submit.submitter.get_info_file",
            return_value=tmp_path / "job.qqinfo",
        ),
        patch.object(Informer, "from_file", return_value=dummy_informer),
        patch.object(Submitter, "_loop_job_continues_loop", return_value=False),
        patch.object(Submitter, "_continuous_job_continues_loop", return_value=True),
    ):
        assert submitter.continues_loop() is True


def test_submitter_continues_loop_returns_false_if_not_valid_loop_and_not_valid_continuous(
    tmp_path,
):
    submitter = Submitter.__new__(Submitter)
    submitter._input_dir = tmp_path

    dummy_informer = MagicMock()

    with (
        patch(
            "qq_lib.submit.submitter.get_info_file",
            return_value=tmp_path / "job.qqinfo",
        ),
        patch.object(Informer, "from_file", return_value=dummy_informer),
        patch.object(Submitter, "_loop_job_continues_loop", return_value=False),
        patch.object(Submitter, "_continuous_job_continues_loop", return_value=False),
    ):
        assert submitter.continues_loop() is False


def test_submitter_continues_loop_returns_false_on_qqerror(tmp_path):
    submitter = Submitter.__new__(Submitter)

    submitter._loop_info = MagicMock(current=2)
    submitter._input_dir = tmp_path

    with patch("qq_lib.submit.submitter.get_info_file", side_effect=QQError("error")):
        result = submitter.continues_loop()

    assert result is False


def test_submitter_submit_calls_all_steps_and_returns_job_id(tmp_path):
    submitter = Submitter.__new__(Submitter)
    submitter._batch_system = MagicMock()
    submitter._resources = Resources()
    submitter._queue = "default"
    submitter._account = None
    submitter._script = tmp_path / "script.sh"
    submitter._job_name = "job1"
    submitter._script_name = "script.sh"
    submitter._job_type = JobType.STANDARD
    submitter._input_dir = tmp_path
    submitter._loop_info = None
    submitter._exclude = []
    submitter._include = []
    submitter._ignore = []
    submitter._depend = []
    submitter._transfer_mode = [Success()]
    submitter._info_file = tmp_path / f"{submitter._job_name}.qqinfo"
    submitter._server = None
    submitter._interpreter = Interpreter.from_str("python3")
    submitter._resubmit_from = []
    env_vars = {CFG.env_vars.guard: "true"}

    with (
        patch.object(
            submitter, "_create_env_vars_dict", return_value=env_vars
        ) as mock_set_env,
        patch.object(
            submitter._batch_system, "job_submit", return_value="jobid123"
        ) as mock_job_submit,
        patch("qq_lib.submit.submitter.Info") as mock_info_class,
        patch("qq_lib.__version__", "1.0"),
    ):
        mock_info_instance = MagicMock()
        mock_info_class.return_value = mock_info_instance

        result = submitter.submit()

    mock_set_env.assert_called_once()
    mock_job_submit.assert_called_once_with(
        submitter._resources,
        submitter._queue,
        submitter._script,
        submitter._job_name,
        submitter._depend,
        env_vars,
        submitter._account,
        submitter._server,
        remote_host=None,
    )
    mock_info_class.assert_called_once()
    mock_info_instance.to_file.assert_called_once_with(submitter._info_file)
    assert result == "jobid123"


def test_submitter_submit(tmp_path):
    submitter = Submitter.__new__(Submitter)
    submitter._batch_system = MagicMock()
    submitter._resources = Resources()
    submitter._queue = "default"
    submitter._account = "fake-account"
    submitter._script = tmp_path / "script.sh"
    submitter._job_name = "job1"
    submitter._script_name = "script.sh"
    submitter._job_type = JobType.STANDARD
    submitter._input_dir = tmp_path
    submitter._loop_info = None
    submitter._exclude = ["exclude1"]
    submitter._include = ["include1"]
    submitter._ignore = ["ignore1"]
    submitter._depend = []
    submitter._transfer_mode = [Always()]
    submitter._server = "fake.server.com"
    submitter._info_file = tmp_path / f"{submitter._job_name}.qqinfo"
    submitter._interpreter = None
    submitter._resubmit_from = [WorkHost()]
    env_vars = {CFG.env_vars.guard: "true"}

    with (
        patch.object(
            submitter, "_create_env_vars_dict", return_value=env_vars
        ) as mock_set_env,
        patch.object(
            submitter._batch_system, "job_submit", return_value="jobid123"
        ) as mock_job_submit,
        patch("qq_lib.submit.submitter.Info") as mock_info_class,
        patch("qq_lib.__version__", "1.0"),
        patch("getpass.getuser", return_value="testuser"),
        patch("socket.getfqdn", return_value="host123"),
        patch("qq_lib.submit.submitter.datetime") as mock_datetime,
    ):
        mock_datetime.now.return_value = datetime(2025, 10, 14, 12, 0, 0)
        mock_info_instance = MagicMock()
        mock_info_class.return_value = mock_info_instance

        result = submitter.submit()

    mock_set_env.assert_called_once()
    mock_job_submit.assert_called_once_with(
        submitter._resources,
        submitter._queue,
        submitter._script,
        submitter._job_name,
        submitter._depend,
        env_vars,
        submitter._account,
        submitter._server,
        remote_host=None,
    )
    mock_info_class.assert_called_once_with(
        batch_system=submitter._batch_system,
        qq_version="1.0",
        username="testuser",
        job_id="jobid123",
        job_name=submitter._job_name,
        script_name=submitter._script_name,
        queue=submitter._queue,
        account=submitter._account,
        job_type=submitter._job_type,
        input_machine="host123",
        input_dir=submitter._input_dir,
        job_state=NaiveState.QUEUED,
        submission_time=datetime(2025, 10, 14, 12, 0, 0),
        stdout_file=str(Path(submitter._job_name).with_suffix(CFG.suffixes.stdout)),
        stderr_file=str(Path(submitter._job_name).with_suffix(CFG.suffixes.stderr)),
        resources=submitter._resources,
        loop_info=submitter._loop_info,
        excluded_files=submitter._exclude,
        included_files=submitter._include,
        ignored_files=submitter._ignore,
        depend=submitter._depend,
        transfer_mode=[Always()],
        server=submitter._server,
        interpreter=None,
        resubmit_from=[WorkHost()],
    )
    mock_info_instance.to_file.assert_called_once_with(submitter._info_file)
    assert result == "jobid123"


@pytest.mark.parametrize(
    "input_pattern, cycle, expected",
    [
        ("job%04d", 1, "job0001"),
        ("md%03d", 643, "md643"),
        ("job%2d", 5, "job 5"),
        ("^abc\\d+$", 7, ""),
        ("file\\d{3}", 123, ""),
    ],
)
def test_submitter_make_pattern(input_pattern, cycle, expected):
    result = Submitter._make_pattern(input_pattern, cycle)
    assert result == expected


@pytest.mark.parametrize("batch_system", [PBS, Slurm])
def test_submitter_expands_glob_patterns_in_exclude_and_include(
    tmp_path: Path, batch_system: AnyBatchClass
) -> None:
    input_dir = tmp_path / "job"
    input_dir.mkdir()

    script = input_dir / "run.sh"
    script.write_text(f"#!/usr/bin/env -S {CFG.binary_name} run\n")

    (input_dir / "topology.pdb").touch()
    (input_dir / "start.gro").touch()
    (input_dir / "old.log").touch()
    (input_dir / "run.log").touch()
    (input_dir / "notes.md").touch()
    (input_dir / "sub").mkdir()
    (input_dir / "sub" / "nested.log").touch()

    shared = tmp_path / "shared"
    shared.mkdir()
    (shared / "params.itp").touch()
    (shared / "forcefield.itp").touch()
    (shared / "readme.md").touch()

    submitter = Submitter(
        batch_system=batch_system,
        queue="default",
        account=None,
        script=script,
        job_type=JobType.STANDARD,
        resources=Resources(ncpus=1, mem="1gb", walltime="1:00:00"),
        exclude=["*.log", "notes.md", "missing.dat"],
        include=[str(shared / "*.itp"), "sub/nested.log"],
    )

    assert submitter._exclude == [
        input_dir / "old.log",
        input_dir / "run.log",
        input_dir / "notes.md",
        input_dir / "missing.dat",
    ]

    assert submitter._include == [
        shared / "forcefield.itp",
        shared / "params.itp",
        input_dir / "sub" / "nested.log",
    ]
