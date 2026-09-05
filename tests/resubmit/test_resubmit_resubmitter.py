# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.error import QQError
from qq_lib.properties.depend import Depend, DependType
from qq_lib.properties.resubmit_host import (
    ExplicitHost,
    InputHost,
    ResubmitHost,
    WorkHost,
)
from qq_lib.resubmit.resubmitter import Resubmitter


def test_resubmitter_advance_loop_cycle_increments_when_loop_info_exists():
    informer = MagicMock()
    informer.info.loop_info.current = 5

    Resubmitter._advance_loop_cycle(informer)

    assert informer.info.loop_info.current == 6


def test_resubmitter_advance_loop_cycle_does_nothing_when_no_loop_info():
    informer = MagicMock()
    informer.info.loop_info = None

    Resubmitter._advance_loop_cycle(informer)

    assert informer.info.loop_info is None


def test_resubmitter_build_submitter_creates_submitter_with_correct_params():
    informer = MagicMock()
    informer.info.job_id = "12345"
    input_dir = Path("/tmp/input")

    with patch("qq_lib.resubmit.resubmitter.Submitter") as mock_submitter_cls:
        Resubmitter._build_submitter(informer, input_dir)

    mock_submitter_cls.assert_called_once_with(
        batch_system=informer.batch_system,
        queue=informer.info.queue,
        account=informer.info.account,
        script=input_dir / informer.info.script_name,
        job_type=informer.info.job_type,
        resources=informer.info.resources,
        loop_info=informer.info.loop_info,
        exclude=[str(x) for x in informer.info.excluded_files],
        include=[str(x) for x in informer.info.included_files],
        ignore=[str(x) for x in informer.info.ignored_files],
        depend=[Depend(type=DependType.AFTER_SUCCESS, jobs=["12345"])],
        transfer_mode=informer.info.transfer_mode,
        server=informer.info.server,
        interpreter=informer.info.interpreter,
        resubmit_from=informer.info.resubmit_from,
    )


def test_resubmitter_build_submitter_depends_on_current_job():
    informer = MagicMock()
    informer.info.job_id = "99999"
    input_dir = Path("/tmp/input")

    with patch("qq_lib.resubmit.resubmitter.Submitter") as mock_submitter_cls:
        Resubmitter._build_submitter(informer, input_dir)

    call_kwargs = mock_submitter_cls.call_args.kwargs
    assert len(call_kwargs["depend"]) == 1
    assert call_kwargs["depend"][0].type == DependType.AFTER_SUCCESS
    assert call_kwargs["depend"][0].jobs == ["99999"]


def test_resubmitter_build_submitter_constructs_script_path():
    informer = MagicMock()
    informer.info.script_name = "run.sh"
    input_dir = Path("/home/user/jobs")

    with patch("qq_lib.resubmit.resubmitter.Submitter") as mock_submitter_cls:
        Resubmitter._build_submitter(informer, input_dir)

    call_kwargs = mock_submitter_cls.call_args.kwargs
    assert call_kwargs["script"] == Path("/home/user/jobs/run.sh")


def test_resubmitter_try_resubmit_returns_job_id_on_success():
    submitter = MagicMock()
    informer = MagicMock()
    informer.info.main_node = "main-node-01"
    informer.info.input_machine = "submit-node"
    hosts: list[ResubmitHost] = [InputHost()]

    with patch("qq_lib.resubmit.resubmitter.Retryer") as mock_retryer_cls:
        mock_retryer_cls.return_value.run.return_value = "67890"
        result = Resubmitter._try_resubmit(submitter, informer, hosts)

    assert result == "67890"


def test_resubmitter_try_resubmit_raises_when_main_node_not_defined():
    submitter = MagicMock()
    informer = MagicMock()
    informer.info.main_node = ""
    hosts: list[ResubmitHost] = [InputHost()]

    with pytest.raises(QQError, match="The 'main_node' of the job is not defined"):
        Resubmitter._try_resubmit(submitter, informer, hosts)


def test_resubmitter_try_resubmit_raises_when_hosts_empty():
    submitter = MagicMock()
    informer = MagicMock()
    informer.info.main_node = "node01"
    hosts: list[ResubmitHost] = []

    with pytest.raises(QQError, match="No resubmission hosts defined"):
        Resubmitter._try_resubmit(submitter, informer, hosts)


def test_resubmitter_try_resubmit_tries_next_host_on_failure():
    submitter = MagicMock()
    informer = MagicMock()
    informer.info.main_node = "main-node-01"
    informer.info.input_machine = "submit-node"
    hosts: list[ResubmitHost] = [InputHost(), ExplicitHost("fallback-node")]

    with patch("qq_lib.resubmit.resubmitter.Retryer") as mock_retryer_cls:
        mock_retryer_cls.return_value.run.side_effect = [
            RuntimeError("connection refused"),
            "99999",
        ]
        result = Resubmitter._try_resubmit(submitter, informer, hosts)

    assert result == "99999"
    assert mock_retryer_cls.return_value.run.call_count == 2


def test_resubmitter_try_resubmit_raises_when_all_hosts_fail():
    submitter = MagicMock()
    informer = MagicMock()
    informer.info.main_node = "main-node-01"
    informer.info.input_machine = "submit-node"
    hosts: list[ResubmitHost] = [InputHost(), WorkHost()]

    with (
        patch("qq_lib.resubmit.resubmitter.Retryer") as mock_retryer_cls,
        pytest.raises(QQError, match="Could not resubmit the job"),
    ):
        mock_retryer_cls.return_value.run.side_effect = RuntimeError("failed")
        Resubmitter._try_resubmit(submitter, informer, hosts)


def test_resubmitter_try_resubmit_resolves_hostnames_correctly():
    submitter = MagicMock()
    informer = MagicMock()
    informer.info.main_node = "compute-01"
    informer.info.input_machine = "login-01"
    hosts: list[ResubmitHost] = [InputHost(), WorkHost(), ExplicitHost("explicit-01")]

    with patch("qq_lib.resubmit.resubmitter.Retryer") as mock_retryer_cls:
        mock_retryer_cls.return_value.run.side_effect = [
            RuntimeError("fail"),
            RuntimeError("fail"),
            "11111",
        ]
        Resubmitter._try_resubmit(submitter, informer, hosts)

    calls = mock_retryer_cls.call_args_list
    assert calls[0].kwargs["remote"] == "login-01"
    assert calls[1].kwargs["remote"] == "compute-01"
    assert calls[2].kwargs["remote"] == "explicit-01"


def test_resubmitter_try_resubmit_uses_configured_retry_params():
    submitter = MagicMock()
    informer = MagicMock()
    informer.info.main_node = "main-node-01"
    informer.info.input_machine = "submit-node"
    hosts: list[ResubmitHost] = [InputHost()]

    with (
        patch("qq_lib.resubmit.resubmitter.Retryer") as mock_retryer_cls,
        patch("qq_lib.resubmit.resubmitter.CFG") as mock_cfg,
    ):
        mock_cfg.resubmitter.retry_tries = 5
        mock_cfg.resubmitter.retry_wait = 30
        mock_retryer_cls.return_value.run.return_value = "12345"

        Resubmitter._try_resubmit(submitter, informer, hosts)

    mock_retryer_cls.assert_called_once_with(
        submitter.submit,
        remote="submit-node",
        max_tries=5,
        wait_seconds=30,
    )


def test_try_resubmit_returns_on_first_success():
    submitter = MagicMock()
    informer = MagicMock()
    informer.info.main_node = "main-node-01"
    informer.info.input_machine = "submit-node"
    hosts: list[ResubmitHost] = [InputHost(), WorkHost(), ExplicitHost("node-03")]

    with patch("qq_lib.resubmit.resubmitter.Retryer") as mock_retryer_cls:
        mock_retryer_cls.return_value.run.return_value = "11111"
        result = Resubmitter._try_resubmit(submitter, informer, hosts)

    assert result == "11111"
    assert mock_retryer_cls.return_value.run.call_count == 1


def test_resubmit_calls_methods_in_order():
    resubmitter = Resubmitter.__new__(Resubmitter)
    resubmitter._info_file = Path("/tmp/input/job.info")

    informer = MagicMock()
    informer.info.resubmit_from = [InputHost()]
    resubmitter.get_informer = MagicMock(return_value=informer)

    with (
        patch.object(Resubmitter, "_advance_loop_cycle") as mock_advance,
        patch.object(Resubmitter, "_build_submitter") as mock_build,
        patch.object(Resubmitter, "_try_resubmit", return_value="12345") as mock_try,
    ):
        result = resubmitter.resubmit()

    assert result == "12345"
    mock_advance.assert_called_once_with(informer)
    mock_build.assert_called_once_with(informer, Path("/tmp/input"))
    mock_try.assert_called_once_with(mock_build.return_value, informer, [InputHost()])
