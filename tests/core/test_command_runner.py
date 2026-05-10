# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.command_runner import CommandRunner
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.error_handlers import handle_not_suitable_error
from qq_lib.info import Informer

if TYPE_CHECKING:
    from collections.abc import Callable


def test_command_runner_build_targets_from_job_ids():
    runner = CommandRunner(("111", "222"), lambda _: None, MagicMock(), n_threads=1)

    with patch("qq_lib.core.command_runner.Informer") as mock_informer:
        mock_informer.from_job_id.side_effect = lambda j: MagicMock(
            name=f"informer_{j}"
        )
        targets = runner._build_targets()

    assert len(targets) == 2


def test_command_runner_build_targets_from_info_files(tmp_path):
    info1 = tmp_path / "job1.qqinfo"
    info2 = tmp_path / "job2.qqinfo"
    info1.touch()
    info2.touch()

    runner = CommandRunner(
        (), lambda _: None, MagicMock(), n_threads=1, directory=tmp_path
    )

    with (
        patch("qq_lib.core.command_runner.get_info_files", return_value=[info1, info2]),
        patch("qq_lib.core.command_runner.Informer"),
    ):
        targets = runner._build_targets()

    assert len(targets) == 2


def test_command_runner_build_targets_raises_when_no_info_files():
    runner = CommandRunner((), lambda _: None, MagicMock(), n_threads=1)

    with (
        patch("qq_lib.core.command_runner.get_info_files", return_value=[]),
        pytest.raises(QQError, match="No qq job info file found"),
    ):
        runner._build_targets()


def test_command_runner_build_targets_resolves_informer_from_job_id():
    runner = CommandRunner(("12345",), lambda _: None, MagicMock(), n_threads=1)

    with patch("qq_lib.core.command_runner.Informer") as mock_informer:
        mock_informer.from_job_id.return_value = MagicMock()
        mock_informer.from_job_id.return_value.load_batch_info = MagicMock()
        targets = runner._build_targets()
        targets[0]()

    mock_informer.from_job_id.assert_called_once_with("12345")


def test_command_runner_build_targets_resolves_informer_from_file(tmp_path):
    info_file = tmp_path / "job.qqinfo"
    info_file.touch()

    runner = CommandRunner(
        (), lambda _: None, MagicMock(), n_threads=1, directory=tmp_path
    )

    with (
        patch("qq_lib.core.command_runner.get_info_files", return_value=[info_file]),
        patch("qq_lib.core.command_runner.Informer") as mock_informer,
    ):
        mock_informer.from_file.return_value = MagicMock()
        mock_informer.from_file.return_value.load_batch_info = MagicMock()
        targets = runner._build_targets()
        targets[0]()

    mock_informer.from_file.assert_called_once_with(info_file)


def test_command_runner_build_targets_calls_load_batch_info():
    runner = CommandRunner(("111",), lambda _: None, MagicMock(), n_threads=1)

    with patch("qq_lib.core.command_runner.Informer") as mock_informer:
        informer = MagicMock()
        mock_informer.from_job_id.return_value = informer
        targets = runner._build_targets()
        targets[0]()

    informer.load_batch_info.assert_called_once()


def test_command_runner_build_targets_uses_specified_directory(tmp_path):
    runner = CommandRunner(
        (), lambda _: None, MagicMock(), n_threads=1, directory=tmp_path
    )

    with (
        patch("qq_lib.core.command_runner.get_info_files", return_value=[]) as mock_get,
        pytest.raises(QQError),
    ):
        runner._build_targets()

    mock_get.assert_called_once_with(tmp_path)


def test_command_runner_execute_calls_callback_with_args():
    callback = MagicMock()
    runner = CommandRunner(
        ("111",), callback, MagicMock(), "arg1", "arg2", n_threads=1, kw="val"
    )

    informer = MagicMock()
    runner._execute(informer)

    callback.assert_called_once_with(informer, "arg1", "arg2", kw="val")


def test_command_runner_execute_passes_unregistered_exception():
    callback = MagicMock(side_effect=RuntimeError("boom"))
    runner = CommandRunner(("111",), callback, MagicMock(), n_threads=1)

    with pytest.raises(RuntimeError, match="boom"):
        runner._execute(MagicMock())


def test_command_runner_execute_handles_registered_exception():
    callback = MagicMock(side_effect=QQError("fail"))
    handler = MagicMock()
    runner = CommandRunner(("111",), callback, MagicMock(), n_threads=1)
    runner.on_exception(QQError, handler)

    runner._execute(MagicMock())

    handler.assert_called_once()
    assert isinstance(handler.call_args[0][0], QQError)
    assert handler.call_args[0][1] is runner


def test_command_runner_handle_error_records_in_encountered_errors():
    runner = CommandRunner(("111",), lambda _: None, MagicMock(), n_threads=1)
    runner.current_iteration = 3
    error = QQError("fail")

    runner.on_exception(QQError, MagicMock())
    runner._handle_error(error)

    assert runner.encountered_errors[3] is error


def test_command_runner_handle_error_calls_registered_handler():
    handler = MagicMock()
    runner = CommandRunner(("111",), lambda _: None, MagicMock(), n_threads=1)
    runner.on_exception(QQError, handler)
    error = QQError("fail")

    runner._handle_error(error)

    handler.assert_called_once_with(error, runner)


def test_command_runner_handle_error_reraises_unregistered_exception():
    runner = CommandRunner(("111",), lambda _: None, MagicMock(), n_threads=1)
    error = RuntimeError("unexpected")

    with pytest.raises(RuntimeError, match="unexpected"):
        runner._handle_error(error)

    assert runner.encountered_errors[0] is error


def test_command_runner_on_exception_returns_self_for_chaining():
    runner = CommandRunner(("111",), lambda _: None, MagicMock(), n_threads=1)

    result = runner.on_exception(QQError, MagicMock())

    assert result is runner


def test_command_runner_on_exception_registers_multiple_handlers():
    runner = CommandRunner(("111",), lambda _: None, MagicMock(), n_threads=1)
    handler1 = MagicMock()
    handler2 = MagicMock()

    runner.on_exception(QQError, handler1).on_exception(QQNotSuitableError, handler2)

    runner.current_iteration = 0
    runner._handle_error(QQError("e1"))
    handler1.assert_called_once()

    runner._handle_error(QQNotSuitableError("e2"))
    handler2.assert_called_once()


def test_command_runner_run_pipeline_executes_callback_for_each_target():
    results = []

    def callback(i, *a: Any, **kw: Any):
        _ = a, kw
        return results.append(i)

    runner = CommandRunner(("111", "222"), callback, MagicMock(), n_threads=1)

    informer1 = MagicMock(spec=Informer)
    informer2 = MagicMock(spec=Informer)
    targets: list[Callable[[], Informer]] = [lambda: informer1, lambda: informer2]

    runner._run_pipeline(targets)

    assert results == [informer1, informer2]


def test_command_runner_run_pipeline_preserves_order_with_multiple_threads():
    import time

    execution_order = []

    def slow_target():
        time.sleep(0.2)
        informer = MagicMock(spec=Informer)
        informer.name = "slow"
        return informer

    def fast_target():
        informer = MagicMock(spec=Informer)
        informer.name = "fast"
        return informer

    def callback(i, *a: Any, **kw: Any):
        _ = a, kw
        return execution_order.append(i.name)

    runner = CommandRunner(("111", "222"), callback, MagicMock(), n_threads=2)
    runner._run_pipeline([slow_target, fast_target])

    assert execution_order == ["slow", "fast"]


def test_command_runner_run_pipeline_handles_preparation_failure():
    handler = MagicMock()
    callback = MagicMock()
    runner = CommandRunner(("111",), callback, MagicMock(), n_threads=1)
    runner.on_exception(QQError, handler)

    def failing_target():
        raise QQError("resolve failed")

    runner._run_pipeline([failing_target])

    handler.assert_called_once()
    callback.assert_not_called()


def test_command_runner_run_pipeline_continues_after_preparation_failure():
    results = []
    handler = MagicMock()

    def callback(i, *a, **kw):
        _ = a, kw
        return results.append(i)

    runner = CommandRunner(("111", "222"), callback, MagicMock(), n_threads=1)
    runner.on_exception(QQError, handler)

    informer = MagicMock(spec=Informer)

    def failing_target():
        raise QQError("resolve failed")

    runner._run_pipeline([failing_target, lambda: informer])

    handler.assert_called_once()
    assert results == [informer]


def test_command_runner_run_pipeline_continues_after_callback_failure():
    call_count = 0

    def callback(informer: Informer, *args: Any, **kwargs: Any):
        _ = informer, args, kwargs
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise QQError("callback failed")

    handler = MagicMock()
    runner = CommandRunner(("111", "222"), callback, MagicMock(), n_threads=1)
    runner.on_exception(QQError, handler)

    def make_informer():
        return MagicMock(spec=Informer)

    runner._run_pipeline([make_informer, make_informer])

    handler.assert_called_once()
    assert call_count == 2


def test_command_runner_run_pipeline_sets_n_jobs():
    def make_informer():
        return MagicMock(spec=Informer)

    runner = CommandRunner(("1", "2", "3"), lambda _: None, MagicMock(), n_threads=1)
    runner._run_pipeline([make_informer, make_informer, make_informer])

    assert runner.n_jobs == 3


def test_command_runner_run_pipeline_tracks_current_iteration():
    iterations = []

    def handler(e: Exception, r: CommandRunner):
        _ = e
        return iterations.append(r.current_iteration)

    runner = CommandRunner(("1", "2", "3"), lambda _: None, MagicMock(), n_threads=1)
    runner.on_exception(QQError, handler)

    def make_informer():
        return MagicMock(spec=Informer)

    def fail():
        raise QQError("fail")

    runner._run_pipeline([make_informer, fail, make_informer])

    assert iterations == [2]


def test_command_runner_run_pipeline_records_all_errors():
    handler = MagicMock()
    runner = CommandRunner(("1", "2", "3"), lambda _: None, MagicMock(), n_threads=1)
    runner.on_exception(QQError, handler)

    def make_informer():
        return MagicMock(spec=Informer)

    def fail():
        raise QQError("fail")

    runner._run_pipeline([fail, make_informer, fail])

    assert len(runner.encountered_errors) == 2
    assert 1 in runner.encountered_errors
    assert 3 in runner.encountered_errors


def test_command_runner_run_pipeline_reraises_unhandled_preparation_error():
    runner = CommandRunner(("111",), lambda _: None, MagicMock(), n_threads=1)

    def failing_target():
        raise RuntimeError("unexpected")

    with pytest.raises(RuntimeError, match="unexpected"):
        runner._run_pipeline([failing_target])


def test_command_runner_run_exits_zero_on_success():
    def make_informer():
        return MagicMock(spec=Informer)

    runner = CommandRunner(("111",), lambda _: None, MagicMock(), n_threads=1)

    with (
        patch.object(runner, "_build_targets", return_value=[make_informer]),
        patch.object(runner, "_run_pipeline"),
    ):
        try:
            runner.run()
            assert False, "Expected SystemExit"
        except SystemExit as e:
            assert e.code == 0


def test_command_runner_run_exits_default_on_qq_error():
    mock_logger = MagicMock()
    runner = CommandRunner(("111",), lambda _: None, mock_logger, n_threads=1)

    with patch.object(runner, "_build_targets", side_effect=QQError("fail")):
        try:
            runner.run()
            assert False, "Expected SystemExit"
        except SystemExit as e:
            assert e.code == CFG.exit_codes.default

    mock_logger.error.assert_called_once()


def test_command_runner_run_exits_unexpected_on_generic_exception():
    mock_logger = MagicMock()
    runner = CommandRunner(("111",), lambda _: None, mock_logger, n_threads=1)

    with patch.object(runner, "_build_targets", side_effect=RuntimeError("boom")):
        try:
            runner.run()
            assert False, "Expected SystemExit"
        except SystemExit as e:
            assert e.code == CFG.exit_codes.unexpected_error

    mock_logger.critical.assert_called_once()


def test_command_runnerrun_with_not_suitable_handler_single_job():
    callback = MagicMock(side_effect=QQNotSuitableError("not suitable"))
    mock_logger = MagicMock()

    def make_informer():
        return MagicMock(spec=Informer)

    runner = CommandRunner(("111",), callback, mock_logger, n_threads=1)
    runner.on_exception(QQNotSuitableError, handle_not_suitable_error)

    with (
        patch.object(runner, "_build_targets", return_value=[make_informer]),
    ):
        try:
            runner.run()
            assert False, "Expected SystemExit"
        except SystemExit as e:
            assert e.code == CFG.exit_codes.default


def test_command_runner_run_preserves_order_end_to_end():
    import time

    execution_order = []

    def callback(informer, *args, **kwargs):
        _ = args, kwargs
        execution_order.append(informer.job_id)

    runner = CommandRunner(("111", "222", "333"), callback, MagicMock(), n_threads=3)

    def make_target(job_id, delay):
        def target():
            time.sleep(delay)
            informer = MagicMock(spec=Informer)
            informer.job_id = job_id
            return informer

        return target

    with patch.object(
        runner,
        "_build_targets",
        return_value=[
            make_target("111", 0.2),
            make_target("222", 0.1),
            make_target("333", 0.0),
        ],
    ):
        try:
            runner.run()
            assert False, "Expected SystemExit"
        except SystemExit as e:
            assert e.code == 0

    assert execution_order == ["111", "222", "333"]
