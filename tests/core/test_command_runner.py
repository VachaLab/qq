# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

import os
import time
from collections.abc import Callable
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.command_runner import CommandRunner
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.error_handlers import handle_not_suitable_error
from qq_lib.info import Informer


def make_runner(
    job_ids: tuple[str, ...] = (),
    directories: tuple[Path, ...] = (),
    all_jobs: bool = False,
    server: str | None = None,
    callback: Callable | None = None,
    n_threads: int = 1,
    recursive: bool = False,
    logger: MagicMock | None = None,
) -> CommandRunner:
    """Construct a CommandRunner with a patched batch system."""
    return CommandRunner(
        job_ids=job_ids,
        directories=directories,
        all=all_jobs,
        server=server,
        callback=callback or MagicMock(),
        logger=logger or MagicMock(),
        recursive=recursive,
        n_threads=n_threads,
    )


def make_mock_batch_job(job_id: str = "111") -> MagicMock:
    """Create a mock BatchJobInterface with a given job ID."""
    batch_job = MagicMock()
    batch_job.get_id.return_value = job_id
    return batch_job


def make_informer(job_id: str = "111") -> MagicMock:
    """Create a mock Informer."""
    informer = MagicMock(spec=Informer)
    informer.job_id = job_id
    return informer


def test_command_runner_build_targets_from_job_ids_returns_correct_count():
    with patch("qq_lib.core.command_runner.BatchInterface") as mock_bi:
        batch_jobs = [make_mock_batch_job("111"), make_mock_batch_job("222")]
        mock_bi.from_env_var_or_guess.return_value.get_batch_jobs_from_ids.return_value = batch_jobs
        runner = make_runner(job_ids=("111", "222"))
        targets = runner._build_targets()

    assert len(targets) == 2


def test_command_runner_build_targets_from_job_ids_uses_from_batch_job():
    with patch("qq_lib.core.command_runner.BatchInterface") as mock_bi:
        batch_job = make_mock_batch_job("111")
        mock_bi.from_env_var_or_guess.return_value.get_batch_jobs_from_ids.return_value = [
            batch_job
        ]
        runner = make_runner(job_ids=("111",))

        with patch("qq_lib.core.command_runner.Informer") as mock_informer:
            targets = runner._build_targets()
            targets[0]()

        mock_informer.from_batch_job.assert_called_once_with(batch_job)


def test_command_runner_build_targets_from_files_returns_correct_count(tmp_path):
    info1 = tmp_path / "job1.qqinfo"
    info2 = tmp_path / "job2.qqinfo"
    info1.touch()
    info2.touch()

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(directories=(tmp_path,))

    with (
        patch("qq_lib.core.command_runner.get_info_files", return_value=[info1, info2]),
        patch("qq_lib.core.command_runner.Informer"),
    ):
        targets = runner._build_targets()

    assert len(targets) == 2


def test_command_runner_build_targets_from_files_calls_from_file(tmp_path):
    info_file = tmp_path / "job.qqinfo"
    info_file.touch()

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(directories=(tmp_path,))

    with (
        patch("qq_lib.core.command_runner.get_info_files", return_value=[info_file]),
        patch("qq_lib.core.command_runner.Informer") as mock_informer,
    ):
        mock_informer.from_file.return_value = MagicMock()
        targets = runner._build_targets()
        targets[0]()

    mock_informer.from_file.assert_called_once_with(info_file)


def test_command_runner_build_targets_from_files_calls_load_batch_info(tmp_path):
    info_file = tmp_path / "job.qqinfo"
    info_file.touch()

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(directories=(tmp_path,))

    with (
        patch("qq_lib.core.command_runner.get_info_files", return_value=[info_file]),
        patch("qq_lib.core.command_runner.Informer") as mock_informer,
    ):
        informer = MagicMock()
        mock_informer.from_file.return_value = informer
        targets = runner._build_targets()
        targets[0]()

    informer.load_batch_info.assert_called_once()


def test_command_runner_build_targets_uses_cwd_when_no_jobs_or_directories():
    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner()

    with (
        patch("qq_lib.core.command_runner.get_info_files", return_value=[]) as mock_get,
        pytest.raises(QQError),
    ):
        runner._build_targets()

    mock_get.assert_called_once_with(Path.cwd())


def test_command_runner_build_targets_uses_specified_directories(tmp_path):
    dir1 = tmp_path / "a"
    dir2 = tmp_path / "b"
    dir1.mkdir()
    dir2.mkdir()

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(directories=(dir1, dir2))

    with (
        patch("qq_lib.core.command_runner.get_info_files", return_value=[]) as mock_get,
        pytest.raises(QQError),
    ):
        runner._build_targets()

    assert mock_get.call_count == 2
    mock_get.assert_any_call(dir1)
    mock_get.assert_any_call(dir2)


def test_command_runner_build_targets_all_queries_unfinished_jobs():
    with patch("qq_lib.core.command_runner.BatchInterface") as mock_bi:
        batch_jobs = [make_mock_batch_job("111"), make_mock_batch_job("222")]
        mock_bi.from_env_var_or_guess.return_value.get_unfinished_batch_jobs.return_value = batch_jobs
        mock_bi.from_env_var_or_guess.return_value.get_batch_jobs_from_ids.return_value = []
        runner = make_runner(all_jobs=True)

        with patch("qq_lib.core.command_runner.Informer"):
            targets = runner._build_targets()

    assert len(targets) == 2


def test_command_runner_build_targets_all_and_job_ids_combined():
    with patch("qq_lib.core.command_runner.BatchInterface") as mock_bi:
        id_jobs = [make_mock_batch_job("111")]
        all_jobs = [make_mock_batch_job("222"), make_mock_batch_job("333")]
        mock_bi.from_env_var_or_guess.return_value.get_batch_jobs_from_ids.return_value = id_jobs
        mock_bi.from_env_var_or_guess.return_value.get_unfinished_batch_jobs.return_value = all_jobs
        runner = make_runner(job_ids=("111",), all_jobs=True)

        with patch("qq_lib.core.command_runner.Informer"):
            targets = runner._build_targets()

    assert len(targets) == 3


def test_command_runner_build_targets_all_and_directories_combined(tmp_path):
    info_file = tmp_path / "job.qqinfo"
    info_file.touch()

    with patch("qq_lib.core.command_runner.BatchInterface") as mock_bi:
        all_jobs = [make_mock_batch_job("111")]
        mock_bi.from_env_var_or_guess.return_value.get_batch_jobs_from_ids.return_value = []
        mock_bi.from_env_var_or_guess.return_value.get_unfinished_batch_jobs.return_value = all_jobs
        runner = make_runner(all_jobs=True, directories=(tmp_path,))

        with (
            patch(
                "qq_lib.core.command_runner.get_info_files", return_value=[info_file]
            ),
            patch("qq_lib.core.command_runner.Informer"),
        ):
            targets = runner._build_targets()

    assert len(targets) == 2


def test_command_runner_build_targets_raises_when_no_info_files():
    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner()

    with (
        patch("qq_lib.core.command_runner.get_info_files", return_value=[]),
        pytest.raises(QQError, match="No qq job info file found"),
    ):
        runner._build_targets()


def test_command_runner_build_targets_raises_when_no_jobs_found():
    with patch("qq_lib.core.command_runner.BatchInterface") as mock_bi:
        mock_bi.from_env_var_or_guess.return_value.get_batch_jobs_from_ids.return_value = []
        runner = make_runner(job_ids=("111",))

        with pytest.raises(QQError, match="No jobs found"):
            runner._build_targets()


def test_command_runner_build_targets_server_ignored_without_all_logs_warning():
    with patch("qq_lib.core.command_runner.BatchInterface") as mock_bi:
        mock_bi.from_env_var_or_guess.return_value.get_batch_jobs_from_ids.return_value = [
            make_mock_batch_job("111")
        ]
        mock_logger = MagicMock()
        runner = CommandRunner(
            job_ids=("111",),
            directories=(),
            all=False,
            server="server",
            callback=MagicMock(),
            logger=mock_logger,
            n_threads=1,
        )

        with patch("qq_lib.core.command_runner.Informer"):
            runner._build_targets()

    mock_logger.warning.assert_called_once()


def test_command_runner_execute_calls_callback_with_args():
    with patch("qq_lib.core.command_runner.BatchInterface"):
        callback = MagicMock()
        runner = make_runner(callback=callback)

    informer = make_informer()
    runner._execute(informer)

    callback.assert_called_once_with(informer)


def test_command_runner_execute_calls_callback_with_extra_args():
    with patch("qq_lib.core.command_runner.BatchInterface"):
        callback = MagicMock()
        runner = CommandRunner(
            (),
            (),
            False,
            None,
            callback,
            MagicMock(),
            "arg1",
            "arg2",
            n_threads=1,
            kw="val",
        )

    informer = make_informer()
    runner._execute(informer)

    callback.assert_called_once_with(informer, "arg1", "arg2", kw="val")


def test_command_runner_execute_reraises_unregistered_exception():
    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(callback=MagicMock(side_effect=RuntimeError("boom")))

    with pytest.raises(RuntimeError, match="boom"):
        runner._execute(make_informer())


def test_command_runner_execute_handles_registered_exception():
    with patch("qq_lib.core.command_runner.BatchInterface"):
        callback = MagicMock(side_effect=QQError("fail"))
        handler = MagicMock()
        runner = make_runner(callback=callback)
        runner.on_exception(QQError, handler)

    runner._execute(make_informer())

    handler.assert_called_once()
    assert isinstance(handler.call_args[0][0], QQError)
    assert handler.call_args[0][1] is runner


def test_command_runner_handle_error_records_in_encountered_errors():
    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner()

    runner.current_iteration = 3
    error = QQError("fail")
    runner.on_exception(QQError, MagicMock())
    runner._handle_error(error)

    assert runner.encountered_errors[3] is error


def test_command_runner_handle_error_calls_registered_handler():
    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner()

    handler = MagicMock()
    runner.on_exception(QQError, handler)
    error = QQError("fail")
    runner._handle_error(error)

    handler.assert_called_once_with(error, runner)


def test_command_runner_handle_error_reraises_unregistered_exception():
    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner()

    error = RuntimeError("unexpected")

    with pytest.raises(RuntimeError, match="unexpected"):
        runner._handle_error(error)

    assert runner.encountered_errors[0] is error


def test_command_runner_on_exception_returns_self_for_chaining():
    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner()

    result = runner.on_exception(QQError, MagicMock())

    assert result is runner


def test_command_runner_on_exception_registers_multiple_handlers():
    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner()

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

    def callback(informer, *args, **kwargs):
        _ = args, kwargs
        results.append(informer)

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(callback=callback)

    informer1 = make_informer("111")
    informer2 = make_informer("222")
    runner._run_pipeline([lambda: informer1, lambda: informer2])

    assert results == [informer1, informer2]


def test_command_runner_run_pipeline_preserves_order_with_multiple_threads():
    execution_order = []

    def slow_target():
        time.sleep(0.2)
        return make_informer("slow")

    def fast_target():
        return make_informer("fast")

    def callback(informer, *args, **kwargs):
        _ = args, kwargs
        execution_order.append(informer.job_id)

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(callback=callback, n_threads=2)

    runner._run_pipeline([slow_target, fast_target])

    assert execution_order == ["slow", "fast"]


def test_command_runner_run_pipeline_handles_preparation_failure():
    handler = MagicMock()
    callback = MagicMock()

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(callback=callback)

    runner.on_exception(QQError, handler)

    def failing_target():
        raise QQError("resolve failed")

    runner._run_pipeline([failing_target])

    handler.assert_called_once()
    callback.assert_not_called()


def test_command_runner_run_pipeline_continues_after_preparation_failure():
    results = []
    handler = MagicMock()

    def callback(informer, *args, **kwargs):
        _ = args, kwargs
        results.append(informer)

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(callback=callback)

    runner.on_exception(QQError, handler)
    informer = make_informer()

    def failing_target():
        raise QQError("resolve failed")

    runner._run_pipeline([failing_target, lambda: informer])

    handler.assert_called_once()
    assert results == [informer]


def test_command_runner_run_pipeline_continues_after_callback_failure():
    call_count = 0

    def callback(informer, *args, **kwargs):
        _ = informer, args, kwargs
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise QQError("callback failed")

    handler = MagicMock()

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(callback=callback)

    runner.on_exception(QQError, handler)
    runner._run_pipeline([make_informer, make_informer])

    handler.assert_called_once()
    assert call_count == 2


def test_command_runner_run_pipeline_sets_n_jobs():
    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner()

    runner._run_pipeline([make_informer, make_informer, make_informer])

    assert runner.n_jobs == 3


def test_command_runner_run_pipeline_tracks_current_iteration():
    iterations = []

    def handler(e, r):
        _ = e
        iterations.append(r.current_iteration)

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner()

    runner.on_exception(QQError, handler)

    def fail():
        raise QQError("fail")

    runner._run_pipeline([make_informer, fail, make_informer])

    assert iterations == [2]


def test_command_runner_run_pipeline_records_all_errors():
    handler = MagicMock()

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner()

    runner.on_exception(QQError, handler)

    def fail():
        raise QQError("fail")

    runner._run_pipeline([fail, make_informer, fail])

    assert len(runner.encountered_errors) == 2
    assert 1 in runner.encountered_errors
    assert 3 in runner.encountered_errors


def test_command_runner_run_pipeline_reraises_unhandled_preparation_error():
    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner()

    def failing_target():
        raise RuntimeError("unexpected")

    with pytest.raises(RuntimeError, match="unexpected"):
        runner._run_pipeline([failing_target])


def test_command_runner_run_exits_zero_on_success():
    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner()

    with (
        patch.object(runner, "_build_targets", return_value=[make_informer]),
        patch.object(runner, "_run_pipeline"),
        pytest.raises(SystemExit) as exc_info,
    ):
        runner.run()

    assert exc_info.value.code == 0


def test_command_runner_run_exits_default_on_qq_error():
    mock_logger = MagicMock()

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = CommandRunner(
            job_ids=(),
            directories=(),
            all=False,
            server=None,
            callback=MagicMock(),
            logger=mock_logger,
            n_threads=1,
        )

    with (
        patch.object(runner, "_build_targets", side_effect=QQError("fail")),
        pytest.raises(SystemExit) as exc_info,
    ):
        runner.run()

    assert exc_info.value.code == CFG.exit_codes.default
    mock_logger.error.assert_called_once()


def test_command_runner_run_exits_unexpected_on_generic_exception():
    mock_logger = MagicMock()

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = CommandRunner(
            job_ids=(),
            directories=(),
            all=False,
            server=None,
            callback=MagicMock(),
            logger=mock_logger,
            n_threads=1,
        )

    with (
        patch.object(runner, "_build_targets", side_effect=RuntimeError("boom")),
        pytest.raises(SystemExit) as exc_info,
    ):
        runner.run()

    assert exc_info.value.code == CFG.exit_codes.unexpected_error
    mock_logger.critical.assert_called_once()


def test_command_runner_run_with_not_suitable_handler_single_job():
    callback = MagicMock(side_effect=QQNotSuitableError("not suitable"))

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(callback=callback)

    runner.on_exception(QQNotSuitableError, handle_not_suitable_error)

    with (
        patch.object(runner, "_build_targets", return_value=[make_informer]),
        pytest.raises(SystemExit) as exc_info,
    ):
        runner.run()

    assert exc_info.value.code == CFG.exit_codes.default


def test_command_runner_run_preserves_order_end_to_end():
    execution_order = []

    def callback(informer, *args, **kwargs):
        _ = args, kwargs
        execution_order.append(informer.job_id)

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(callback=callback, n_threads=3)

    def make_target(job_id, delay):
        def target():
            time.sleep(delay)
            return make_informer(job_id)

        return target

    with (
        patch.object(
            runner,
            "_build_targets",
            return_value=[
                make_target("111", 0.2),
                make_target("222", 0.1),
                make_target("333", 0.0),
            ],
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        runner.run()

    assert exc_info.value.code == 0
    assert execution_order == ["111", "222", "333"]


def fake_get_info_files(directory: Path) -> list[Path]:
    """Stand-in for get_info_files: every *.qqinfo file directly in `directory`."""
    return sorted(directory.glob("*.qqinfo"))


def make_job(directory: Path, name: str = "job") -> Path:
    """Create a qq job directory with a single info file and return the file."""
    directory.mkdir(parents=True, exist_ok=True)
    info_file = directory / f"{name}.qqinfo"
    info_file.touch()
    return info_file


@pytest.fixture
def fake_info_files():
    with patch(
        "qq_lib.core.command_runner.get_info_files", side_effect=fake_get_info_files
    ) as mock_get:
        yield mock_get


def collect(directories: list[Path], recursive: bool, logger=None) -> list[Path]:
    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(
            directories=tuple(directories), recursive=recursive, logger=logger
        )
    return runner._collect_info_files(directories)


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_collect_non_recursive_ignores_subdirectories(tmp_path):
    make_job(tmp_path / "sub")

    assert collect([tmp_path], recursive=False) == []


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_collect_recursive_finds_jobs_in_subdirectories(tmp_path):
    job1 = make_job(tmp_path / "a")
    job2 = make_job(tmp_path / "b" / "c" / "d")

    assert collect([tmp_path], recursive=True) == [job1, job2]


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_collect_recursive_finds_job_in_root(tmp_path):
    job = make_job(tmp_path)

    assert collect([tmp_path], recursive=True) == [job]


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_collect_recursive_does_not_descend_into_job_directory(tmp_path):
    outer = make_job(tmp_path / "job")
    make_job(tmp_path / "job" / "nested")

    assert collect([tmp_path], recursive=True) == [outer]


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_collect_recursive_does_not_descend_into_root_job_directory(
    tmp_path,
):
    root_job = make_job(tmp_path)
    make_job(tmp_path / "nested")

    assert collect([tmp_path], recursive=True) == [root_job]


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_collect_recursive_pruning_does_not_affect_siblings(tmp_path):
    job_a = make_job(tmp_path / "a")
    make_job(tmp_path / "a" / "nested")
    job_b = make_job(tmp_path / "b")

    assert collect([tmp_path], recursive=True) == [job_a, job_b]


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_collect_recursive_returns_sorted_order(tmp_path):
    # create in reverse order so filesystem order is unlikely to match sorted order
    job_c = make_job(tmp_path / "c")
    job_b = make_job(tmp_path / "b")
    job_a = make_job(tmp_path / "a")

    assert collect([tmp_path], recursive=True) == [job_a, job_b, job_c]


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_collect_recursive_multiple_jobs_in_one_directory(tmp_path):
    job1 = make_job(tmp_path / "a", name="job1")
    job2 = make_job(tmp_path / "a", name="job2")

    assert collect([tmp_path], recursive=True) == [job1, job2]


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_collect_recursive_overlapping_directories_no_duplicates(
    tmp_path,
):
    job1 = make_job(tmp_path / "a" / "b")
    job2 = make_job(tmp_path / "a" / "c" / "d")

    result = collect([tmp_path, tmp_path / "a" / "c"], recursive=True)

    assert result == [job1, job2]


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_collect_recursive_nested_directory_given_first(tmp_path):
    inner = make_job(tmp_path / "a" / "b")
    other = make_job(tmp_path / "c")

    result = collect([tmp_path / "a", tmp_path], recursive=True)

    assert result == [inner, other]


def test_command_runner_collect_recursive_overlap_does_not_rescan_directories(
    tmp_path, fake_info_files
):
    (tmp_path / "a" / "b").mkdir(parents=True)

    collect([tmp_path, tmp_path / "a"], recursive=True)

    searched = [call.args[0].resolve() for call in fake_info_files.call_args_list]
    assert len(searched) == len(set(searched))


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_collect_non_recursive_same_directory_twice(tmp_path):
    job = make_job(tmp_path)

    assert collect([tmp_path, tmp_path], recursive=False) == [job]


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_collect_recursive_does_not_follow_symlinks(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    make_job(tmp_path / "outside")
    try:
        (root / "link").symlink_to(tmp_path / "outside", target_is_directory=True)
    except OSError:
        # symlinks not supported
        pytest.skip()

    assert collect([root], recursive=True) == []


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_collect_recursive_symlink_cycle_terminates(tmp_path):
    job = make_job(tmp_path / "a")
    try:
        (tmp_path / "a" / "..loop").symlink_to(tmp_path, target_is_directory=True)
        (tmp_path / "loop").symlink_to(tmp_path, target_is_directory=True)
    except OSError:
        # symlinks not supported
        pytest.skip()

    assert collect([tmp_path], recursive=True) == [job]


@pytest.mark.skipif(
    not hasattr(os, "geteuid") or os.geteuid() == 0,
    reason="permission checks are bypassed for root",
)
@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_collect_recursive_unreadable_directory_logs_warning(tmp_path):
    readable = make_job(tmp_path / "a")
    locked = tmp_path / "b"
    make_job(locked / "inner")
    locked.chmod(0o000)
    logger = MagicMock()

    try:
        result = collect([tmp_path], recursive=True, logger=logger)
    finally:
        locked.chmod(0o755)

    assert result == [readable]
    logger.warning.assert_called_once()


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_build_targets_recursive_from_cwd(tmp_path, monkeypatch):
    make_job(tmp_path / "a")
    make_job(tmp_path / "b")
    monkeypatch.chdir(tmp_path)
    logger = MagicMock()

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(recursive=True, logger=logger)

    with patch("qq_lib.core.command_runner.Informer"):
        targets = runner._build_targets()

    assert len(targets) == 2
    logger.warning.assert_not_called()


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_build_targets_recursive_with_directories(tmp_path):
    make_job(tmp_path / "a" / "b")

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(directories=(tmp_path,), recursive=True)

    with patch("qq_lib.core.command_runner.Informer"):
        targets = runner._build_targets()

    assert len(targets) == 1


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_build_targets_recursive_creates_informer_from_nested_file(
    tmp_path,
):
    info_file = make_job(tmp_path / "a" / "b")

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(directories=(tmp_path,), recursive=True)

    with patch("qq_lib.core.command_runner.Informer") as mock_informer:
        targets = runner._build_targets()
        targets[0]()

    mock_informer.from_file.assert_called_once_with(info_file)


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_build_targets_recursive_with_job_ids_and_directories(tmp_path):
    make_job(tmp_path / "a")
    logger = MagicMock()

    with patch("qq_lib.core.command_runner.BatchInterface") as mock_bi:
        mock_bi.from_env_var_or_guess.return_value.get_batch_jobs_from_ids.return_value = [
            make_mock_batch_job("111")
        ]
        runner = make_runner(
            job_ids=("111",), directories=(tmp_path,), recursive=True, logger=logger
        )

        with patch("qq_lib.core.command_runner.Informer"):
            targets = runner._build_targets()

    assert len(targets) == 2
    logger.warning.assert_not_called()


@pytest.mark.usefixtures("fake_info_files")
def test_command_runner_build_targets_recursive_raises_when_nothing_found(tmp_path):
    (tmp_path / "a" / "b").mkdir(parents=True)

    with patch("qq_lib.core.command_runner.BatchInterface"):
        runner = make_runner(directories=(tmp_path,), recursive=True)

    with pytest.raises(QQError, match="No qq job info file found"):
        runner._build_targets()
