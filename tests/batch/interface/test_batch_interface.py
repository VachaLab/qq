# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

import os
import subprocess
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.interface import BatchInterface, BatchJobInterface
from qq_lib.batch.interface.interface import CFG, _BatchMeta
from qq_lib.batch.pbs import PBS
from qq_lib.core.array_spec import ArraySpec
from qq_lib.core.error import QQError

type ArrayElement = int | tuple[int, int] | tuple[int, int, int]


def test_translate_ssh_command():
    host = "node1"
    directory = Path("/tmp/work")
    cmd = BatchInterface._translate_ssh_command(host, directory)
    assert cmd == [
        "ssh",
        "-o PasswordAuthentication=no",
        "-o GSSAPIAuthentication=yes",
        "-o StrictHostKeyChecking=no",
        f"-o ConnectTimeout={CFG.timeouts.ssh}",
        host,
        "-t",
        f"cd {directory} || exit {BatchInterface._CD_FAIL} && exec bash -l",
    ]


def test_navigate_same_host_success(tmp_path):
    directory = tmp_path

    with patch("subprocess.run") as mock_run:
        BatchInterface._navigate_same_host(directory)
        # check that subprocess was called properly
        mock_run.assert_called_once_with(["bash"], cwd=directory)

        # should not raise


def test_navigate_same_host_error():
    # nonexistent directory
    directory = Path("/non/existent/directory")

    with (
        patch("subprocess.run") as mock_run,
        pytest.raises(QQError, match="Could not reach"),
    ):
        BatchInterface._navigate_same_host(directory)

        # check that subprocess was not called
        mock_run.assert_not_called()


def test_guess_pbs():
    _BatchMeta._registry.clear()
    _BatchMeta._registry[PBS.env_name()] = PBS

    with patch.object(PBS, "is_available", return_value=True):
        assert BatchInterface.guess() is PBS

    with (
        patch.object(PBS, "is_available", return_value=False),
        pytest.raises(QQError, match="Could not guess a batch system"),
    ):
        BatchInterface.guess()


def test_guess_empty_registry():
    _BatchMeta._registry.clear()
    with pytest.raises(QQError, match="Could not guess a batch system"):
        BatchInterface.guess()


def test_from_str_success():
    _BatchMeta._registry.clear()
    _BatchMeta._registry[PBS.env_name()] = PBS

    assert BatchInterface.from_str("PBS") is PBS


def test_from_str_pbs_not_registered():
    _BatchMeta._registry.clear()

    with pytest.raises(QQError, match="No batch system registered"):
        BatchInterface.from_str("PBS")


def test_from_str_none_registered():
    _BatchMeta._registry.clear()

    with pytest.raises(QQError, match="No batch system registered"):
        BatchInterface.from_str("PBS")


def test_env_var_or_guess_from_env_var_returns_value(monkeypatch):
    _BatchMeta._registry.clear()
    _BatchMeta._registry[PBS.env_name()] = PBS
    monkeypatch.setenv(CFG.env_vars.batch_system, "PBS")

    assert BatchInterface.from_env_var_or_guess() is PBS


def test_env_var_or_guess_from_env_var_not_set_calls_guess():
    _BatchMeta._registry.clear()
    _BatchMeta._registry[PBS.env_name()] = PBS
    if CFG.env_vars.batch_system in os.environ:
        del os.environ[CFG.env_vars.batch_system]

    with (
        patch.object(PBS, "is_available", return_value=True),
    ):
        assert BatchInterface.from_env_var_or_guess() is PBS


def test_from_env_var_not_set_calls_guess():
    _BatchMeta._registry.clear()
    if CFG.env_vars.batch_system in os.environ:
        del os.environ[CFG.env_vars.batch_system]

    with pytest.raises(QQError, match="Could not guess a batch system"):
        BatchInterface.from_env_var_or_guess()


def test_obtain_with_name_registered():
    _BatchMeta._registry.clear()
    _BatchMeta._registry[PBS.env_name()] = PBS

    assert BatchInterface.obtain("PBS") is PBS


def test_obtain_with_name_not_registered():
    _BatchMeta._registry.clear()

    with pytest.raises(QQError, match="No batch system registered"):
        BatchInterface.obtain("PBS")


def test_obtain_without_name_env_var(monkeypatch):
    _BatchMeta._registry.clear()
    _BatchMeta._registry[PBS.env_name()] = PBS
    monkeypatch.setenv(CFG.env_vars.batch_system, "PBS")

    assert BatchInterface.obtain(None) is PBS


def test_obtain_without_name_and_guess_fails():
    _BatchMeta._registry.clear()
    if CFG.env_vars.batch_system in os.environ:
        del os.environ[CFG.env_vars.batch_system]

    with (
        patch.object(PBS, "is_available", return_value=False),
        pytest.raises(QQError, match="Could not guess a batch system"),
    ):
        BatchInterface.obtain(None)


def test_sync_with_exclusions_copies_new_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # create files in src
    (src / "file1.txt").write_text("data1")
    (src / "file2.txt").write_text("data2")

    BatchInterface.sync_with_exclusions(src, dest, None, None)

    # all files from src should exist in dest with same content
    for f in src.iterdir():
        dest_file = dest / f.name
        assert dest_file.exists()
        assert dest_file.read_text() == f.read_text()


def test_sync_with_exclusions_preserves_dest_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # file in dest that is not in src
    (dest / "keep.txt").write_text("keep_me")
    # file in src
    (src / "new.txt").write_text("new_data")

    BatchInterface.sync_with_exclusions(src, dest, None, None)

    # new file copied
    assert (dest / "new.txt").exists()
    assert (dest / "new.txt").read_text() == "new_data"
    # old file preserved
    assert (dest / "keep.txt").exists()
    assert (dest / "keep.txt").read_text() == "keep_me"
    # destination file not copied to src
    assert not (src / "keep.txt").exists()


def test_sync_with_exclusions_skips_excluded_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    (src / "include.txt").write_text("include")
    (src / "exclude.txt").write_text("exclude")

    BatchInterface.sync_with_exclusions(
        src, dest, None, None, exclude_files=[src / "exclude.txt"]
    )

    assert (dest / "include.txt").exists()
    assert not (dest / "exclude.txt").exists()


def test_sync_with_exclusions_updates_changed_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # same file in both, dest outdated
    # note that these files have the same time of creation,
    # so they have to have different size for rsync to work properly
    (src / "file.txt").write_text("new")
    (dest / "file.txt").write_text("older")

    BatchInterface.sync_with_exclusions(src, dest, None, None)

    assert (dest / "file.txt").exists()
    assert (dest / "file.txt").read_text() == "new"


def test_sync_with_exclusions_rsync_failure(tmp_path, monkeypatch):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # create a file to sync
    (src / "file.txt").write_text("data")

    # patch subprocess.run to simulate rsync failure
    def fake_run(_command, capture_output=True, text=True, timeout=0.0):
        _ = capture_output
        _ = text
        _ = timeout

        class Result:
            returncode = 1
            stderr = "rsync error"

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(QQError, match="Could not rsync files between"):
        BatchInterface.sync_with_exclusions(src, dest, None, None)


def test_sync_with_exclusions_rsync_timeout(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # create files in src
    (src / "file1.txt").write_text("data1")
    (src / "file2.txt").write_text("data2")

    with (
        pytest.raises(QQError, match="Could not rsync files"),
        patch("qq_lib.batch.interface.interface.CFG") as cfg_mock,
    ):
        cfg_mock.timeouts.rsync = 0
        BatchInterface.sync_with_exclusions(src, dest, None, None)


def test_translate_rsync_excluded_command_local_to_local():
    src = Path("/source")
    dest = Path("/dest")
    cmd = BatchInterface._translate_rsync_excluded_command(src, dest, None, None, [])
    assert cmd == [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=no",
        "-rltD",
        "/source/",
        "/dest",
    ]


def test_translate_rsync_excluded_command_local_to_remote():
    src = Path("/source")
    dest = Path("/dest")
    cmd = BatchInterface._translate_rsync_excluded_command(
        src, dest, None, "remotehost", []
    )
    assert cmd == [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=no",
        "-rltD",
        "/source/",
        "remotehost:/dest",
    ]


def test_translate_rsync_excluded_command_remote_to_local():
    src = Path("/source")
    dest = Path("/dest")
    cmd = BatchInterface._translate_rsync_excluded_command(
        src, dest, "remotehost", None, []
    )
    assert cmd == [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=no",
        "-rltD",
        "remotehost:/source/",
        "/dest",
    ]


def test_translate_rsync_excluded_command_with_excludes():
    src = Path("/source")
    dest = Path("/dest")
    excludes = [Path("temp"), Path("logs/debug.log")]
    cmd = BatchInterface._translate_rsync_excluded_command(
        src, dest, None, None, excludes
    )
    expected = [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=no",
        "-rltD",
        "--exclude",
        "temp",
        "--exclude",
        "logs/debug.log",
        "/source/",
        "/dest",
    ]
    assert cmd == expected


def test_translate_rsync_excluded_command_empty_excludes_list():
    src = Path("/source")
    dest = Path("/dest")
    cmd = BatchInterface._translate_rsync_excluded_command(src, dest, None, None, [])
    expected = [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=no",
        "-rltD",
        "/source/",
        "/dest",
    ]
    assert cmd == expected


def test_sync_selected_copies_only_included_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    (src / "include.txt").write_text("include")
    (src / "skip.txt").write_text("skip")

    BatchInterface.sync_selected(
        src, dest, None, None, include_files=[src / "include.txt"]
    )

    assert (dest / "include.txt").exists()
    assert (dest / "include.txt").read_text() == "include"
    assert not (dest / "skip.txt").exists()


def test_sync_selected_preserves_other_dest_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    (src / "new.txt").write_text("new_data")
    (dest / "keep.txt").write_text("keep_me")

    BatchInterface.sync_selected(src, dest, None, None, include_files=[src / "new.txt"])

    # new file copied
    assert (dest / "new.txt").exists()
    assert (dest / "new.txt").read_text() == "new_data"
    # old file preserved
    assert (dest / "keep.txt").exists()
    assert (dest / "keep.txt").read_text() == "keep_me"


def test_sync_selected_empty_include_list(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    (src / "file.txt").write_text("data")

    # no include_files provided -> nothing should be synced
    BatchInterface.sync_selected(src, dest, None, None)

    assert not (dest / "file.txt").exists()


def test_sync_selected_rsync_failure(tmp_path, monkeypatch):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()
    (src / "file.txt").write_text("data")

    def fake_run(_command, capture_output=True, text=True, timeout=0.0):
        _ = capture_output
        _ = text
        _ = timeout

        class Result:
            returncode = 1
            stderr = "rsync error"

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(QQError, match="Could not rsync files between"):
        BatchInterface.sync_selected(
            src, dest, None, None, include_files=[src / "file.txt"]
        )


def test_sync_selected_rsync_timeout(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()
    (src / "file.txt").write_text("data")

    with (
        pytest.raises(QQError, match="Could not rsync files"),
        patch("qq_lib.batch.interface.interface.CFG") as cfg_mock,
    ):
        cfg_mock.timeouts.rsync = 0
        BatchInterface.sync_selected(
            src, dest, None, None, include_files=[src / "file.txt"]
        )


def test_translate_rsync_included_command_local_to_local():
    src = Path("/source")
    dest = Path("/dest")
    included = [Path("file1.txt"), Path("dir/file2.txt")]

    cmd = BatchInterface._translate_rsync_included_command(
        src, dest, None, None, included
    )

    expected = [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=no",
        "-rltD",
        "--include",
        "file1.txt",
        "--include",
        "file1.txt/***",
        "--include",
        "dir/file2.txt",
        "--include",
        "dir/file2.txt/***",
        "--exclude",
        "*",
        "/source/",
        "/dest",
    ]
    assert cmd == expected


def test_translate_rsync_included_command_local_to_remote():
    src = Path("/source")
    dest = Path("/dest")
    included = [Path("file1.txt")]

    cmd = BatchInterface._translate_rsync_included_command(
        src, dest, None, "remotehost", included
    )

    expected = [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=no",
        "-rltD",
        "--include",
        "file1.txt",
        "--include",
        "file1.txt/***",
        "--exclude",
        "*",
        "/source/",
        "remotehost:/dest",
    ]
    assert cmd == expected


def test_translate_rsync_included_command_remote_to_local():
    src = Path("/source")
    dest = Path("/dest")
    included = [Path("file1.txt")]

    cmd = BatchInterface._translate_rsync_included_command(
        src, dest, "remotehost", None, included
    )

    expected = [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=no",
        "-rltD",
        "--include",
        "file1.txt",
        "--include",
        "file1.txt/***",
        "--exclude",
        "*",
        "remotehost:/source/",
        "/dest",
    ]
    assert cmd == expected


def test_translate_rsync_included_command_no_files():
    src = Path("/source")
    dest = Path("/dest")
    included = []

    cmd = BatchInterface._translate_rsync_included_command(
        src, dest, None, None, included
    )

    expected = [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=no",
        "-rltD",
        "--exclude",
        "*",
        "/source/",
        "/dest",
    ]
    assert cmd == expected


def test_translate_move_command_single_file():
    files = [Path("source.txt")]
    moved_files = [Path("dest") / "dest.txt"]

    cmd = BatchInterface._translate_move_command(files, moved_files)
    assert cmd == "mv 'source.txt' 'dest/dest.txt'"


def test_translate_move_command_multiple_files():
    files = [Path("a.txt"), Path("b.txt")]
    moved_files = [Path("x.txt"), Path("y.txt")]

    cmd = BatchInterface._translate_move_command(files, moved_files)
    assert cmd == "mv 'a.txt' 'x.txt' && mv 'b.txt' 'y.txt'"


def test_translate_move_command_mismatched_lengths():
    files = [Path("a.txt")]
    moved_files = [Path("b.txt"), Path("c.txt")]

    with pytest.raises(QQError, match="must have the same length"):
        BatchInterface._translate_move_command(files, moved_files)


def test_translate_move_command_empty_lists():
    cmd = BatchInterface._translate_move_command([], [])
    assert cmd == ""


def test_is_shared_returns_false_for_local(monkeypatch, tmp_path):
    def fake_run(cmd, **kwargs):
        _ = cmd
        _ = kwargs

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert BatchInterface.is_shared(tmp_path) is False


def test_is_shared_returns_true_for_shared(monkeypatch, tmp_path):
    def fake_run(cmd, **kwargs):
        _ = cmd
        _ = kwargs

        class Result:
            returncode = 1

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert BatchInterface.is_shared(tmp_path) is True


def test_is_shared_passes_correct_command(monkeypatch, tmp_path):
    captured = {}

    def fake_run(cmd, **kwargs):
        _ = kwargs
        captured["cmd"] = cmd

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    BatchInterface.is_shared(tmp_path)

    assert captured["cmd"][0:2] == ["df", "-l"]
    assert Path(captured["cmd"][2]) == tmp_path


class DummyJob:
    def __init__(self, job_id):
        self._id = job_id

    def get_id(self):
        return self._id


def test_batch_interface_sort_jobs_sorts_by_id():
    jobs = [DummyJob("c"), DummyJob("a"), DummyJob("b")]
    BatchInterface.sort_jobs(cast("list[BatchJobInterface]", jobs))
    ids = [job.get_id() for job in jobs]
    assert ids == ["a", "b", "c"]


def test_batch_interface_sort_jobs_with_numeric_ids():
    jobs = [DummyJob("10"), DummyJob("2"), DummyJob("1")]
    BatchInterface.sort_jobs(cast("list[BatchJobInterface]", jobs))
    ids = [job.get_id() for job in jobs]
    assert ids == ["1", "10", "2"]


def test_batch_interface_sort_jobs_empty_list():
    jobs = []
    BatchInterface.sort_jobs(jobs)
    assert jobs == []


@patch("qq_lib.batch.interface.interface.subprocess.run")
def test_batchinterface_delete_remote_dir_success(mock_run):
    mock_run.return_value = MagicMock(returncode=0)

    BatchInterface.delete_remote_dir("remote_host", Path("/remote/dir"))

    mock_run.assert_called_once_with(
        [
            "ssh",
            "-o PasswordAuthentication=no",
            "-o GSSAPIAuthentication=yes",
            "-o StrictHostKeyChecking=no",
            f"-o ConnectTimeout={CFG.timeouts.ssh}",
            "remote_host",
            "yes | rm -r /remote/dir",
        ],
        capture_output=True,
        text=True,
    )


@patch("qq_lib.batch.interface.interface.subprocess.run")
def test_batchinterface_delete_remote_dir_raises_error(mock_run):
    mock_run.return_value = MagicMock(returncode=1, stderr="permission denied")

    with pytest.raises(
        QQError,
        match="Could not delete remote directory '/remote/dir' on 'remote_host': permission denied.",
    ):
        BatchInterface.delete_remote_dir("remote_host", Path("/remote/dir"))

    mock_run.assert_called_once()


def test_array_spec_single_index() -> None:
    spec = ArraySpec([0])
    assert spec.elements == [0]


def test_array_spec_multiple_indices() -> None:
    spec = ArraySpec([0, 5, 100])
    assert spec.elements == [0, 5, 100]


def test_array_spec_range_two_tuple() -> None:
    spec = ArraySpec([(0, 10)])
    assert spec.elements == [(0, 10)]


def test_array_spec_range_three_tuple() -> None:
    spec = ArraySpec([(0, 10, 2)])
    assert spec.elements == [(0, 10, 2)]


def test_array_spec_mixed_elements() -> None:
    elements: list[ArrayElement] = [1, (10, 20), (30, 50, 5)]
    spec = ArraySpec(elements)
    assert spec.elements == elements


def test_array_spec_range_start_equals_stop() -> None:
    spec = ArraySpec([(5, 5)])
    assert spec.elements == [5]


def test_array_spec_step_of_one() -> None:
    spec = ArraySpec([(0, 10, 1)])
    assert spec.elements == [(0, 10)]


def test_array_spec_zero_index() -> None:
    spec = ArraySpec([0])
    assert spec.elements == [0]


def test_array_spec_empty_list_raises() -> None:
    with pytest.raises(QQError, match="no elements provided"):
        ArraySpec([])


def test_array_spec_negative_bare_index() -> None:
    with pytest.raises(QQError, match="index must be >= 0"):
        ArraySpec([-1])


def test_array_spec_negative_start_two_tuple() -> None:
    with pytest.raises(QQError, match="start must be >= 0"):
        ArraySpec([(-1, 10)])


def test_array_spec_negative_start_three_tuple() -> None:
    with pytest.raises(QQError, match="start must be >= 0"):
        ArraySpec([(-1, 10, 2)])


def test_array_spec_start_greater_than_stop_two_tuple() -> None:
    with pytest.raises(QQError, match="stop must be >= start"):
        ArraySpec([(10, 5)])


def test_array_spec_start_greater_than_stop_three_tuple() -> None:
    with pytest.raises(QQError, match="stop must be >= start"):
        ArraySpec([(10, 5, 1)])


def test_array_spec_zero_step() -> None:
    with pytest.raises(QQError, match="step must be >= 1"):
        ArraySpec([(0, 10, 0)])


def test_array_spec_negative_step() -> None:
    with pytest.raises(QQError, match="step must be >= 1"):
        ArraySpec([(0, 10, -1)])


def test_array_spec_string_element() -> None:
    with pytest.raises(QQError, match="expected int or tuple of 2-3 ints"):
        ArraySpec(["1-10"])  # type: ignore


def test_array_spec_float_element() -> None:
    with pytest.raises(QQError, match="expected int or tuple of 2-3 ints"):
        ArraySpec([1.5])  # type: ignore


def test_array_spec_single_element_tuple() -> None:
    with pytest.raises(QQError, match="expected int or tuple of 2-3 ints"):
        ArraySpec([(1,)])  # type: ignore


def test_array_spec_four_element_tuple() -> None:
    with pytest.raises(QQError, match="expected int or tuple of 2-3 ints"):
        ArraySpec([(1, 2, 3, 4)])  # type: ignore


def test_array_spec_none_element() -> None:
    with pytest.raises(QQError, match="expected int or tuple of 2-3 ints"):
        ArraySpec([None])  # type: ignore


@pytest.mark.parametrize(
    ("elements", "expected"),
    [
        # no merging needed
        pytest.param([5], [5], id="single_index"),
        pytest.param([1, 3, 5], [1, 3, 5], id="disjoint_indices"),
        pytest.param([(0, 10)], [(0, 10)], id="single_range"),
        pytest.param([(0, 10, 3)], [(0, 10, 3)], id="single_strided_range"),
        pytest.param(
            [1, (10, 20), (30, 50, 5)],
            [1, (10, 20), (30, 50, 5)],
            id="disjoint_mixed",
        ),
        # duplicate indices and ranges
        pytest.param([3, 3, 3], [3], id="duplicate_indices"),
        pytest.param([5, 1, 5, 1], [1, 5], id="duplicate_indices_unsorted"),
        pytest.param(
            [(0, 10), (0, 10)],
            [(0, 10)],
            id="duplicate_ranges",
        ),
        # sorting
        pytest.param([10, 2, 7], [2, 7, 10], id="indices_sorted"),
        pytest.param(
            [(20, 30), (0, 5)],
            [(0, 5), (20, 30)],
            id="ranges_sorted",
        ),
        # adjacent indices merging into ranges
        pytest.param([1, 2], [(1, 2)], id="two_adjacent_indices"),
        pytest.param([1, 2, 3], [(1, 3)], id="three_consecutive_indices"),
        pytest.param(
            [5, 6, 7, 8, 9, 10],
            [(5, 10)],
            id="many_consecutive_indices",
        ),
        pytest.param(
            [1, 2, 3, 10, 11, 12],
            [(1, 3), (10, 12)],
            id="two_groups_of_consecutive_indices",
        ),
        pytest.param(
            [1, 2, 3, 5, 10, 11],
            [(1, 3), 5, (10, 11)],
            id="consecutive_groups_with_loner",
        ),
        # adjacent ranges merging
        pytest.param(
            [(1, 5), (6, 10)],
            [(1, 10)],
            id="adjacent_ranges",
        ),
        pytest.param(
            [(1, 10), (20, 30), (11, 19)],
            [(1, 30)],
            id="three_ranges_fill_gap",
        ),
        # overlapping ranges
        pytest.param(
            [(1, 5), (5, 10)],
            [(1, 10)],
            id="overlapping_ranges_shared_boundary",
        ),
        pytest.param(
            [(1, 10), (5, 15)],
            [(1, 15)],
            id="overlapping_ranges",
        ),
        pytest.param(
            [(0, 100), (10, 20)],
            [(0, 100)],
            id="range_fully_contained",
        ),
        pytest.param(
            [(0, 10), (3, 7), (8, 20)],
            [(0, 20)],
            id="multiple_overlapping_ranges",
        ),
        # index absorbed by range
        pytest.param(
            [5, (0, 10)],
            [(0, 10)],
            id="index_inside_range",
        ),
        pytest.param(
            [11, (0, 10)],
            [(0, 11)],
            id="index_adjacent_to_range",
        ),
        pytest.param(
            [0, (1, 5), 6],
            [(0, 6)],
            id="indices_extend_range_both_sides",
        ),
        # merging strided ranges
        pytest.param(
            [(0, 10, 2), (12, 20, 2)],
            [(0, 20, 2)],
            id="adjacent_strided_same_phase",
        ),
        pytest.param(
            [(0, 8, 2), (4, 12, 2)],
            [(0, 12, 2)],
            id="overlapping_strided_same_phase",
        ),
        pytest.param(
            [(0, 10, 5), (15, 30, 5)],
            [(0, 30, 5)],
            id="adjacent_strided_step_5",
        ),
        # strided ranges with different phase are not merged
        pytest.param(
            [(0, 10, 2), (1, 11, 2)],
            [(0, 10, 2), (1, 11, 2)],
            id="strided_different_phase",
        ),
        pytest.param(
            [(0, 10, 3), (1, 10, 3)],
            [(0, 10, 3), (1, 10, 3)],
            id="strided_step_3_different_phase",
        ),
        # strided ranges with different step are not merged
        pytest.param(
            [(0, 10, 2), (0, 10, 3)],
            [(0, 10, 2), (0, 10, 3)],
            id="strided_different_step_same_start",
        ),
        # large ranges
        pytest.param(
            [(0, 1_000_000), (1_000_001, 2_000_000)],
            [(0, 2_000_000)],
            id="large_adjacent_ranges",
        ),
        pytest.param(
            [(0, 1_000_000), (500_000, 1_500_000)],
            [(0, 1_500_000)],
            id="large_overlapping_ranges",
        ),
        pytest.param(
            [999_999, (0, 1_000_000)],
            [(0, 1_000_000)],
            id="index_inside_large_range",
        ),
        # range collapses to a single index
        pytest.param(
            [(5, 5)],
            [5],
            id="degenerate_range_to_index",
        ),
        pytest.param(
            [(5, 5, 3)],
            [5],
            id="degenerate_strided_range_to_index",
        ),
        # step-1 strided range loses step
        pytest.param(
            [(0, 10, 1)],
            [(0, 10)],
            id="step_one_normalized",
        ),
        # complex mixed scenarios
        pytest.param(
            [1, 2, (3, 7), 8, (10, 20, 2), (14, 22, 2)],
            [(1, 8), (10, 22, 2)],
            id="indices_ranges_strided_mixed",
        ),
        pytest.param(
            [(0, 5), 3, (4, 8), 12, 13, (11, 11)],
            [(0, 8), (11, 13)],
            id="many_overlapping_with_indices",
        ),
        pytest.param(
            [0, 2, 4, (10, 20), (25, 30), 21, 22, 23, 24],
            [0, 2, 4, (10, 30)],
            id="indices_bridge_gap_between_ranges",
        ),
        # same index repeated in many forms
        pytest.param(
            [5, (5, 5), (5, 5, 1)],
            [5],
            id="same_index_many_representations",
        ),
        # multiple single element ranges
        pytest.param(
            [(0, 0), (1, 1), (2, 2)],
            [(0, 2)],
            id="degenerate_ranges_merge_consecutive",
        ),
    ],
)
def test_array_spec_merge(
    elements: list[ArrayElement],
    expected: list[ArrayElement],
) -> None:
    spec = ArraySpec(elements)
    assert spec.elements == expected
