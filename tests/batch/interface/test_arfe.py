# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import base64
import io
from pathlib import Path
from typing import IO
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.interface._arfe import _AtomicRemoteFileEditor
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError


def test_atomic_remote_file_editor_init_sets_attributes():
    editor = _AtomicRemoteFileEditor("node01", Path("/data/jobs.txt"))

    assert editor._host == "node01"
    assert editor._file == Path("/data/jobs.txt")
    assert editor._lockfile == Path("/data/.jobs.txt.lock")
    assert editor._timeout == CFG.timeouts.flock


def test_build_script_contains_key_elements():
    editor = _AtomicRemoteFileEditor("node01", Path("/data/jobs.txt"))
    script = editor._build_script()

    assert "flock" in script
    assert str(CFG.timeouts.flock) in script
    assert "/data/jobs.txt" in script
    assert "/data/.jobs.txt.lock" in script

    assert "---READY---" in script
    assert "---DONE---" in script
    assert "LOCK_FAILED" in script

    assert "base64" in script


def test_read_current_content_decodes_base64():
    editor = _AtomicRemoteFileEditor("node01", Path("/data/jobs.txt"))
    encoded = base64.b64encode(b"hello world").decode("ascii")
    stdout = io.StringIO(f"{encoded}\n---READY---\n")

    assert editor._read_current_content(stdout) == "hello world"


def test_read_current_content_handles_multiline_base64():
    editor = _AtomicRemoteFileEditor("node01", Path("/data/jobs.txt"))
    content = "line1\nline2\nline3"
    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
    # simulate the remote side splitting base64 across lines
    mid = len(encoded) // 2
    stdout = io.StringIO(f"{encoded[:mid]}\n{encoded[mid:]}\n---READY---\n")

    assert editor._read_current_content(stdout) == content


def test_read_current_content_returns_empty_for_missing_file():
    editor = _AtomicRemoteFileEditor("node01", Path("/data/jobs.txt"))
    stdout = io.StringIO("\n---READY---\n")

    assert editor._read_current_content(stdout) == ""


def test_read_current_content_raises_on_lock_failed():
    editor = _AtomicRemoteFileEditor("node01", Path("/data/jobs.txt"))
    stdout = io.StringIO("LOCK_FAILED\n")

    with pytest.raises(QQError, match="Could not acquire lock"):
        editor._read_current_content(stdout)


def test_write_new_content_sends_base64():
    written = []

    class CapturingStream(IO):
        def write(self, s):
            written.append(s)

        def flush(self):
            pass

        def close(self):
            pass

    _AtomicRemoteFileEditor._write_new_content(CapturingStream(), "hello world")

    raw = "".join(written).strip()
    assert base64.b64decode(raw).decode("utf-8") == "hello world"


def test_write_new_content_sends_single_line():
    written = []

    class CapturingStream(IO):
        def write(self, s):
            written.append(s)

        def flush(self):
            pass

        def close(self):
            pass

    _AtomicRemoteFileEditor._write_new_content(
        CapturingStream(), "multi\nline\ncontent"
    )

    payload = "".join(written)
    assert payload.endswith("\n")
    assert payload.count("\n") == 1


def test_wait_for_confirmation_succeeds_on_done_marker():
    editor = _AtomicRemoteFileEditor("node01", Path("/data/jobs.txt"))
    stdout = io.StringIO("---DONE---\n")
    proc = MagicMock(returncode=0, stdout=stdout)

    editor._wait_for_confirmation(stdout, proc)

    proc.wait.assert_called_once()


def test_wait_for_confirmation_raises_on_nonzero_exit():
    editor = _AtomicRemoteFileEditor("node01", Path("/data/jobs.txt"))
    stdout = io.StringIO("---DONE---\n")
    proc = MagicMock(returncode=1)
    proc.stderr.read.return_value = "connection refused"

    with pytest.raises(QQError, match="connection refused"):
        editor._wait_for_confirmation(stdout, proc)


@patch("qq_lib.batch.pbs.pbs.subprocess.Popen")
def test_start_ssh_passes_arguments(mock_popen):
    editor = _AtomicRemoteFileEditor("node01", Path("/data/jobs.txt"))
    editor._start_ssh("echo test")

    args = mock_popen.call_args[0][0]
    assert args[0] == "ssh"
    assert "node01" in args
    assert "echo test" in args
    assert "-o PasswordAuthentication=no" in args
    assert "-o GSSAPIAuthentication=yes" in args
    assert "-o StrictHostKeyChecking=no" in args
    assert f"-o ConnectTimeout={CFG.timeouts.ssh}" in args


@patch("qq_lib.batch.pbs.pbs.subprocess.Popen")
def test_modify_applies_fn_to_remote_content(mock_popen):
    original = "count=1"
    encoded_original = base64.b64encode(original.encode("utf-8")).decode("ascii")

    fake_stdout = io.StringIO(f"{encoded_original}\n---READY---\n---DONE---\n")
    fake_stdin = io.StringIO()
    # prevent close() from discarding StringIO buffer
    fake_stdin.close = lambda: None  # type: ignore

    mock_proc = MagicMock(
        stdout=fake_stdout,
        stdin=fake_stdin,
        returncode=0,
    )
    mock_popen.return_value = mock_proc

    editor = _AtomicRemoteFileEditor("node01", Path("/data/jobs.txt"))
    editor.modify(lambda c: c.replace("1", "2"))

    written_b64 = fake_stdin.getvalue().strip()
    assert base64.b64decode(written_b64).decode("utf-8") == "count=2"


@patch("qq_lib.batch.pbs.pbs.subprocess.Popen")
def test_modify_raises_when_streams_unavailable(mock_popen):
    mock_popen.return_value = MagicMock(stdout=None, stdin=None)

    editor = _AtomicRemoteFileEditor("node01", Path("/data/jobs.txt"))

    with pytest.raises(QQError, match="Could not open streams"):
        editor.modify(lambda c: c)


@patch("qq_lib.batch.pbs.pbs.subprocess.Popen")
def test_modify_handles_nonexistent_remote_file(mock_popen):
    fake_stdout = io.StringIO("\n---READY---\n---DONE---\n")
    fake_stdin = io.StringIO()
    fake_stdin.close = lambda: None  # type: ignore

    mock_proc = MagicMock(
        stdout=fake_stdout,
        stdin=fake_stdin,
        returncode=0,
    )
    mock_popen.return_value = mock_proc

    received = []
    editor = _AtomicRemoteFileEditor("node01", Path("/data/jobs.txt"))
    editor.modify(lambda c: (received.append(c), "new")[1])

    assert received == [""]
    written_b64 = fake_stdin.getvalue().strip()
    assert base64.b64decode(written_b64).decode("utf-8") == "new"
