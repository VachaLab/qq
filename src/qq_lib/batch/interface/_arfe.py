# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

"""
Atomic Remote File Editor (ARFE) module for atomically modifying remote files over SSH.
Only for internal use inside the qq library.
"""

import base64
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import IO

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError


class _AtomicRemoteFileEditor:
    """
    Atomic read-modify-write of a remote file over SSH under flock.

    Opens a single SSH session that holds an flock for the entire
    read-modify-write cycle. File contents are transported as base64
    over stdin/stdout of the SSH process.
    """

    def __init__(self, host: str, file: Path):
        self._host = host
        self._file = file
        # we lock a separate file so we can freely
        # read/write the data file while holding the lock
        self._lockfile = file.parent / f".{file.name}.lock"
        self._timeout = CFG.timeouts.flock

    def modify(self, modify_fn: Callable[[str], str]) -> None:
        """
        Apply a transformation to the remote file under flock.

        Args:
            modify_fn (Callable[[str], str]): A function that takes
                the current file content as a string and returns the new content.

        Raises:
            QQError: If the SSH connection, lock acquisition, or write-back fails.
        """
        script = self._build_script()
        proc = self._start_ssh(script)

        if proc.stdout is None or proc.stdin is None:
            raise QQError(f"Could not open streams to {self._host}:{self._file}.")

        current_content = self._read_current_content(proc.stdout)
        new_content = modify_fn(current_content)
        self._write_new_content(proc.stdin, new_content)
        self._wait_for_confirmation(proc.stdout, proc)

    def _build_script(self) -> str:
        """Return the bash script that will be executed on the remote host."""

        # bash script that
        # - acquires flock
        # - reads file and base64-encodes it to stdout
        # - prints a marker so we know reading is done
        # - waits for new content on stdin
        # - decodes the content and writes it back to the file
        return f"""
    exec 9>'{self._lockfile}'
    if ! flock -w {self._timeout} 9; then
        echo "LOCK_FAILED"
        exit 1
    fi
    if [ -f '{self._file}' ]; then
        base64 -w 0 '{self._file}'
        echo
    else
        echo ""
    fi
    echo "---READY---"
    IFS= read -r NEW_B64
    echo "$NEW_B64" | base64 -d > '{self._file}'
    echo "---DONE---"
    """

    def _start_ssh(self, script: str) -> subprocess.Popen:
        """
        Open an SSH connection and runs the given script.

        Args:
            script (str): Bash script to execute on the remote host.

        Returns:
            subprocess.Popen: The Popen handle for the SSH process.
        """
        return subprocess.Popen(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                "-o GSSAPIAuthentication=yes",
                "-o StrictHostKeyChecking=no",
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
                self._host,
                script,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def _read_current_content(self, stdout: IO[str]) -> str:
        """
        Read the current file content from the remote side.

        Consumes base64-encoded lines from stdout until the READY
        marker is received.

        Args:
            stdout (IO[str]): The stdout stream of the SSH process.

        Returns:
            str: The decoded file content, or an empty string if the
            file does not exist.

        Raises:
            QQError: If the lock could not be acquired within the timeout.
        """
        encoded_lines = []
        for line in stdout:
            stripped = line.rstrip("\n")
            if stripped == "LOCK_FAILED":
                raise QQError(
                    f"Could not acquire lock on {self._host}:{self._file} "
                    f"within {self._timeout} seconds."
                )
            if stripped == "---READY---":
                break
            encoded_lines.append(stripped)

        current_b64 = "".join(encoded_lines)
        return base64.b64decode(current_b64).decode("utf-8") if current_b64 else ""

    @staticmethod
    def _write_new_content(stdin: IO[str], content: str) -> None:
        """
        Send new file content to the remote side.

        Args:
            stdin (IO[str]): The stdin stream of the SSH process.
            content (str): The new file content to write.
        """
        new_b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
        stdin.write(new_b64 + "\n")
        stdin.flush()
        stdin.close()

    def _wait_for_confirmation(self, stdout: IO[str], proc: subprocess.Popen) -> None:
        """
        Wait for the remote side to confirm the write.

        Args:
            stdout (IO[str]): The stdout stream of the SSH process.
            proc (subprocess.Popen): The SSH process handle.

        Raises:
            QQError: If the remote process exits with a non-zero status.
        """
        for line in stdout:
            if "---DONE---" in line:
                break

        proc.wait()
        if proc.returncode != 0:
            stderr = proc.stderr.read() if proc.stderr else "stderr unavailable"
            raise QQError(f"Could not update {self._host}:{self._file}: {stderr}")
