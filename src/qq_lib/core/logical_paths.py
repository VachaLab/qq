# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

"""
Symlink-preserving path resolution for shared network filesystems.

On network storage clusters, the same filesystem is often mounted at different
physical paths on different machines, but is also accessible via a stable
symlinked path that is consistent across all machines (e.g.
/storage/brno14-ceitec/home/user). Python's Path.resolve() and Path.cwd()
query the kernel, which returns the physical path after resolving all symlinks.
This makes resolved paths machine-specific and therefore non-portable when
shared between machines (e.g. passed in a job submission).

This module solves the problem by maintaining the logical current working
directory in a module-level variable that is kept in sync with every os.chdir()
call. os.chdir() is patched at import time to intercept all directory changes -
including those made by third-party libraries - and update the logical cwd
accordingly. The logical cwd is initialized from $PWD at import time, which is
the only moment the shell-maintained logical path is reliably available.
"""

import os
from pathlib import Path

from qq_lib.core.logger import get_logger

logger = get_logger(__name__)

# initialized from $PWD at import time, while the shell-maintained logical path
# is still valid; after this point, _chdir keeps it in sync
_logical_cwd = Path(os.environ.get("PWD", Path.cwd()))

_real_chdir = os.chdir


def _chdir(path: str | Path) -> None:
    """
    Replacement for os.chdir that keeps _logical_cwd in sync.

    The shell maintains $PWD as the logical current working directory, updating
    it on every cd. Python's os.chdir does not do this, causing the logical cwd
    to be unknowable after the first directory change. This replacement
    replicates the shell's behaviour by computing the new logical path
    lexically - without following symlinks - and storing it in _logical_cwd
    before delegating to the real os.chdir.

    Args:
        path (str | Path): The directory to change to. May be relative or absolute.
    """
    global _logical_cwd
    path = Path(path)
    if not path.is_absolute():
        path = _logical_cwd / path
    _logical_cwd = Path(os.path.normpath(path))
    _real_chdir(path)
    logger.debug(
        f"Hijacking `os.chdir` to update `_logical_cwd`. Current `_logical_cwd` is '{_logical_cwd}'."
    )


# replacing the standard os.chdir with the patched version
os.chdir = _chdir  # type: ignore


def logical_resolve(path: Path, base: Path | None = None) -> Path:
    """
    Resolve a path to a logical absolute path without expanding symlinks.

    `Path.resolve()` and `Path.cwd()` both return the physical absolute
    path by querying the kernel, which follows all symlinks. On shared network
    filesystems this produces machine-specific paths that cannot be shared
    across machines. This function instead produces the logical absolute path,
    which remains consistent across all machines that mount the same filesystem
    via the same symlinked prefix.

    Relative paths are anchored to `base` (or the logical current working
    directory if `base` is not given), then `.` and `..` components are
    collapsed lexically using `os.path.normpath`. No filesystem access is
    performed, so symlinks are never followed.

    Args:
        path (Path): The path to resolve. May be relative or absolute.
        base (Path | None): The base directory to anchor relative paths to. Must be a
            logical absolute path. Defaults to the logical current working
            directory maintained by the os.chdir patch.

    Returns:
        Path: A logical absolute path with `.` and `..` components resolved
        but symlinks left intact.
    """
    if not path.is_absolute():
        if base is None:
            base = _logical_cwd
        path = base / path
    return Path(os.path.normpath(path))
