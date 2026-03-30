# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import patch

import pytest

from qq_lib.core import logical_paths


@pytest.fixture(autouse=True)
def reset_logical_cwd():
    original = logical_paths._logical_cwd
    logical_paths._logical_cwd = Path("/mnt/shared/home/alice")
    yield
    logical_paths._logical_cwd = original


@pytest.fixture(autouse=True)
def mock_real_chdir():
    with patch.object(logical_paths, "_real_chdir"):
        yield


def test_chdir_absolute_path_updates_logical_cwd():
    logical_paths._chdir(Path("/mnt/shared/home/bob"))
    assert logical_paths._logical_cwd == Path("/mnt/shared/home/bob")


def test_chdir_absolute_string_updates_logical_cwd():
    logical_paths._chdir("/mnt/shared/home/bob")
    assert logical_paths._logical_cwd == Path("/mnt/shared/home/bob")


def test_chdir_relative_path_updates_logical_cwd():
    logical_paths._chdir(Path("projects"))
    assert logical_paths._logical_cwd == Path("/mnt/shared/home/alice/projects")


def test_chdir_relative_string_updates_logical_cwd():
    logical_paths._chdir("projects")
    assert logical_paths._logical_cwd == Path("/mnt/shared/home/alice/projects")


def test_chdir_relative_dot_dot_updates_logical_cwd():
    logical_paths._chdir(Path(".."))
    assert logical_paths._logical_cwd == Path("/mnt/shared/home")


def test_chdir_relative_multiple_dot_dot_updates_logical_cwd():
    logical_paths._chdir(Path("../../.."))
    assert logical_paths._logical_cwd == Path("/mnt")


def test_chdir_relative_dot_does_not_change_logical_cwd():
    logical_paths._chdir(Path())
    assert logical_paths._logical_cwd == Path("/mnt/shared/home/alice")


def test_chdir_relative_path_with_dot_dot_in_middle():
    logical_paths._chdir(Path("projects/../documents"))
    assert logical_paths._logical_cwd == Path("/mnt/shared/home/alice/documents")


def test_chdir_absolute_path_with_dot_dot_is_normalized():
    logical_paths._chdir(Path("/mnt/shared/home/alice/../bob"))
    assert logical_paths._logical_cwd == Path("/mnt/shared/home/bob")


def test_chdir_absolute_path_with_dot_is_normalized():
    logical_paths._chdir(Path("/mnt/shared/home/./alice"))
    assert logical_paths._logical_cwd == Path("/mnt/shared/home/alice")


def test_chdir_calls_real_chdir_with_absolute_path():
    with patch.object(logical_paths, "_real_chdir") as mock:
        logical_paths._chdir(Path("/mnt/shared/home/bob"))
        mock.assert_called_once_with(Path("/mnt/shared/home/bob"))


def test_chdir_calls_real_chdir_with_relative_path():
    with patch.object(logical_paths, "_real_chdir") as mock:
        logical_paths._chdir(Path("projects"))
        mock.assert_called_once_with(Path("/mnt/shared/home/alice/projects"))


def test_chdir_multiple_calls_accumulate_correctly():
    logical_paths._chdir(Path("projects"))
    logical_paths._chdir(Path("myproject"))
    logical_paths._chdir(Path(".."))
    assert logical_paths._logical_cwd == Path("/mnt/shared/home/alice/projects")


def test_chdir_to_root():
    logical_paths._chdir(Path("/"))
    assert logical_paths._logical_cwd == Path("/")


@pytest.mark.parametrize(
    "path, base, expected",
    [
        # already absolute path is returned as-is
        (
            Path("/mnt/shared/home/alice/file.txt"),
            None,
            Path("/mnt/shared/home/alice/file.txt"),
        ),
        # absolute path with dot components gets normalized
        (
            Path("/mnt/shared/home/alice/./subdir/../file.txt"),
            None,
            Path("/mnt/shared/home/alice/file.txt"),
        ),
        # relative path is anchored to provided base
        (
            Path("file.txt"),
            Path("/mnt/shared/home/alice"),
            Path("/mnt/shared/home/alice/file.txt"),
        ),
        # relative path with .. is resolved against base
        (
            Path("../bob/file.txt"),
            Path("/mnt/shared/home/alice"),
            Path("/mnt/shared/home/bob/file.txt"),
        ),
        # deep relative path with multiple .. components
        (
            Path("../../projects/file.txt"),
            Path("/mnt/shared/home/alice/subdir"),
            Path("/mnt/shared/home/projects/file.txt"),
        ),
        # dot-only relative path resolves to base itself
        (Path(), Path("/mnt/shared/home/alice"), Path("/mnt/shared/home/alice")),
        # relative path with redundant separators (via string construction)
        (
            Path("subdir/./nested/../file.txt"),
            Path("/mnt/shared/home/alice"),
            Path("/mnt/shared/home/alice/subdir/file.txt"),
        ),
    ],
)
def test_logical_resolve(path, base, expected):
    assert logical_paths.logical_resolve(path, base=base) == expected


@pytest.mark.parametrize(
    "path, logical_cwd, expected",
    [
        # relative path uses _logical_cwd when no base is given
        (
            Path("file.txt"),
            Path("/mnt/shared/home/alice"),
            Path("/mnt/shared/home/alice/file.txt"),
        ),
        # _logical_cwd is used as base and .. is resolved correctly
        (
            Path("../bob/file.txt"),
            Path("/mnt/shared/home/alice"),
            Path("/mnt/shared/home/bob/file.txt"),
        ),
    ],
)
def test_logical_resolve_uses_logical_cwd(path, logical_cwd, expected):
    original = logical_paths._logical_cwd
    logical_paths._logical_cwd = logical_cwd
    try:
        assert logical_paths.logical_resolve(path) == expected
    finally:
        logical_paths._logical_cwd = original
