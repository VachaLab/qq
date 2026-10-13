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


@pytest.fixture(autouse=True)
def clear_physical_cache():
    logical_paths._physical.cache_clear()
    yield
    logical_paths._physical.cache_clear()


@pytest.fixture
def fs(tmp_path):
    """
    Create a filesystem with a symlinked prefix:

        <tmp>/real/home/alice/project/run1    (physical)
        <tmp>/link -> <tmp>/real              (logical prefix)

    Returns (physical_root, logical_root), both with all symlinks
    in `tmp_path` itself resolved, so tests also work where the
    temporary directory is behind a symlink (e.g. macOS).
    """
    base = tmp_path.resolve()
    physical = base / "real"
    (physical / "home" / "alice" / "project" / "run1").mkdir(parents=True)
    logical = base / "link"
    try:
        logical.symlink_to(physical, target_is_directory=True)
    except OSError:
        # symlinks not supported
        pytest.skip()
    return physical, logical


def test_physical_returns_same_path_without_symlinks(fs):
    physical, _ = fs
    directory = physical / "home" / "alice"
    assert logical_paths._physical(directory) == directory


def test_physical_resolves_symlinks(fs):
    physical, logical = fs
    assert (
        logical_paths._physical(logical / "home" / "alice")
        == physical / "home" / "alice"
    )


def test_physical_returns_none_for_missing_path(fs):
    _, logical = fs
    assert logical_paths._physical(logical / "missing") is None


def test_physical_returns_none_for_broken_symlink(tmp_path):
    broken = tmp_path / "broken"
    try:
        broken.symlink_to(tmp_path / "nowhere")
    except OSError:
        # symlinks not supported
        pytest.skip()

    assert logical_paths._physical(broken) is None


def test_physical_caches_results(fs):
    _, logical = fs
    directory = logical / "home"

    logical_paths._physical(directory)
    logical_paths._physical(directory)

    info = logical_paths._physical.cache_info()
    assert info.misses == 1
    assert info.hits == 1


def test_logical_cwd_returns_logical_cwd():
    assert logical_paths.logical_cwd() == Path("/mnt/shared/home/alice")


def test_logical_cwd_follows_chdir():
    logical_paths._chdir(Path("projects"))
    assert logical_paths.logical_cwd() == Path("/mnt/shared/home/alice/projects")


def test_to_logical_maps_physical_path_below_anchor(fs):
    physical, logical = fs
    anchor = logical / "home" / "alice"

    result = logical_paths.to_logical(
        physical / "home" / "alice" / "project" / "run1", anchors=[anchor]
    )

    assert result == anchor / "project" / "run1"


def test_to_logical_maps_physical_anchor_itself(fs):
    physical, logical = fs
    anchor = logical / "home" / "alice"

    result = logical_paths.to_logical(physical / "home" / "alice", anchors=[anchor])

    assert result == anchor


def test_to_logical_maps_through_ancestor_of_anchor(fs):
    physical, logical = fs
    (physical / "home" / "bob").mkdir()
    anchor = logical / "home" / "alice" / "project"

    result = logical_paths.to_logical(physical / "home" / "bob", anchors=[anchor])

    assert result == logical / "home" / "bob"


def test_to_logical_maps_path_that_no_longer_exists(fs):
    physical, logical = fs
    anchor = logical / "home" / "alice"

    result = logical_paths.to_logical(
        physical / "home" / "alice" / "deleted" / "run9", anchors=[anchor]
    )

    assert result == anchor / "deleted" / "run9"


def test_to_logical_uses_existing_ancestor_of_missing_anchor(fs):
    physical, logical = fs
    anchor = logical / "home" / "alice" / "missing"

    result = logical_paths.to_logical(
        physical / "home" / "alice" / "project", anchors=[anchor]
    )

    assert result == logical / "home" / "alice" / "project"


def test_to_logical_leaves_logical_path_unchanged(fs):
    _, logical = fs
    path = logical / "home" / "alice" / "project"

    assert logical_paths.to_logical(path, anchors=[logical / "home"]) == path


def test_to_logical_leaves_unrelated_path_unchanged(fs, tmp_path):
    _, logical = fs
    other = tmp_path.resolve() / "other"
    other.mkdir()

    assert logical_paths.to_logical(other, anchors=[logical / "home"]) == other


def test_to_logical_leaves_path_unchanged_without_symlinked_anchor(fs):
    physical, _ = fs
    path = physical / "home" / "alice" / "project"

    result = logical_paths.to_logical(path, anchors=[physical / "home" / "alice"])

    assert result == path


def test_to_logical_without_anchors_leaves_path_unchanged(fs):
    physical, _ = fs
    path = physical / "home" / "alice"

    assert logical_paths.to_logical(path, anchors=[]) == path


def test_to_logical_normalizes_path(fs):
    physical, logical = fs
    anchor = logical / "home" / "alice"

    result = logical_paths.to_logical(
        physical / "home" / "alice" / "project" / ".." / "project" / "." / "run1",
        anchors=[anchor],
    )

    assert result == anchor / "project" / "run1"


def test_to_logical_anchors_relative_path_to_logical_cwd(fs):
    _, logical = fs
    logical_paths._logical_cwd = logical / "home" / "alice"

    result = logical_paths.to_logical(Path("project/run1"), anchors=[logical])

    assert result == logical / "home" / "alice" / "project" / "run1"


def test_to_logical_first_matching_anchor_wins(fs):
    physical, _ = fs
    second_link = physical.parent / "link2"
    second_link.symlink_to(physical, target_is_directory=True)
    first_link = physical.parent / "link"

    result = logical_paths.to_logical(
        physical / "home" / "alice", anchors=[second_link, first_link]
    )

    assert result == second_link / "home" / "alice"


def test_to_logical_default_anchors_use_logical_cwd(fs, monkeypatch, tmp_path):
    physical, logical = fs
    logical_paths._logical_cwd = logical / "home" / "alice"
    unrelated_home = tmp_path.resolve() / "unrelated_home"
    unrelated_home.mkdir()
    monkeypatch.setenv("HOME", str(unrelated_home))

    result = logical_paths.to_logical(physical / "home" / "alice" / "project")

    assert result == logical / "home" / "alice" / "project"


def test_to_logical_default_anchors_use_home(fs, monkeypatch):
    physical, logical = fs
    logical_paths._logical_cwd = Path("/nonexistent/cwd")
    monkeypatch.setenv("HOME", str(logical / "home" / "alice"))

    result = logical_paths.to_logical(physical / "home" / "alice" / "project")

    assert result == logical / "home" / "alice" / "project"
