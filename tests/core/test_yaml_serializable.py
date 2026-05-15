# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from typing import Self
from unittest.mock import MagicMock, patch

import pytest
import yaml
from yaml import SafeLoader

from qq_lib.batch.interface import BatchInterface
from qq_lib.core._yaml_serializable import _YAMLSerializable
from qq_lib.core.error import QQError


class _DummySpec(_YAMLSerializable):
    """Minimal concrete subclass used by every test."""

    _file_label = "dummy"
    _file_comment = "dummy spec file"

    def __init__(self, name: str, count: int) -> None:
        self.name = name
        self.count = count

    @classmethod
    def _from_dict(cls, data: dict[str, object]) -> Self:
        return cls(name=str(data["name"]), count=int(data["count"]))  # type: ignore

    def _to_dict(self) -> dict[str, object]:
        return {"name": self.name, "count": self.count}

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _DummySpec):
            return NotImplemented
        return self.name == other.name and self.count == other.count


_SAMPLE_DATA: dict[str, object] = {"name": "test-job", "count": 42}
_SAMPLE_YAML = yaml.dump(_SAMPLE_DATA, default_flow_style=False, sort_keys=False)


def test_from_file_local_roundtrip(tmp_path: Path) -> None:
    file = tmp_path / "spec.yaml"
    file.write_text(_SAMPLE_YAML)

    result = _DummySpec.from_file(file)

    assert result == _DummySpec("test-job", 42)


def test_from_file_local_file_not_found(tmp_path: Path) -> None:
    missing = tmp_path / "nope.yaml"

    with pytest.raises(QQError, match="does not exist"):
        _DummySpec.from_file(missing)


def test_from_file_local_permission_error(tmp_path: Path) -> None:
    file = tmp_path / "secret.yaml"
    file.write_text(_SAMPLE_YAML)
    file.chmod(0o000)

    try:
        with pytest.raises(QQError, match="No permission"):
            _DummySpec.from_file(file)
    finally:
        file.chmod(0o644)  # cleanup so tmp_path removal succeeds


def test_from_file_local_is_a_directory(tmp_path: Path) -> None:
    with pytest.raises(QQError, match="path is a directory"):
        _DummySpec.from_file(tmp_path)


def test_from_file_local_invalid_utf8(tmp_path: Path) -> None:
    file = tmp_path / "bad.yaml"
    file.write_bytes(b"\x80\x81\x82")

    with pytest.raises(QQError, match="not valid UTF-8"):
        _DummySpec.from_file(file)


def test_from_file_local_invalid_yaml(tmp_path: Path) -> None:
    file = tmp_path / "bad.yaml"
    file.write_text(":\n  :\n- ][")

    with pytest.raises(QQError, match="(?i)parse|yaml"):
        _DummySpec.from_file(file)


def test_from_file_local_type_error(tmp_path: Path) -> None:
    file = tmp_path / "spec.yaml"
    file.write_text("not_a_mapping\n")

    with pytest.raises(QQError, match="(?i)invalid|parse"):
        _DummySpec.from_file(file)


def test_from_file_remote_success() -> None:
    mock_batch = MagicMock()
    mock_batch.read_remote_file.return_value = _SAMPLE_YAML

    with patch.object(
        BatchInterface,
        "from_env_var_or_guess",
        return_value=mock_batch,
    ):
        result = _DummySpec.from_file(Path("/remote/spec.yaml"), host="node01")

    assert result == _DummySpec("test-job", 42)
    mock_batch.read_remote_file.assert_called_once_with(
        "node01", Path("/remote/spec.yaml")
    )


def test_from_file_remote_invalid_yaml() -> None:
    mock_batch = MagicMock()
    mock_batch.read_remote_file.return_value = ":\n  :\n- ]["

    with (
        patch.object(
            BatchInterface,
            "from_env_var_or_guess",
            return_value=mock_batch,
        ),
        pytest.raises(QQError, match="Could not parse"),
    ):
        _DummySpec.from_file(Path("/remote/bad.yaml"), host="node01")


def test_from_file_remote_type_error() -> None:
    mock_batch = MagicMock()
    mock_batch.read_remote_file.return_value = "just a string\n"

    with (
        patch.object(
            BatchInterface,
            "from_env_var_or_guess",
            return_value=mock_batch,
        ),
        pytest.raises(QQError, match="Invalid"),
    ):
        _DummySpec.from_file(Path("/remote/bad.yaml"), host="node01")


def test_to_file_local_roundtrip(tmp_path: Path) -> None:
    file = tmp_path / "out.yaml"
    original = _DummySpec("round-trip", 7)

    original.to_file(file)
    restored = _DummySpec.from_file(file)

    assert restored == original


def test_to_file_local_contains_header_comment(tmp_path: Path) -> None:
    file = tmp_path / "out.yaml"
    _DummySpec("x", 1).to_file(file)

    first_line = file.read_text().splitlines()[0]
    assert first_line == f"# {_DummySpec._file_comment}"


def test_to_file_local_write_error(tmp_path: Path) -> None:
    bad_path = tmp_path / "no_such_dir" / "file.yaml"

    with pytest.raises(QQError, match="Cannot create or write"):
        _DummySpec("x", 1).to_file(bad_path)


def test_to_file_remote_success() -> None:
    """Remote writing delegates to BatchInterface.write_remote_file."""
    mock_batch = MagicMock()
    spec = _DummySpec("remote-write", 99)

    with patch.object(
        BatchInterface,
        "from_env_var_or_guess",
        return_value=mock_batch,
    ):
        spec.to_file(Path("/remote/out.yaml"), host="node02")

    mock_batch.write_remote_file.assert_called_once()
    _host, _path, content = mock_batch.write_remote_file.call_args[0]
    assert _host == "node02"
    assert _path == Path("/remote/out.yaml")
    assert "remote-write" in content
    assert content.startswith(f"# {_DummySpec._file_comment}\n")


def test_to_file_remote_write_error() -> None:
    """A failure in write_remote_file is wrapped in QQError."""
    mock_batch = MagicMock()
    mock_batch.write_remote_file.side_effect = OSError("ssh failed")

    with (
        patch.object(
            BatchInterface,
            "from_env_var_or_guess",
            return_value=mock_batch,
        ),
        pytest.raises(QQError, match="Cannot create or write"),
    ):
        _DummySpec("x", 1).to_file(Path("/remote/out.yaml"), host="node02")


def test_to_yaml_output_is_valid_yaml() -> None:
    spec = _DummySpec("yaml-check", 5)
    raw = spec._to_yaml()
    parsed = yaml.load(raw, Loader=SafeLoader)

    assert parsed == {"name": "yaml-check", "count": 5}
