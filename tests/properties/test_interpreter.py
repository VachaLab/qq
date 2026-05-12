# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import socket
from pathlib import Path
from unittest.mock import patch

import pytest

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.properties.interpreter import Interpreter


def test_interpreter_default_executable_only():
    with patch("qq_lib.properties.interpreter.CFG") as mock_cfg:
        mock_cfg.runner.default_interpreter = "bash"
        interp = Interpreter()

    assert interp.executable == "bash"
    assert interp.arguments == []


def test_interpreter_default_with_arguments():
    with patch("qq_lib.properties.interpreter.CFG") as mock_cfg:
        mock_cfg.runner.default_interpreter = "python3 -u -O"
        interp = Interpreter()

    assert interp.executable == "python3"
    assert interp.arguments == ["-u", "-O"]


def test_interpreter_from_str_executable_only():
    interp = Interpreter.from_str("python3")
    assert interp.executable == "python3"
    assert interp.arguments == []


def test_interpreter_from_str_executable_with_single_argument():
    interp = Interpreter.from_str("python3 -u")
    assert interp.executable == "python3"
    assert interp.arguments == ["-u"]


def test_interpreter_from_str_executable_with_multiple_arguments():
    interp = Interpreter.from_str("python3 -u -O")
    assert interp.executable == "python3"
    assert interp.arguments == ["-u", "-O"]


def test_interpreter_from_str_absolute_path():
    interp = Interpreter.from_str("/usr/bin/bash --norc --noprofile")
    assert interp.executable == "/usr/bin/bash"
    assert interp.arguments == ["--norc", "--noprofile"]


def test_interpreter_from_str_strips_extra_whitespace():
    interp = Interpreter.from_str("  python3   -u   -O  ")
    assert interp.executable == "python3"
    assert interp.arguments == ["-u", "-O"]


def test_interpreter_from_str_empty_string_raises():
    with pytest.raises(ValueError):
        Interpreter.from_str("")


def test_interpreter_from_str_whitespace_only_raises():
    with pytest.raises(ValueError):
        Interpreter.from_str("   ")


def test_intepreter_from_dict_full():
    interp = Interpreter.from_dict({"executable": "bash", "arguments": ["-x", "-e"]})
    assert interp.executable == "bash"
    assert interp.arguments == ["-x", "-e"]


def test_interpreter_from_dict_executable_only():
    interp = Interpreter.from_dict({"executable": "python3", "arguments": []})
    assert interp.executable == "python3"
    assert interp.arguments == []


def test_interpreter_from_dict_with_defaults():
    interp = Interpreter.from_dict({})
    assert interp.executable == CFG.runner.default_interpreter
    assert interp.arguments == []


def test_intepreter_to_dict_default():
    interp = Interpreter()
    result = interp.to_dict()
    assert result == {"executable": CFG.runner.default_interpreter, "arguments": []}


def test_interpreter_to_dict_custom():
    interp = Interpreter(executable="bash", arguments=["-x"])
    result = interp.to_dict()
    assert result == {"executable": "bash", "arguments": ["-x"]}


def test_intepreter_to_dict_from_dict_round_trip():
    original = Interpreter(executable="python3", arguments=["-u", "-O"])
    reconstructed = Interpreter.from_dict(original.to_dict())
    assert reconstructed == original


def test_interpreter_from_dict_to_dict_round_trip():
    d = {"executable": "/usr/bin/bash", "arguments": ["--norc"]}
    assert Interpreter.from_dict(d).to_dict() == d


def test_interpreter_from_str_to_dict_round_trip():
    interp = Interpreter.from_str("python3 -u")
    assert interp.to_dict() == {"executable": "python3", "arguments": ["-u"]}


def test_interpreter_to_command_list_resolves_to_full_path():
    interp = Interpreter(executable="python3", arguments=[])
    result = interp.to_command_list()

    assert Path(result[0]).is_absolute()
    assert result[0].endswith("python3")
    assert len(result) == 1


def test_interpreter_to_command_list_full_path_stays_unchanged():
    interp = Interpreter(executable="/usr/bin/bash", arguments=[])
    result = interp.to_command_list()

    assert result[0] == "/usr/bin/bash"
    assert len(result) == 1


def test_interpreter_to_command_list_raises_when_executable_not_found():
    interp = Interpreter(executable="nonexistent_interpreter_xyz")

    with pytest.raises(QQError, match="not available on node"):
        interp.to_command_list()


def test_interpreter_to_command_list_error_includes_executable_name():
    interp = Interpreter(executable="fake_executable_abc")

    with pytest.raises(QQError, match="fake_executable_abc"):
        interp.to_command_list()


def test_interpreter_to_command_list_error_includes_node_name():
    interp = Interpreter(executable="nonexistent_interpreter_xyz")

    with pytest.raises(QQError, match=socket.getfqdn()):
        interp.to_command_list()
