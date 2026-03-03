# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import pytest

from qq_lib.core.error import QQError
from qq_lib.properties.transfer_mode import (
    Always,
    ExitCode,
    Failure,
    Never,
    Success,
    TransferMode,
)


@pytest.mark.parametrize(
    "input_str,expected_type",
    [
        ("always", Always),
        ("ALWAYS", Always),
        ("Always", Always),
        ("  always  ", Always),
        ("never", Never),
        ("NEVER", Never),
        ("Never", Never),
        ("  never  ", Never),
        ("success", Success),
        ("SUCCESS", Success),
        ("Success", Success),
        ("  success  ", Success),
        ("failure", Failure),
        ("FAILURE", Failure),
        ("Failure", Failure),
        ("  failure  ", Failure),
    ],
)
def test_from_str_valid_strings(input_str: str, expected_type):
    result = TransferMode.fromStr(input_str)
    assert isinstance(result, expected_type)


@pytest.mark.parametrize(
    "exit_code_str",
    ["0", "1", "42", "255", "999", "-1", "-24"],
)
def test_from_str_numeric_strings(exit_code_str: str):
    result = TransferMode.fromStr(exit_code_str)
    assert isinstance(result, ExitCode)
    assert result.code == int(exit_code_str)


@pytest.mark.parametrize(
    "exit_code_str",
    ["0", "1", "42", "255", "-1", "-24"],
)
def test_from_str_numeric_strings_with_whitespace(exit_code_str: str):
    padded = f"  {exit_code_str}  "
    result = TransferMode.fromStr(padded)
    assert isinstance(result, ExitCode)
    assert result.code == int(exit_code_str)


@pytest.mark.parametrize(
    "invalid_str",
    [
        "unknown",
        "maybe",
        "sometimes",
        "exit_code_42",
        "exit_code:42",
        "42.5",
        "--42",
        "invalid_mode",
        "",
        "   ",
    ],
)
def test_from_str_invalid_strings_raise_error(invalid_str: str):
    with pytest.raises(QQError) as exc_info:
        TransferMode.fromStr(invalid_str)
    assert "Could not recognize a transfer mode variant" in str(exc_info.value)


@pytest.mark.parametrize("exit_code", [0, 1, 42, 127, 255, -24])
def test_always_transfers_on_all_exit_codes(exit_code: int):
    mode = Always()
    assert mode.shouldTransfer(exit_code) is True


@pytest.mark.parametrize("exit_code", [0, 1, 42, 127, 255, -24])
def test_never_transfers_on_no_exit_codes(exit_code: int):
    mode = Never()
    assert mode.shouldTransfer(exit_code) is False


def test_success_transfers_on_zero():
    mode = Success()
    assert mode.shouldTransfer(0) is True


@pytest.mark.parametrize("exit_code", [1, 42, 127, 255, -24])
def test_success_does_not_transfer_on_nonzero(exit_code: int):
    mode = Success()
    assert mode.shouldTransfer(exit_code) is False


def test_failure_does_not_transfer_on_zero():
    mode = Failure()
    assert mode.shouldTransfer(0) is False


@pytest.mark.parametrize("exit_code", [1, 42, 127, 255, -24])
def test_failure_transfers_on_nonzero(exit_code: int):
    mode = Failure()
    assert mode.shouldTransfer(exit_code) is True


@pytest.mark.parametrize(
    "specified_code,test_code,expected",
    [
        (0, 0, True),
        (0, 1, False),
        (42, 42, True),
        (42, 41, False),
        (42, 43, False),
        (255, 255, True),
        (255, 254, False),
        (127, 127, True),
        (127, 128, False),
        (-24, -24, True),
        (-24, 24, False),
    ],
)
def test_exit_code_transfers_only_on_matching_code(
    specified_code: int, test_code: int, expected: bool
):
    mode = ExitCode(specified_code)
    assert mode.shouldTransfer(test_code) is expected


def test_exit_code_stores_code_value():
    mode = ExitCode(42)
    assert mode.code == 42


@pytest.mark.parametrize(
    "mode,exit_code,expected",
    [
        (Always(), 0, True),
        (Always(), 1, True),
        (Never(), 0, False),
        (Never(), 1, False),
        (Success(), 0, True),
        (Success(), 1, False),
        (Failure(), 0, False),
        (Failure(), 1, True),
        (ExitCode(0), 0, True),
        (ExitCode(0), 1, False),
        (ExitCode(42), 42, True),
        (ExitCode(42), 43, False),
    ],
)
def test_transfer_decision_logic(mode: TransferMode, exit_code: int, expected: bool):
    assert mode.shouldTransfer(exit_code) is expected


@pytest.mark.parametrize(
    "input_str",
    [
        "always",
        "success:failure",
        "success,failure",
        "success failure",
        "always:never,success failure",
        "  success  :  failure  ",
    ],
)
def test_transfer_modes_list_multi_from_str_valid_strings(input_str: str):
    result = TransferMode.multiFromStr(input_str)
    assert isinstance(result, list)
    assert len(result) >= 1


def test_transfer_modes_list_multi_from_str_colon_separator():
    result = TransferMode.multiFromStr("success:failure")
    assert len(result) == 2
    assert isinstance(result[0], Success)
    assert isinstance(result[1], Failure)


def test_transfer_modes_list_multi_from_str_comma_separator():
    result = TransferMode.multiFromStr("success,failure")
    assert len(result) == 2
    assert isinstance(result[0], Success)
    assert isinstance(result[1], Failure)


def test_transfer_modes_list_multi_from_str_space_separator():
    result = TransferMode.multiFromStr("success failure")
    assert len(result) == 2
    assert isinstance(result[0], Success)
    assert isinstance(result[1], Failure)


def test_transfer_modes_list_multi_from_str_mixed_separators():
    result = TransferMode.multiFromStr("success:failure,always failure")
    assert len(result) == 4
    assert isinstance(result[0], Success)
    assert isinstance(result[1], Failure)
    assert isinstance(result[2], Always)
    assert isinstance(result[3], Failure)


def test_transfer_modes_list_multi_from_str_with_exit_codes():
    result = TransferMode.multiFromStr("success:42,100")
    assert len(result) == 3
    assert isinstance(result[0], Success)
    assert isinstance(result[1], ExitCode)
    assert result[1].code == 42
    assert isinstance(result[2], ExitCode)
    assert result[2].code == 100


def test_transfer_modes_list_multi_from_str_with_whitespace():
    result = TransferMode.multiFromStr("  success  :  failure  ")
    assert len(result) == 2
    assert isinstance(result[0], Success)
    assert isinstance(result[1], Failure)


def test_transfer_modes_list_multi_from_str_single_mode():
    result = TransferMode.multiFromStr("always")
    assert len(result) == 1
    assert isinstance(result[0], Always)


def test_transfer_modes_list_multi_from_str_empty_string():
    result = TransferMode.multiFromStr("")
    assert len(result) == 0


def test_transfer_modes_list_multi_from_str_whitespace_only():
    result = TransferMode.multiFromStr("     ")
    assert len(result) == 0


def test_transfer_modes_list_multi_from_str_invalid_mode_raises_error():
    with pytest.raises(QQError):
        TransferMode.multiFromStr("success:invalid_mode")
