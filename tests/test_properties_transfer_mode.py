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
    TransferModesList,
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
def test_transfer_modes_list_from_str_valid_strings(input_str: str):
    result = TransferModesList.fromStr(input_str)
    assert isinstance(result, TransferModesList)
    assert len(result.modes) >= 1


def test_transfer_modes_list_from_str_colon_separator():
    result = TransferModesList.fromStr("success:failure")
    assert len(result.modes) == 2
    assert isinstance(result.modes[0], Success)
    assert isinstance(result.modes[1], Failure)


def test_transfer_modes_list_from_str_comma_separator():
    result = TransferModesList.fromStr("success,failure")
    assert len(result.modes) == 2
    assert isinstance(result.modes[0], Success)
    assert isinstance(result.modes[1], Failure)


def test_transfer_modes_list_from_str_space_separator():
    result = TransferModesList.fromStr("success failure")
    assert len(result.modes) == 2
    assert isinstance(result.modes[0], Success)
    assert isinstance(result.modes[1], Failure)


def test_transfer_modes_list_from_str_mixed_separators():
    result = TransferModesList.fromStr("success:failure,always failure")
    assert len(result.modes) == 4
    assert isinstance(result.modes[0], Success)
    assert isinstance(result.modes[1], Failure)
    assert isinstance(result.modes[2], Always)
    assert isinstance(result.modes[3], Failure)


def test_transfer_modes_list_from_str_with_exit_codes():
    result = TransferModesList.fromStr("success:42,100")
    assert len(result.modes) == 3
    assert isinstance(result.modes[0], Success)
    assert isinstance(result.modes[1], ExitCode)
    assert result.modes[1].code == 42
    assert isinstance(result.modes[2], ExitCode)
    assert result.modes[2].code == 100


def test_transfer_modes_list_from_str_with_whitespace():
    result = TransferModesList.fromStr("  success  :  failure  ")
    assert len(result.modes) == 2
    assert isinstance(result.modes[0], Success)
    assert isinstance(result.modes[1], Failure)


def test_transfer_modes_list_from_str_single_mode():
    result = TransferModesList.fromStr("always")
    assert len(result.modes) == 1
    assert isinstance(result.modes[0], Always)


def test_transfer_modes_list_from_str_empty_string_raises_error():
    with pytest.raises(QQError) as exc_info:
        TransferModesList.fromStr("")
    assert "must contain at least one" in str(exc_info.value)


def test_transfer_modes_list_from_str_whitespace_only_raises_error():
    with pytest.raises(QQError) as exc_info:
        TransferModesList.fromStr("   ")
    assert "must contain at least one" in str(exc_info.value)


def test_transfer_modes_list_from_str_invalid_mode_raises_error():
    with pytest.raises(QQError):
        TransferModesList.fromStr("success:invalid_mode")


def test_transfer_modes_list_should_transfer_single_mode_true():
    mode_list = TransferModesList.fromStr("success")
    assert mode_list.shouldTransfer(0) is True


def test_transfer_modes_list_should_transfer_single_mode_false():
    mode_list = TransferModesList.fromStr("success")
    assert mode_list.shouldTransfer(1) is False


def test_transfer_modes_list_should_transfer_multiple_modes_all_false():
    mode_list = TransferModesList.fromStr("success:4")
    assert mode_list.shouldTransfer(2) is False


def test_transfer_modes_list_should_transfer_multiple_modes_one_true():
    mode_list = TransferModesList.fromStr("success:failure")
    assert mode_list.shouldTransfer(0) is True
    assert mode_list.shouldTransfer(1) is True


def test_transfer_modes_list_should_transfer_multiple_modes_all_true():
    mode_list = TransferModesList.fromStr("always:failure")
    assert mode_list.shouldTransfer(1) is True
    assert mode_list.shouldTransfer(42) is True


def test_transfer_modes_list_should_transfer_never_mode():
    mode_list = TransferModesList.fromStr("never:success")
    assert mode_list.shouldTransfer(0) is True
    assert mode_list.shouldTransfer(1) is False


@pytest.mark.parametrize(
    "mode_str,exit_code,expected",
    [
        ("always:never", 0, True),  # always is True
        ("success:failure", 0, True),  # success is True
        ("success:failure", 1, True),  # failure is True
        ("success:1", 2, False),  # both False
        ("never:never", 0, False),  # both False
        ("42:43", 42, True),  # one matches
        ("42:43", 44, False),  # none match
        ("always:42:43", 0, True),  # always is True
    ],
)
def test_transfer_modes_list_should_transfer_various_combinations(
    mode_str: str, exit_code: int, expected: bool
):
    mode_list = TransferModesList.fromStr(mode_str)
    assert mode_list.shouldTransfer(exit_code) is expected


def test_transfer_modes_list_to_str_single_mode():
    mode_list = TransferModesList.fromStr("always")
    assert mode_list.toStr() == "always"


def test_transfer_modes_list_to_str_multiple_modes():
    mode_list = TransferModesList.fromStr("always:never:success")
    assert mode_list.toStr() == "always:never:success"


def test_transfer_modes_list_to_str_with_exit_codes():
    mode_list = TransferModesList.fromStr("success:42:failure")
    assert mode_list.toStr() == "success:42:failure"


def test_transfer_modes_list_to_str_preserves_order():
    mode_list = TransferModesList.fromStr("failure:success:always")
    assert mode_list.toStr() == "failure:success:always"


def test_transfer_modes_list_to_str_roundtrip():
    original = "success:42:failure"
    mode_list = TransferModesList.fromStr(original)
    result = mode_list.toStr()
    assert result == original

    # verify roundtrip
    mode_list2 = TransferModesList.fromStr(result)
    assert mode_list2.toStr() == original


def test_transfer_modes_list_default_returns_instance():
    mode_list = TransferModesList.default()
    assert isinstance(mode_list, TransferModesList)


def test_transfer_modes_list_default_contains_success_mode():
    mode_list = TransferModesList.default()
    assert len(mode_list.modes) == 1
    assert isinstance(mode_list.modes[0], Success)


def test_transfer_modes_list_default_transfers_on_success():
    mode_list = TransferModesList.default()
    assert mode_list.shouldTransfer(0) is True


def test_transfer_modes_list_default_does_not_transfer_on_failure():
    mode_list = TransferModesList.default()
    assert mode_list.shouldTransfer(1) is False
    assert mode_list.shouldTransfer(42) is False


def test_transfer_modes_list_default_multiple_calls_are_independent():
    mode_list1 = TransferModesList.default()
    mode_list2 = TransferModesList.default()

    assert mode_list1 is not mode_list2
    assert mode_list1.modes is not mode_list2.modes
