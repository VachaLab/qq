# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import pytest

from qq_lib.core.error import (
    QQError,
    QQJobMismatchError,
    QQNotSuitableError,
    QQRunCommunicationError,
    QQRunFatalError,
    terminate,
)


@pytest.mark.parametrize(
    "message, expected",
    [
        ("could not submit script", "could not submit script."),
        ("could not submit script.", "could not submit script."),
        ("what went wrong?", "what went wrong?"),
        ("job failed!", "job failed!"),
        ("reasons:", "reasons:"),
        ("first; second;", "first; second;"),
    ],
)
def test_terminate_adds_dot_only_when_needed(message: str, expected: str) -> None:
    assert terminate(message) == expected


@pytest.mark.parametrize(
    "message, expected",
    [
        ("could not submit script ", "could not submit script."),
        ("could not submit script\n", "could not submit script."),
        ("could not submit script.  \n", "could not submit script."),
        ("  leading space kept", "  leading space kept."),
    ],
)
def test_terminate_strips_trailing_whitespace(message: str, expected: str) -> None:
    assert terminate(message) == expected


@pytest.mark.parametrize("message", ["", " ", "\n", " \t\n "])
def test_terminate_returns_empty_string_for_blank_message(message: str) -> None:
    assert terminate(message) == ""


def test_terminate_does_not_double_dot_ellipsis() -> None:
    assert terminate("waiting...") == "waiting..."


def test_terminate_appends_after_quote() -> None:
    assert terminate("could not find 'job.sh'") == "could not find 'job.sh'."


@pytest.mark.parametrize(
    "exception_type",
    [
        QQError,
        QQJobMismatchError,
        QQNotSuitableError,
        QQRunFatalError,
        QQRunCommunicationError,
    ],
)
def test_qq_terminated_mixin_adds_dot(exception_type: type[Exception]) -> None:
    exception = exception_type("something broke")
    assert str(exception) == "something broke"
    assert exception.terminated == "something broke."  # ty:ignore[unresolved-attribute]


def test_qq_terminated_mixin_keeps_nested_message_undotted() -> None:
    inner = QQError("scratch directory is missing")
    outer = QQError(f"could not run job: {inner}")
    assert outer.terminated == "could not run job: scratch directory is missing."
