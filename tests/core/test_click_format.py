# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import MagicMock, patch

import click
import pytest

from qq_lib.core.click_format import GlobDirectoryMixin, GNUHelpColorsCommand


def test_no_resilient_parsing_leaves_args_unchanged():
    @click.command(cls=GNUHelpColorsCommand)
    @click.option("--option")
    def cmd(option):
        pass

    ctx = MagicMock(spec=click.Context)
    ctx.resilient_parsing = False
    args = ["--option", "=", "foo"]
    with patch.object(type(cmd).__bases__[0], "parse_args", return_value=args) as mock:
        cmd.parse_args(ctx, args)
        assert mock.call_args[0][1] == ["--option", "=", "foo"]


def test_combined_simple():
    @click.command(cls=GNUHelpColorsCommand)
    @click.option("--option")
    def cmd(option):
        pass

    ctx = MagicMock(spec=click.Context)
    ctx.resilient_parsing = True
    args = ["--option=value"]
    with patch.object(type(cmd).__bases__[0], "parse_args", return_value=args) as mock:
        cmd.parse_args(ctx, args)
        assert mock.call_args[0][1] == ["--option", "value"]


def test_combined_value_contains_equals():
    @click.command(cls=GNUHelpColorsCommand)
    @click.option("--option")
    def cmd(option):
        pass

    ctx = MagicMock(spec=click.Context)
    ctx.resilient_parsing = True
    args = ["--option=foo=bar"]
    with patch.object(type(cmd).__bases__[0], "parse_args", return_value=args) as mock:
        cmd.parse_args(ctx, args)
        assert mock.call_args[0][1] == ["--option", "foo=bar"]


def test_combined_value_contains_multiple_equals():
    @click.command(cls=GNUHelpColorsCommand)
    @click.option("--option")
    def cmd(option):
        pass

    ctx = MagicMock(spec=click.Context)
    ctx.resilient_parsing = True
    args = ["--option=foo=bar=baz"]
    with patch.object(type(cmd).__bases__[0], "parse_args", return_value=args) as mock:
        cmd.parse_args(ctx, args)
        assert mock.call_args[0][1] == ["--option", "foo=bar=baz"]


def test_combined_empty_value():
    @click.command(cls=GNUHelpColorsCommand)
    @click.option("--option")
    def cmd(option):
        pass

    ctx = MagicMock(spec=click.Context)
    ctx.resilient_parsing = True
    args = ["--option="]
    with patch.object(type(cmd).__bases__[0], "parse_args", return_value=args) as mock:
        cmd.parse_args(ctx, args)
        assert mock.call_args[0][1] == ["--option"]


def test_bash_split_simple():
    @click.command(cls=GNUHelpColorsCommand)
    @click.option("--option")
    def cmd(option):
        pass

    ctx = MagicMock(spec=click.Context)
    ctx.resilient_parsing = True
    args = ["--option", "=", "value"]
    with patch.object(type(cmd).__bases__[0], "parse_args", return_value=args) as mock:
        cmd.parse_args(ctx, args)
        assert mock.call_args[0][1] == ["--option", "value"]


def test_bash_split_value_contains_equals():
    @click.command(cls=GNUHelpColorsCommand)
    @click.option("--option")
    def cmd(option):
        pass

    ctx = MagicMock(spec=click.Context)
    ctx.resilient_parsing = True
    args = ["--option", "=", "foo", "=", "bar"]
    with patch.object(type(cmd).__bases__[0], "parse_args", return_value=args) as mock:
        cmd.parse_args(ctx, args)
        assert mock.call_args[0][1] == ["--option", "foo=bar"]


def test_bash_split_value_contains_equal_but_option_does_not():
    @click.command(cls=GNUHelpColorsCommand)
    @click.option("--option")
    def cmd(option):
        pass

    ctx = MagicMock(spec=click.Context)
    ctx.resilient_parsing = True
    args = ["--option", "foo", "=", "bar"]
    with patch.object(type(cmd).__bases__[0], "parse_args", return_value=args) as mock:
        cmd.parse_args(ctx, args)
        assert mock.call_args[0][1] == ["--option", "foo=bar"]


def test_bash_split_value_contains_multiple_equals():
    @click.command(cls=GNUHelpColorsCommand)
    @click.option("--option")
    def cmd(option):
        pass

    ctx = MagicMock(spec=click.Context)
    ctx.resilient_parsing = True
    args = ["--option", "=", "a", "=", "b", "=", "c"]
    with patch.object(type(cmd).__bases__[0], "parse_args", return_value=args) as mock:
        cmd.parse_args(ctx, args)
        assert mock.call_args[0][1] == ["--option", "a=b=c"]


def test_plain_argument_untouched():
    @click.command(cls=GNUHelpColorsCommand)
    @click.argument("script")
    def cmd(script):
        pass

    ctx = MagicMock(spec=click.Context)
    ctx.resilient_parsing = True
    args = ["somescript.sh"]
    with patch.object(type(cmd).__bases__[0], "parse_args", return_value=args) as mock:
        cmd.parse_args(ctx, args)
        assert mock.call_args[0][1] == ["somescript.sh"]


def test_plain_argument_with_equals_untouched():
    @click.command(cls=GNUHelpColorsCommand)
    @click.argument("script")
    def cmd(script):
        pass

    ctx = MagicMock(spec=click.Context)
    ctx.resilient_parsing = True
    args = ["some=value"]
    with patch.object(type(cmd).__bases__[0], "parse_args", return_value=args) as mock:
        cmd.parse_args(ctx, args)
        assert mock.call_args[0][1] == ["some=value"]


def test_multiple_options_bash_split():
    @click.command(cls=GNUHelpColorsCommand)
    @click.option("--opt1")
    @click.option("--opt2")
    def cmd(opt1, opt2):
        pass

    ctx = MagicMock(spec=click.Context)
    ctx.resilient_parsing = True
    args = ["--opt1", "foo", "=", "bar", "--opt2", "=", "baz"]
    with patch.object(type(cmd).__bases__[0], "parse_args", return_value=args) as mock:
        cmd.parse_args(ctx, args)
        assert mock.call_args[0][1] == ["--opt1", "foo=bar", "--opt2", "baz"]


def test_mixed_combined_and_bash_split():
    @click.command(cls=GNUHelpColorsCommand)
    @click.option("--opt1")
    @click.option("--opt2")
    @click.argument("script")
    def cmd(opt1, opt2, script):
        pass

    ctx = MagicMock(spec=click.Context)
    ctx.resilient_parsing = True
    args = ["--opt1=val1", "--opt2", "=", "foo", "=", "bar", "script.sh"]
    with patch.object(type(cmd).__bases__[0], "parse_args", return_value=args) as mock:
        cmd.parse_args(ctx, args)
        assert mock.call_args[0][1] == [
            "--opt1",
            "val1",
            "--opt2",
            "foo=bar",
            "script.sh",
        ]


class CapturingCommand(GlobDirectoryMixin):
    """Minimal concrete class that records the rewritten args instead of parsing them."""

    def __init__(self) -> None:
        self.received_args: list[str] = []

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        return super().parse_args(ctx, args)

    # super().parse_args() will call this
    def _parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        _ = ctx
        self.received_args = args
        return args


class _Base:
    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        _ = ctx
        return args


class CommandForTesting(GlobDirectoryMixin, _Base):
    def __init__(self) -> None:
        self.received_args: list[str] = []

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        result = super().parse_args(ctx, args)
        self.received_args = result
        return result


@pytest.fixture()
def ctx() -> click.Context:
    cmd = click.Command("test")
    return click.Context(cmd)


@pytest.fixture()
def command() -> CommandForTesting:
    return CommandForTesting()


def test_glob_directory_mixin_single_short_flag(command, ctx, tmp_path):
    d = tmp_path / "jobs"
    d.mkdir()

    command.parse_args(ctx, ["-d", str(d)])

    assert command.received_args == ["-d", str(d)]


def test_glob_directory_mixin_single_long_flag(command, ctx, tmp_path):
    d = tmp_path / "jobs"
    d.mkdir()

    command.parse_args(ctx, ["--dir", str(d)])

    assert command.received_args == ["-d", str(d)]


def test_glob_directory_mixin_multiple_directories(command, ctx, tmp_path):
    d1 = tmp_path / "jobs1"
    d2 = tmp_path / "jobs2"
    d1.mkdir()
    d2.mkdir()

    command.parse_args(ctx, ["-d", str(d1), str(d2)])

    assert command.received_args == ["-d", str(d1), "-d", str(d2)]


def test_glob_directory_mixin_multiple_directories_long_flag(command, ctx, tmp_path):
    d1 = tmp_path / "jobs1"
    d2 = tmp_path / "jobs2"
    d1.mkdir()
    d2.mkdir()

    command.parse_args(ctx, ["--dir", str(d1), str(d2)])

    assert command.received_args == ["-d", str(d1), "-d", str(d2)]


def test_glob_directory_mixin_glob_expands_matching_directories(command, ctx, tmp_path):
    (tmp_path / "jobs_a").mkdir()
    (tmp_path / "jobs_b").mkdir()
    (tmp_path / "other").mkdir()

    pattern = str(tmp_path / "jobs_*")
    command.parse_args(ctx, ["-d", pattern])

    assert command.received_args == [
        "-d",
        str(tmp_path / "jobs_a"),
        "-d",
        str(tmp_path / "jobs_b"),
    ]


def test_glob_directory_mixin_glob_expansion_is_sorted(command, ctx, tmp_path):
    (tmp_path / "jobs_c").mkdir()
    (tmp_path / "jobs_a").mkdir()
    (tmp_path / "jobs_b").mkdir()

    pattern = str(tmp_path / "jobs_*")
    command.parse_args(ctx, ["-d", pattern])

    assert command.received_args == [
        "-d",
        str(tmp_path / "jobs_a"),
        "-d",
        str(tmp_path / "jobs_b"),
        "-d",
        str(tmp_path / "jobs_c"),
    ]


def test_glob_directory_mixin_no_glob_match_passes_through(command, ctx, tmp_path):
    pattern = str(tmp_path / "nonexistent_*")
    command.parse_args(ctx, ["-d", pattern])

    assert command.received_args == ["-d", pattern]


def test_glob_directory_mixin_job_ids_before_flag_preserved(command, ctx, tmp_path):
    d = tmp_path / "jobs"
    d.mkdir()

    command.parse_args(ctx, ["12345", "-d", str(d)])

    assert command.received_args == ["12345", "-d", str(d)]


def test_glob_directory_mixin_flag_stops_at_next_flag(command, ctx, tmp_path):
    d = tmp_path / "jobs"
    d.mkdir()

    command.parse_args(ctx, ["-d", str(d), "--short"])

    assert command.received_args == ["-d", str(d), "--short"]


def test_glob_directory_mixin_non_directory_flags_unaffected(command, ctx):
    command.parse_args(ctx, ["--short", "--all"])

    assert command.received_args == ["--short", "--all"]


def test_glob_directory_mixin_empty_args(command, ctx):
    command.parse_args(ctx, [])

    assert command.received_args == []


def test_glob_directory_mixin_repeated_flags_each_expanded(command, ctx, tmp_path):
    d1 = tmp_path / "jobs1"
    d2 = tmp_path / "jobs2"
    d1.mkdir()
    d2.mkdir()

    command.parse_args(ctx, ["-d", str(d1), "-d", str(d2)])

    assert command.received_args == ["-d", str(d1), "-d", str(d2)]


def test_glob_directory_mixin_glob_with_job_ids(command, ctx, tmp_path):
    (tmp_path / "jobs_a").mkdir()
    (tmp_path / "jobs_b").mkdir()

    pattern = str(tmp_path / "jobs_*")
    command.parse_args(ctx, ["12345", "67890", "-d", pattern])

    assert command.received_args == [
        "12345",
        "67890",
        "-d",
        str(tmp_path / "jobs_a"),
        "-d",
        str(tmp_path / "jobs_b"),
    ]
