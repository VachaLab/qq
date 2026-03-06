# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import MagicMock, patch

import click

from qq_lib.core.click_format import GNUHelpColorsCommand


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
