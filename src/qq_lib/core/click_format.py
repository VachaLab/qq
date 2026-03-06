# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

"""
GNU-style help formatting for Click commands.

This module defines `GNUHelpColorsCommand`, a Click command class that prints
help text using GNU-style formatting with customizable colors, headings, and
option layouts.
"""

from collections.abc import Sequence

import click
from click import Context, HelpFormatter
from click_help_colors import HelpColorsCommand


class GNUHelpColorsCommand(HelpColorsCommand):
    """Custom formatter that prints options in GNU-style."""

    def get_help(self, ctx: Context) -> str:
        class GNUHelpFormatter(HelpFormatter):
            def __init__(self, width=None, headers_color=None, options_color=None):
                super().__init__(width=width)
                self.headers_color = headers_color or "white"
                self.options_color = options_color or "white"

            def write_heading(self, heading: str) -> None:
                styled_heading = click.style(heading, fg=self.headers_color, bold=True)
                self.write(f"{styled_heading}\n")

            def write_usage(
                self, prog_name: str, args: str | None, prefix: str | None = None
            ) -> None:  # ty: ignore[invalid-method-override]
                """Override to make Usage: header bold"""
                if prefix is None:
                    prefix = "Usage:"

                styled_prefix = click.style(prefix, fg=self.headers_color, bold=True)
                usage_line = f"{styled_prefix} {prog_name}"

                if args:
                    usage_line += f" {args}"

                self.write(f"{usage_line}\n")

            def write_dl(
                self,
                rows: Sequence[tuple[str, str | None]],
                _col_max: int = 30,
                _col_spacing: int = 2,
            ) -> None:  # ty: ignore[invalid-method-override]
                for term, definition in rows:
                    colored_term = click.style(term, fg=self.options_color, bold=True)
                    self.write(f"  {colored_term}\n")

                    if definition:
                        for line in definition.splitlines():
                            if line.strip():
                                self.write(f"      {line}\n")
                    self.write("\n")

        formatter = GNUHelpFormatter(
            width=ctx.terminal_width,
            headers_color=getattr(self, "help_headers_color", "white"),
            options_color=getattr(self, "help_options_color", "white"),
        )

        self.format_help(ctx, formatter)
        return formatter.getvalue()

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        """
        Reassemble bash-split option tokens before delegating to Click's parser.

        Bash splits tokens on `=` (which appears in `COMP_WORDBREAKS` by
        default) before invoking the completion handler. This means that an
        option like ``--option=foo=bar`` arrives as the fragments
        `["--option", "=", "foo", "=", "bar"]` rather than as a single token.
        Click's parser cannot handle this fragmented form, so autocompletion
        breaks whenever an option value contains `=`.

        This override reassembles such fragments back into `["--option",
        "foo=bar"]` during resilient (completion) parsing, leaving normal
        invocations entirely unaffected.

        Args:
            ctx: The current Click context.
            args: The raw argument list as received from the shell, potentially
                containing `=`-fragmented option tokens produced by bash's
                completion machinery.

        Returns:
            The remaining unparsed arguments, after reassembly and delegation
            to the parent parser.
        """

        if ctx.resilient_parsing:
            processed = []
            i = 0
            while i < len(args):
                arg = args[i]
                if arg.startswith("-") and "=" in arg:
                    # already combined: --option=foo=bar
                    option, value = arg.split("=", 1)
                    processed.append(option)
                    if value:
                        processed.append(value)
                    i += 1
                elif arg.startswith("-") and i + 1 < len(args) and args[i + 1] == "=":
                    # bash pre-split starting with "=": ["--opt", "=", "foo", "=", "bar"]
                    processed.append(arg)
                    i += 2  # skip option and bare "="
                    value_parts = []
                    while i < len(args) and not args[i].startswith("-"):
                        if args[i] == "=":
                            value_parts.append("=")
                        else:
                            if value_parts and value_parts[-1] != "=":
                                break  # previous wasn't "=", this is a new arg
                            value_parts.append(args[i])
                        i += 1
                    if value_parts:
                        processed.append("".join(value_parts))
                elif (
                    arg.startswith("-")
                    and i + 1 < len(args)
                    and not args[i + 1].startswith("-")
                    and i + 2 < len(args)
                    and args[i + 2] == "="
                ):
                    # bash pre-split starting with fragment: ["--opt", "foo", "=", "bar"]
                    processed.append(arg)
                    i += 1
                    value_parts = [args[i]]
                    i += 1
                    while i < len(args) and not args[i].startswith("-"):
                        if args[i] == "=":
                            value_parts.append("=")
                            i += 1
                            if i < len(args) and not args[i].startswith("-"):
                                value_parts.append(args[i])
                                i += 1
                        else:
                            break
                    processed.append("".join(value_parts))
                else:
                    processed.append(arg)
                    i += 1
            args = processed
        return super().parse_args(ctx, args)
