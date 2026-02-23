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
        """Split --option=value tokens into --option value during completion.

        Args:
            ctx: The current Click context.
            args: The raw argument list from the shell.

        Returns:
            The remaining unparsed arguments.
        """
        if ctx.resilient_parsing:
            args = [
                part
                for arg in args
                for part in (
                    arg.split("=", 1) if arg.startswith("-") and "=" in arg else [arg]
                )
                if part != "="
            ]

        return super().parse_args(ctx, args)
