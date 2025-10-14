from __future__ import annotations

import textwrap

MF_INDENT_2_SPACE = "  "


def mf_newline_join(*args: str) -> str:
    """Convenience function for joining string with newlines."""
    return "\n".join(args)


def mf_indent(message: str, indent_level: int = 1, indent_prefix: str = MF_INDENT_2_SPACE) -> str:  # noqa: D103
    return textwrap.indent(message, prefix=indent_prefix * indent_level)


def mf_dedent(text: str) -> str:
    """Remove leading newlines, dedents, and remove tailing newlines.

    This function simplifies the somewhat-frequently used:

        text = textwrap.dedent(
            [triple quote][backslash]
            Line 0
            Line 1
            [triple quote]
        ).rstrip()

    to:

       text = mf_dedent(
           [triple quote]
           Line 0
           Line 1
           [triple quote]
       )
    """
    return textwrap.dedent(text.lstrip("\n")).rstrip("\n")
