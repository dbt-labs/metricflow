from __future__ import annotations

import textwrap


def mf_newline_join(*args: str) -> str:
    """Convenience function for joining string with newlines."""
    return "\n".join(args)


def mf_indent(message: str, indent_level: int = 1, indent_prefix: str = "  ") -> str:  # noqa: D103
    return textwrap.indent(message, prefix=indent_prefix * indent_level)
