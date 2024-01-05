from __future__ import annotations

import textwrap


def indent(message: str, indent_level: int = 1, indent_prefix: str = "  ") -> str:  # noqa: D
    return textwrap.indent(message, prefix=indent_prefix * indent_level)
