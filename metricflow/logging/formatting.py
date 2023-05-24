from __future__ import annotations

import textwrap


def indent_log_line(message: str, indent_level: int = 1) -> str:  # noqa: D
    return textwrap.indent(message, prefix="    " * indent_level)
