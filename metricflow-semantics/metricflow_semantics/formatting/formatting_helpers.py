from __future__ import annotations

import textwrap


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
