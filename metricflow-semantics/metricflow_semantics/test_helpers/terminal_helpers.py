from __future__ import annotations

import logging

import termcolor

logger = logging.getLogger(__name__)


def mf_colored_link_text(uri: str) -> str:
    """Generates a string with color codes that looks like a link for logging on the terminal.

    Using `termcolor` to handle the cases where terminal color has been disabled, and we don't want to be printing
    color codes (e.g. `NO_COLOR` https://no-color.org/).
    """
    return termcolor.colored(uri, "blue", attrs=["bold"])
