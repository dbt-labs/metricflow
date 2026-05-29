from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def mf_hyperlink(uri: str, label: str | None = None) -> str:
    """Return an OSC 8 hyperlink that displays the path and links to the file URI.

    When ``NO_COLOR`` is set (https://no-color.org/), returns ``str(path)`` with no escape sequences.
    """
    if os.environ.get("NO_COLOR"):
        if label is None:
            return uri
        return f"{label} ({uri})"

    display = uri if label is None else label
    return f"\033]8;;{uri}\033\\{display}\033]8;;\033\\"


def mf_path_hyperlink(path: Path) -> str:
    """Return an OSC 8 hyperlink that displays the path and links to the file URI."""
    resolved_uri = path.resolve().as_uri()
    path_display = str(path)
    return mf_hyperlink(uri=resolved_uri, label=path_display)
