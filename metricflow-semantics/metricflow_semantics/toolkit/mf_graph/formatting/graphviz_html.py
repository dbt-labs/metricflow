from __future__ import annotations

import html
import logging
from enum import Enum

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from typing_extensions import override

logger = logging.getLogger(__name__)


class GraphvizHtmlAlignment(Enum):
    """Enumerates the alignment options in `Graphviz` HTML."""

    LEFT = "LEFT"
    CENTER = "CENTER"
    RIGHT = "RIGHT"


class GraphvizHtmlTextStyle(Enum):
    """Enumeration for HTML styles used in this project."""

    TITLE = "title"
    DESCRIPTION = "description"


class GraphvizHtmlText:
    """Wraps a string that should be put into `Graphviz` HTML.

    This is helpful as special characters need to be escaped exactly once.
    """

    def __init__(  # noqa: D107
        self, text: str = "", style: GraphvizHtmlTextStyle = GraphvizHtmlTextStyle.DESCRIPTION
    ) -> None:
        self._text = text
        self._style = style

    @override
    def __str__(self) -> str:
        escaped_text = html.escape(self._text)
        if len(self._text) == 0:
            return ""
        if self._style is GraphvizHtmlTextStyle.TITLE:
            return f'<FONT POINT-SIZE="16">{escaped_text}</FONT>'
        elif self._style is GraphvizHtmlTextStyle.DESCRIPTION:
            return f'<FONT POINT-SIZE="6">{escaped_text}</FONT>'
        else:
            assert_values_exhausted(self._style)
