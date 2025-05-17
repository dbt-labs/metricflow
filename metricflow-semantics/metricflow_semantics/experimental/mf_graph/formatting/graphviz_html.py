from __future__ import annotations

import html
import logging
from enum import Enum

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted

logger = logging.getLogger(__name__)


class GraphvizHtmlAlignment(Enum):
    LEFT = "LEFT"
    CENTER = "CENTER"


class GraphvizHtmlTextStyle(Enum):
    TITLE = "title"
    DESCRIPTION = "description"


class GraphvizHtmlText:
    def __init__(self, text: str = "", style: GraphvizHtmlTextStyle = GraphvizHtmlTextStyle.DESCRIPTION) -> None:
        self._text = text
        self._style = style

    def __str__(self) -> str:
        escaped_text = html.escape(self._text)
        escaped_text = escaped_text.replace("\n", "<BR/>")
        if len(self._text) == 0:
            return ""
        if self._style is GraphvizHtmlTextStyle.TITLE:
            return f'<FONT POINT-SIZE="12">{escaped_text}</FONT>'
        elif self._style is GraphvizHtmlTextStyle.DESCRIPTION:
            return f'<FONT POINT-SIZE="6">{escaped_text}</FONT>'
        else:
            assert_values_exhausted(self._style)
