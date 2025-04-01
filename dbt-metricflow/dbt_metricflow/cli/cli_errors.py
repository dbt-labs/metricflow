from __future__ import annotations

import textwrap


class LoadSemanticManifestException(Exception):
    """Errors related to loading a semantic manifest."""

    def __init__(self, msg: str = "") -> None:  # noqa: D107
        error_msg = "Unable to load the semantic manifest."
        if msg:
            error_msg += f"\n{textwrap.indent(msg, prefix='  ')}"
        super().__init__(error_msg)
