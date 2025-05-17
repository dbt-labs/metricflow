from __future__ import annotations

from enum import Enum


class DefinedGraphvizLabel(Enum):
    """Enumeration of labels used for generating the `graphviz` representation."""

    HAS = "has"
    SOURCE = "source"
