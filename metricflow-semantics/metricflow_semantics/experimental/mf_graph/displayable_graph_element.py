from __future__ import annotations

from abc import ABC, abstractmethod


class DisplayableGraphElement(ABC):
    @property
    @abstractmethod
    def dot_label(self) -> str:
        """A short name that is used to describe this in DOT notation."""
        raise NotImplementedError

    @property
    def graphviz_label(self) -> str:
        """The label to use when rendering this element using `graphviz`."""
        raise NotImplementedError
