from __future__ import annotations

from abc import ABC, abstractmethod


class SemanticGraphNodeId(ABC):
    @property
    @abstractmethod
    def dot_label(self) -> str:
        raise NotImplementedError
