from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Protocol


class NodeRelation(Protocol):
    """Path object to where the data should be."""

    @property
    @abstractmethod
    def alias(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def schema_name(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def database(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def relation_name(self) -> str:  # noqa: D
        pass
