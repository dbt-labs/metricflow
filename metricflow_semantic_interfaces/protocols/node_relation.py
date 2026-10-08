from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Protocol


class NodeRelation(Protocol):
    """Path object to where the data should be."""

    @property
    @abstractmethod
    def alias(self) -> str:  # noqa: D102
        pass

    @property
    @abstractmethod
    def schema_name(self) -> str:  # noqa: D102
        pass

    @property
    @abstractmethod
    def database(self) -> Optional[str]:  # noqa: D102
        pass

    @property
    @abstractmethod
    def relation_name(self) -> str:  # noqa: D102
        pass

    @property
    @abstractmethod
    def compiled_sql(self) -> Optional[str]:
        """The compiled SQL for ephemeral models. When set, this is used as the source instead of relation_name."""
        pass
