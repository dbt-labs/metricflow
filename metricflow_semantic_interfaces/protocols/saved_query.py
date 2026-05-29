from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Protocol, Sequence

from metricflow_semantic_interfaces.protocols.export import Export
from metricflow_semantic_interfaces.protocols.metadata import Metadata
from metricflow_semantic_interfaces.protocols.where_filter import WhereFilterIntersection


class SavedQueryQueryParams(Protocol):
    """The parameters that will be passed into the MF query."""

    @property
    @abstractmethod
    def metrics(self) -> Sequence[str]:  # noqa: D102
        pass

    @property
    @abstractmethod
    def group_by(self) -> Sequence[str]:  # noqa: D102
        pass

    @property
    @abstractmethod
    def where(self) -> Optional[WhereFilterIntersection]:
        """Returns the intersection class containing any where-filters specified in the saved query."""
        pass

    @property
    @abstractmethod
    def order_by(self) -> Sequence[str]:
        """If specified, order by these query items - should match an item in `metrics` or `group_by`."""
        pass

    @property
    @abstractmethod
    def limit(self) -> Optional[int]:
        """If specified, limit the number of rows."""
        pass


class SavedQuery(Protocol):
    """Represents a query that the user wants to run repeatedly."""

    @property
    @abstractmethod
    def metadata(self) -> Optional[Metadata]:  # noqa: D102
        pass

    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D102
        pass

    @property
    @abstractmethod
    def description(self) -> Optional[str]:  # noqa: D102
        pass

    @property
    @abstractmethod
    def query_params(self) -> SavedQueryQueryParams:
        """Parameters that should be passed into the MF query."""
        pass

    @property
    @abstractmethod
    def label(self) -> Optional[str]:
        """Returns a string representing a human readable label for the saved query."""
        pass

    @property
    @abstractmethod
    def exports(self) -> Sequence[Export]:
        """Exports that can run using this saved query."""
        pass

    @property
    @abstractmethod
    def tags(self) -> Sequence[str]:
        """List of tags to be used as part of resource selection in dbt."""
        pass
