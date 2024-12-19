from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Protocol, Union, runtime_checkable

from dbt_semantic_interfaces.type_enums.date_part import DatePart

if TYPE_CHECKING:
    from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
    from metricflow_semantics.query.resolver_inputs.query_resolver_inputs import (
        ResolverInputForGroupByItem,
        ResolverInputForMetric,
        ResolverInputForOrderByItem,
    )


@runtime_checkable
class MetricQueryParameter(Protocol):
    """Metric requested in a query."""

    @property
    def name(self) -> str:
        """The name of the metric."""
        raise NotImplementedError

    @property
    def alias(self) -> Optional[str]:
        """The alias of the metric."""
        raise NotImplementedError

    def query_resolver_input(  # noqa: D102
        self, semantic_manifest_lookup: SemanticManifestLookup
    ) -> ResolverInputForMetric:
        raise NotImplementedError


@runtime_checkable
class DimensionOrEntityQueryParameter(Protocol):
    """Generic group by parameter for queries. Might be an entity or a dimension."""

    @property
    def name(self) -> str:
        """The name of the metric."""
        raise NotImplementedError

    def query_resolver_input(  # noqa: D102
        self, semantic_manifest_lookup: SemanticManifestLookup
    ) -> ResolverInputForGroupByItem:
        raise NotImplementedError


@runtime_checkable
class TimeDimensionQueryParameter(Protocol):  # noqa: D101
    @property
    def name(self) -> str:
        """The name of the item."""
        raise NotImplementedError

    @property
    def grain(self) -> Optional[str]:
        """The name of the time granularity.

        This may be the name of a custom granularity or the string value of an entry in the standard
        TimeGranularity enum.
        """
        raise NotImplementedError

    @property
    def date_part(self) -> Optional[DatePart]:
        """Date part to extract from the dimension."""
        raise NotImplementedError

    def query_resolver_input(  # noqa: D102
        self, semantic_manifest_lookup: SemanticManifestLookup
    ) -> ResolverInputForGroupByItem:
        raise NotImplementedError


GroupByParameter = Union[DimensionOrEntityQueryParameter, TimeDimensionQueryParameter]
InputOrderByParameter = Union[MetricQueryParameter, GroupByParameter]


class OrderByQueryParameter(Protocol):
    """Parameter to order by, specifying ascending or descending."""

    @property
    def order_by(self) -> InputOrderByParameter:
        """Parameter to order results by."""
        raise NotImplementedError

    @property
    def descending(self) -> bool:
        """Indicates if the order should be ascending or descending."""
        raise NotImplementedError

    def query_resolver_input(  # noqa: D102
        self, semantic_manifest_lookup: SemanticManifestLookup
    ) -> ResolverInputForOrderByItem:
        raise NotImplementedError


class SavedQueryParameter(Protocol):
    """Name of the saved query to execute."""

    @property
    def name(self) -> str:  # noqa: D102
        raise NotImplementedError
