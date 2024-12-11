from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimensionTypeParams
from dbt_semantic_interfaces.protocols.dimension import (
    Dimension as SemanticManifestDimension,
)
from dbt_semantic_interfaces.protocols.dimension import (
    DimensionType,
    DimensionTypeParams,
)
from dbt_semantic_interfaces.protocols.entity import Entity as SemanticManifestEntity
from dbt_semantic_interfaces.protocols.export import Export
from dbt_semantic_interfaces.protocols.measure import MeasureAggregationParameters
from dbt_semantic_interfaces.protocols.metadata import Metadata
from dbt_semantic_interfaces.protocols.metric import Metric as SemanticManifestMetric
from dbt_semantic_interfaces.protocols.metric import (
    MetricInputMeasure,
    MetricType,
    MetricTypeParams,
    SemanticLayerElementConfig,
)
from dbt_semantic_interfaces.protocols.saved_query import (
    SavedQuery as SemanticManifestSavedQuery,
)
from dbt_semantic_interfaces.protocols.saved_query import (
    SavedQueryQueryParams,
)
from dbt_semantic_interfaces.protocols.where_filter import WhereFilterIntersection
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.transformations.add_input_metric_measures import AddInputMetricMeasuresRule
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.type_enums.entity_type import EntityType
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.specs.dimension_spec import DimensionSpec


@dataclass(frozen=True)
class Metric:
    """Dataclass representation of a Metric."""

    name: str
    description: Optional[str]
    type: MetricType
    type_params: MetricTypeParams
    filter: Optional[WhereFilterIntersection]
    metadata: Optional[Metadata]
    dimensions: List[Dimension]
    label: Optional[str]
    config: Optional[SemanticLayerElementConfig]

    @classmethod
    def from_pydantic(cls, pydantic_metric: SemanticManifestMetric, dimensions: List[Dimension]) -> Metric:
        """Build from pydantic Metric object and list of Dimensions."""
        return cls(
            name=pydantic_metric.name,
            description=pydantic_metric.description,
            type=pydantic_metric.type,
            type_params=pydantic_metric.type_params,
            filter=pydantic_metric.filter,
            metadata=pydantic_metric.metadata,
            dimensions=dimensions,
            label=pydantic_metric.label,
            config=pydantic_metric.config,
        )

    @property
    def input_measures(self) -> Sequence[MetricInputMeasure]:
        """Return the complete list of input measure configurations for this metric."""
        assert self.type_params.input_measures, (
            f"Metric {self.name} should have had input_measures populated by "
            f"{AddInputMetricMeasuresRule.__class__.__name__}"
        )
        return self.type_params.input_measures


@dataclass(frozen=True)
class Dimension:
    """Dataclass representation of a Dimension."""

    name: str
    qualified_name: str
    description: Optional[str]
    type: DimensionType
    entity_links: Tuple[EntityReference, ...]
    type_params: Optional[DimensionTypeParams]
    metadata: Optional[Metadata]
    config: Optional[SemanticLayerElementConfig] = None
    is_partition: bool = False
    expr: Optional[str] = None
    label: Optional[str] = None

    @classmethod
    def from_pydantic(
        cls,
        pydantic_dimension: SemanticManifestDimension,
        entity_links: Tuple[EntityReference, ...],
    ) -> Dimension:
        """Build from pydantic Dimension and entity_key."""
        qualified_name = DimensionSpec(element_name=pydantic_dimension.name, entity_links=entity_links).qualified_name
        parsed_type_params: Optional[DimensionTypeParams] = None
        if pydantic_dimension.type_params:
            parsed_type_params = PydanticDimensionTypeParams(
                time_granularity=pydantic_dimension.type_params.time_granularity,
                validity_params=pydantic_dimension.type_params.validity_params,
            )
        return cls(
            name=pydantic_dimension.name,
            qualified_name=qualified_name,
            description=pydantic_dimension.description,
            type=pydantic_dimension.type,
            type_params=parsed_type_params,
            metadata=pydantic_dimension.metadata,
            config=pydantic_dimension.config,
            is_partition=pydantic_dimension.is_partition,
            expr=pydantic_dimension.expr,
            label=pydantic_dimension.label,
            entity_links=entity_links,
        )

    @property
    def granularity_free_qualified_name(self) -> str:
        """Renders the qualified name without the granularity suffix.

        In the list metrics and list dimensions outputs we want to render the qualified name of the dimension, but
        without including the base granularity for time dimensions. This method is useful in those contexts.

        Note: in most cases you should be using the qualified_name - this is only useful in cases where the
        Dimension set has de-duplicated TimeDimensions such that you never have more than one granularity
        in your set for each TimeDimension.
        """
        return StructuredLinkableSpecName(
            entity_link_names=tuple(e.element_name for e in self.entity_links), element_name=self.name
        ).qualified_name


@dataclass(frozen=True)
class Entity:
    """Dataclass representation of a Entity."""

    name: str
    description: Optional[str]
    type: EntityType
    role: Optional[str]
    config: Optional[SemanticLayerElementConfig] = None
    expr: Optional[str] = None

    @classmethod
    def from_pydantic(cls, pydantic_entity: SemanticManifestEntity) -> Entity:
        """Build from pydantic Entity."""
        return cls(
            name=pydantic_entity.name,
            description=pydantic_entity.description,
            type=pydantic_entity.type,
            role=pydantic_entity.role,
            config=pydantic_entity.config,
            expr=pydantic_entity.expr,
        )


@dataclass(frozen=True)
class Measure:
    """Dataclass representation of a Measure."""

    name: str
    agg: AggregationType
    agg_time_dimension: str
    config: Optional[SemanticLayerElementConfig] = None
    description: Optional[str] = None
    expr: Optional[str] = None
    agg_params: Optional[MeasureAggregationParameters] = None


@dataclass(frozen=True)
class SavedQuery:
    """Dataclass representation of a SavedQuery."""

    name: str
    description: Optional[str]
    label: Optional[str]
    query_params: SavedQueryQueryParams
    metadata: Optional[Metadata]
    exports: Sequence[Export]
    tags: Sequence[str]

    @classmethod
    def from_pydantic(cls, pydantic_saved_query: SemanticManifestSavedQuery) -> SavedQuery:
        """Build from pydantic SavedQuery object."""
        return cls(
            name=pydantic_saved_query.name,
            description=pydantic_saved_query.description,
            label=pydantic_saved_query.label,
            query_params=pydantic_saved_query.query_params,
            metadata=pydantic_saved_query.metadata,
            exports=pydantic_saved_query.exports,
            tags=pydantic_saved_query.tags,
        )
