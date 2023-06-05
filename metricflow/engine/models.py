from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from dbt_semantic_interfaces.protocols.dimension import (
    Dimension as SemanticManifestDimension,
)
from dbt_semantic_interfaces.protocols.dimension import (
    DimensionType,
    DimensionTypeParams,
)
from dbt_semantic_interfaces.protocols.metadata import Metadata
from dbt_semantic_interfaces.protocols.metric import Metric as SemanticManifestMetric
from dbt_semantic_interfaces.protocols.metric import MetricInputMeasure, MetricType, MetricTypeParams
from dbt_semantic_interfaces.protocols.where_filter import WhereFilter

from metricflow.model.semantics.linkable_spec_resolver import ElementPathKey
from metricflow.specs.specs import DimensionSpec, EntityReference


@dataclass(frozen=True)
class Metric:
    """Dataclass representation of a Metric."""

    name: str
    description: Optional[str]
    type: MetricType
    type_params: MetricTypeParams
    filter: Optional[WhereFilter]
    metadata: Optional[Metadata]
    dimensions: List[Dimension]

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
        )

    @property
    def input_measures(self) -> List[MetricInputMeasure]:
        """Return the complete list of input measure configurations for this metric."""
        type_params = self.type_params

        measures: List[MetricInputMeasure] = list(
            type_params.measures if type_params is not None and type_params.measures is not None else []
        )
        if type_params.measure:
            measures.append(type_params.measure)
        if type_params.numerator:
            measures.append(type_params.numerator)
        if type_params.denominator:
            measures.append(type_params.denominator)
        return measures


@dataclass(frozen=True)
class Dimension:
    """Dataclass representation of a Dimension."""

    name: str
    qualified_name: str
    description: Optional[str]
    type: DimensionType
    type_params: Optional[DimensionTypeParams]
    metadata: Optional[Metadata]
    is_partition: bool = False
    expr: Optional[str] = None

    @classmethod
    def from_pydantic(cls, pydantic_dimension: SemanticManifestDimension, path_key: ElementPathKey) -> Dimension:
        """Build from pydantic Dimension and entity_key."""
        qualified_name = DimensionSpec(
            element_name=path_key.element_name,
            entity_links=tuple(EntityReference(element_name=x) for x in path_key.entity_links),
        ).qualified_name
        return cls(
            name=pydantic_dimension.name,
            qualified_name=qualified_name,
            description=pydantic_dimension.description,
            type=pydantic_dimension.type,
            type_params=pydantic_dimension.type_params,
            metadata=pydantic_dimension.metadata,
            is_partition=pydantic_dimension.is_partition,
            expr=pydantic_dimension.expr,
        )
