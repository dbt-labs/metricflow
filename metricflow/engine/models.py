from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from dbt_semantic_interfaces.implementations.elements.dimension import PydanticDimension, PydanticDimensionTypeParams
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
)
from dbt_semantic_interfaces.implementations.metadata import PydanticMetadata
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricInputMeasure,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.type_enums.dimension_type import DimensionType
from dbt_semantic_interfaces.type_enums.metric_type import MetricType
from metricflow.model.semantics.linkable_spec_resolver import ElementPathKey
from metricflow.specs.specs import DimensionSpec, EntityReference


@dataclass(frozen=True)
class Metric:
    """Dataclass representation of a Metric."""

    name: str
    description: Optional[str]
    type: MetricType
    type_params: PydanticMetricTypeParams
    filter: Optional[PydanticWhereFilter]
    metadata: Optional[PydanticMetadata]
    dimensions: List[Dimension]

    @classmethod
    def from_pydantic(cls, pydantic_metric: PydanticMetric, dimensions: List[Dimension]) -> Metric:
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
    def input_measures(self) -> List[PydanticMetricInputMeasure]:
        """Return the complete list of input measure configurations for this metric."""
        type_params = self.type_params

        measures: List[PydanticMetricInputMeasure] = list(
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
    type_params: Optional[PydanticDimensionTypeParams]
    metadata: Optional[PydanticMetadata]
    is_partition: bool = False
    expr: Optional[str] = None

    @classmethod
    def from_pydantic(cls, pydantic_dimension: PydanticDimension, path_key: ElementPathKey) -> Dimension:
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
