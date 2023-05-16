from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from dbt_semantic_interfaces.objects.metric import MetricType, MetricTypeParams, Metric as PydanticMetric
from dbt_semantic_interfaces.objects.filters.where_filter import WhereFilter
from dbt_semantic_interfaces.objects.metadata import Metadata
from dbt_semantic_interfaces.objects.elements.dimension import Dimension


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
