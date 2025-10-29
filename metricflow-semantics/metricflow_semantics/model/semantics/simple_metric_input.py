from __future__ import annotations

import logging
from functools import cached_property
from typing import Optional

from dbt_semantic_interfaces.protocols import (
    MeasureAggregationParameters,
    NonAdditiveDimensionParameters,
    WhereFilterIntersection,
)
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.type_enums import AggregationType, TimeGranularity

from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class SimpleMetricInputAggregation:
    """See `SimpleMetricInput`."""

    percentile: Optional[float] = None
    use_discrete_percentile: bool = False
    use_approximate_percentile: bool = False

    @staticmethod
    def create_from_pydantic(  # noqa: D102
        aggregation: Optional[MeasureAggregationParameters],
    ) -> Optional[SimpleMetricInputAggregation]:
        if aggregation is None:
            return None
        return SimpleMetricInputAggregation(
            percentile=aggregation.percentile,
            use_discrete_percentile=aggregation.use_discrete_percentile,
            use_approximate_percentile=aggregation.use_approximate_percentile,
        )


@fast_frozen_dataclass()
class SimpleMetricInputNonAdditiveDimension:
    """See `SimpleMetricInput`."""

    name: str
    window_choice: AggregationType
    window_groupings: AnyLengthTuple[str]

    @staticmethod
    def create_from_non_additive_dimension(  # noqa: D102
        non_additive_dimension: Optional[NonAdditiveDimensionParameters],
    ) -> Optional[SimpleMetricInputNonAdditiveDimension]:
        if non_additive_dimension is None:
            return None

        return SimpleMetricInputNonAdditiveDimension(
            name=non_additive_dimension.name,
            window_choice=non_additive_dimension.window_choice,
            window_groupings=tuple(non_additive_dimension.window_groupings),
        )


@fast_frozen_dataclass()
class SimpleMetricInput:
    """Represents the input arguments to construct a simple metric.

    This class should contain all relevant values from the semantic manifest for a simple metric. i.e. use this
    instead of fetching the `Metric` from the manifest.

    These fields are based on the current Pydantic classes, but they can be better organized / consolidated.
    """

    name: str
    agg: AggregationType
    expr: str
    agg_params: Optional[SimpleMetricInputAggregation]
    non_additive_dimension: Optional[SimpleMetricInputNonAdditiveDimension]
    agg_time_dimension_name: str
    agg_time_dimension_grain: TimeGranularity
    model_id: SemanticModelId
    join_to_timespine: bool
    fill_nulls_with: Optional[int]
    # This is the filter in the definition of the simple metric.
    filter: WhereFilterIntersection

    @cached_property
    def metric_reference(self) -> MetricReference:  # noqa: D102
        return MetricReference(self.name)
