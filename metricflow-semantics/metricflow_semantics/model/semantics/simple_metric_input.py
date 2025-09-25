from __future__ import annotations

import logging
from functools import cached_property
from typing import Iterable, Optional, Sequence

from dbt_semantic_interfaces.protocols import (
    MeasureAggregationParameters,
    NonAdditiveDimensionParameters,
    WhereFilterIntersection,
)
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.type_enums import AggregationType, TimeGranularity

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class SimpleMetricInputAggregation:
    """See `SimpleMetricInput`."""

    percentile: Optional[float] = None
    use_discrete_percentile: bool = False
    use_approximate_percentile: bool = False

    @staticmethod
    def create_from_measure_aggregation(  # noqa: D102
        measure_aggregation: Optional[MeasureAggregationParameters],
    ) -> Optional[SimpleMetricInputAggregation]:
        if measure_aggregation is None:
            return None
        return SimpleMetricInputAggregation(
            percentile=measure_aggregation.percentile,
            use_discrete_percentile=measure_aggregation.use_discrete_percentile,
            use_approximate_percentile=measure_aggregation.use_approximate_percentile,
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
    """Indirection class used for `measure -> simple metric` migration to represent how a simple metric is constructed.

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
    filter: WhereFilterIntersection

    @cached_property
    def metric_reference(self) -> MetricReference:  # noqa: D102
        return MetricReference(self.name)

    def match_agg_time_dimension_name(
        self, time_dimension_specs: Iterable[TimeDimensionSpec]
    ) -> Sequence[TimeDimensionSpec]:
        """Return only the time dimension specs that have the same aggregation time dimension name as this input."""
        return tuple(
            time_dimension_spec
            for time_dimension_spec in time_dimension_specs
            if time_dimension_spec.element_name == self.agg_time_dimension_name
        )
