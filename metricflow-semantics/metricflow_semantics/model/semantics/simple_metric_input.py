from __future__ import annotations

import logging
from typing import Optional

from dbt_semantic_interfaces.protocols import Measure
from dbt_semantic_interfaces.type_enums import AggregationType

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class SimpleMetricInputAggregation:
    """Indirection class used for measure -> simple metric migration."""

    percentile: Optional[float] = None
    use_discrete_percentile: bool = False
    use_approximate_percentile: bool = False


@fast_frozen_dataclass()
class SimpleMetricInputNonAdditiveDimension:
    """Indirection class used for measure -> simple metric migration."""

    name: str
    window_choice: AggregationType = AggregationType.MIN
    window_groupings: AnyLengthTuple[str] = ()


@fast_frozen_dataclass()
class SimpleMetricInput:
    """Indirection class used for measure -> simple metric migration."""

    name: str
    agg: AggregationType
    expr: Optional[str] = None
    agg_params: Optional[SimpleMetricInputAggregation] = None
    non_additive_dimension: Optional[SimpleMetricInputNonAdditiveDimension] = None
    agg_time_dimension: Optional[str] = None

    @staticmethod
    def create_from_measure(measure: Measure) -> SimpleMetricInput:  # noqa: D102
        simple_metric_input_non_additive_dimension: Optional[SimpleMetricInputNonAdditiveDimension] = None
        measure_non_additive_dimension = measure.non_additive_dimension
        if measure_non_additive_dimension is not None:
            simple_metric_input_non_additive_dimension = SimpleMetricInputNonAdditiveDimension(
                name=measure_non_additive_dimension.name,
                window_choice=measure_non_additive_dimension.window_choice,
                window_groupings=tuple(measure_non_additive_dimension.window_groupings),
            )

        simple_metric_agg_params: Optional[SimpleMetricInputAggregation] = None
        measure_agg_params = measure.agg_params
        if measure_agg_params is not None:
            simple_metric_agg_params = SimpleMetricInputAggregation(
                percentile=measure_agg_params.percentile,
                use_discrete_percentile=measure_agg_params.use_discrete_percentile,
                use_approximate_percentile=measure_agg_params.use_approximate_percentile,
            )
        return SimpleMetricInput(
            name=measure.name,
            agg=measure.agg,
            expr=measure.expr,
            agg_params=simple_metric_agg_params,
            non_additive_dimension=simple_metric_input_non_additive_dimension,
        )

@fast_frozen_dataclass()
class SimpleMetricInputReference:
    """Indirection class used for measure -> simple metric migration."""

    element_name: str
