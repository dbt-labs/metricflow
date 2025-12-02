from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.references import TimeDimensionReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.model.semantics.simple_metric_input import SimpleMetricInput
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.specs.non_additive_dimension_spec import NonAdditiveDimensionSpec
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass


@dataclass(frozen=True)
class SimpleMetricInputSpecProperties:
    """Input dataclass for grouping properties of a sequence of `SimpleMetricInputSpec`s."""

    simple_metric_input_spec: SimpleMetricInputSpec
    semantic_model_name: str
    agg_time_dimension: TimeDimensionReference
    agg_time_dimension_grain: TimeGranularity
    non_additive_dimension_spec: Optional[NonAdditiveDimensionSpec] = None

    @staticmethod
    def create_from_simple_metric_input(  # noqa: D102
        simple_metric_input: SimpleMetricInput,
    ) -> SimpleMetricInputSpecProperties:
        return SimpleMetricInputSpecProperties(
            simple_metric_input_spec=SimpleMetricInputSpec(
                element_name=simple_metric_input.name,
            ),
            semantic_model_name=simple_metric_input.model_id.model_name,
            agg_time_dimension=TimeDimensionReference(simple_metric_input.agg_time_dimension_name),
            agg_time_dimension_grain=simple_metric_input.agg_time_dimension_grain,
            non_additive_dimension_spec=NonAdditiveDimensionSpec.create_from_simple_metric_input(simple_metric_input),
        )


@fast_frozen_dataclass()
class _SimpleMetricInputGroupingKey:
    model_id: SemanticModelId
    agg_time_dimension_name: str
    agg_time_dimension_grain: TimeGranularity
    non_additive_dimension_spec: Optional[NonAdditiveDimensionSpec]

    @staticmethod
    def create_from_simple_metric_input(  # noqa: D102
        simple_metric_input: SimpleMetricInput,
    ) -> _SimpleMetricInputGroupingKey:
        return _SimpleMetricInputGroupingKey(
            model_id=simple_metric_input.model_id,
            agg_time_dimension_name=simple_metric_input.agg_time_dimension_name,
            agg_time_dimension_grain=simple_metric_input.agg_time_dimension_grain,
            non_additive_dimension_spec=NonAdditiveDimensionSpec.create_from_simple_metric_input(simple_metric_input),
        )
