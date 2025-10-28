from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, Sequence

from dbt_semantic_interfaces.references import TimeDimensionReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.model.semantics.simple_metric_input import SimpleMetricInput
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.specs.non_additive_dimension_spec import NonAdditiveDimensionSpec
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.syntactic_sugar import mf_first_item


@dataclass(frozen=True)
class SimpleMetricInputSpecProperties:
    """Input dataclass for grouping properties of a sequence of `SimpleMetricInputSpec`s."""

    simple_metric_input_specs: Sequence[SimpleMetricInputSpec]
    semantic_model_name: str
    agg_time_dimension: TimeDimensionReference
    agg_time_dimension_grain: TimeGranularity
    non_additive_dimension_spec: Optional[NonAdditiveDimensionSpec] = None

    @staticmethod
    def create_from_simple_metric_inputs(  # noqa: D102
        simple_metric_inputs: Sequence[SimpleMetricInput],
    ) -> SimpleMetricInputSpecProperties:
        if len(simple_metric_inputs) == 0:
            raise ValueError("No simple-metric inputs provided")

        key_to_inputs: defaultdict[_SimpleMetricInputGroupingKey, list[SimpleMetricInput]] = defaultdict(list)
        for simple_metric_input in simple_metric_inputs:
            key_to_inputs[_SimpleMetricInputGroupingKey.create_from_simple_metric_input(simple_metric_input)].append(
                simple_metric_input
            )
        if len(key_to_inputs) > 1:
            raise ValueError(
                LazyFormat(
                    "The given simple-metric inputs do not have the same grouping key", key_to_inputs=key_to_inputs
                )
            )
        common_key = mf_first_item(key_to_inputs)

        return SimpleMetricInputSpecProperties(
            simple_metric_input_specs=tuple(
                SimpleMetricInputSpec(
                    element_name=simple_metric_input.name,
                )
                for simple_metric_input in simple_metric_inputs
            ),
            semantic_model_name=common_key.model_id.model_name,
            agg_time_dimension=TimeDimensionReference(common_key.agg_time_dimension_name),
            agg_time_dimension_grain=common_key.agg_time_dimension_grain,
            non_additive_dimension_spec=common_key.non_additive_dimension_spec,
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
