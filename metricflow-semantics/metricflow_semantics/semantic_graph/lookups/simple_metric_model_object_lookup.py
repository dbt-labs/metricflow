from __future__ import annotations

import logging
from collections import defaultdict
from functools import cached_property
from typing import Iterable, Mapping, Optional, Sequence

from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.protocols import Metric, SemanticModel
from dbt_semantic_interfaces.type_enums import DimensionType, MetricType, TimeGranularity
from typing_extensions import override

from metricflow_semantics.errors.error_classes import InvalidManifestException, MetricFlowInternalError
from metricflow_semantics.model.semantics.simple_metric_input import (
    SimpleMetricInput,
    SimpleMetricInputAggregation,
    SimpleMetricInputNonAdditiveDimension,
)
from metricflow_semantics.semantic_graph.lookups.model_object_lookup import ModelObjectLookup
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.attribute_pretty_format import AttributeMapping
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class SimpleMetricAggregationConfiguration:
    """Key that is used to group the simple metrics in a semantic model by the associated aggregation time dimension."""

    time_dimension_name: str
    time_grain: TimeGranularity


class SimpleMetricModelObjectLookup(ModelObjectLookup):
    """A lookup for models associated with simple metrics.

    A separate lookup class helps to break out the lookup classes and provide better typing (fewer `None` cases).
    """

    def __init__(  # noqa: D107
        self, semantic_model: SemanticModel, simple_metrics: Optional[Sequence[Metric]] = None
    ) -> None:
        super().__init__(semantic_model)
        model_name = semantic_model.name
        # Sanity checks.
        if not simple_metrics:
            raise MetricFlowInternalError(
                LazyFormat(
                    "Can't initialize with empty `simple_metrics`",
                    simple_metrics=simple_metrics,
                )
            )
        for metric in simple_metrics:
            if metric.type is not MetricType.SIMPLE:
                raise MetricFlowInternalError(
                    LazyFormat("Can't initialize with a metric that is not a simple metric", metric=metric)
                )
            metric_aggregation_params = metric.type_params.metric_aggregation_params
            if metric_aggregation_params is None or metric_aggregation_params.semantic_model != model_name:
                raise MetricFlowInternalError(
                    LazyFormat(
                        "Can't initialize with a metric that is not associated with this semantic model",
                        metric=metric,
                        model_name=model_name,
                    )
                )
        self._simple_metrics = simple_metrics

    @cached_property
    def _simple_metric_inputs_from_metrics(self) -> Sequence[SimpleMetricInput]:
        simple_metric_inputs: list[SimpleMetricInput] = []
        for metric in self._simple_metrics:
            metric_type_params = metric.type_params
            metric_aggregation_params = metric.type_params.metric_aggregation_params
            assert metric_aggregation_params is not None, "`metric_aggregation_params` should be set for all metrics"

            agg_time_dimension_name = metric_aggregation_params.agg_time_dimension or (
                self._semantic_model.defaults.agg_time_dimension if self._semantic_model.defaults is not None else None
            )
            agg_time_dimension_grain = (
                self._time_dimension_name_to_grain.get(agg_time_dimension_name)
                if agg_time_dimension_name is not None
                else None
            )
            if agg_time_dimension_name is None or agg_time_dimension_grain is None:
                raise InvalidManifestException(
                    LazyFormat(
                        "Invalid aggregation time dimension configuration",
                        metric=metric,
                        semantic_model=self._semantic_model,
                    )
                )

            simple_metric_input = SimpleMetricInput(
                name=metric.name,
                agg=metric_aggregation_params.agg,
                expr=metric_type_params.expr or metric.name,
                agg_params=SimpleMetricInputAggregation.create_from_pydantic(metric_aggregation_params.agg_params),
                join_to_timespine=metric.type_params.join_to_timespine,
                fill_nulls_with=metric.type_params.fill_nulls_with,
                non_additive_dimension=SimpleMetricInputNonAdditiveDimension.create_from_non_additive_dimension(
                    metric_aggregation_params.non_additive_dimension
                ),
                agg_time_dimension_name=agg_time_dimension_name,
                agg_time_dimension_grain=agg_time_dimension_grain,
                model_id=self.model_id,
                filter=metric.filter or PydanticWhereFilterIntersection(where_filters=[]),
            )
            simple_metric_inputs.append(simple_metric_input)

        return simple_metric_inputs

    @cached_property
    def aggregation_time_dimension_name_to_simple_metric_inputs(self) -> Mapping[str, Iterable[SimpleMetricInput]]:
        """Mapping from the name of the aggregation time dimension to the `SimpleMetricInputs` for this model."""
        current_aggregation_time_dimension_name_to_simple_metric_inputs: dict[
            str, list[SimpleMetricInput]
        ] = defaultdict(list)

        for simple_metric_input in self._simple_metric_inputs_from_metrics:
            current_aggregation_time_dimension_name_to_simple_metric_inputs[
                simple_metric_input.agg_time_dimension_name
            ].append(simple_metric_input)

        return current_aggregation_time_dimension_name_to_simple_metric_inputs

    @cached_property
    def aggregation_configuration_to_simple_metric_inputs(
        self,
    ) -> Mapping[SimpleMetricAggregationConfiguration, Iterable[SimpleMetricInput]]:
        """Mapping from the aggregation configuration to the `SimpleMetricInputs` for this model."""
        current_aggregation_configuration_to_simple_metric_inputs: defaultdict[
            SimpleMetricAggregationConfiguration, list[SimpleMetricInput]
        ] = defaultdict(list)

        for simple_metric_input in self._simple_metric_inputs_from_metrics:
            aggregation_configuration = SimpleMetricAggregationConfiguration(
                time_dimension_name=simple_metric_input.agg_time_dimension_name,
                time_grain=simple_metric_input.agg_time_dimension_grain,
            )

            current_aggregation_configuration_to_simple_metric_inputs[aggregation_configuration].append(
                simple_metric_input
            )

        return current_aggregation_configuration_to_simple_metric_inputs

    @cached_property
    def _time_dimension_name_to_grain(self) -> Mapping[str, TimeGranularity]:
        return {
            dimension.name: dimension.type_params.time_granularity
            for dimension in self._semantic_model.dimensions
            if (dimension.type is DimensionType.TIME and dimension.type_params is not None)
        }

    @cached_property
    @override
    def _attribute_mapping(self) -> AttributeMapping:
        return dict(
            **super()._attribute_mapping,
            **{
                "aggregation_configuration_to_simple_metric_inputs": {
                    configuration: [simple_metric_input.name for simple_metric_input in simple_metric_inputs]
                    for configuration, simple_metric_inputs in self.aggregation_configuration_to_simple_metric_inputs.items()
                }
            },
        )
