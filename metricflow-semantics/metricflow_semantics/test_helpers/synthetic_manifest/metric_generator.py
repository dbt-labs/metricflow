from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricAggregationParams,
    PydanticMetricInput,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.type_enums import AggregationType, MetricType

from metricflow_semantics.test_helpers.synthetic_manifest.simple_metric_semantic_model_generator import (
    SimpleMetricSemanticModelGenerator,
)
from metricflow_semantics.test_helpers.synthetic_manifest.synthetic_manifest_parameter_set import (
    SyntheticManifestParameterSet,
)


@dataclass(frozen=True)
class MetricIndex:
    """Index for a generated metric in the semantic manifest.

    Since metrics can be defined through other metrics, the `depth_index` describes the number of hops between a metric
    and the simple metrics that it is based on when looking at the definition tree of a metric.

    For example:
    * `depth_index=0` describes a simple metric that does not depend on any other metrics.
    * `depth_index=1` describes a derived metric that is defined using metrics at `depth_index=0`. i.e. a derived metric
    based on simple metrics.
    * `depth_index=n` describes a derived metric that is defined using metrics at `depth_index=n-1`.

    The `width_index` enumerates the nth metric generated for the given depth (name needs improvement).
    """

    depth_index: int
    width_index: int

    def __post_init__(self) -> None:  # noqa: D105
        if self.depth_index < 0:
            raise ValueError(f"{self.depth_index=} should be >= 0")
        if self.width_index < 0:
            raise ValueError(f"{self.width_index=} should be >=0")


class MetricGenerator:
    """Helps generate metrics for the synthetic manifest."""

    def __init__(  # noqa: D107
        self, parameter_set: SyntheticManifestParameterSet, semantic_model_generator: SimpleMetricSemanticModelGenerator
    ) -> None:
        self._parameter_set = parameter_set
        self._semantic_model_generator = semantic_model_generator

    def generate_metrics(self) -> Sequence[PydanticMetric]:  # noqa: D102
        metrics = []
        for depth_index in range(self._parameter_set.max_metric_depth):
            for width_index in range(self._parameter_set.max_metric_width):
                metrics.append(self._generate_metric(MetricIndex(depth_index=depth_index, width_index=width_index)))

        return metrics

    def get_first_index_at_max_depth(self) -> MetricIndex:
        """For the highest possible metric depth in the semantic manifest, return the index of the first metric."""
        return MetricIndex(
            depth_index=self._parameter_set.max_metric_depth - 1,
            width_index=0,
        )

    def get_next_wrapped_width_index(self, metric_index: MetricIndex) -> MetricIndex:
        """Return the index of the next metric at the same depth level."""
        return MetricIndex(
            depth_index=metric_index.depth_index,
            width_index=(metric_index.width_index + 1) % self._parameter_set.max_metric_width,
        )

    def get_metric_name(self, index: MetricIndex) -> str:  # noqa: D102
        return f"metric_{index.depth_index}_{index.width_index:03}"

    def _metric_indexes_at_depth(self, depth_index: int) -> Sequence[MetricIndex]:
        return tuple(
            MetricIndex(depth_index=depth_index, width_index=width_index)
            for width_index in range(self._parameter_set.max_metric_width)
        )

    def _generate_metric(self, metric_index: MetricIndex) -> PydanticMetric:
        if metric_index.depth_index == 0:
            return PydanticMetric(
                name=self.get_metric_name(metric_index),
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(
                    measure=None,
                    numerator=None,
                    denominator=None,
                    expr=None,
                    window=None,
                    grain_to_date=None,
                    metrics=None,
                    conversion_type_params=None,
                    cumulative_type_params=None,
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        agg=AggregationType.SUM,
                        agg_params=None,
                        agg_time_dimension="ds",
                        non_additive_dimension=None,
                        semantic_model=self._semantic_model_generator.get_semantic_model_name(
                            int(metric_index.width_index / self._parameter_set.simple_metrics_per_semantic_model)
                            % self._parameter_set.simple_metric_semantic_model_count
                        ),
                    ),
                ),
                description=None,
                filter=None,
                metadata=None,
                config=None,
            )
        else:
            input_metric_names = tuple(
                self.get_metric_name(lower_depth_metric_index)
                for lower_depth_metric_index in self._metric_indexes_at_depth(metric_index.depth_index - 1)
            )
            return PydanticMetric(
                name=self.get_metric_name(metric_index),
                type=MetricType.DERIVED,
                type_params=PydanticMetricTypeParams(
                    metrics=[
                        PydanticMetricInput(
                            name=input_metric_name, filter=None, alias=None, offset_window=None, offset_to_grain=None
                        )
                        for input_metric_name in input_metric_names
                    ],
                    expr=" + ".join(input_metric_names),
                ),
            )
