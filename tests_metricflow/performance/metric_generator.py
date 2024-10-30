from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricInput,
    PydanticMetricInputMeasure,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.type_enums import MetricType

from tests_metricflow.performance.measure_generator import MeasureGenerator
from tests_metricflow.performance.synthetic_manifest_parameter_set import SyntheticManifestParameterSet


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
        self, parameter_set: SyntheticManifestParameterSet, measure_generator: MeasureGenerator
    ) -> None:
        self._parameter_set = parameter_set
        self._measure_generator = measure_generator

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
                    measure=PydanticMetricInputMeasure(
                        name=self._measure_generator.get_measure_name(
                            measure_index=metric_index.width_index % self._measure_generator.unique_measure_count
                        )
                    )
                ),
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
                    metrics=[PydanticMetricInput(name=input_metric_name) for input_metric_name in input_metric_names],
                    expr=" + ".join(input_metric_names),
                ),
            )
