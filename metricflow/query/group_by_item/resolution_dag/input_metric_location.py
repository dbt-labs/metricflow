from __future__ import annotations

from dataclasses import dataclass

from dbt_semantic_interfaces.protocols import MetricInput
from dbt_semantic_interfaces.references import MetricReference

from metricflow.model.semantics.metric_lookup import MetricLookup


@dataclass(frozen=True)
class InputMetricDefinitionLocation:
    """Describes the location of input metric of a derived metric."""

    derived_metric_reference: MetricReference
    input_metric_list_index: int

    def get_metric_input(self, metric_lookup: MetricLookup) -> MetricInput:
        """Get the associated MetricInput object that this describes."""
        metric = metric_lookup.get_metric(self.derived_metric_reference)
        if metric.input_metrics is None or self.input_metric_list_index >= len(metric.input_metrics):
            raise ValueError(f"The metric input index is invalid for metric: {metric}")

        return metric.input_metrics[self.input_metric_list_index]
