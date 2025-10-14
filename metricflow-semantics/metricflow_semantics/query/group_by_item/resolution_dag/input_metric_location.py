from __future__ import annotations

from dataclasses import dataclass

from dbt_semantic_interfaces.protocols import MetricInput
from dbt_semantic_interfaces.references import MetricReference

from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat


@dataclass(frozen=True)
class InputMetricDefinitionLocation:
    """Describes the location of input metric of a derived metric."""

    derived_metric_reference: MetricReference
    input_metric_list_index: int

    def get_metric_input(self, metric_lookup: MetricLookup) -> MetricInput:
        """Get the associated MetricInput object that this describes."""
        metric = metric_lookup.get_metric(self.derived_metric_reference)
        metric_inputs = MetricLookup.metric_inputs(metric, include_conversion_metric_input=False)
        if self.input_metric_list_index >= len(metric_inputs):
            raise ValueError(
                LazyFormat(
                    "The metric-input list index is invalid for the given metric",
                    metric=metric,
                    input_metric_list_index=self.input_metric_list_index,
                )
            )

        return metric_inputs[self.input_metric_list_index]
