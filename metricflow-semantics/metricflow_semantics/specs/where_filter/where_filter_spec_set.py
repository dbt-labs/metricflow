from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass

from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec


@dataclass(frozen=True)
class WhereFilterSpecSet(SerializableDataclass):
    """Class to encapsulate filters needed at a certain point of the queried metric.

    This class splits up the filters based on where it was defined at, which can then be used to
    determine where each filter should be applied during each step of the metric building process.

    measure-level: filters defined on the input_measure
    metric-level: filters defined on the input_metric or metric:filter
    query-level: filters defined at query time
    """

    measure_level_filter_specs: Tuple[WhereFilterSpec, ...] = ()
    metric_level_filter_specs: Tuple[WhereFilterSpec, ...] = ()
    query_level_filter_specs: Tuple[WhereFilterSpec, ...] = ()

    @property
    def after_measure_aggregation_filter_specs(self) -> Tuple[WhereFilterSpec, ...]:
        """Returns filters relevant to post-measure aggregation."""
        return self.metric_level_filter_specs + self.query_level_filter_specs

    @property
    def all_filter_specs(self) -> Tuple[WhereFilterSpec, ...]:
        """Returns all the filters in this class.

        Generally, before measure aggregation, all filters should be applied.
        """
        return self.measure_level_filter_specs + self.metric_level_filter_specs + self.query_level_filter_specs

    def merge(self, other: WhereFilterSpecSet) -> WhereFilterSpecSet:
        """Merge 2 WhereFilterSpecSet together."""
        return WhereFilterSpecSet(
            measure_level_filter_specs=self.measure_level_filter_specs + other.measure_level_filter_specs,
            metric_level_filter_specs=self.metric_level_filter_specs + other.metric_level_filter_specs,
            query_level_filter_specs=self.query_level_filter_specs + other.query_level_filter_specs,
        )
