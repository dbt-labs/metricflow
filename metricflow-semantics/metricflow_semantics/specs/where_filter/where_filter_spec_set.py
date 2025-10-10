from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Sized, Tuple

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from typing_extensions import override

from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.toolkit.merger import Mergeable


@dataclass(frozen=True)
class WhereFilterSpecSet(SerializableDataclass, Mergeable, Sized):
    """Class to encapsulate filters needed at a certain point of the queried metric.

    This class splits up the filters based on where it was defined at, which can then be used to
    determine where each filter should be applied during each step of the metric building process.

    measure-level: filters defined on the input_measure
    metric-level: filters defined on the input_metric or metric:filter
    query-level: filters defined at query time
    """

    metric_level_filter_specs: Tuple[WhereFilterSpec, ...] = ()
    query_level_filter_specs: Tuple[WhereFilterSpec, ...] = ()

    @cached_property
    def all_filter_specs(self) -> Tuple[WhereFilterSpec, ...]:
        """Returns all the filters in this class.

        Generally, before measure aggregation, all filters should be applied.
        """
        return self.metric_level_filter_specs + self.query_level_filter_specs

    def merge(self, other: WhereFilterSpecSet) -> WhereFilterSpecSet:
        """Merge 2 WhereFilterSpecSet together."""
        return WhereFilterSpecSet(
            metric_level_filter_specs=self.metric_level_filter_specs + other.metric_level_filter_specs,
            query_level_filter_specs=self.query_level_filter_specs + other.query_level_filter_specs,
        )

    @classmethod
    @override
    def empty_instance(cls) -> WhereFilterSpecSet:
        return WhereFilterSpecSet()

    @cached_property
    def _spec_count(self) -> int:
        return sum(
            len(specs)
            for specs in (
                self.metric_level_filter_specs,
                self.query_level_filter_specs,
            )
        )

    @override
    def __len__(self) -> int:
        return self._spec_count
