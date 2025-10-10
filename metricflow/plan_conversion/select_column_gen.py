from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import cached_property
from typing import Iterable

from metricflow_semantics.toolkit.merger import Mergeable
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple
from typing_extensions import override

from metricflow.sql.sql_plan import SqlSelectColumn

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SelectColumnSet(Mergeable):
    """A set of SQL select columns that represent the different instance types in a data set.

    TODO: Evaluate using a single field instead of one for every instance type.
    """

    metric_columns: AnyLengthTuple[SqlSelectColumn]
    simple_metric_input_columns: AnyLengthTuple[SqlSelectColumn]
    dimension_columns: AnyLengthTuple[SqlSelectColumn]
    time_dimension_columns: AnyLengthTuple[SqlSelectColumn]
    entity_columns: AnyLengthTuple[SqlSelectColumn]
    group_by_metric_columns: AnyLengthTuple[SqlSelectColumn]
    metadata_columns: AnyLengthTuple[SqlSelectColumn]

    @staticmethod
    def create(  # noqa: D102
        metric_columns: Iterable[SqlSelectColumn] = (),
        simple_metric_input_columns: Iterable[SqlSelectColumn] = (),
        dimension_columns: Iterable[SqlSelectColumn] = (),
        time_dimension_columns: Iterable[SqlSelectColumn] = (),
        entity_columns: Iterable[SqlSelectColumn] = (),
        group_by_metric_columns: Iterable[SqlSelectColumn] = (),
        metadata_columns: Iterable[SqlSelectColumn] = (),
    ) -> SelectColumnSet:
        metric_columns = tuple(metric_columns)
        simple_metric_input_columns = tuple(simple_metric_input_columns)
        dimension_columns = tuple(dimension_columns)
        time_dimension_columns = tuple(time_dimension_columns)
        entity_columns = tuple(entity_columns)
        group_by_metric_columns = tuple(group_by_metric_columns)
        metadata_columns = tuple(metadata_columns)

        return SelectColumnSet(
            metric_columns=tuple(metric_columns),
            simple_metric_input_columns=tuple(simple_metric_input_columns),
            dimension_columns=tuple(dimension_columns),
            time_dimension_columns=tuple(time_dimension_columns),
            entity_columns=tuple(entity_columns),
            group_by_metric_columns=tuple(group_by_metric_columns),
            metadata_columns=tuple(metadata_columns),
        )

    @cached_property
    def columns_in_default_order(self) -> AnyLengthTuple[SqlSelectColumn]:  # noqa: D102
        return (
            # This order was chosen to match the column sequence data consumers typically prefer.
            self.time_dimension_columns
            + self.entity_columns
            + self.dimension_columns
            + self.group_by_metric_columns
            + self.metric_columns
            + self.simple_metric_input_columns
            + self.metadata_columns
        )

    @override
    def merge(self, other_set: SelectColumnSet) -> SelectColumnSet:
        """Combine the select columns by type."""
        return SelectColumnSet.create(
            metric_columns=self.metric_columns + other_set.metric_columns,
            simple_metric_input_columns=self.simple_metric_input_columns + other_set.simple_metric_input_columns,
            dimension_columns=self.dimension_columns + other_set.dimension_columns,
            time_dimension_columns=self.time_dimension_columns + other_set.time_dimension_columns,
            entity_columns=self.entity_columns + other_set.entity_columns,
            group_by_metric_columns=self.group_by_metric_columns + other_set.group_by_metric_columns,
            metadata_columns=self.metadata_columns + other_set.metadata_columns,
        )

    @classmethod
    @override
    def empty_instance(cls) -> SelectColumnSet:
        return SelectColumnSet.create()
