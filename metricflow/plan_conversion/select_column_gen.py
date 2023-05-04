from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Tuple

from metricflow.sql.sql_plan import SqlSelectColumn

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SelectColumnSet:
    """A set of SQL select columns that represent the different instance types in a data set."""

    metric_columns: List[SqlSelectColumn] = field(default_factory=list)
    measure_columns: List[SqlSelectColumn] = field(default_factory=list)
    dimension_columns: List[SqlSelectColumn] = field(default_factory=list)
    time_dimension_columns: List[SqlSelectColumn] = field(default_factory=list)
    entity_columns: List[SqlSelectColumn] = field(default_factory=list)
    metadata_columns: List[SqlSelectColumn] = field(default_factory=list)

    def merge(self, other_set: SelectColumnSet) -> SelectColumnSet:
        """Combine the select columns by type."""
        return SelectColumnSet(
            metric_columns=self.metric_columns + other_set.metric_columns,
            measure_columns=self.measure_columns + other_set.measure_columns,
            dimension_columns=self.dimension_columns + other_set.dimension_columns,
            time_dimension_columns=self.time_dimension_columns + other_set.time_dimension_columns,
            entity_columns=self.entity_columns + other_set.entity_columns,
            metadata_columns=self.metadata_columns + other_set.metadata_columns,
        )

    def as_tuple(self) -> Tuple[SqlSelectColumn, ...]:
        """Return all select columns as a tuple."""
        return tuple(
            # This order was chosen to match the column sequence data consumers typically prefer.
            self.time_dimension_columns
            + self.entity_columns
            + self.dimension_columns
            + self.metric_columns
            + self.measure_columns
            + self.metadata_columns
        )

    def without_measure_columns(self) -> SelectColumnSet:
        """Returns this but with the measure columns removed."""
        return SelectColumnSet(
            metric_columns=self.metric_columns,
            dimension_columns=self.dimension_columns,
            time_dimension_columns=self.time_dimension_columns,
            entity_columns=self.entity_columns,
            metadata_columns=self.metadata_columns,
        )
