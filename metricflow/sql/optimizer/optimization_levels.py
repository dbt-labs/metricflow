from __future__ import annotations

import functools
from dataclasses import dataclass
from enum import Enum
from typing import Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted

from metricflow.sql.optimizer.column_pruner import SqlColumnPrunerOptimizer
from metricflow.sql.optimizer.rewriting_sub_query_reducer import SqlRewritingSubQueryReducer
from metricflow.sql.optimizer.sql_query_plan_optimizer import SqlQueryPlanOptimizer
from metricflow.sql.optimizer.sub_query_reducer import SqlSubQueryReducer
from metricflow.sql.optimizer.table_alias_simplifier import SqlTableAliasSimplifier


@functools.total_ordering
class SqlQueryOptimizationLevel(Enum):
    """Defines the level of query optimization and the associated optimizers to apply."""

    O0 = "O0"
    O1 = "O1"
    O2 = "O2"
    O3 = "O3"
    O4 = "O4"
    O5 = "O5"

    @staticmethod
    def default_level() -> SqlQueryOptimizationLevel:  # noqa: D102
        return SqlQueryOptimizationLevel.O5

    def __lt__(self, other: SqlQueryOptimizationLevel) -> bool:  # noqa: D105
        if not isinstance(other, SqlQueryOptimizationLevel):
            return NotImplemented

        return self.name < other.name


@dataclass(frozen=True)
class SqlGenerationOptionSet:
    """Defines the different SQL generation optimizers / options that should be used at each level."""

    optimizers: Tuple[SqlQueryPlanOptimizer, ...]

    # Specifies whether CTEs can be used to simplify generated SQL.
    allow_cte: bool

    @staticmethod
    def options_for_level(  # noqa: D102
        level: SqlQueryOptimizationLevel, use_column_alias_in_group_by: bool
    ) -> SqlGenerationOptionSet:
        optimizers: Tuple[SqlQueryPlanOptimizer, ...] = ()
        allow_cte = False
        if level is SqlQueryOptimizationLevel.O0:
            pass
        elif level is SqlQueryOptimizationLevel.O1:
            optimizers = (SqlTableAliasSimplifier(),)
        elif level is SqlQueryOptimizationLevel.O2:
            optimizers = (SqlColumnPrunerOptimizer(), SqlTableAliasSimplifier())
        elif level is SqlQueryOptimizationLevel.O3:
            optimizers = (SqlColumnPrunerOptimizer(), SqlSubQueryReducer(), SqlTableAliasSimplifier())
        elif level is SqlQueryOptimizationLevel.O4:
            optimizers = (
                SqlColumnPrunerOptimizer(),
                SqlRewritingSubQueryReducer(use_column_alias_in_group_bys=use_column_alias_in_group_by),
                SqlTableAliasSimplifier(),
            )
        elif level is SqlQueryOptimizationLevel.O5:
            optimizers = (
                SqlColumnPrunerOptimizer(),
                SqlRewritingSubQueryReducer(use_column_alias_in_group_bys=use_column_alias_in_group_by),
                SqlTableAliasSimplifier(),
            )
            allow_cte = True
        else:
            assert_values_exhausted(level)

        return SqlGenerationOptionSet(
            optimizers=optimizers,
            allow_cte=allow_cte,
        )
