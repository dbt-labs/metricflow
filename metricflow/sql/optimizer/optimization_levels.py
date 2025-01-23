from __future__ import annotations

import functools
from dataclasses import dataclass
from enum import Enum
from typing import Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted

from metricflow.sql.optimizer.column_pruning.column_pruner import SqlColumnPrunerOptimizer
from metricflow.sql.optimizer.rewriting_sub_query_reducer import SqlRewritingSubQueryReducer
from metricflow.sql.optimizer.sql_query_plan_optimizer import SqlPlanOptimizer
from metricflow.sql.optimizer.table_alias_simplifier import SqlTableAliasSimplifier


@functools.total_ordering
class SqlOptimizationLevel(Enum):
    """Defines the level of query optimization and the associated optimizers to apply."""

    O0 = "O0"
    O1 = "O1"
    O2 = "O2"
    O3 = "O3"
    O4 = "O4"
    O5 = "O5"

    @staticmethod
    def default_level() -> SqlOptimizationLevel:  # noqa: D102
        return SqlOptimizationLevel.O5

    def __lt__(self, other: SqlOptimizationLevel) -> bool:  # noqa: D105
        if not isinstance(other, SqlOptimizationLevel):
            return NotImplemented

        return self.name < other.name


@dataclass(frozen=True)
class SqlGenerationOptionSet:
    """Defines the different SQL generation optimizers / options that should be used at each level."""

    optimizers: Tuple[SqlPlanOptimizer, ...]

    # Specifies whether CTEs can be used to simplify generated SQL.
    allow_cte: bool

    @staticmethod
    def options_for_level(  # noqa: D102
        level: SqlOptimizationLevel, use_column_alias_in_group_by: bool
    ) -> SqlGenerationOptionSet:
        optimizers: Tuple[SqlPlanOptimizer, ...] = ()
        allow_cte = False
        if level is SqlOptimizationLevel.O0:
            pass
        elif level is SqlOptimizationLevel.O1:
            optimizers = (SqlTableAliasSimplifier(),)
        elif level is SqlOptimizationLevel.O2:
            optimizers = (SqlColumnPrunerOptimizer(), SqlTableAliasSimplifier())
        elif level is SqlOptimizationLevel.O3:
            optimizers = (SqlColumnPrunerOptimizer(), SqlTableAliasSimplifier())
        elif level is SqlOptimizationLevel.O4:
            optimizers = (
                SqlColumnPrunerOptimizer(),
                SqlRewritingSubQueryReducer(use_column_alias_in_group_bys=use_column_alias_in_group_by),
                SqlTableAliasSimplifier(),
            )
        elif level is SqlOptimizationLevel.O5:
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
