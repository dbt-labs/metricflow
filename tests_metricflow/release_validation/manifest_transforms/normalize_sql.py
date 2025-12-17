from __future__ import annotations

import copy
import logging
import re
from collections.abc import Sequence
from typing import Optional

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.transformations.transform_rule import SemanticManifestTransformRule
from dbt_semantic_interfaces.type_enums import DimensionType, MetricType
from metricflow_semantics.sql.sql_table import SqlTable
from typing_extensions import override

logger = logging.getLogger(__name__)


class NormalizeSqlRule(SemanticManifestTransformRule[PydanticSemanticManifest]):
    """Replace user-specified SQL in the manifest with SQL that works with DuckDB.

    This rule replaces user-specified SQL in fields such as:

    * Node relations for semantic models.
    * Element expressions.
    * Filters.
    * Derived metric expressions.

    with dummy SQL that is compatible with DuckDB. This enables validation (to a degree) using a local DuckDB instance.
    """

    def __init__(self, dummy_table: SqlTable) -> None:  # noqa: D107
        self._dummy_table_node_relation = PydanticNodeRelation.from_string(dummy_table.sql)

    @staticmethod
    def _extract_variable_expression(jinja_template_str: str) -> Sequence[str]:
        """Return the variable expressions in the Jinja template.

        For example:
            `{{ user.first_name }} AND {{ user.last_name }}` -> ["{{ user.first_name }}", "{{ user.last_name}}"]
        """
        # Pattern to match {{ ... }} expressions, capturing everything between {{ and }}
        pattern = r"\{\{[^}]*\}\}"
        matches = re.findall(pattern, jinja_template_str)
        return matches

    def _update_filter(self, filter_intersection: Optional[PydanticWhereFilterIntersection]) -> None:
        """Replace the user filter SQL with a similar one that can run on DuckDB.

        The user filter SQL is replaced by an expression that checks if the items specified in the Jinja template are
        null.

        e.g.

            {{ Dimension('listing__country_latest') }} = 'us'

            ->

            {{ Dimension('listing__country_latest') }} IS NOT NULL

        It's necessary to retain the Jinja template to better reproduce how the engine would have generated SQL
        before this transformation.
        """
        if filter_intersection is None:
            return

        for where_filter in filter_intersection.where_filters:
            variable_expressions = NormalizeSqlRule._extract_variable_expression(where_filter.where_sql_template)

            # Could be the case if the filter only contains a custom SQL expression.
            if len(variable_expressions) == 0:
                where_filter.where_sql_template = "TRUE"
                continue

            rewritten_expressions = tuple(f"({expression} IS NOT NULL)" for expression in variable_expressions)
            where_filter.where_sql_template = " AND ".join(rewritten_expressions)

    @override
    def transform_model(self, semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:
        transformed_manifest = copy.deepcopy(semantic_manifest)

        # Replace all element expressions in semantic models.
        for semantic_model in transformed_manifest.semantic_models:
            for measure in semantic_model.measures:
                measure.expr = "1"
            for entity in semantic_model.entities:
                entity.expr = "1"
            for dimension in semantic_model.dimensions:
                dimension_type = dimension.type
                if dimension_type is DimensionType.CATEGORICAL:
                    dimension.expr = "'1'"
                elif dimension_type is DimensionType.TIME:
                    dimension.expr = "CAST('2020-01-01' AS TIMESTAMP)"
                else:
                    assert_values_exhausted(dimension_type)

            semantic_model.node_relation = copy.deepcopy(self._dummy_table_node_relation)

        # Replace all metric filters and derived metric expressions.
        for metric in transformed_manifest.metrics:
            self._update_filter(metric.filter)

            for input_metric in metric.type_params.metrics or ():
                self._update_filter(input_metric.filter)

            metric_type = metric.type
            if metric_type is MetricType.SIMPLE:
                metric.type_params.expr = "1"
            elif metric_type is MetricType.DERIVED:
                referenced_metric_names = [
                    (input_metric.alias or input_metric.name) for input_metric in metric.type_params.metrics or ()
                ]
                metric.type_params.expr = " + ".join(referenced_metric_names)
            elif metric_type is MetricType.RATIO:
                numerator = metric.type_params.numerator
                if numerator is not None:
                    self._update_filter(numerator.filter)
                denominator = metric.type_params.denominator
                if denominator is not None:
                    self._update_filter(denominator.filter)
            elif (
                metric_type is MetricType.SIMPLE
                or metric_type is MetricType.CONVERSION
                or metric_type is MetricType.CUMULATIVE
            ):
                pass
            else:
                assert_values_exhausted(metric_type)

        # Replace all filters in saved queries.
        for saved_query in transformed_manifest.saved_queries:
            self._update_filter(saved_query.query_params.where)

        return transformed_manifest
