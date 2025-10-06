from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

from metricflow_semantics.experimental.dsi.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.instances import InstanceSet, InstanceSetTransform, MeasureInstance
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.measure_spec import SimpleMetricInputSpec
from metricflow_semantics.sql.sql_exprs import SqlColumnReference, SqlColumnReferenceExpression, SqlFunctionExpression

from metricflow.dataflow.builder.aggregation_helper import InstanceAliasMapping
from metricflow.plan_conversion.instance_set_transforms.select_columns import CreateSelectColumnsForInstances
from metricflow.plan_conversion.select_column_gen import SelectColumnSet
from metricflow.sql.sql_plan import SqlSelectColumn


@dataclass(frozen=True)
class CreateAggregatedMeasuresResult:
    """Result class for `CreateAggregatedMeasuresTransform`."""

    # Columns that should be in the `SELECT` clause.
    select_column_set: SelectColumnSet
    # Columns that should be in the `GROUP BY` clause.
    group_by_column_set: SelectColumnSet


class CreateAggregatedMeasuresTransform(InstanceSetTransform[CreateAggregatedMeasuresResult]):
    """Create select columns of the form "fct_bookings.bookings AS bookings" for all the instances.

    However, for measure columns, convert them into expressions like "SUM(fct_bookings.bookings) AS bookings" so that
    the resulting expressions can be used for aggregations.

    Also add an output alias that conforms to the alias
    """

    def __init__(  # noqa: D107
        self,
        table_alias: str,
        column_resolver: ColumnAssociationResolver,
        manifest_object_lookup: ManifestObjectLookup,
        alias_mapping: InstanceAliasMapping,
    ) -> None:
        self._table_alias = table_alias
        self._column_resolver = column_resolver
        self._manifest_object_lookup = manifest_object_lookup
        self._alias_mapping = alias_mapping
        self._create_select_column_transform = CreateSelectColumnsForInstances(
            table_alias=table_alias,
            column_resolver=column_resolver,
        )

    def _make_sql_column_expression_to_aggregate_measures(
        self, measure_instances: Tuple[MeasureInstance, ...]
    ) -> SelectColumnSet:
        output_columns: List[SqlSelectColumn] = []
        for instance in measure_instances:
            spec = instance.spec
            output_measure_spec = self._alias_mapping.aliased_spec(spec) or spec

            output_columns.append(
                self._make_sql_column_expression_to_aggregate_measure(
                    measure_instance=instance, output_measure_spec=output_measure_spec
                )
            )

        return SelectColumnSet.create(measure_columns=output_columns)

    def _make_sql_column_expression_to_aggregate_measure(
        self, measure_instance: MeasureInstance, output_measure_spec: SimpleMetricInputSpec
    ) -> SqlSelectColumn:
        """Convert one measure instance into a SQL column."""
        # Get the column name of the measure in the table that we're reading from
        column_name_in_table = measure_instance.associated_column.column_name

        # Create an expression that will aggregate the given measure.
        # Figure out the aggregation function for the measure.
        simple_metric_input = self._manifest_object_lookup.simple_metric_name_to_input[
            measure_instance.spec.element_name
        ]

        expression_to_get_measure = SqlColumnReferenceExpression.create(
            SqlColumnReference(self._table_alias, column_name_in_table)
        )

        expression_to_aggregate_measure = SqlFunctionExpression.build_expression_from_aggregation_type(
            aggregation_type=simple_metric_input.agg,
            sql_column_expression=expression_to_get_measure,
            agg_params=simple_metric_input.agg_params,
        )

        # Get the output column name from the measure/alias

        new_column_association_for_aggregated_measure = self._column_resolver.resolve_spec(output_measure_spec)
        new_column_name_for_aggregated_measure = new_column_association_for_aggregated_measure.column_name

        return SqlSelectColumn(
            expr=expression_to_aggregate_measure,
            column_alias=new_column_name_for_aggregated_measure,
        )

    def transform(self, instance_set: InstanceSet) -> CreateAggregatedMeasuresResult:  # noqa: D102
        instance_set_without_measures = instance_set.without_measures()
        column_set_without_measures = self._create_select_column_transform.transform(
            instance_set_without_measures
        ).select_column_set
        group_by_column_set = self._create_select_column_transform.transform(
            instance_set_without_measures
        ).select_column_set
        measure_column_set = self._make_sql_column_expression_to_aggregate_measures(instance_set.measure_instances)

        return CreateAggregatedMeasuresResult(
            select_column_set=column_set_without_measures.merge(measure_column_set),
            group_by_column_set=group_by_column_set,
        )
