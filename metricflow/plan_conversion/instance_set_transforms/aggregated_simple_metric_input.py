from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

from metricflow_semantics.instances import InstanceSet, InstanceSetTransform, SimpleMetricInputInstance
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.sql.sql_exprs import SqlColumnReference, SqlColumnReferenceExpression, SqlFunctionExpression

from metricflow.plan_conversion.instance_set_transforms.select_columns import CreateSelectColumnsForInstances
from metricflow.plan_conversion.select_column_gen import SelectColumnSet
from metricflow.sql.sql_plan import SqlSelectColumn


@dataclass(frozen=True)
class CreateAggregatedSimpleMetricInputsResult:
    """Result class for `CreateAggregatedSimpleMetricInputsTransform`."""

    # Columns that should be in the `SELECT` clause.
    select_column_set: SelectColumnSet
    # Columns that should be in the `GROUP BY` clause.
    group_by_column_set: SelectColumnSet


class CreateAggregatedSimpleMetricInputsTransform(InstanceSetTransform[CreateAggregatedSimpleMetricInputsResult]):
    """Create select columns of the form "fct_bookings.bookings AS bookings" for all the instances.

    However, for simple-metric-input columns, convert them into expressions like
    "SUM(fct_bookings.bookings) AS bookings" so that the resulting expressions can be used for aggregations.

    Also add an output alias that conforms to the alias
    """

    def __init__(  # noqa: D107
        self,
        table_alias: str,
        column_resolver: ColumnAssociationResolver,
        manifest_object_lookup: ManifestObjectLookup,
    ) -> None:
        self._table_alias = table_alias
        self._column_resolver = column_resolver
        self._manifest_object_lookup = manifest_object_lookup
        self._create_select_column_transform = CreateSelectColumnsForInstances(
            table_alias=table_alias,
            column_resolver=column_resolver,
        )

    def _make_sql_column_expression_for_aggregation(
        self, instances: Tuple[SimpleMetricInputInstance, ...]
    ) -> SelectColumnSet:
        output_columns: List[SqlSelectColumn] = []
        for instance in instances:
            output_columns.append(
                self._make_aggregation_sql_column_expression(instance=instance, output_spec=instance.spec)
            )

        return SelectColumnSet.create(simple_metric_input_columns=output_columns)

    def _make_aggregation_sql_column_expression(
        self, instance: SimpleMetricInputInstance, output_spec: SimpleMetricInputSpec
    ) -> SqlSelectColumn:
        """Convert one simple-metric input instance into a SQL column."""
        # Get the column name of the simple-metric input in the table that we're reading from
        column_name_in_table = instance.associated_column.column_name

        # Create an expression that will aggregate the given simple-metric input.
        simple_metric_input = self._manifest_object_lookup.simple_metric_name_to_input[instance.spec.element_name]

        read_expression = SqlColumnReferenceExpression.create(
            SqlColumnReference(self._table_alias, column_name_in_table)
        )

        # Figure out the aggregation function for the simple metric.
        aggregation_expression = SqlFunctionExpression.build_expression_from_aggregation_type(
            aggregation_type=simple_metric_input.agg,
            sql_column_expression=read_expression,
            agg_params=simple_metric_input.agg_params,
        )

        # Get the output column name for the simple-metric input.

        aggregated_column_association = self._column_resolver.resolve_spec(output_spec)
        aggregated_column_name = aggregated_column_association.column_name

        return SqlSelectColumn(
            expr=aggregation_expression,
            column_alias=aggregated_column_name,
        )

    def transform(self, instance_set: InstanceSet) -> CreateAggregatedSimpleMetricInputsResult:  # noqa: D102
        instance_set_without_simple_metric_inputs = instance_set.without_simple_metric_inputs()
        column_set_without_simple_metric_inputs = self._create_select_column_transform.transform(
            instance_set_without_simple_metric_inputs
        ).select_column_set
        group_by_column_set = self._create_select_column_transform.transform(
            instance_set_without_simple_metric_inputs
        ).select_column_set
        simple_metric_input_column_set = self._make_sql_column_expression_for_aggregation(
            instance_set.simple_metric_input_instances
        )

        return CreateAggregatedSimpleMetricInputsResult(
            select_column_set=column_set_without_simple_metric_inputs.merge(simple_metric_input_column_set),
            group_by_column_set=group_by_column_set,
        )
