from __future__ import annotations

from itertools import chain
from typing import List, Sequence, Tuple

from metricflow_semantics.instances import InstanceSet, MeasureInstance
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.measure_spec import MeasureSpec, MetricInputMeasureSpec
from metricflow_semantics.sql.sql_exprs import SqlColumnReference, SqlColumnReferenceExpression, SqlFunctionExpression

from metricflow.plan_conversion.instance_set_transforms.instance_converters import CreateSelectColumnsForInstances
from metricflow.plan_conversion.select_column_gen import SelectColumnSet
from metricflow.sql.sql_plan import SqlSelectColumn


class CreateSelectColumnsWithMeasuresAggregated(CreateSelectColumnsForInstances):
    """Create select columns of the form "fct_bookings.bookings AS bookings" for all the instances.

    However, for measure columns, convert them into expressions like "SUM(fct_bookings.bookings) AS bookings" so that
    the resulting expressions can be used for aggregations.

    Also add an output alias that conforms to the alias
    """

    def __init__(  # noqa: D107
        self,
        table_alias: str,
        column_resolver: ColumnAssociationResolver,
        semantic_model_lookup: SemanticModelLookup,
        metric_input_measure_specs: Sequence[MetricInputMeasureSpec],
    ) -> None:
        self._semantic_model_lookup = semantic_model_lookup
        self.metric_input_measure_specs = metric_input_measure_specs
        super().__init__(table_alias=table_alias, column_resolver=column_resolver)

    def _make_sql_column_expression_to_aggregate_measures(
        self, measure_instances: Tuple[MeasureInstance, ...]
    ) -> List[SqlSelectColumn]:
        output_columns: List[SqlSelectColumn] = []
        aliased_input_specs = [spec for spec in self.metric_input_measure_specs if spec.alias]
        for instance in measure_instances:
            matches = [spec for spec in aliased_input_specs if spec.measure_spec == instance.spec]
            if matches:
                aliased_spec = matches[0]
                aliased_input_specs.remove(aliased_spec)
                output_measure_spec = aliased_spec.post_aggregation_spec
            else:
                output_measure_spec = instance.spec

            output_columns.append(
                self._make_sql_column_expression_to_aggregate_measure(
                    measure_instance=instance, output_measure_spec=output_measure_spec
                )
            )

        return output_columns

    def _make_sql_column_expression_to_aggregate_measure(
        self, measure_instance: MeasureInstance, output_measure_spec: MeasureSpec
    ) -> SqlSelectColumn:
        """Convert one measure instance into a SQL column."""
        # Get the column name of the measure in the table that we're reading from
        column_name_in_table = measure_instance.associated_column.column_name

        # Create an expression that will aggregate the given measure.
        # Figure out the aggregation function for the measure.
        measure = self._semantic_model_lookup.measure_lookup.get_measure(measure_instance.spec.reference)
        aggregation_type = measure.agg

        expression_to_get_measure = SqlColumnReferenceExpression.create(
            SqlColumnReference(self._table_alias, column_name_in_table)
        )

        expression_to_aggregate_measure = SqlFunctionExpression.build_expression_from_aggregation_type(
            aggregation_type=aggregation_type,
            sql_column_expression=expression_to_get_measure,
            agg_params=measure.agg_params,
        )

        # Get the output column name from the measure/alias

        new_column_association_for_aggregated_measure = self._column_resolver.resolve_spec(output_measure_spec)
        new_column_name_for_aggregated_measure = new_column_association_for_aggregated_measure.column_name

        return SqlSelectColumn(
            expr=expression_to_aggregate_measure,
            column_alias=new_column_name_for_aggregated_measure,
        )

    def transform(self, instance_set: InstanceSet) -> SelectColumnSet:  # noqa: D102
        metric_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.metric_instances])
        )

        measure_cols = self._make_sql_column_expression_to_aggregate_measures(instance_set.measure_instances)
        dimension_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.dimension_instances])
        )
        time_dimension_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.time_dimension_instances])
        )
        entity_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.entity_instances])
        )
        metadata_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.metadata_instances])
        )
        group_by_metric_cols = list(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.group_by_metric_instances])
        )
        return SelectColumnSet.create(
            metric_columns=metric_cols,
            measure_columns=measure_cols,
            dimension_columns=dimension_cols,
            time_dimension_columns=time_dimension_cols,
            entity_columns=entity_cols,
            group_by_metric_columns=group_by_metric_cols,
            metadata_columns=metadata_cols,
        )
