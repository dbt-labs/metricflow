from __future__ import annotations

import logging
from collections import OrderedDict
from typing import List, Optional, Sequence, Union

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols.metric import MetricType
from dbt_semantic_interfaces.references import MetricModelReference
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType

from metricflow.aggregation_properties import AggregationState
from metricflow.dag.id_generation import IdGeneratorRegistry
from metricflow.dataflow.dataflow_plan import (
    AggregateMeasuresNode,
    BaseOutput,
    CombineMetricsNode,
    ComputedMetricsOutput,
    ComputeMetricsNode,
    ConstrainTimeRangeNode,
    DataflowPlanNodeVisitor,
    FilterElementsNode,
    JoinAggregatedMeasuresByGroupByColumnsNode,
    JoinOverTimeRangeNode,
    JoinToBaseOutputNode,
    JoinToTimeSpineNode,
    MetricTimeDimensionTransformNode,
    OrderByLimitNode,
    ReadSqlSourceNode,
    SemiAdditiveJoinNode,
    WhereConstraintNode,
    WriteToResultDataframeNode,
    WriteToResultTableNode,
)
from metricflow.dataset.dataset import DataSet
from metricflow.dataset.sql_dataset import SqlDataSet
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.instances import (
    InstanceSet,
    MetricInstance,
    TimeDimensionInstance,
)
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.instance_converters import (
    AddLinkToLinkableElements,
    AddMetrics,
    AliasAggregatedMeasures,
    ChangeAssociatedColumns,
    ChangeMeasureAggregationState,
    CreateSelectColumnsForInstances,
    CreateSelectColumnsWithMeasuresAggregated,
    FilterElements,
    FilterLinkableInstancesWithLeadingLink,
    RemoveMeasures,
    RemoveMetrics,
    create_select_columns_for_instance_sets,
)
from metricflow.plan_conversion.select_column_gen import (
    SelectColumnSet,
)
from metricflow.plan_conversion.spec_transforms import (
    CreateColumnAssociations,
    CreateSelectCoalescedColumnsForLinkableSpecs,
    SelectOnlyLinkableSpecs,
)
from metricflow.plan_conversion.sql_join_builder import (
    AnnotatedSqlDataSet,
    ColumnEqualityDescription,
    SqlQueryPlanJoinBuilder,
)
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlEngine
from metricflow.specs.column_assoc import ColumnAssociation, ColumnAssociationResolver, SingleColumnCorrelationKey
from metricflow.specs.specs import (
    MeasureSpec,
    MetricSpec,
    TimeDimensionSpec,
)
from metricflow.sql.optimizer.optimization_levels import (
    SqlQueryOptimizationLevel,
    SqlQueryOptimizerConfiguration,
)
from metricflow.sql.sql_exprs import (
    SqlBetweenExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlComparison,
    SqlComparisonExpression,
    SqlDateTruncExpression,
    SqlExpressionNode,
    SqlExtractExpression,
    SqlFunctionExpression,
    SqlLogicalExpression,
    SqlLogicalOperator,
    SqlRatioComputationExpression,
    SqlStringExpression,
    SqlStringLiteralExpression,
)
from metricflow.sql.sql_plan import (
    SqlJoinDescription,
    SqlJoinType,
    SqlOrderByDescription,
    SqlQueryPlan,
    SqlQueryPlanNode,
    SqlSelectColumn,
    SqlSelectStatementNode,
    SqlTableFromClauseNode,
)
from metricflow.time.time_constants import ISO8601_PYTHON_FORMAT

logger = logging.getLogger(__name__)


def _make_time_range_comparison_expr(
    table_alias: str, column_alias: str, time_range_constraint: TimeRangeConstraint
) -> SqlExpressionNode:
    """Build an expression like "ds BETWEEN CAST('2020-01-01' AS TIMESTAMP) AND CAST('2020-01-02' AS TIMESTAMP)."""
    # TODO: Update when adding < day granularity support.
    return SqlBetweenExpression(
        column_arg=SqlColumnReferenceExpression(
            SqlColumnReference(
                table_alias=table_alias,
                column_name=column_alias,
            )
        ),
        start_expr=SqlStringLiteralExpression(
            literal_value=time_range_constraint.start_time.strftime(ISO8601_PYTHON_FORMAT),
        ),
        end_expr=SqlStringLiteralExpression(
            literal_value=time_range_constraint.end_time.strftime(ISO8601_PYTHON_FORMAT),
        ),
    )


class DataflowToSqlQueryPlanConverter(DataflowPlanNodeVisitor[SqlDataSet]):
    """Generates an SQL query plan from a node in the a metric dataflow plan."""

    def __init__(
        self,
        column_association_resolver: ColumnAssociationResolver,
        semantic_manifest_lookup: SemanticManifestLookup,
    ) -> None:
        """Constructor.

        Args:
            column_association_resolver: controls how columns for instances are generated and used between nested
            queries.
            semantic_manifest_lookup: Self-explanatory.
        """
        self._column_association_resolver = column_association_resolver
        self._metric_lookup = semantic_manifest_lookup.metric_lookup
        self._semantic_model_lookup = semantic_manifest_lookup.semantic_model_lookup
        self._time_spine_source = semantic_manifest_lookup.time_spine_source

    @property
    def column_association_resolver(self) -> ColumnAssociationResolver:  # noqa: D
        return self._column_association_resolver

    def _next_unique_table_alias(self) -> str:
        """Return the next unique table alias to use in generating queries."""
        return IdGeneratorRegistry.for_class(self.__class__).create_id(prefix="subq")

    def _make_time_spine_data_set(
        self,
        metric_time_dimension_instance: TimeDimensionInstance,
        metric_time_dimension_column_name: str,
        time_spine_source: TimeSpineSource,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
    ) -> SqlDataSet:
        """Make a time spine data set, which contains all date values like '2020-01-01', '2020-01-02'...

        This is useful in computing cumulative metrics. This will need to be updated to support granularities finer than a
        day.
        """
        time_spine_instance = (
            TimeDimensionInstance(
                defined_from=metric_time_dimension_instance.defined_from,
                associated_columns=(
                    ColumnAssociation(
                        column_name=metric_time_dimension_column_name,
                        single_column_correlation_key=SingleColumnCorrelationKey(),
                    ),
                ),
                spec=metric_time_dimension_instance.spec,
            ),
        )
        time_spine_instance_set = InstanceSet(
            time_dimension_instances=time_spine_instance,
        )
        description = "Date Spine"
        time_spine_table_alias = self._next_unique_table_alias()

        # If the requested granularity is the same as the granularity of the spine, do a direct select.
        if metric_time_dimension_instance.spec.time_granularity == time_spine_source.time_column_granularity:
            return SqlDataSet(
                instance_set=time_spine_instance_set,
                sql_select_node=SqlSelectStatementNode(
                    description=description,
                    # This creates select expressions for all columns referenced in the instance set.
                    select_columns=(
                        SqlSelectColumn(
                            expr=SqlColumnReferenceExpression(
                                SqlColumnReference(
                                    table_alias=time_spine_table_alias,
                                    column_name=time_spine_source.time_column_name,
                                ),
                            ),
                            column_alias=metric_time_dimension_column_name,
                        ),
                    ),
                    from_source=SqlTableFromClauseNode(sql_table=time_spine_source.spine_table),
                    from_source_alias=time_spine_table_alias,
                    joins_descs=(),
                    group_bys=(),
                    where=_make_time_range_comparison_expr(
                        table_alias=time_spine_table_alias,
                        column_alias=time_spine_source.time_column_name,
                        time_range_constraint=time_range_constraint,
                    )
                    if time_range_constraint
                    else None,
                    order_bys=(),
                ),
            )
        # If the granularity is different, apply a DATE_TRUNC() and aggregate.
        else:
            select_columns = (
                SqlSelectColumn(
                    expr=SqlDateTruncExpression(
                        time_granularity=metric_time_dimension_instance.spec.time_granularity,
                        arg=SqlColumnReferenceExpression(
                            SqlColumnReference(
                                table_alias=time_spine_table_alias,
                                column_name=time_spine_source.time_column_name,
                            ),
                        ),
                    ),
                    column_alias=metric_time_dimension_column_name,
                ),
            )
            return SqlDataSet(
                instance_set=time_spine_instance_set,
                sql_select_node=SqlSelectStatementNode(
                    description=description,
                    # This creates select expressions for all columns referenced in the instance set.
                    select_columns=select_columns,
                    from_source=SqlTableFromClauseNode(sql_table=time_spine_source.spine_table),
                    from_source_alias=time_spine_table_alias,
                    joins_descs=(),
                    group_bys=select_columns,
                    where=_make_time_range_comparison_expr(
                        table_alias=time_spine_table_alias,
                        column_alias=time_spine_source.time_column_name,
                        time_range_constraint=time_range_constraint,
                    )
                    if time_range_constraint
                    else None,
                    order_bys=(),
                ),
            )

    def visit_source_node(self, node: ReadSqlSourceNode) -> SqlDataSet:
        """Generate the SQL to read from the source."""
        return SqlDataSet(
            sql_select_node=node.data_set.sql_select_node,
            instance_set=node.data_set.instance_set,
        )

    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> SqlDataSet:
        """Generate time range join SQL."""
        table_alias_to_instance_set: OrderedDict[str, InstanceSet] = OrderedDict()

        input_data_set = node.parent_node.accept(self)
        input_data_set_alias = self._next_unique_table_alias()

        metric_time_dimension_spec: Optional[TimeDimensionSpec] = None
        metric_time_dimension_instance: Optional[TimeDimensionInstance] = None
        for instance in input_data_set.metric_time_dimension_instances:
            if len(instance.spec.entity_links) == 0:
                metric_time_dimension_instance = instance
                metric_time_dimension_spec = instance.spec
                break

        assert metric_time_dimension_spec
        time_spine_data_set_alias = self._next_unique_table_alias()

        metric_time_dimension_column_name = self.column_association_resolver.resolve_spec(
            metric_time_dimension_spec
        ).column_name

        # Assemble time_spine dataset with metric_time_dimension to join.
        # Granularity of time_spine column should match granularity of metric_time column from parent dataset.
        assert metric_time_dimension_instance
        time_spine_data_set = self._make_time_spine_data_set(
            metric_time_dimension_instance=metric_time_dimension_instance,
            metric_time_dimension_column_name=metric_time_dimension_column_name,
            time_spine_source=self._time_spine_source,
            time_range_constraint=node.time_range_constraint,
        )
        table_alias_to_instance_set[time_spine_data_set_alias] = time_spine_data_set.instance_set

        # Figure out which columns correspond to the time dimension that we want to join on.
        input_data_set_metric_time_column_association = input_data_set.column_association_for_time_dimension(
            metric_time_dimension_spec
        )
        input_data_set_metric_time_col = input_data_set_metric_time_column_association.column_name

        time_spine_data_set_column_associations = time_spine_data_set.column_association_for_time_dimension(
            metric_time_dimension_spec
        )
        time_spine_data_set_time_dimension_col = time_spine_data_set_column_associations.column_name

        annotated_input_data_set = AnnotatedSqlDataSet(
            data_set=input_data_set, alias=input_data_set_alias, _metric_time_column_name=input_data_set_metric_time_col
        )
        annotated_time_spine_data_set = AnnotatedSqlDataSet(
            data_set=time_spine_data_set,
            alias=time_spine_data_set_alias,
            _metric_time_column_name=time_spine_data_set_time_dimension_col,
        )

        join_desc = SqlQueryPlanJoinBuilder.make_cumulative_metric_time_range_join_description(
            node=node, metric_data_set=annotated_input_data_set, time_spine_data_set=annotated_time_spine_data_set
        )

        modified_input_instance_set = InstanceSet(
            measure_instances=input_data_set.instance_set.measure_instances,
            dimension_instances=input_data_set.instance_set.dimension_instances,
            entity_instances=input_data_set.instance_set.entity_instances,
            metric_instances=input_data_set.instance_set.metric_instances,
            # we omit the metric time dimension from the right side of the self-join because we need to use
            # the metric time dimension from the right side
            time_dimension_instances=tuple(
                [
                    time_dimension_instance
                    for time_dimension_instance in input_data_set.instance_set.time_dimension_instances
                    if time_dimension_instance.spec != metric_time_dimension_spec
                ]
            ),
        )
        table_alias_to_instance_set[input_data_set_alias] = modified_input_instance_set

        # The output instances are the same as the input instances.
        output_instance_set = ChangeAssociatedColumns(self._column_association_resolver).transform(
            input_data_set.instance_set
        )

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                select_columns=create_select_columns_for_instance_sets(
                    self._column_association_resolver, table_alias_to_instance_set
                ),
                from_source=time_spine_data_set.sql_select_node,
                from_source_alias=time_spine_data_set_alias,
                joins_descs=(join_desc,),
                group_bys=(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_join_to_base_output_node(self, node: JoinToBaseOutputNode) -> SqlDataSet:
        """Generates the query that realizes the behavior of the JoinToStandardOutputNode."""
        # Keep a mapping between the table aliases that would be used in the query and the MDO instances in that source.
        # e.g. when building "FROM from_table a JOIN right_table b", the value for key "a" would be the instances in
        # "from_table"
        table_alias_to_instance_set: OrderedDict[str, InstanceSet] = OrderedDict()

        # Convert the dataflow from the left node to a DataSet and add context for it to table_alias_to_instance_set
        # A DataSet is a bundle of the SQL query (in object form) and the MDO instances that the SQL query contains.
        from_data_set = node.left_node.accept(self)
        from_data_set_alias = self._next_unique_table_alias()
        table_alias_to_instance_set[from_data_set_alias] = from_data_set.instance_set

        # Build the join descriptions for the SqlQueryPlan - different from node.join_descriptions which are the join
        # descriptions from the dataflow plan.
        sql_join_descs: List[SqlJoinDescription] = []

        # The dataflow plan describes how the data sets coming from the parent nodes should be joined together. Use
        # those descriptions to convert them to join descriptions for the SQL query plan.
        for join_description in node.join_targets:
            join_on_entity = join_description.join_on_entity

            right_node_to_join: BaseOutput = join_description.join_node
            right_data_set: SqlDataSet = right_node_to_join.accept(self)
            right_data_set_alias = self._next_unique_table_alias()

            sql_join_descs.append(
                SqlQueryPlanJoinBuilder.make_base_output_join_description(
                    left_data_set=AnnotatedSqlDataSet(data_set=from_data_set, alias=from_data_set_alias),
                    right_data_set=AnnotatedSqlDataSet(data_set=right_data_set, alias=right_data_set_alias),
                    join_description=join_description,
                )
            )

            # Remove the linkable instances with the join_on_entity as the leading link as the next step adds the
            # link. This is to avoid cases where there is a primary entity and a dimension in the data set, and we
            # create an instance in the next step that has the same entity link.
            # e.g. a data set has the dimension "listing__country_latest" and "listing" is a primary entity in the
            # data set. The next step would create an instance like "listing__listing__country_latest" without this
            # filter.

            # logger.error(f"before filter is:\n{pformat_big_objects(right_data_set.instance_set.spec_set)}")
            right_data_set_instance_set_filtered = FilterLinkableInstancesWithLeadingLink(
                entity_link=join_on_entity,
            ).transform(right_data_set.instance_set)
            # logger.error(f"after filter is:\n{pformat_big_objects(right_data_set_instance_set_filtered.spec_set)}")

            # After the right data set is joined to the "from" data set, we need to change the links for some of the
            # instances that represent the right data set. For example, if the "from" data set contains the "bookings"
            # measure instance and the right dataset contains the "country" dimension instance, then after the join,
            # the output data set should have the "country" dimension instance with the "user_id" entity link
            # (if "user_id" equality was the join condition). "country" -> "user_id__country"
            right_data_set_instance_set_after_join = right_data_set_instance_set_filtered.transform(
                AddLinkToLinkableElements(join_on_entity=join_on_entity)
            )
            table_alias_to_instance_set[right_data_set_alias] = right_data_set_instance_set_after_join

        from_data_set_output_instance_set = from_data_set.instance_set.transform(
            FilterElements(include_specs=from_data_set.instance_set.spec_set)
        )

        # Change the aggregation state for the measures to be partially aggregated if it was previously aggregated
        # since we removed the entities and added the dimensions. The dimensions could have the same value for
        # multiple rows, so we'll need to re-aggregate.
        from_data_set_output_instance_set = from_data_set_output_instance_set.transform(
            ChangeMeasureAggregationState(
                {
                    AggregationState.NON_AGGREGATED: AggregationState.NON_AGGREGATED,
                    AggregationState.COMPLETE: AggregationState.PARTIAL,
                    AggregationState.PARTIAL: AggregationState.PARTIAL,
                }
            )
        )

        table_alias_to_instance_set[from_data_set_alias] = from_data_set_output_instance_set

        # Construct the data set that contains the updated instances and the SQL nodes that should go in the various
        # clauses.
        return SqlDataSet(
            instance_set=InstanceSet.merge(list(table_alias_to_instance_set.values())),
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                select_columns=create_select_columns_for_instance_sets(
                    self._column_association_resolver, table_alias_to_instance_set
                ),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=tuple(sql_join_descs),
                group_bys=(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_join_aggregated_measures_by_groupby_columns_node(  # noqa: D
        self, node: JoinAggregatedMeasuresByGroupByColumnsNode
    ) -> SqlDataSet:
        """Generates the query that realizes the behavior of the JoinAggregatedMeasuresByGroupByColumnsNode.

        This node is a straight inner join against all of the columns used for grouping in the input
        aggregation steps. Every column should be used, and at this point all inputs are fully aggregated,
        meaning we can make assumptions about things like NULL handling and there being one row per value
        set in each semantic model.

        In addition, this is used in cases where we expect a final metric to be computed using these
        measures as input. Therefore, we make the assumption that any mismatch should be discarded, as
        the behavior of the metric will be undefined in that case. This is why the INNER JOIN type is
        appropriate - if a dimension value set exists in one aggregated set but not the other, there is
        no sensible metric value for that dimension set.
        """
        assert len(node.parent_nodes) > 1, "This cannot happen, the node initializer would have failed"

        table_alias_to_instance_set: OrderedDict[str, InstanceSet] = OrderedDict()

        from_data_set: SqlDataSet = node.parent_nodes[0].accept(self)
        from_data_set_alias = self._next_unique_table_alias()
        table_alias_to_instance_set[from_data_set_alias] = from_data_set.instance_set
        join_aliases = [column.column_name for column in from_data_set.groupable_column_associations]
        use_cross_join = len(join_aliases) == 0

        sql_join_descs: List[SqlJoinDescription] = []
        for aggregated_node in node.parent_nodes[1:]:
            right_data_set: SqlDataSet = aggregated_node.accept(self)
            right_data_set_alias = self._next_unique_table_alias()
            right_column_names = {column.column_name for column in right_data_set.groupable_column_associations}
            if right_column_names != set(join_aliases):
                # TODO test multi-hop dimensions and address any issues. For now, let's raise an exception.
                raise ValueError(
                    f"We only support joins where all dimensions have the same name, but we got {right_column_names} "
                    f"and {join_aliases}, which differ by {right_column_names.difference(set(join_aliases))}!"
                )
            # sort column names to ensure consistent join ordering for ease of debugging and testing
            ordered_right_column_names = sorted(right_column_names)
            column_equality_descriptions = [
                ColumnEqualityDescription(
                    left_column_alias=colname, right_column_alias=colname, treat_nulls_as_equal=True
                )
                for colname in ordered_right_column_names
            ]
            sql_join_descs.append(
                SqlQueryPlanJoinBuilder.make_column_equality_sql_join_description(
                    right_source_node=right_data_set.sql_select_node,
                    right_source_alias=right_data_set_alias,
                    left_source_alias=from_data_set_alias,
                    column_equality_descriptions=column_equality_descriptions,
                    join_type=SqlJoinType.INNER if not use_cross_join else SqlJoinType.CROSS_JOIN,
                )
            )
            # All groupby columns are shared by all inputs, so we only want the measure/metric columns
            # from the semantic models on the right side of the join
            table_alias_to_instance_set[right_data_set_alias] = InstanceSet(
                measure_instances=right_data_set.instance_set.measure_instances,
                metric_instances=right_data_set.instance_set.metric_instances,
            )

        return SqlDataSet(
            instance_set=InstanceSet.merge(list(table_alias_to_instance_set.values())),
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                select_columns=create_select_columns_for_instance_sets(
                    self._column_association_resolver, table_alias_to_instance_set
                ),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=tuple(sql_join_descs),
                group_bys=(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> SqlDataSet:
        """Generates the query that realizes the behavior of AggregateMeasuresNode.

        This will produce a query that aggregates all measures from a given input semantic model per the
        measure spec

        In the event the input aggregations are applied to measures with aliases set, in case of, e.g.,
        a constraint applied to one instance of the measure but not another one, this method will
        apply the rename in the select statement for this node, and propagate that further along via an
        instance set transform to rename the measures.

        Any node operating on the output of this node will need to use the measure aliases instead of
        the measure names as references.

        """
        # Get the data from the parent, and change measure instances to the aggregated state.
        from_data_set: SqlDataSet = node.parent_node.accept(self)
        aggregated_instance_set = from_data_set.instance_set.transform(
            ChangeMeasureAggregationState(
                {
                    AggregationState.NON_AGGREGATED: AggregationState.COMPLETE,
                    AggregationState.COMPLETE: AggregationState.COMPLETE,
                    AggregationState.PARTIAL: AggregationState.COMPLETE,
                }
            )
        )
        # Also, the columns should always follow the resolver format.
        aggregated_instance_set = aggregated_instance_set.transform(
            ChangeAssociatedColumns(self._column_association_resolver)
        )

        from_data_set_alias = self._next_unique_table_alias()

        # Convert the instance set into a set of select column statements with updated aliases
        # Note any measure with an alias requirement will be recast at this point, and
        # downstream consumers of the resulting node must therefore request aggregated measures
        # by their appropriate aliases
        select_column_set: SelectColumnSet = aggregated_instance_set.transform(
            CreateSelectColumnsWithMeasuresAggregated(
                table_alias=from_data_set_alias,
                column_resolver=self._column_association_resolver,
                semantic_model_lookup=self._semantic_model_lookup,
                metric_input_measure_specs=node.metric_input_measure_specs,
            )
        )

        if any((spec.alias for spec in node.metric_input_measure_specs)):
            # This is a little silly, but we need to update the column instance set with the new aliases
            # There are a number of refactoring options - simplest is to consolidate this with
            # ChangeMeasureAggregationState, assuming there are no ordering dependencies up above
            aggregated_instance_set = aggregated_instance_set.transform(
                AliasAggregatedMeasures(metric_input_measure_specs=node.metric_input_measure_specs)
            )
            # and make sure we follow the resolver format for any newly aliased measures....
            aggregated_instance_set = aggregated_instance_set.transform(
                ChangeAssociatedColumns(self._column_association_resolver)
            )

        return SqlDataSet(
            instance_set=aggregated_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                # This will generate expressions with the appropriate aggregation functions e.g. SUM()
                select_columns=select_column_set.as_tuple(),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(),
                # This will generate expressions to group by the columns that don't correspond to a measure instance.
                group_bys=select_column_set.without_measure_columns().as_tuple(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> SqlDataSet:
        """Generates the query that realizes the behavior of ComputeMetricsNode."""
        from_data_set: SqlDataSet = node.parent_node.accept(self)
        from_data_set_alias = self._next_unique_table_alias()

        # TODO: Check that all measures for the metrics are in the input instance set
        # The desired output instance set has no measures, so create a copy with those removed.
        output_instance_set: InstanceSet = from_data_set.instance_set.transform(RemoveMeasures())

        # Also, the output columns should always follow the resolver format.
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))
        output_instance_set = output_instance_set.transform(RemoveMetrics())

        non_metric_select_column_set: SelectColumnSet = output_instance_set.transform(
            CreateSelectColumnsForInstances(
                table_alias=from_data_set_alias,
                column_resolver=self._column_association_resolver,
            )
        )

        # Add select columns that would compute the metrics to the select columns.
        metric_select_columns = []
        metric_instances = []
        for metric_spec in node.metric_specs:
            metric = self._metric_lookup.get_metric(metric_spec.as_reference)

            metric_expr: Optional[SqlExpressionNode] = None
            if metric.type is MetricType.RATIO:
                numerator = metric.type_params.numerator
                denominator = metric.type_params.denominator
                assert (
                    numerator is not None and denominator is not None
                ), "Missing numerator or denominator for ratio metric, this should have been caught in validation!"
                numerator_column_name = self._column_association_resolver.resolve_spec(
                    MetricSpec.from_reference(numerator.post_aggregation_reference)
                ).column_name
                denominator_column_name = self._column_association_resolver.resolve_spec(
                    MetricSpec.from_reference(denominator.post_aggregation_reference)
                ).column_name

                metric_expr = SqlRatioComputationExpression(
                    numerator=SqlColumnReferenceExpression(
                        SqlColumnReference(
                            table_alias=from_data_set_alias,
                            column_name=numerator_column_name,
                        )
                    ),
                    denominator=SqlColumnReferenceExpression(
                        SqlColumnReference(
                            table_alias=from_data_set_alias,
                            column_name=denominator_column_name,
                        )
                    ),
                )
            elif metric.type is MetricType.SIMPLE:
                if len(metric.input_measures) > 0:
                    assert (
                        len(metric.input_measures) == 1
                    ), "Measure proxy metrics should always source from exactly 1 measure."
                    expr = self._column_association_resolver.resolve_spec(
                        MeasureSpec(
                            element_name=metric.input_measures[0].post_aggregation_measure_reference.element_name
                        )
                    ).column_name
                else:
                    expr = metric.name
                # Use a column reference to improve query optimization.
                metric_expr = SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=from_data_set_alias,
                        column_name=expr,
                    )
                )
            elif metric.type is MetricType.CUMULATIVE:
                assert (
                    len(metric.measure_references) == 1
                ), "Cumulative metrics should always source from exactly 1 measure."
                expr = self._column_association_resolver.resolve_spec(
                    MeasureSpec(element_name=metric.input_measures[0].post_aggregation_measure_reference.element_name)
                ).column_name
                metric_expr = SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=from_data_set_alias,
                        column_name=expr,
                    )
                )
            elif metric.type is MetricType.DERIVED:
                assert metric.type_params.expr
                metric_expr = SqlStringExpression(sql_expr=metric.type_params.expr)
            else:
                assert_values_exhausted(metric.type)

            assert metric_expr

            output_column_association = self._column_association_resolver.resolve_spec(metric_spec.alias_spec)
            metric_select_columns.append(
                SqlSelectColumn(
                    expr=metric_expr,
                    column_alias=output_column_association.column_name,
                )
            )
            metric_instances.append(
                MetricInstance(
                    associated_columns=(output_column_association,),
                    defined_from=(MetricModelReference(metric_name=metric_spec.element_name),),
                    spec=metric_spec.alias_spec,
                )
            )
        output_instance_set = output_instance_set.transform(AddMetrics(metric_instances))

        combined_select_column_set = non_metric_select_column_set.merge(
            SelectColumnSet(metric_columns=metric_select_columns)
        )

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                select_columns=combined_select_column_set.as_tuple(),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(),
                group_bys=(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> SqlDataSet:  # noqa: D
        from_data_set: SqlDataSet = node.parent_node.accept(self)
        output_instance_set = from_data_set.instance_set
        from_data_set_alias = self._next_unique_table_alias()

        # Also, the output columns should always follow the resolver format.
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))

        order_by_descriptions = []
        for order_by_spec in node.order_by_specs:
            order_by_descriptions.append(
                SqlOrderByDescription(
                    expr=SqlColumnReferenceExpression(
                        col_ref=SqlColumnReference(
                            table_alias=from_data_set_alias,
                            column_name=self._column_association_resolver.resolve_spec(
                                order_by_spec.instance_spec
                            ).column_name,
                        )
                    ),
                    desc=order_by_spec.descending,
                )
            )

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=output_instance_set.transform(
                    CreateSelectColumnsForInstances(from_data_set_alias, self._column_association_resolver)
                ).as_tuple(),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(),
                group_bys=(),
                where=None,
                order_bys=tuple(order_by_descriptions),
                limit=node.limit,
            ),
        )

    def visit_write_to_result_dataframe_node(self, node: WriteToResultDataframeNode) -> SqlDataSet:
        """This is an operation that can't be represented as an SQL query.

        Instead, it should be handled in the execution plan as an operation that runs an SQL query and saves it to
        a dataframe.
        """
        raise RuntimeError("This node type is not supported.")

    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> SqlDataSet:
        """Similar to visit_write_to_result_dataframe_node()."""
        raise RuntimeError("This node type is not supported.")

    def visit_pass_elements_filter_node(self, node: FilterElementsNode) -> SqlDataSet:
        """Generates the query that realizes the behavior of FilterElementsNode."""
        from_data_set: SqlDataSet = node.parent_node.accept(self)
        output_instance_set = from_data_set.instance_set.transform(FilterElements(node.include_specs))
        from_data_set_alias = self._next_unique_table_alias()

        # Also, the output columns should always follow the resolver format.
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=output_instance_set.transform(
                    CreateSelectColumnsForInstances(from_data_set_alias, self._column_association_resolver)
                ).as_tuple(),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(),
                group_bys=(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_where_constraint_node(self, node: WhereConstraintNode) -> SqlDataSet:
        """Adds where clause to SQL statement from parent node."""
        from_data_set: SqlDataSet = node.parent_node.accept(self)
        output_instance_set = from_data_set.instance_set
        from_data_set_alias = self._next_unique_table_alias()

        column_associations_in_where_sql: Sequence[ColumnAssociation] = CreateColumnAssociations(
            column_association_resolver=self._column_association_resolver
        ).transform(spec_set=node.where.linkable_spec_set.as_spec_set)

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=output_instance_set.transform(
                    CreateSelectColumnsForInstances(from_data_set_alias, self._column_association_resolver)
                ).as_tuple(),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(),
                group_bys=(),
                where=SqlStringExpression(
                    sql_expr=node.where.where_sql,
                    used_columns=tuple(
                        column_association.column_name for column_association in column_associations_in_where_sql
                    ),
                    bind_parameters=node.where.bind_parameters,
                ),
                order_bys=(),
            ),
        )

    def _make_select_columns_for_metrics(
        self,
        table_alias_to_metric_specs: OrderedDict[str, Sequence[MetricSpec]],
        aggregation_type: Optional[AggregationType],
    ) -> List[SqlSelectColumn]:
        """Creates select columns that get the given metric using the given table alias.

        e.g.

        with table_alias_to_metric_specs = {"a": MetricSpec(element_name="bookings")}

        ->

        a.bookings AS bookings
        """
        select_columns = []
        for table_alias, metric_specs in table_alias_to_metric_specs.items():
            for metric_spec in metric_specs:
                metric_column_name = self._column_association_resolver.resolve_spec(metric_spec).column_name
                column_reference_expression = SqlColumnReferenceExpression(
                    col_ref=SqlColumnReference(
                        table_alias=table_alias,
                        column_name=metric_column_name,
                    )
                )
                if aggregation_type:
                    select_expression: SqlExpressionNode = SqlFunctionExpression.build_expression_from_aggregation_type(
                        aggregation_type=aggregation_type, sql_column_expression=column_reference_expression
                    )
                else:
                    select_expression = column_reference_expression

                select_columns.append(
                    SqlSelectColumn(
                        expr=select_expression,
                        column_alias=metric_column_name,
                    )
                )
        return select_columns

    def visit_combine_metrics_node(self, node: CombineMetricsNode) -> SqlDataSet:
        """Join computed metric datasets together to return a single dataset containing all metrics.

        This node may exist in one of two situations: when metrics need to be combined in order to produce a single
        dataset with all required inputs for a derived metric (in which case the join type is INNER), or when
        metrics need to be combined in order to produce a single dataset of output for downstream consumption by
        the end user, in which case we will use FULL OUTER JOIN.

        In the case of a multi-data-source FULL OUTER JOIN the join key will be a coalesced set of all previously
        seen dimension values. For example:
            FROM (
              ...
            ) subq_9
            FULL OUTER JOIN (
              ...
            ) subq_10
            ON
              subq_9.is_instant = subq_10.is_instant
              AND subq_9.ds = subq_10.ds
            FULL OUTER JOIN (
              ...
            ) subq_11
            ON
              COALESCE(subq_9.is_instant, subq_10.is_instant) = subq_11.is_instant
              AND COALESCE(subq_9.ds, subq_10.ds) = subq_11.ds

        Whenever these nodes are joined using a FULL OUTER JOIN, we must also do a subsequent re-aggregation pass to
        deduplicate the dimension value outputs across different metrics. This can happen if one or more of the
        dimensions contains a NULL value. In that case, the FULL OUTER JOIN condition will fail, because NULL = NULL
        returns NULL. Unfortunately, there's no way to do a robust NULL-safe comparison across engines in a FULL
        OUTER JOIN context, because many engines do not support complex ON conditions or other techniques we might
        use to apply a sentinel value for NULL to NULL comparisons.
        """
        assert (
            len(node.parent_nodes) > 1
        ), "Shouldn't have a CombineMetricsNode in the dataflow plan if there's only 1 parent."

        parent_data_sets: List[AnnotatedSqlDataSet] = []
        table_alias_to_metric_specs: OrderedDict[str, Sequence[MetricSpec]] = OrderedDict()

        for parent_node in node.parent_nodes:
            parent_sql_data_set = parent_node.accept(self)
            table_alias = self._next_unique_table_alias()
            parent_data_sets.append(AnnotatedSqlDataSet(data_set=parent_sql_data_set, alias=table_alias))
            table_alias_to_metric_specs[table_alias] = parent_sql_data_set.instance_set.spec_set.metric_specs

        # When we create the components of the join that combines metrics it will be one of INNER, FULL OUTER,
        # or CROSS JOIN. Order doesn't matter for these join types, so we will use the first element in the FROM
        # clause and create join descriptions from the rest.
        from_data_set = parent_data_sets[0]
        join_data_sets = parent_data_sets[1:]

        # Sanity check that all parents have the same linkable specs before building the join descriptions.
        linkable_specs = from_data_set.data_set.instance_set.spec_set.linkable_specs
        assert all(
            [set(x.data_set.instance_set.spec_set.linkable_specs) == set(linkable_specs) for x in join_data_sets]
        ), "All parent nodes should have the same set of linkable instances since all values are coalesced."

        linkable_spec_set = from_data_set.data_set.instance_set.spec_set.transform(SelectOnlyLinkableSpecs())
        join_type = SqlJoinType.CROSS_JOIN if len(linkable_spec_set.all_specs) == 0 else node.join_type

        joins_descriptions: List[SqlJoinDescription] = []
        # TODO: refactor this loop into SqlQueryPlanJoinBuilder
        column_associations = tuple(
            self._column_association_resolver.resolve_spec(spec) for spec in linkable_spec_set.all_specs
        )
        column_names = tuple(association.column_name for association in column_associations)
        aliases_seen = [from_data_set.alias]
        for join_data_set in join_data_sets:
            joins_descriptions.append(
                SqlQueryPlanJoinBuilder.make_combine_metrics_join_description(
                    from_data_set=from_data_set,
                    join_data_set=join_data_set,
                    join_type=join_type,
                    column_names=column_names,
                    table_aliases_for_coalesce=aliases_seen,
                )
            )
            aliases_seen.append(join_data_set.alias)

        # We can merge all parent instances since the common linkable instances will be de-duped.
        output_instance_set = InstanceSet.merge([x.data_set.instance_set for x in parent_data_sets])
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))

        metric_aggregation_type = AggregationType.MAX if node.join_type is SqlJoinType.FULL_OUTER else None
        metric_select_column_set = SelectColumnSet(
            metric_columns=self._make_select_columns_for_metrics(
                table_alias_to_metric_specs, aggregation_type=metric_aggregation_type
            )
        )
        linkable_select_column_set = linkable_spec_set.transform(
            CreateSelectCoalescedColumnsForLinkableSpecs(
                column_association_resolver=self._column_association_resolver,
                table_aliases=[x.alias for x in parent_data_sets],
            )
        )
        combined_select_column_set = linkable_select_column_set.merge(metric_select_column_set)

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                select_columns=combined_select_column_set.as_tuple(),
                from_source=from_data_set.data_set.sql_select_node,
                from_source_alias=from_data_set.alias,
                joins_descs=tuple(joins_descriptions),
                group_bys=linkable_select_column_set.as_tuple() if node.join_type is SqlJoinType.FULL_OUTER else (),
                where=None,
                order_bys=(),
            ),
        )

    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> SqlDataSet:
        """Convert ConstrainTimeRangeNode to a SqlDataSet by building the time constraint comparison.

        Use the smallest time granularity to build the comparison since that's what was used in the semantic model
        definition and it wouldn't have a DATE_TRUNC() in the expression. We want to build this:

            ds >= '2020-01-01' AND ds <= '2020-02-01'

        instead of this: DATE_TRUNC('month', ds) >= '2020-01-01' AND DATE_TRUNC('month', ds <= '2020-02-01')
        """
        from_data_set: SqlDataSet = node.parent_node.accept(self)
        from_data_set_alias = self._next_unique_table_alias()

        time_dimension_instances_for_metric_time = sorted(
            from_data_set.metric_time_dimension_instances,
            key=lambda x: x.spec.time_granularity.to_int(),
        )

        assert (
            len(time_dimension_instances_for_metric_time) > 0
        ), "No metric time dimensions found in the input data set for this node"

        time_dimension_instance_for_metric_time = time_dimension_instances_for_metric_time[0]

        # Build an expression like "ds >= CAST('2020-01-01' AS TIMESTAMP) AND ds <= CAST('2020-01-02' AS TIMESTAMP)"
        constrain_metric_time_column_condition = _make_time_range_comparison_expr(
            table_alias=from_data_set_alias,
            column_alias=time_dimension_instance_for_metric_time.associated_column.column_name,
            time_range_constraint=node.time_range_constraint,
        )

        output_instance_set = from_data_set.instance_set
        # Output columns should always follow the resolver format.
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=output_instance_set.transform(
                    CreateSelectColumnsForInstances(from_data_set_alias, self._column_association_resolver)
                ).as_tuple(),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(),
                group_bys=(),
                where=constrain_metric_time_column_condition,
                order_bys=(),
            ),
        )

    def convert_to_sql_query_plan(
        self,
        sql_engine_type: SqlEngine,
        sql_query_plan_id: str,
        dataflow_plan_node: Union[BaseOutput, ComputedMetricsOutput],
        optimization_level: SqlQueryOptimizationLevel = SqlQueryOptimizationLevel.O4,
    ) -> SqlQueryPlan:
        """Create an SQL query plan that represents the computation up to the given dataflow plan node."""
        sql_select_node: SqlQueryPlanNode = dataflow_plan_node.accept(self).sql_select_node

        # TODO: Make this a more generally accessible attribute instead of checking against the
        # BigQuery-ness of the engine
        use_column_alias_in_group_by = sql_engine_type is SqlEngine.BIGQUERY

        for optimizer in SqlQueryOptimizerConfiguration.optimizers_for_level(
            optimization_level, use_column_alias_in_group_by=use_column_alias_in_group_by
        ):
            logger.info(f"Applying optimizer: {optimizer.__class__.__name__}")
            sql_select_node = optimizer.optimize(sql_select_node)

        return SqlQueryPlan(plan_id=sql_query_plan_id, render_node=sql_select_node)

    def visit_metric_time_dimension_transform_node(self, node: MetricTimeDimensionTransformNode) -> SqlDataSet:
        """Implement the behavior of the MetricTimeDimensionTransformNode.

        This node will create an output data set that is similar to the input data set, but the measure instances it
        contains is a subset of the input data set. Only measure instances that have an aggregation time dimension
        matching the one defined in the node will be passed. In addition, an additional time dimension instance for
        "metric time" will be included. See DataSet.metric_time_dimension_reference().
        """
        input_data_set: SqlDataSet = node.parent_node.accept(self)

        # Find which measures have an aggregation time dimension that is the same as the one specified in the node.
        # Only these measures will be in the output data set.
        output_measure_instances = []
        for measure_instance in input_data_set.instance_set.measure_instances:
            semantic_model = self._semantic_model_lookup.get_by_reference(
                semantic_model_reference=measure_instance.origin_semantic_model_reference.semantic_model_reference
            )
            assert semantic_model is not None, (
                f"{measure_instance} was defined from {measure_instance.origin_semantic_model_reference}, but that"
                f"can't be found"
            )
            aggregation_time_dimension_for_measure = semantic_model.checked_agg_time_dimension_for_measure(
                measure_reference=measure_instance.spec.as_reference
            )
            if aggregation_time_dimension_for_measure == node.aggregation_time_dimension_reference:
                output_measure_instances.append(measure_instance)

        if len(output_measure_instances) == 0:
            raise RuntimeError(
                f"No measure instances in the input source match the aggregation time dimension "
                f"{node.aggregation_time_dimension_reference}. Check if the dataflow plan was constructed correctly."
            )

        # Find time dimension instances that refer to the same dimension as the one specified in the node.
        matching_time_dimension_instances = []
        for time_dimension_instance in input_data_set.instance_set.time_dimension_instances:
            # The specification for the time dimension to use for aggregation is the local one.
            if (
                len(time_dimension_instance.spec.entity_links) == 0
                and time_dimension_instance.spec.reference == node.aggregation_time_dimension_reference
            ):
                matching_time_dimension_instances.append(time_dimension_instance)

        output_time_dimension_instances: List[TimeDimensionInstance] = []
        output_time_dimension_instances.extend(input_data_set.instance_set.time_dimension_instances)
        output_column_to_input_column: OrderedDict[str, str] = OrderedDict()

        # For those matching time dimension instances, create the analog metric time dimension instances for the output.
        for matching_time_dimension_instance in matching_time_dimension_instances:
            metric_time_dimension_spec = DataSet.metric_time_dimension_spec(
                time_granularity=matching_time_dimension_instance.spec.time_granularity,
                date_part=matching_time_dimension_instance.spec.date_part,
            )
            metric_time_dimension_column_association = self._column_association_resolver.resolve_spec(
                metric_time_dimension_spec
            )
            output_time_dimension_instances.append(
                TimeDimensionInstance(
                    defined_from=matching_time_dimension_instance.defined_from,
                    associated_columns=(self._column_association_resolver.resolve_spec(metric_time_dimension_spec),),
                    spec=metric_time_dimension_spec,
                )
            )
            output_column_to_input_column[
                metric_time_dimension_column_association.column_name
            ] = matching_time_dimension_instance.associated_column.column_name

        output_instance_set = InstanceSet(
            measure_instances=tuple(output_measure_instances),
            dimension_instances=input_data_set.instance_set.dimension_instances,
            time_dimension_instances=tuple(output_time_dimension_instances),
            entity_instances=input_data_set.instance_set.entity_instances,
            metric_instances=input_data_set.instance_set.metric_instances,
        )
        output_instance_set = ChangeAssociatedColumns(self._column_association_resolver).transform(output_instance_set)

        from_data_set_alias = self._next_unique_table_alias()

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=CreateSelectColumnsForInstances(
                    column_resolver=self._column_association_resolver,
                    table_alias=from_data_set_alias,
                    output_to_input_column_mapping=output_column_to_input_column,
                )
                .transform(output_instance_set)
                .as_tuple(),
                from_source=input_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(),
                group_bys=(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> SqlDataSet:
        """Implements the behaviour of SemiAdditiveJoinNode.

        This node will get the build a data set row filtered by the aggregate function on the
        specified dimension that is non-additive. Then that dataset would be joined with the input data
        on that dimension along with grouping by entities that are also passed in.
        """
        from_data_set: SqlDataSet = node.parent_node.accept(self)

        from_data_set_alias = self._next_unique_table_alias()

        # Get the output_instance_set of the parent_node
        output_instance_set = from_data_set.instance_set
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))

        # Build the JoinDescriptions to handle the row base filtering on the output_data_set
        inner_join_data_set_alias = self._next_unique_table_alias()

        column_equality_descriptions: List[ColumnEqualityDescription] = []

        # Build Time Dimension SqlSelectColumn
        time_dimension_column_name = self.column_association_resolver.resolve_spec(node.time_dimension_spec).column_name
        join_time_dimension_column_name = self.column_association_resolver.resolve_spec(
            node.time_dimension_spec.with_aggregation_state(AggregationState.COMPLETE),
        ).column_name
        time_dimension_select_column = SqlSelectColumn(
            expr=SqlFunctionExpression.build_expression_from_aggregation_type(
                aggregation_type=node.agg_by_function,
                sql_column_expression=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=inner_join_data_set_alias,
                        column_name=time_dimension_column_name,
                    ),
                ),
            ),
            column_alias=join_time_dimension_column_name,
        )
        column_equality_descriptions.append(
            ColumnEqualityDescription(
                left_column_alias=time_dimension_column_name,
                right_column_alias=join_time_dimension_column_name,
            )
        )

        # Build optional window grouping SqlSelectColumn
        entity_select_columns: List[SqlSelectColumn] = []
        for entity_spec in node.entity_specs:
            entity_column_name = self.column_association_resolver.resolve_spec(entity_spec).column_name
            entity_select_columns.append(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression(
                        SqlColumnReference(
                            table_alias=inner_join_data_set_alias,
                            column_name=entity_column_name,
                        ),
                    ),
                    column_alias=entity_column_name,
                )
            )
            column_equality_descriptions.append(
                ColumnEqualityDescription(
                    left_column_alias=entity_column_name,
                    right_column_alias=entity_column_name,
                )
            )

        # Propogate additional group by during query time of the non-additive time dimension
        queried_time_dimension_select_column: Optional[SqlSelectColumn] = None
        if node.queried_time_dimension_spec:
            query_time_dimension_column_name = self.column_association_resolver.resolve_spec(
                node.queried_time_dimension_spec
            ).column_name
            queried_time_dimension_select_column = SqlSelectColumn(
                expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=inner_join_data_set_alias,
                        column_name=query_time_dimension_column_name,
                    ),
                ),
                column_alias=query_time_dimension_column_name,
            )

        row_filter_group_bys = tuple(entity_select_columns)
        if queried_time_dimension_select_column:
            row_filter_group_bys += (queried_time_dimension_select_column,)
        # Construct SelectNode for Row filtering
        row_filter_sql_select_node = SqlSelectStatementNode(
            description=f"Filter row on {node.agg_by_function.name}({time_dimension_column_name})",
            select_columns=row_filter_group_bys + (time_dimension_select_column,),
            from_source=from_data_set.sql_select_node,
            from_source_alias=inner_join_data_set_alias,
            joins_descs=(),
            group_bys=row_filter_group_bys,
            where=None,
            order_bys=(),
        )

        join_data_set_alias = self._next_unique_table_alias()
        sql_join_desc = SqlQueryPlanJoinBuilder.make_column_equality_sql_join_description(
            right_source_node=row_filter_sql_select_node,
            left_source_alias=from_data_set_alias,
            right_source_alias=join_data_set_alias,
            column_equality_descriptions=column_equality_descriptions,
            join_type=SqlJoinType.INNER,
        )
        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                select_columns=output_instance_set.transform(
                    CreateSelectColumnsForInstances(from_data_set_alias, self._column_association_resolver)
                ).as_tuple(),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(sql_join_desc,),
                group_bys=(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> SqlDataSet:  # noqa: D
        parent_data_set = node.parent_node.accept(self)
        parent_alias = self._next_unique_table_alias()

        # Build time spine dataset
        metric_time_dimension_instance: Optional[TimeDimensionInstance] = None
        for instance in parent_data_set.metric_time_dimension_instances:
            if len(instance.spec.entity_links) == 0:
                # Use the instance with the lowest granularity
                if not metric_time_dimension_instance or (
                    instance.spec.time_granularity < metric_time_dimension_instance.spec.time_granularity
                ):
                    metric_time_dimension_instance = instance
        assert (
            metric_time_dimension_instance
        ), "Can't query offset metric without a time dimension. Validations should have prevented this."
        metric_time_dimension_column_name = self.column_association_resolver.resolve_spec(
            metric_time_dimension_instance.spec
        ).column_name
        time_spine_alias = self._next_unique_table_alias()
        time_spine_dataset = self._make_time_spine_data_set(
            metric_time_dimension_instance=metric_time_dimension_instance,
            metric_time_dimension_column_name=metric_time_dimension_column_name,
            time_spine_source=self._time_spine_source,
            time_range_constraint=node.time_range_constraint,
        )

        # Build join expression
        join_description = SqlQueryPlanJoinBuilder.make_join_to_time_spine_join_description(
            node=node,
            time_spine_alias=time_spine_alias,
            metric_time_dimension_column_name=metric_time_dimension_column_name,
            parent_sql_select_node=parent_data_set.sql_select_node,
            parent_alias=parent_alias,
        )

        # Use all instances EXCEPT metric_time from parent data set.
        non_metric_time_parent_instance_set = InstanceSet(
            measure_instances=parent_data_set.instance_set.measure_instances,
            dimension_instances=parent_data_set.instance_set.dimension_instances,
            time_dimension_instances=tuple(
                time_dimension_instance
                for time_dimension_instance in parent_data_set.instance_set.time_dimension_instances
                if time_dimension_instance.spec.element_name != DataSet.metric_time_dimension_reference().element_name
            ),
            entity_instances=parent_data_set.instance_set.entity_instances,
            metric_instances=parent_data_set.instance_set.metric_instances,
            metadata_instances=parent_data_set.instance_set.metadata_instances,
        )
        non_metric_time_select_columns = create_select_columns_for_instance_sets(
            self._column_association_resolver, OrderedDict({parent_alias: non_metric_time_parent_instance_set})
        )

        # Use metric_time column from time spine.
        assert (
            len(time_spine_dataset.instance_set.time_dimension_instances) == 1
            and len(time_spine_dataset.sql_select_node.select_columns) == 1
        ), "Time spine dataset not configured properly. Expected exactly one column."
        time_dim_instance = time_spine_dataset.instance_set.time_dimension_instances[0]
        time_spine_column_select_expr: Union[
            SqlColumnReferenceExpression, SqlDateTruncExpression
        ] = SqlColumnReferenceExpression(
            SqlColumnReference(table_alias=time_spine_alias, column_name=time_dim_instance.spec.qualified_name)
        )

        # Add requested granularities (skip for default granularity) and date_parts.
        metric_time_select_columns = []
        metric_time_dimension_instances = []
        where: Optional[SqlExpressionNode] = None
        for metric_time_dimension_spec in node.metric_time_dimension_specs:
            # Apply granularity to SQL.
            if metric_time_dimension_spec.time_granularity == self._time_spine_source.time_column_granularity:
                select_expr: SqlExpressionNode = time_spine_column_select_expr
            else:
                select_expr = SqlDateTruncExpression(
                    time_granularity=metric_time_dimension_spec.time_granularity, arg=time_spine_column_select_expr
                )
                if node.offset_to_grain:
                    # Filter down to one row per granularity period
                    new_filter = SqlComparisonExpression(
                        left_expr=select_expr, comparison=SqlComparison.EQUALS, right_expr=time_spine_column_select_expr
                    )
                    if not where:
                        where = new_filter
                    else:
                        where = SqlLogicalExpression(operator=SqlLogicalOperator.OR, args=(where, new_filter))
            # Apply date_part to SQL.
            if metric_time_dimension_spec.date_part:
                select_expr = SqlExtractExpression(date_part=metric_time_dimension_spec.date_part, arg=select_expr)
            time_dim_spec = TimeDimensionSpec(
                element_name=time_dim_instance.spec.element_name,
                entity_links=time_dim_instance.spec.entity_links,
                time_granularity=metric_time_dimension_spec.time_granularity,
                date_part=metric_time_dimension_spec.date_part,
                aggregation_state=time_dim_instance.spec.aggregation_state,
            )
            time_dim_instance = TimeDimensionInstance(
                defined_from=time_dim_instance.defined_from,
                associated_columns=(self._column_association_resolver.resolve_spec(time_dim_spec),),
                spec=time_dim_spec,
            )
            metric_time_dimension_instances.append(time_dim_instance)
            metric_time_select_columns.append(
                SqlSelectColumn(expr=select_expr, column_alias=time_dim_instance.associated_column.column_name)
            )
        metric_time_instance_set = InstanceSet(time_dimension_instances=tuple(metric_time_dimension_instances))

        return SqlDataSet(
            instance_set=InstanceSet.merge([metric_time_instance_set, non_metric_time_parent_instance_set]),
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                select_columns=tuple(metric_time_select_columns) + non_metric_time_select_columns,
                from_source=time_spine_dataset.sql_select_node,
                from_source_alias=time_spine_alias,
                joins_descs=(join_description,),
                group_bys=(),
                order_bys=(),
                where=where,
            ),
        )
