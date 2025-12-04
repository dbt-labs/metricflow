from __future__ import annotations

import datetime as dt
import logging
from collections import OrderedDict, defaultdict
from typing import Dict, List, Optional, Sequence, Set, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.time_spine import PydanticTimeSpineCustomGranularityColumn
from dbt_semantic_interfaces.references import (
    MetricModelReference,
    SemanticModelElementReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import AggregationType, ConversionCalculationType, MetricType, PeriodAggregation
from dbt_semantic_interfaces.validations.unique_valid_name import MetricFlowReservedKeywords
from metricflow_semantics.aggregation_properties import AggregationState
from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.sequential_id import SequentialIdGenerator
from metricflow_semantics.errors.error_classes import MetricFlowInternalError, SemanticManifestConfigurationError
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.instances import (
    GroupByMetricInstance,
    InstanceSet,
    InstanceSetTransform,
    MdoInstance,
    MetadataInstance,
    MetricInstance,
    TimeDimensionInstance,
    group_instances_by_type,
)
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.group_by_metric_spec import GroupByMetricSpec
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.metadata_spec import MetadataSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlAggregateFunctionExpression,
    SqlArithmeticExpression,
    SqlArithmeticOperator,
    SqlBetweenExpression,
    SqlCaseExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlComparison,
    SqlComparisonExpression,
    SqlDateTruncExpression,
    SqlExpressionNode,
    SqlExtractExpression,
    SqlFunction,
    SqlFunctionExpression,
    SqlGenerateUuidExpression,
    SqlIntegerExpression,
    SqlLogicalExpression,
    SqlLogicalOperator,
    SqlNullExpression,
    SqlRatioComputationExpression,
    SqlStringExpression,
    SqlStringLiteralExpression,
    SqlWindowFunction,
    SqlWindowFunctionExpression,
    SqlWindowOrderByArgument,
)
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.time.time_constants import ISO8601_PYTHON_FORMAT, ISO8601_PYTHON_TS_FORMAT
from metricflow_semantics.time.time_spine_source import TimeSpineSource
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.runtime import log_block_runtime

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor
from metricflow.dataflow.nodes.add_generated_uuid import AddGeneratedUuidColumnNode
from metricflow.dataflow.nodes.aggregate_simple_metric_inputs import AggregateSimpleMetricInputsNode
from metricflow.dataflow.nodes.alias_specs import AliasSpecsNode
from metricflow.dataflow.nodes.combine_aggregated_outputs import CombineAggregatedOutputsNode
from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode
from metricflow.dataflow.nodes.constrain_time import ConstrainTimeRangeNode
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.dataflow.nodes.join_conversion_events import JoinConversionEventsNode
from metricflow.dataflow.nodes.join_over_time import JoinOverTimeRangeNode
from metricflow.dataflow.nodes.join_to_base import JoinOnEntitiesNode
from metricflow.dataflow.nodes.join_to_custom_granularity import JoinToCustomGranularityNode
from metricflow.dataflow.nodes.join_to_time_spine import JoinToTimeSpineNode
from metricflow.dataflow.nodes.metric_time_transform import MetricTimeDimensionTransformNode
from metricflow.dataflow.nodes.min_max import MinMaxNode
from metricflow.dataflow.nodes.offset_base_grain_by_custom_grain import OffsetBaseGrainByCustomGrainNode
from metricflow.dataflow.nodes.offset_custom_granularity import OffsetCustomGranularityNode
from metricflow.dataflow.nodes.order_by_limit import OrderByLimitNode
from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
from metricflow.dataflow.nodes.semi_additive_join import SemiAdditiveJoinNode
from metricflow.dataflow.nodes.where_filter import WhereConstraintNode
from metricflow.dataflow.nodes.window_reaggregation_node import WindowReaggregationNode
from metricflow.dataflow.nodes.write_to_data_table import WriteToResultDataTableNode
from metricflow.dataflow.nodes.write_to_table import WriteToResultTableNode
from metricflow.dataset.dataset_classes import DataSet
from metricflow.dataset.sql_dataset import AnnotatedSqlDataSet, SqlDataSet
from metricflow.plan_conversion.instance_set_transforms.aggregated_simple_metric_input import (
    CreateAggregatedSimpleMetricInputsTransform,
)
from metricflow.plan_conversion.instance_set_transforms.instance_converters import (
    AddGroupByMetric,
    AddMetadata,
    AddMetrics,
    ChangeAssociatedColumns,
    ChangeSimpleMetricInputAggregationState,
    ConvertToMetadata,
    CreateSelectColumnForCombineOutputNode,
    CreateSqlColumnReferencesForInstances,
    FilterElements,
    FilterLinkableInstancesWithLeadingLink,
    RemoveMetrics,
    RemoveSimpleMetricInputTransform,
    UpdateSimpleMetricInputFillNullsWith,
)
from metricflow.plan_conversion.instance_set_transforms.select_columns import (
    CreateSelectColumnsForInstances,
    create_simple_select_columns_for_instance_sets,
)
from metricflow.plan_conversion.select_column_gen import SelectColumnSet
from metricflow.plan_conversion.spec_transforms import (
    CreateColumnAssociations,
    CreateSelectCoalescedColumnsForLinkableSpecs,
    SelectOnlyLinkableSpecs,
)
from metricflow.plan_conversion.to_sql_plan.sql_join_builder import ColumnEqualityDescription, SqlPlanJoinBuilder
from metricflow.sql.column_alias_renamer import ColumnAliasRenamer
from metricflow.sql.sql_ctas_node import SqlCreateTableAsNode
from metricflow.sql.sql_cte_node import SqlCteNode
from metricflow.sql.sql_plan import (
    SqlSelectColumn,
)
from metricflow.sql.sql_select_node import SqlJoinDescription, SqlOrderByDescription, SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode

logger = logging.getLogger(__name__)


class DataflowNodeToSqlSubqueryVisitor(DataflowPlanNodeVisitor[SqlDataSet]):
    """Generates a SQL query plan by converting a node's parents to sub-queries.

    TODO: Split classes in this file to separate files.
    """

    def __init__(
        self,
        column_association_resolver: ColumnAssociationResolver,
        semantic_manifest_lookup: SemanticManifestLookup,
        spec_output_order: Sequence[InstanceSpec] = (),
        _node_to_output_data_set: Optional[Dict[DataflowPlanNode, SqlDataSet]] = None,
    ) -> None:
        """Initializer.

        Args:
            column_association_resolver: controls how columns for instances are generated and used between nested
            queries.
            semantic_manifest_lookup: Self-explanatory.
        """
        self._column_association_resolver = column_association_resolver
        self._semantic_manifest_lookup = semantic_manifest_lookup
        self._metric_lookup = semantic_manifest_lookup.metric_lookup
        self._semantic_model_lookup = semantic_manifest_lookup.semantic_model_lookup
        self._manifest_object_lookup = semantic_manifest_lookup.manifest_object_lookup
        self._time_spine_sources = TimeSpineSource.build_standard_time_spine_sources(
            semantic_manifest_lookup.semantic_manifest
        )
        self._custom_granularity_time_spine_sources = TimeSpineSource.build_custom_time_spine_sources(
            tuple(self._time_spine_sources.values())
        )
        self._node_to_output_data_set: Dict[DataflowPlanNode, SqlDataSet] = _node_to_output_data_set or {}
        self._spec_output_order = spec_output_order

    def _next_unique_table_alias(self) -> str:
        """Return the next unique table alias to use in generating queries."""
        return SequentialIdGenerator.create_next_id(StaticIdPrefix.SUB_QUERY).str_value

    def _next_unique_cte_alias(self) -> str:
        """Return the next unique CTE alias to use in generating queries."""
        return SequentialIdGenerator.create_next_id(StaticIdPrefix.CTE).str_value

    def get_output_data_set(self, node: DataflowPlanNode) -> SqlDataSet:
        """Cached since this will be called repeatedly during the computation of multiple metrics.

        # TODO: The cache needs to be pruned, but has not yet been an issue.
        """
        if node not in self._node_to_output_data_set:
            result = node.accept(self)
            self._node_to_output_data_set[node] = result
            return result

        return self._node_to_output_data_set[node].with_copied_sql_node()

    def cache_output_data_sets(self, nodes: Sequence[DataflowPlanNode]) -> None:
        """Cache the output of the given nodes for consistent retrieval with `get_output_data_set`."""
        with log_block_runtime(f"cache_output_data_sets for {len(nodes)} nodes"):
            for node in nodes:
                self.get_output_data_set(node)

    def copy(self) -> DataflowNodeToSqlSubqueryVisitor:
        """Return a copy of this with the same nodes cached."""
        return DataflowNodeToSqlSubqueryVisitor(
            column_association_resolver=self._column_association_resolver,
            semantic_manifest_lookup=self._semantic_manifest_lookup,
            _node_to_output_data_set=dict(self._node_to_output_data_set),
        )

    # TODO: replace this with a dataflow plan node for cumulative metrics - SL-3324
    def _make_time_spine_data_set(
        self,
        agg_time_dimension_instances: Tuple[TimeDimensionInstance, ...],
        time_range_constraint: Optional[TimeRangeConstraint] = None,
        time_spine_where_constraints: Sequence[WhereFilterSpec] = (),
    ) -> SqlDataSet:
        """Returns a dataset with a datetime column for each agg_time_dimension granularity requested.

        Column alias will use 'metric_time' or the agg_time_dimension name depending on which the user requested.
        """
        time_spine_table_alias = self._next_unique_table_alias()

        queried_specs = [instance.spec for instance in agg_time_dimension_instances]
        queried_specs_set = set(queried_specs)
        specs_required_for_where_constraints = [
            spec
            for constraint in time_spine_where_constraints
            for spec in constraint.linkable_spec_set.time_dimension_specs
            if spec not in queried_specs_set
        ]
        required_specs = queried_specs + specs_required_for_where_constraints

        time_spine_sources = TimeSpineSource.choose_time_spine_sources(
            required_time_spine_specs=required_specs, time_spine_sources=self._time_spine_sources
        )
        if len(time_spine_sources) != 1:
            raise RuntimeError(
                LazyFormat(
                    "Unexpected number of time spine sources required for query - cumulative metrics should require exactly one.",
                    time_spine_sources=time_spine_sources,
                )
            )
        time_spine_source = time_spine_sources[0]
        time_spine_base_granularity = ExpandedTimeGranularity.from_time_granularity(time_spine_source.base_granularity)

        base_column_expr = SqlColumnReferenceExpression.from_column_reference(
            table_alias=time_spine_table_alias, column_name=time_spine_source.base_column
        )
        select_columns: Tuple[SqlSelectColumn, ...] = ()
        for agg_time_dimension_spec in required_specs:
            column_alias = self._column_association_resolver.resolve_spec(agg_time_dimension_spec).column_name
            agg_time_grain = agg_time_dimension_spec.time_granularity
            # If there is a date_part selected, apply an EXTRACT() to the base column.
            if agg_time_dimension_spec.date_part:
                expr: SqlExpressionNode = SqlExtractExpression.create(
                    date_part=agg_time_dimension_spec.date_part, arg=base_column_expr
                )
            else:
                assert (
                    agg_time_grain is not None
                ), f"No date part or time granularity specified for time dimension spec {agg_time_dimension_spec}"

                # If the requested granularity is the same as the granularity of the spine, do a direct select.
                if agg_time_grain == time_spine_base_granularity:
                    expr = base_column_expr
                # If the granularity is custom, select the appropriate custom granularity column.
                elif agg_time_grain.is_custom_granularity:
                    for custom_granularity in time_spine_source.custom_granularities:
                        expr = SqlColumnReferenceExpression.from_column_reference(
                            table_alias=time_spine_table_alias, column_name=custom_granularity.parsed_column_name
                        )
                # Otherwise, apply the requested standard granularity using a DATE_TRUNC() on the base column.
                else:
                    expr = SqlDateTruncExpression.create(
                        time_granularity=agg_time_grain.base_granularity, arg=base_column_expr
                    )
            select_columns += (SqlSelectColumn(expr=expr, column_alias=column_alias),)

        output_instance_set = InstanceSet(
            time_dimension_instances=tuple(
                [
                    TimeDimensionInstance(
                        defined_from=(
                            SemanticModelElementReference(
                                semantic_model_name=time_spine_source.sql_table.table_name,
                                element_name=spec.element_name,
                            ),
                        ),
                        associated_columns=(self._column_association_resolver.resolve_spec(spec),),
                        spec=spec,
                    )
                    for spec in queried_specs
                ]
            )
        )

        # A group by will be needed to ensure unique rows unless the time spine base grain is included.
        apply_group_by_in_inner_select_node = all(
            spec.time_granularity != time_spine_base_granularity for spec in required_specs
        )
        apply_group_by_in_outer_select_node = apply_group_by_in_inner_select_node is False and all(
            spec.time_granularity != time_spine_base_granularity for spec in queried_specs
        )

        inner_sql_select_node = SqlSelectStatementNode.create(
            description=time_spine_source.data_set_description,
            select_columns=select_columns,
            from_source=SqlTableNode.create(sql_table=time_spine_source.sql_table),
            from_source_alias=time_spine_table_alias,
            group_bys=select_columns if apply_group_by_in_inner_select_node else (),
            where=(
                DataflowNodeToSqlSubqueryVisitor._make_time_range_comparison_expr(
                    table_alias=time_spine_table_alias,
                    column_alias=time_spine_source.base_column,
                    time_range_constraint=time_range_constraint,
                )
                if time_range_constraint
                else None
            ),
        )

        # Where constraints must be applied in an outer query since they are using an alias (e.g., 'metric_time__day'),
        # and some engines do not support using an alias in the WHERE clause.
        if len(time_spine_where_constraints) == 0:
            return SqlDataSet(instance_set=output_instance_set, sql_select_node=inner_sql_select_node)

        # Build outer query to apply where constraints.
        inner_query_alias = self._next_unique_table_alias()
        where_constraint_exprs = [
            self._render_where_constraint_expr(constraint) for constraint in time_spine_where_constraints
        ]
        complete_outer_where_filter: Optional[SqlExpressionNode] = None
        if len(where_constraint_exprs) > 1:
            complete_outer_where_filter = SqlLogicalExpression.create(
                operator=SqlLogicalOperator.AND, args=where_constraint_exprs
            )
        elif len(where_constraint_exprs) == 1:
            complete_outer_where_filter = where_constraint_exprs[0]

        outer_query_output_instance_set = InstanceSet(time_dimension_instances=agg_time_dimension_instances)
        outer_query_select_columns = create_simple_select_columns_for_instance_sets(
            column_resolver=self._column_association_resolver,
            table_alias_to_instance_set=OrderedDict({inner_query_alias: outer_query_output_instance_set}),
        )

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description="Filter Time Spine",
                select_columns=outer_query_select_columns,
                from_source=inner_sql_select_node,
                from_source_alias=inner_query_alias,
                where=complete_outer_where_filter,
                group_bys=outer_query_select_columns if apply_group_by_in_outer_select_node else (),
            ),
        )

    def visit_source_node(self, node: ReadSqlSourceNode) -> SqlDataSet:
        """Generate the SQL to read from the source."""
        return SqlDataSet(
            # This visitor is assumed to create a unique SELECT node for each dataflow node, and since
            # `ReadSqlSourceNode` may be used multiple times in the plan, create a copy of the SELECT.
            # `SqlColumnPrunerOptimizer` relies on this assumption to keep track of what columns are required at each
            # node.
            sql_select_node=node.data_set.checked_sql_select_node.copy(),
            instance_set=node.data_set.instance_set,
        )

    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> SqlDataSet:
        """Generate time range join SQL."""
        table_alias_to_instance_set: OrderedDict[str, InstanceSet] = OrderedDict()
        parent_data_set = self.get_output_data_set(node.parent_node)
        parent_data_set_alias = self._next_unique_table_alias()

        # Assemble time_spine dataset with a column for each agg_time_dimension requested.
        agg_time_dimension_instances = parent_data_set.instances_for_time_dimensions(
            node.queried_agg_time_dimension_specs
        )
        time_spine_data_set_alias = self._next_unique_table_alias()
        time_spine_data_set = self._make_time_spine_data_set(
            agg_time_dimension_instances=agg_time_dimension_instances, time_range_constraint=node.time_range_constraint
        )

        # Build the join description.
        join_spec = self._choose_instance_for_time_spine_join(agg_time_dimension_instances).spec
        annotated_parent = parent_data_set.annotate(alias=parent_data_set_alias, metric_time_spec=join_spec)
        annotated_time_spine = time_spine_data_set.annotate(alias=time_spine_data_set_alias, metric_time_spec=join_spec)
        join_desc = SqlPlanJoinBuilder.make_cumulative_metric_time_range_join_description(
            node=node, metric_data_set=annotated_parent, time_spine_data_set=annotated_time_spine
        )

        # Build select columns, replacing agg_time_dimensions from the parent node with columns from the time spine.
        table_alias_to_instance_set[time_spine_data_set_alias] = time_spine_data_set.instance_set
        table_alias_to_instance_set[parent_data_set_alias] = parent_data_set.instance_set.transform(
            FilterElements(exclude_specs=InstanceSpecSet(time_dimension_specs=node.queried_agg_time_dimension_specs))
        )
        select_columns = create_simple_select_columns_for_instance_sets(
            column_resolver=self._column_association_resolver, table_alias_to_instance_set=table_alias_to_instance_set
        )

        return SqlDataSet(
            instance_set=parent_data_set.instance_set,  # The output instances are the same as the input instances.
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                select_columns=select_columns,
                from_source=time_spine_data_set.checked_sql_select_node,
                from_source_alias=time_spine_data_set_alias,
                join_descs=(join_desc,),
            ),
        )

    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> SqlDataSet:
        """Generates the query that realizes the behavior of the JoinOnEntitiesNode."""
        from_data_set = self.get_output_data_set(node.left_node)
        from_data_set_alias = self._next_unique_table_alias()

        # Change the aggregation state for the simple-metric inputs to be partially aggregated if it was previously aggregated
        # since we removed the entities and added the dimensions. The dimensions could have the same value for
        # multiple rows, so we'll need to re-aggregate.
        from_data_set_output_instance_set = from_data_set.instance_set.transform(
            # TODO: is this filter doing anything? seems like no?
            FilterElements(include_specs=from_data_set.instance_set.spec_set)
        ).transform(
            ChangeSimpleMetricInputAggregationState(
                {
                    AggregationState.NON_AGGREGATED: AggregationState.NON_AGGREGATED,
                    AggregationState.COMPLETE: AggregationState.PARTIAL,
                    AggregationState.PARTIAL: AggregationState.PARTIAL,
                }
            )
        )
        instances_to_build_simple_select_columns_for = OrderedDict(
            {from_data_set_alias: from_data_set_output_instance_set}
        )

        # Build SQL join description, instance set, and select columns for each join target.
        output_instance_set = from_data_set_output_instance_set
        select_columns: Tuple[SqlSelectColumn, ...] = ()
        sql_join_descs: List[SqlJoinDescription] = []
        for join_description in node.join_targets:
            join_on_entity = join_description.join_on_entity
            right_node_to_join = join_description.join_node
            right_data_set: SqlDataSet = self.get_output_data_set(right_node_to_join)
            right_data_set_alias = self._next_unique_table_alias()

            # Build join description.
            sql_join_desc = SqlPlanJoinBuilder.make_base_output_join_description(
                left_data_set=AnnotatedSqlDataSet(data_set=from_data_set, alias=from_data_set_alias),
                right_data_set=AnnotatedSqlDataSet(data_set=right_data_set, alias=right_data_set_alias),
                join_description=join_description,
            )
            sql_join_descs.append(sql_join_desc)

            if join_on_entity:
                # Remove any instances that already have the join_on_entity as the leading link. This will prevent a duplicate
                # entity link when we add it in the next step.
                right_instance_set_filtered = FilterLinkableInstancesWithLeadingLink(
                    join_on_entity.reference
                ).transform(right_data_set.instance_set)

                # After the right data set is joined, update the entity links to indicate that joining on the entity was
                # required to reach the spec. If the "country" dimension was joined and "user_id" is the join_on_entity,
                # then the joined data set should have the "user__country" dimension.
                new_instances: Tuple[MdoInstance, ...] = ()
                for original_instance in right_instance_set_filtered.linkable_instances:
                    new_instance = original_instance.with_entity_prefix(
                        join_on_entity.reference, column_association_resolver=self._column_association_resolver
                    )
                    # Build new select column using the old column name as the expr and the new column name as the alias.
                    select_column = SqlSelectColumn(
                        expr=SqlColumnReferenceExpression.from_column_reference(
                            table_alias=right_data_set_alias,
                            column_name=original_instance.associated_column.column_name,
                        ),
                        column_alias=new_instance.associated_column.column_name,
                    )
                    new_instances += (new_instance,)
                    select_columns += (select_column,)
                right_instance_set_after_join = group_instances_by_type(new_instances)
            else:
                right_instance_set_after_join = right_data_set.instance_set
                instances_to_build_simple_select_columns_for[right_data_set_alias] = right_instance_set_after_join

            output_instance_set = InstanceSet.merge([output_instance_set, right_instance_set_after_join])

        select_columns += create_simple_select_columns_for_instance_sets(
            column_resolver=self._column_association_resolver,
            table_alias_to_instance_set=instances_to_build_simple_select_columns_for,
        )
        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                select_columns=select_columns,
                from_source=from_data_set.checked_sql_select_node,
                from_source_alias=from_data_set_alias,
                join_descs=tuple(sql_join_descs),
            ),
        )

    def visit_aggregate_simple_metric_inputs_node(self, node: AggregateSimpleMetricInputsNode) -> SqlDataSet:
        """Generates the query that realizes the behavior of `AggregateSimpleMetricInputsNode`.

        This will produce a query that aggregates all simple-metric inputs from a given input semantic model per the
        simple-metric spec

        In the event the input aggregations are applied to simple-metric inputs with aliases set, in case of, e.g.,
        a constraint applied to one instance of the simple metric but not another one, this method will
        apply the rename in the select statement for this node, and propagate that further along via an
        instance set transform to rename the simple-metric inputs.

        Any node operating on the output of this node will need to use the simple-metric aliases instead of
        the simple-metric names as references.

        """
        # Get the data from the parent, and change simple-metric input instances to the aggregated state.
        from_data_set: SqlDataSet = self.get_output_data_set(node.parent_node)
        aggregated_instance_set = from_data_set.instance_set.transform(
            ChangeSimpleMetricInputAggregationState(
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

        # Add fill null property to corresponding simple-metric input spec
        aggregated_instance_set = aggregated_instance_set.transform(
            UpdateSimpleMetricInputFillNullsWith(null_fill_value_mapping=node.null_fill_value_mapping)
        )
        from_data_set_alias = self._next_unique_table_alias()

        # Convert the instance set into a set of select column statements with updated aliases
        # Note any simple-metric input with an alias requirement will be recast at this point, and
        # downstream consumers of the resulting node must therefore request aggregated simple-metric inputs
        # by their appropriate aliases
        create_columns_result = aggregated_instance_set.transform(
            CreateAggregatedSimpleMetricInputsTransform(
                table_alias=from_data_set_alias,
                column_resolver=self._column_association_resolver,
                manifest_object_lookup=self._manifest_object_lookup,
            )
        )

        return SqlDataSet(
            instance_set=aggregated_instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                # This will generate expressions with the appropriate aggregation functions e.g. SUM()
                select_columns=create_columns_result.select_column_set.columns_in_default_order,
                from_source=from_data_set.checked_sql_select_node,
                from_source_alias=from_data_set_alias,
                # This will generate expressions to group by the columns that don't correspond to a simple-metric input instance.
                group_bys=create_columns_result.group_by_column_set.columns_in_default_order,
            ),
        )

    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> SqlDataSet:
        """Generates the query that realizes the behavior of ComputeMetricsNode."""
        from_data_set: SqlDataSet = self.get_output_data_set(node.parent_node)
        from_data_set_alias = self._next_unique_table_alias()

        # TODO: Check that all simple-metric inputs for the metrics are in the input instance set
        # The desired output instance set has no simple-metric inputs, so create a copy with those removed.
        output_instance_set: InstanceSet = from_data_set.instance_set.transform(RemoveSimpleMetricInputTransform())

        # Also, the output columns should always follow the resolver format.
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))
        output_instance_set = output_instance_set.transform(RemoveMetrics())

        if node.for_group_by_source_node:
            assert (
                len(node.metric_specs) == 1 and len(output_instance_set.entity_instances) == 1
            ), "Group by metrics currently only support exactly one metric grouped by exactly one entity."

        non_metric_select_column_set: SelectColumnSet = output_instance_set.transform(
            CreateSelectColumnsForInstances(
                table_alias=from_data_set_alias,
                column_resolver=self._column_association_resolver,
            )
        ).select_column_set

        # Add select columns that would compute the metrics to the select columns.
        metric_select_columns = []
        metric_instances = []
        group_by_metric_instance: Optional[GroupByMetricInstance] = None
        simple_metric_input_element_name_to_instance = {
            instance.spec.element_name: instance
            for instance in from_data_set.instance_set.simple_metric_input_instances
        }
        for metric_spec in node.metric_specs:
            metric = self._metric_lookup.get_metric(metric_spec.reference)

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

                metric_expr = SqlRatioComputationExpression.create(
                    numerator=SqlColumnReferenceExpression.create(
                        SqlColumnReference(
                            table_alias=from_data_set_alias,
                            column_name=numerator_column_name,
                        )
                    ),
                    denominator=SqlColumnReferenceExpression.create(
                        SqlColumnReference(
                            table_alias=from_data_set_alias,
                            column_name=denominator_column_name,
                        )
                    ),
                )
            elif metric.type is MetricType.SIMPLE:
                metric_name = metric_spec.element_name
                simple_metric_input_instance = simple_metric_input_element_name_to_instance.get(metric_name)
                if simple_metric_input_instance is None:
                    raise MetricFlowInternalError(
                        LazyFormat(
                            "Expected a simple metric instance with a element name matching the metric name to be"
                            " present in the input",
                            metric_name=metric_name,
                            input_instance_spec_set=from_data_set.instance_set.spec_set,
                        )
                    )
                metric_expr = self.__make_col_reference_or_coalesce_expr(
                    column_name=simple_metric_input_instance.associated_column.column_name,
                    null_fill_value=metric.type_params.fill_nulls_with,
                    from_data_set_alias=from_data_set_alias,
                )
            elif metric.type is MetricType.CUMULATIVE:
                cumulative_type_params = metric.type_params.cumulative_type_params
                assert cumulative_type_params is not None and cumulative_type_params.metric is not None, LazyFormat(
                    "A cumulative metric should have `cumulative_type_params.metric` set", metric=metric
                )
                metric_expr = self.__make_col_reference_or_coalesce_expr(
                    column_name=cumulative_type_params.metric.name,
                    null_fill_value=metric.type_params.fill_nulls_with,
                    from_data_set_alias=from_data_set_alias,
                )
            elif metric.type is MetricType.DERIVED:
                if metric.type_params.expr is None:
                    raise SemanticManifestConfigurationError(
                        "Derived metrics are required to have an `expr` in their YAML definition."
                    )
                metric_expr = SqlStringExpression.create(sql_expr=metric.type_params.expr)
            elif metric.type == MetricType.CONVERSION:
                conversion_type_params = metric.type_params.conversion_type_params
                assert (
                    conversion_type_params
                ), "A conversion metric should have type_params.conversion_type_params defined."
                input_base_metric = conversion_type_params.base_metric
                assert input_base_metric is not None, "A conversion metric must have a base metric."
                input_conversion_metric = conversion_type_params.conversion_metric
                assert input_conversion_metric is not None, "A conversion metric must have a conversion metric."

                base_input_metric_instance = simple_metric_input_element_name_to_instance.get(input_base_metric.name)
                conversion_input_metric_instance = simple_metric_input_element_name_to_instance.get(
                    input_conversion_metric.name
                )
                if base_input_metric_instance is None or conversion_input_metric_instance is None:
                    raise MetricFlowInternalError(
                        LazyFormat(
                            "Expected a simple metric instances with an element name matching the"
                            " input metrics for the conversion metric name to be"
                            " present in the input",
                            base_input_metric_instance=base_input_metric_instance,
                            conversion_input_metric_instance=conversion_input_metric_instance,
                            input_instance_spec_set=from_data_set.instance_set.spec_set,
                        )
                    )

                calculation_type = conversion_type_params.calculation
                conversion_column_reference = SqlColumnReferenceExpression.create(
                    SqlColumnReference(
                        table_alias=from_data_set_alias,
                        column_name=conversion_input_metric_instance.associated_column.column_name,
                    )
                )
                base_column_reference = SqlColumnReferenceExpression.create(
                    SqlColumnReference(
                        table_alias=from_data_set_alias,
                        column_name=base_input_metric_instance.associated_column.column_name,
                    )
                )
                if calculation_type == ConversionCalculationType.CONVERSION_RATE:
                    metric_expr = SqlRatioComputationExpression.create(
                        numerator=conversion_column_reference,
                        denominator=base_column_reference,
                    )
                elif calculation_type == ConversionCalculationType.CONVERSIONS:
                    metric_expr = conversion_column_reference
            else:
                assert_values_exhausted(metric.type)

            assert metric_expr

            defined_from = MetricModelReference(metric_name=metric_spec.element_name)

            if node.for_group_by_source_node:
                entity_spec = output_instance_set.entity_instances[0].spec
                group_by_metric_spec = GroupByMetricSpec(
                    element_name=metric_spec.element_name,
                    entity_links=(),
                    metric_subquery_entity_links=entity_spec.entity_links + (entity_spec.reference,),
                )
                output_column_association = self._column_association_resolver.resolve_spec(group_by_metric_spec)
                group_by_metric_instance = GroupByMetricInstance(
                    associated_columns=(output_column_association,),
                    defined_from=defined_from,
                    spec=group_by_metric_spec,
                )
            else:
                output_column_association = self._column_association_resolver.resolve_spec(metric_spec)
                metric_instances.append(
                    MetricInstance(
                        associated_columns=(output_column_association,),
                        defined_from=defined_from,
                        spec=metric_spec,
                    )
                )
            metric_select_columns.append(
                SqlSelectColumn(expr=metric_expr, column_alias=output_column_association.column_name)
            )

        transform_func: InstanceSetTransform = AddMetrics(metric_instances)
        if group_by_metric_instance:
            transform_func = AddGroupByMetric(group_by_metric_instance)

        output_instance_set = output_instance_set.transform(transform_func)

        combined_select_column_set = non_metric_select_column_set.merge(
            SelectColumnSet.create(metric_columns=metric_select_columns)
        )

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                select_columns=combined_select_column_set.columns_in_default_order,
                from_source=from_data_set.checked_sql_select_node,
                from_source_alias=from_data_set_alias,
            ),
        )

    def __make_col_reference_or_coalesce_expr(
        self, column_name: str, null_fill_value: Optional[int], from_data_set_alias: str
    ) -> SqlExpressionNode:
        # Use a column reference to improve query optimization.
        metric_expr: SqlExpressionNode = SqlColumnReferenceExpression.create(
            SqlColumnReference(table_alias=from_data_set_alias, column_name=column_name)
        )
        # Coalesce nulls to requested integer value, if requested.
        if null_fill_value is not None:
            metric_expr = SqlAggregateFunctionExpression.create(
                sql_function=SqlFunction.COALESCE,
                sql_function_args=[metric_expr, SqlStringExpression.create(str(null_fill_value))],
            )
        return metric_expr

    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> SqlDataSet:  # noqa: D102
        from_data_set: SqlDataSet = self.get_output_data_set(node.parent_node)
        output_instance_set = from_data_set.instance_set
        from_data_set_alias = self._next_unique_table_alias()

        # Also, the output columns should always follow the resolver format.
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))

        order_by_descriptions = []
        for order_by_spec in node.order_by_specs:
            order_by_descriptions.append(
                SqlOrderByDescription(
                    expr=SqlColumnReferenceExpression.create(
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
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=output_instance_set.transform(
                    CreateSelectColumnsForInstances(from_data_set_alias, self._column_association_resolver)
                ).get_columns(),
                from_source=from_data_set.checked_sql_select_node,
                from_source_alias=from_data_set_alias,
                order_bys=tuple(order_by_descriptions),
                limit=node.limit,
            ),
        )

    def visit_write_to_result_data_table_node(self, node: WriteToResultDataTableNode) -> SqlDataSet:  # noqa: D102
        # Currently, this is a SELECT statement that passes all columns through. This will be updated in a later PR
        # to support specific column ordering.
        input_data_set: SqlDataSet = self.get_output_data_set(node.parent_node)
        input_data_set_alias = self._next_unique_table_alias()
        transform_result = input_data_set.instance_set.transform(
            CreateSelectColumnsForInstances(
                table_alias=input_data_set_alias,
                column_resolver=self._column_association_resolver,
            )
        )

        return SqlDataSet(
            instance_set=input_data_set.instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                select_columns=transform_result.get_columns(self._spec_output_order),
                from_source=input_data_set.checked_sql_select_node,
                from_source_alias=input_data_set_alias,
            ),
        )

    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> SqlDataSet:  # noqa: D102
        input_data_set: SqlDataSet = self.get_output_data_set(node.parent_node)
        input_data_set_alias = self._next_unique_table_alias()

        transform_result = input_data_set.instance_set.transform(
            CreateSelectColumnsForInstances(
                table_alias=input_data_set_alias,
                column_resolver=self._column_association_resolver,
            )
        )

        return SqlDataSet(
            instance_set=input_data_set.instance_set,
            sql_node=SqlCreateTableAsNode.create(
                sql_table=node.output_sql_table,
                parent_node=SqlSelectStatementNode.create(
                    description=node.description,
                    select_columns=transform_result.get_columns(self._spec_output_order),
                    from_source=input_data_set.checked_sql_select_node,
                    from_source_alias=input_data_set_alias,
                ),
            ),
        )

    def visit_filter_elements_node(self, node: FilterElementsNode) -> SqlDataSet:
        """Generates the query that realizes the behavior of FilterElementsNode."""
        from_data_set: SqlDataSet = self.get_output_data_set(node.parent_node)
        output_instance_set = from_data_set.instance_set.transform(FilterElements(node.include_specs))
        from_data_set_alias = self._next_unique_table_alias()

        # Also, the output columns should always follow the resolver format.
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))

        # This creates select expressions for all columns referenced in the instance set.
        select_columns = output_instance_set.transform(
            CreateSelectColumnsForInstances(from_data_set_alias, self._column_association_resolver)
        ).get_columns()

        # If distinct values requested, group by all select columns.
        group_bys = select_columns if node.distinct else ()
        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                select_columns=select_columns,
                from_source=from_data_set.checked_sql_select_node,
                from_source_alias=from_data_set_alias,
                group_bys=group_bys,
            ),
        )

    def visit_where_constraint_node(self, node: WhereConstraintNode) -> SqlDataSet:
        """Adds a `WHERE` clause to the SQL statement from parent / input node."""
        input_dataset: SqlDataSet = self.get_output_data_set(node.parent_node)

        # For the migration, render simple-metric-inputs without the dunder.
        modified_resolver = self._column_association_resolver.with_options(
            dunder_prefix_simple_metric_inputs=False,
        )

        # The input dataset has different column aliases, so create a mapping to an intermediate alias.
        # In this case, the intermediate aliases are the simple-metric-input names without the dunder.
        # e.g. for SimpleMetricInputInstance("bookings") -> "__bookings"
        # input_column_alias_to_intermediate_column_alias = {"__bookings": "bookings"}
        column_alias_renamer = ColumnAliasRenamer()
        input_column_alias_to_intermediate_column_alias = {}
        intermediate_column_alias_to_instance = {}

        for instance in input_dataset.instance_set.as_tuple:
            next_column_alias = modified_resolver.resolve_spec(instance.spec).column_name
            input_column_alias_to_intermediate_column_alias[
                instance.associated_column.column_name
            ] = modified_resolver.resolve_spec(instance.spec).column_name
            intermediate_column_alias_to_instance[next_column_alias] = instance

        inner_query_alias = self._next_unique_table_alias()

        # Generate SQL that uses a subquery to change column aliases to the intermediate aliases.
        # See `rename_via_subquery` for why a subquery is needed.
        #
        # e.g.
        #   From the parent SQL:
        #
        #   SELECT
        #       col as __bookings
        #       ...
        #
        #   ->
        #   -- Outer query
        #   SELECT
        #       subq.bookings AS bookings
        #       ...
        #   FROM (
        #       -- Inner query
        #       SELECT col AS bookings
        #       ...
        #   ) subq

        outer_select_node = column_alias_renamer.rename_via_subquery(
            select_statement_node=input_dataset.checked_sql_select_node,
            previous_column_name_to_next_column_name=input_column_alias_to_intermediate_column_alias,
            description="Constrain Output with WHERE",
            inner_query_alias=inner_query_alias,
        )

        # Add the `WHERE` clause now that the column identifier without the dunder is valid.
        # e.g.
        #   SELECT
        #       subq.bookings AS bookings
        #       ...
        #   FROM (
        #       SELECT col AS bookings
        #       ...
        #   ) subq
        #   WHERE bookings IS NOT NULL

        outer_select_node = outer_select_node.with_where_clause(self._render_where_constraint_expr(node.where))

        # Since the outer query sets the column aliases to the intermediate values, change them to the
        # ones generated by the default resolver with the dunder prefix.
        #
        # e.g.
        #
        #   SELECT
        #       subq.bookings AS __bookings
        #       ...
        #   FROM (
        #       SELECT col AS bookings
        #       ...
        #   ) subq
        #   WHERE bookings IS NOT NULL
        intermediate_column_alias_to_output_column_alias = {}
        for intermediate_column_alias, instance in intermediate_column_alias_to_instance.items():
            intermediate_column_alias_to_output_column_alias[
                intermediate_column_alias
            ] = self._column_association_resolver.resolve_spec(instance.spec).column_name

        outer_select_node = column_alias_renamer.rename(
            select_statement_node=outer_select_node,
            previous_column_alias_to_next_column_alias=intermediate_column_alias_to_output_column_alias,
        )

        # Create the output instance set by copying the input instance set since this does not add or remove
        # instances.
        #
        # Since the input instances could have mapped them to different column aliases, use `ChangeAssociatedColumns`
        # to change them to the ones generated above.
        #
        # e.g. while the input instances usually map SimpleMetricInputSpec("bookings") -> "__bookings",
        # SimpleMetricInputInstance("bookings") -> "some_other_col" would have been a valid input too.
        output_instance_set = input_dataset.instance_set.transform(
            ChangeAssociatedColumns(self._column_association_resolver)
        )

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=outer_select_node,
        )

    def _render_where_constraint_expr(self, where_filter: WhereFilterSpec) -> SqlStringExpression:
        """Build SqlStringExpression from WhereFilterSpec."""
        column_associations_in_where_sql = CreateColumnAssociations(
            column_association_resolver=self._column_association_resolver
        ).transform(spec_set=where_filter.instance_spec_set)
        return SqlStringExpression.create(
            sql_expr=where_filter.where_sql,
            used_columns=tuple(
                column_association.column_name for column_association in column_associations_in_where_sql
            ),
            bind_parameter_set=where_filter.bind_parameters,
        )

    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> SqlDataSet:
        """Join aggregated output datasets together to return a single dataset containing all metrics/simple-metric inputs.

        This node may exist in one of two situations: when metrics need to be combined in order to produce a single
        dataset with all required inputs for a metric (ie., derived metric), or when metrics need to be combined in order to
        produce a single dataset of output for downstream consumption by the end user.

        The join key will be a coalesced set of all previously seen dimension values. For example:
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
        ), "Shouldn't have a CombineAggregatedOutputsNode in the dataflow plan if there's only 1 parent."

        parent_data_sets: List[AnnotatedSqlDataSet] = []
        table_alias_to_instance_set: OrderedDict[str, InstanceSet] = OrderedDict()

        for parent_node in node.parent_nodes:
            parent_sql_data_set = self.get_output_data_set(parent_node)
            table_alias = self._next_unique_table_alias()
            parent_data_sets.append(AnnotatedSqlDataSet(data_set=parent_sql_data_set, alias=table_alias))
            table_alias_to_instance_set[table_alias] = parent_sql_data_set.instance_set

        # When we create the components of the join that combines metrics it will be one of INNER, FULL OUTER,
        # or CROSS JOIN. Order doesn't matter for these join types, so we will use the first element in the FROM
        # clause and create join descriptions from the rest.
        from_data_set = parent_data_sets[0]
        join_data_sets = parent_data_sets[1:]

        # Sanity check that all parents have the same linkable specs before building the join descriptions.
        linkable_specs = from_data_set.data_set.instance_set.spec_set.linkable_specs
        assert all(
            [set(x.data_set.instance_set.spec_set.linkable_specs) == set(linkable_specs) for x in join_data_sets]
        ), (
            "All join data sets should have the same set of linkable instances as the from dataset since all values are coalesced.\n"
            f"From dataset instance set: {from_data_set.data_set.instance_set}\n"
            f"Join dataset instance sets: {[join_data_set.data_set.instance_set for join_data_set in join_data_sets]}"
        )

        linkable_spec_set = from_data_set.data_set.instance_set.spec_set.transform(SelectOnlyLinkableSpecs())
        join_type = SqlJoinType.CROSS_JOIN if len(linkable_spec_set.all_specs) == 0 else SqlJoinType.FULL_OUTER

        joins_descriptions: List[SqlJoinDescription] = []
        # TODO: refactor this loop into SqlQueryPlanJoinBuilder
        column_associations = tuple(
            self._column_association_resolver.resolve_spec(spec) for spec in linkable_spec_set.all_specs
        )
        column_names = tuple(association.column_name for association in column_associations)
        aliases_seen = [from_data_set.alias]
        for join_data_set in join_data_sets:
            joins_descriptions.append(
                SqlPlanJoinBuilder.make_join_description_for_combining_datasets(
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

        aggregated_select_columns = SelectColumnSet.create()
        for table_alias, instance_set in table_alias_to_instance_set.items():
            aggregated_select_columns = aggregated_select_columns.merge(
                instance_set.transform(
                    CreateSelectColumnForCombineOutputNode(
                        table_alias=table_alias,
                        column_resolver=self._column_association_resolver,
                        metric_lookup=self._metric_lookup,
                    )
                )
            )
        linkable_select_column_set = linkable_spec_set.transform(
            CreateSelectCoalescedColumnsForLinkableSpecs(
                column_association_resolver=self._column_association_resolver,
                table_aliases=[x.alias for x in parent_data_sets],
            )
        )
        combined_select_column_set = linkable_select_column_set.merge(aggregated_select_columns)

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                select_columns=combined_select_column_set.columns_in_default_order,
                from_source=from_data_set.data_set.checked_sql_select_node,
                from_source_alias=from_data_set.alias,
                join_descs=tuple(joins_descriptions),
                group_bys=linkable_select_column_set.columns_in_default_order,
            ),
        )

    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> SqlDataSet:
        """Convert ConstrainTimeRangeNode to a SqlDataSet by building the time constraint comparison.

        Use the smallest time granularity to build the comparison since that's what was used in the semantic model
        definition and it wouldn't have a DATE_TRUNC() in the expression. We want to build this:

            ds >= '2020-01-01' AND ds <= '2020-02-01'

        instead of this: DATE_TRUNC('month', ds) >= '2020-01-01' AND DATE_TRUNC('month', ds <= '2020-02-01')

        Since time range constraints are always bound by a range of standard date/time values, this conversion
        cannot use custom granularities.
        """
        from_data_set: SqlDataSet = self.get_output_data_set(node.parent_node)
        from_data_set_alias = self._next_unique_table_alias()

        time_dimension_instance_for_metric_time = from_data_set.metric_time_instance_for_time_constraint

        # Build an expression like "ds >= CAST('2020-01-01' AS TIMESTAMP) AND ds <= CAST('2020-01-02' AS TIMESTAMP)"
        constrain_metric_time_column_condition = DataflowNodeToSqlSubqueryVisitor._make_time_range_comparison_expr(
            table_alias=from_data_set_alias,
            column_alias=time_dimension_instance_for_metric_time.associated_column.column_name,
            time_range_constraint=node.time_range_constraint,
        )

        output_instance_set = from_data_set.instance_set
        # Output columns should always follow the resolver format.
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=output_instance_set.transform(
                    CreateSelectColumnsForInstances(from_data_set_alias, self._column_association_resolver)
                ).get_columns(),
                from_source=from_data_set.checked_sql_select_node,
                from_source_alias=from_data_set_alias,
                where=constrain_metric_time_column_condition,
            ),
        )

    def visit_metric_time_dimension_transform_node(self, node: MetricTimeDimensionTransformNode) -> SqlDataSet:
        """Implement the behavior of the MetricTimeDimensionTransformNode.

        This node will create an output data set that is similar to the input data set, but the simple metric instances it
        contains is a subset of the input data set. Only simple-metric instances that have an aggregation time dimension
        matching the one defined in the node will be passed. In addition, an additional time dimension instance for
        "metric time" will be included. See DataSet.metric_time_dimension_reference().
        """
        input_data_set: SqlDataSet = self.get_output_data_set(node.parent_node)

        # Find which simple-metric inputs have an aggregation time dimension that is the same as the one specified in the node.
        # Only these simple-metric inputs will be in the output data set.
        output_simple_metric_input_instances = []
        for simple_metric_input_instance in input_data_set.instance_set.simple_metric_input_instances:
            simple_metric_input = self._manifest_object_lookup.simple_metric_name_to_input[
                simple_metric_input_instance.spec.element_name
            ]
            aggregation_time_dimension_for_measure = TimeDimensionReference(simple_metric_input.agg_time_dimension_name)

            if aggregation_time_dimension_for_measure == node.aggregation_time_dimension_reference:
                output_simple_metric_input_instances.append(simple_metric_input_instance)

        # Find time dimension instances that refer to the same dimension as the one specified in the node.
        matching_time_dimension_instances: List[TimeDimensionInstance] = []
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
            simple_metric_input_instances=tuple(output_simple_metric_input_instances),
            dimension_instances=input_data_set.instance_set.dimension_instances,
            time_dimension_instances=tuple(output_time_dimension_instances),
            entity_instances=input_data_set.instance_set.entity_instances,
            metric_instances=input_data_set.instance_set.metric_instances,
        )
        output_instance_set = ChangeAssociatedColumns(self._column_association_resolver).transform(output_instance_set)

        from_data_set_alias = self._next_unique_table_alias()

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=CreateSelectColumnsForInstances(
                    column_resolver=self._column_association_resolver,
                    table_alias=from_data_set_alias,
                    output_to_input_column_mapping=output_column_to_input_column,
                )
                .transform(output_instance_set)
                .get_columns(),
                from_source=input_data_set.checked_sql_select_node,
                from_source_alias=from_data_set_alias,
            ),
        )

    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> SqlDataSet:
        """Implements the behaviour of SemiAdditiveJoinNode.

        This node will get the build a data set row filtered by the aggregate function on the
        specified dimension that is non-additive. Then that dataset would be joined with the input data
        on that dimension along with grouping by entities that are also passed in.
        """
        from_data_set: SqlDataSet = self.get_output_data_set(node.parent_node)

        from_data_set_alias = self._next_unique_table_alias()

        # Get the output_instance_set of the parent_node
        output_instance_set = from_data_set.instance_set
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))

        # Build the JoinDescriptions to handle the row base filtering on the output_data_set
        inner_join_data_set_alias = self._next_unique_table_alias()

        column_equality_descriptions: List[ColumnEqualityDescription] = []

        # Build Time Dimension SqlSelectColumn
        time_dimension_column_name = self._column_association_resolver.resolve_spec(
            node.time_dimension_spec
        ).column_name
        join_time_dimension_column_name = self._column_association_resolver.resolve_spec(
            node.time_dimension_spec.with_aggregation_state(AggregationState.COMPLETE),
        ).column_name
        time_dimension_select_column = SqlSelectColumn(
            expr=SqlFunctionExpression.build_expression_from_aggregation_type(
                aggregation_type=node.agg_by_function,
                sql_column_expression=SqlColumnReferenceExpression.create(
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
            entity_column_name = self._column_association_resolver.resolve_spec(entity_spec).column_name
            entity_select_columns.append(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
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

        # Propagate additional group by during query time of the non-additive time dimension
        queried_time_dimension_select_column: Optional[SqlSelectColumn] = None
        if node.queried_time_dimension_spec:
            query_time_dimension_column_name = self._column_association_resolver.resolve_spec(
                node.queried_time_dimension_spec
            ).column_name
            queried_time_dimension_select_column = SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
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
        right_source_select_node = SqlSelectStatementNode.create(
            description=f"Filter row on {node.agg_by_function.name}({time_dimension_column_name})",
            select_columns=row_filter_group_bys + (time_dimension_select_column,),
            from_source=from_data_set.checked_sql_select_node.copy(),
            from_source_alias=inner_join_data_set_alias,
            group_bys=row_filter_group_bys,
        )

        right_source_alias = self._next_unique_table_alias()
        sql_join_desc = SqlPlanJoinBuilder.make_column_equality_sql_join_description(
            right_source_node=right_source_select_node,
            left_source_alias=from_data_set_alias,
            right_source_alias=right_source_alias,
            column_equality_descriptions=column_equality_descriptions,
            join_type=SqlJoinType.INNER,
        )
        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                select_columns=output_instance_set.transform(
                    CreateSelectColumnsForInstances(from_data_set_alias, self._column_association_resolver)
                ).get_columns(),
                from_source=from_data_set.checked_sql_select_node,
                from_source_alias=from_data_set_alias,
                join_descs=(sql_join_desc,),
            ),
        )

    def _choose_instance_for_time_spine_join(
        self, agg_time_dimension_instances: Sequence[TimeDimensionInstance]
    ) -> TimeDimensionInstance:
        """Find the agg_time_dimension instance with the smallest grain to use for the time spine join."""
        # We can't use a date part spec to join to the time spine, so filter those out.
        agg_time_dimension_instances = [
            instance for instance in agg_time_dimension_instances if not instance.spec.date_part
        ]
        assert len(agg_time_dimension_instances) > 0, (
            "No appropriate agg_time_dimension was found to join to the time spine. "
            "This indicates that the dataflow plan was configured incorrectly."
        )
        agg_time_dimension_instances.sort(key=lambda instance: (instance.spec.base_granularity_sort_key))
        return agg_time_dimension_instances[0]

    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> SqlDataSet:  # noqa: D102
        parent_data_set = self.get_output_data_set(node.metric_source_node)
        parent_alias = self._next_unique_table_alias()
        time_spine_data_set = self.get_output_data_set(node.time_spine_node)
        time_spine_alias = self._next_unique_table_alias()

        required_agg_time_dimension_specs = tuple(node.requested_agg_time_dimension_specs)
        join_spec_was_requested = node.join_on_time_dimension_spec in node.requested_agg_time_dimension_specs
        if not join_spec_was_requested:
            required_agg_time_dimension_specs += (node.join_on_time_dimension_spec,)

        # Build join expression.
        parent_join_column_name = self._column_association_resolver.resolve_spec(
            node.join_on_time_dimension_spec
        ).column_name
        time_spine_join_column_name = time_spine_data_set.instance_from_time_dimension_grain_and_date_part(
            time_granularity_name=node.join_on_time_dimension_spec.time_granularity_name,
            date_part=node.join_on_time_dimension_spec.date_part,
        ).associated_column.column_name
        join_description = SqlPlanJoinBuilder.make_join_to_time_spine_join_description(
            node=node,
            time_spine_alias=time_spine_alias,
            time_spine_column_name=time_spine_join_column_name,
            parent_column_name=parent_join_column_name,
            parent_sql_select_node=parent_data_set.checked_sql_select_node,
            parent_alias=parent_alias,
        )

        # Build new instances and columns.
        time_spine_required_spec_set = InstanceSpecSet(time_dimension_specs=required_agg_time_dimension_specs)
        output_parent_instance_set = parent_data_set.instance_set.transform(
            FilterElements(
                exclude_specs=InstanceSpecSet.create_from_specs(
                    tuple(
                        spec
                        for spec in time_spine_required_spec_set.time_dimension_specs
                        if spec in parent_data_set.instance_set.spec_set.time_dimension_specs
                    )
                )
            )
        )
        output_time_spine_instances: Tuple[TimeDimensionInstance, ...] = ()
        output_time_spine_columns: Tuple[SqlSelectColumn, ...] = ()
        for old_instance in time_spine_data_set.instance_set.time_dimension_instances:
            new_spec = old_instance.spec.with_window_functions(())
            if new_spec not in required_agg_time_dimension_specs:
                continue
            if old_instance.spec.window_functions:
                new_instance = old_instance.with_new_spec(
                    new_spec=new_spec, column_association_resolver=self._column_association_resolver
                )
                column = SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.from_column_reference(
                        table_alias=time_spine_alias, column_name=old_instance.associated_column.column_name
                    ),
                    column_alias=new_instance.associated_column.column_name,
                )
            else:
                new_instance = old_instance
                column = SqlSelectColumn.from_column_reference(
                    table_alias=time_spine_alias, column_name=old_instance.associated_column.column_name
                )
            output_time_spine_instances += (new_instance,)
            output_time_spine_columns += (column,)

        output_instance_set = InstanceSet.merge(
            [output_parent_instance_set, InstanceSet(time_dimension_instances=output_time_spine_instances)]
        )
        select_columns = output_time_spine_columns + create_simple_select_columns_for_instance_sets(
            self._column_association_resolver,
            OrderedDict({parent_alias: output_parent_instance_set}),
        )

        # If offset_to_grain is used, will need to filter down to rows that match selected granularities.
        # Does not apply if one of the granularities selected matches the time spine column granularity.
        where_filter: Optional[SqlExpressionNode] = None
        need_where_filter = node.offset_to_grain and not join_spec_was_requested

        # Filter down to one row per granularity period requested in the group by. Any other granularities
        # included here will be filtered out before aggregation and so should not be included in where filter.
        if need_where_filter:
            join_column_expr = SqlColumnReferenceExpression.from_column_reference(
                table_alias=time_spine_alias, column_name=parent_join_column_name
            )
            for requested_spec in node.requested_agg_time_dimension_specs:
                column_name = self._column_association_resolver.resolve_spec(requested_spec).column_name
                column_to_filter_expr = SqlColumnReferenceExpression.from_column_reference(
                    table_alias=time_spine_alias, column_name=column_name
                )
                new_where_filter = SqlComparisonExpression.create(
                    left_expr=column_to_filter_expr, comparison=SqlComparison.EQUALS, right_expr=join_column_expr
                )
                where_filter = (
                    SqlLogicalExpression.create(operator=SqlLogicalOperator.OR, args=(where_filter, new_where_filter))
                    if where_filter
                    else new_where_filter
                )

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                select_columns=select_columns,
                from_source=time_spine_data_set.checked_sql_select_node,
                from_source_alias=time_spine_alias,
                join_descs=(join_description,),
                where=where_filter,
            ),
        )

    def _get_time_spine_for_custom_granularity(self, custom_granularity_name: str) -> TimeSpineSource:
        time_spine_source = self._custom_granularity_time_spine_sources.get(custom_granularity_name)
        assert time_spine_source, (
            f"Custom granularity {custom_granularity_name} does not not exist in time spine sources. "
            f"Available custom granularities: {list(self._custom_granularity_time_spine_sources.keys())}"
        )
        return time_spine_source

    def _get_custom_granularity_column_name(self, custom_granularity_name: str) -> str:
        time_spine_source = self._get_time_spine_for_custom_granularity(custom_granularity_name)
        custom_granularity: Optional[PydanticTimeSpineCustomGranularityColumn] = None
        for custom_granularity in time_spine_source.custom_granularities:
            if custom_granularity.name == custom_granularity_name:
                return custom_granularity.parsed_column_name

        raise RuntimeError(
            f"Custom granularity {custom_granularity} not found. This indicates internal misconfiguration."
        )

    def visit_alias_specs_node(self, node: AliasSpecsNode) -> SqlDataSet:  # noqa: D102
        parent_data_set = self.get_output_data_set(node.parent_node)
        parent_alias = self._next_unique_table_alias()

        input_specs_to_output_specs: Dict[InstanceSpec, List[InstanceSpec]] = defaultdict(list)
        for change_spec in node.change_specs:
            input_specs_to_output_specs[change_spec.input_spec].append(change_spec.output_spec)

        # Build output instances & select columns.
        output_instances: Tuple[MdoInstance, ...] = ()
        output_select_columns: Tuple[SqlSelectColumn, ...] = ()
        for parent_instance in parent_data_set.instance_set.as_tuple:
            parent_spec = parent_instance.spec.without_filter_specs()
            if parent_spec in input_specs_to_output_specs:
                # If an alias was requested, build new instance & select column to match requested spec.
                new_specs = input_specs_to_output_specs[parent_spec]
                for new_spec in new_specs:
                    new_instance = parent_instance.with_new_spec(
                        new_spec=new_spec, column_association_resolver=self._column_association_resolver
                    )
                    new_select_column = SqlSelectColumn(
                        expr=SqlColumnReferenceExpression.from_column_reference(
                            table_alias=parent_alias, column_name=parent_instance.associated_column.column_name
                        ),
                        column_alias=new_instance.associated_column.column_name,
                    )
                    output_instances += (new_instance,)
                    output_select_columns += (new_select_column,)
            else:
                # Keep the instance the same and build a column that just references the parent column.
                output_instances += (parent_instance,)
                column_name = parent_instance.associated_column.column_name
                output_select_columns += (
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression.from_column_reference(
                            table_alias=parent_alias, column_name=column_name
                        ),
                        column_alias=column_name,
                    ),
                )

        return SqlDataSet(
            instance_set=group_instances_by_type(output_instances),
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                select_columns=output_select_columns,
                from_source=parent_data_set.checked_sql_select_node,
                from_source_alias=parent_alias,
            ),
        )

    def visit_join_to_custom_granularity_node(self, node: JoinToCustomGranularityNode) -> SqlDataSet:  # noqa: D102
        parent_data_set = self.get_output_data_set(node.parent_node)

        # New dataset will be joined to parent dataset without a subquery, so use the same FROM alias as the parent node.
        parent_alias = parent_data_set.checked_sql_select_node.from_source_alias
        parent_time_dimension_instance = parent_data_set.instance_for_time_dimension(
            node.time_dimension_spec.with_base_grain()
        )
        parent_column: Optional[SqlSelectColumn] = None
        for select_column in parent_data_set.checked_sql_select_node.select_columns:
            if select_column.column_alias == parent_time_dimension_instance.associated_column.column_name:
                parent_column = select_column
                break
        assert parent_column, (
            "JoinToCustomGranularityNode's expected time_dimension_spec not found in parent columns. "
            f"This indicates internal misconfiguration. Expected: "
            f"{parent_time_dimension_instance.associated_column.column_name}; Got: "
            f"{[column.column_alias for column in parent_data_set.checked_sql_select_node.select_columns]}"
        )

        # Build join expression.
        time_spine_alias = self._next_unique_table_alias()
        custom_granularity_name = node.time_dimension_spec.time_granularity_name
        assert custom_granularity_name, "Got no custom granularity name for JoinToCustomGranularityNode."

        time_spine_source = self._get_time_spine_for_custom_granularity(custom_granularity_name)
        join_description = SqlJoinDescription(
            right_source=SqlTableNode.create(sql_table=time_spine_source.sql_table),
            right_source_alias=time_spine_alias,
            on_condition=SqlComparisonExpression.create(
                left_expr=parent_column.expr,
                comparison=SqlComparison.EQUALS,
                right_expr=SqlColumnReferenceExpression.from_column_reference(
                    table_alias=time_spine_alias, column_name=time_spine_source.base_column
                ),
            ),
            join_type=SqlJoinType.LEFT_OUTER,
        )

        # Build output time spine instances and columns.
        time_spine_instance = TimeDimensionInstance(
            defined_from=parent_time_dimension_instance.defined_from,
            associated_columns=(self._column_association_resolver.resolve_spec(node.time_dimension_spec),),
            spec=node.time_dimension_spec,
        )
        time_spine_instance_set = InstanceSet(time_dimension_instances=(time_spine_instance,))
        time_spine_select_columns = (
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.from_column_reference(
                    table_alias=time_spine_alias,
                    column_name=self._get_custom_granularity_column_name(custom_granularity_name),
                ),
                column_alias=time_spine_instance.associated_column.column_name,
            ),
        )
        return SqlDataSet(
            instance_set=InstanceSet.merge([time_spine_instance_set, parent_data_set.instance_set]),
            sql_select_node=SqlSelectStatementNode.create(
                description=parent_data_set.checked_sql_select_node.description + "\n" + node.description,
                select_columns=parent_data_set.checked_sql_select_node.select_columns + time_spine_select_columns,
                from_source=parent_data_set.checked_sql_select_node.from_source,
                from_source_alias=parent_alias,
                cte_sources=parent_data_set.checked_sql_select_node.cte_sources,
                join_descs=parent_data_set.checked_sql_select_node.join_descs + (join_description,),
                where=parent_data_set.checked_sql_select_node.where,
                group_bys=parent_data_set.checked_sql_select_node.group_bys,
                order_bys=parent_data_set.checked_sql_select_node.order_bys,
                limit=parent_data_set.checked_sql_select_node.limit,
                distinct=parent_data_set.checked_sql_select_node.distinct,
            ),
        )

    def visit_min_max_node(self, node: MinMaxNode) -> SqlDataSet:  # noqa: D102
        parent_data_set = self.get_output_data_set(node.parent_node)
        parent_table_alias = self._next_unique_table_alias()
        assert (
            len(parent_data_set.checked_sql_select_node.select_columns) == 1
        ), "MinMaxNode supports exactly one parent select column."
        parent_column_alias = parent_data_set.checked_sql_select_node.select_columns[0].column_alias

        select_columns: List[SqlSelectColumn] = []
        metadata_instances: List[MetadataInstance] = []
        for agg_type in (AggregationType.MIN, AggregationType.MAX):
            metadata_spec = MetadataSpec(element_name=parent_column_alias, agg_type=agg_type)
            output_column_association = self._column_association_resolver.resolve_spec(metadata_spec)
            select_columns.append(
                SqlSelectColumn(
                    expr=SqlFunctionExpression.build_expression_from_aggregation_type(
                        aggregation_type=agg_type,
                        sql_column_expression=SqlColumnReferenceExpression.create(
                            SqlColumnReference(table_alias=parent_table_alias, column_name=parent_column_alias)
                        ),
                    ),
                    column_alias=output_column_association.column_name,
                )
            )
            metadata_instances.append(
                MetadataInstance(associated_columns=(output_column_association,), spec=metadata_spec)
            )

        return SqlDataSet(
            instance_set=parent_data_set.instance_set.transform(ConvertToMetadata(metadata_instances)),
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                select_columns=tuple(select_columns),
                from_source=parent_data_set.checked_sql_select_node,
                from_source_alias=parent_table_alias,
            ),
        )

    def visit_add_generated_uuid_column_node(self, node: AddGeneratedUuidColumnNode) -> SqlDataSet:
        """Implements the behaviour of AddGeneratedUuidColumnNode.

        Builds a new dataset that is the same as the output dataset, but with an additional column
        that contains a randomly generated UUID.
        """
        input_data_set: SqlDataSet = self.get_output_data_set(node.parent_node)
        input_data_set_alias = self._next_unique_table_alias()

        gen_uuid_spec = MetadataSpec(MetricFlowReservedKeywords.MF_INTERNAL_UUID.value)
        output_column_association = self._column_association_resolver.resolve_spec(gen_uuid_spec)
        output_instance_set = input_data_set.instance_set.transform(
            AddMetadata(
                (
                    MetadataInstance(
                        associated_columns=(output_column_association,),
                        spec=gen_uuid_spec,
                    ),
                )
            )
        )
        gen_uuid_sql_select_column = SqlSelectColumn(
            expr=SqlGenerateUuidExpression.create(), column_alias=output_column_association.column_name
        )

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description="Add column with generated UUID",
                select_columns=input_data_set.instance_set.transform(
                    CreateSelectColumnsForInstances(input_data_set_alias, self._column_association_resolver)
                ).get_columns()
                + (gen_uuid_sql_select_column,),
                from_source=input_data_set.checked_sql_select_node,
                from_source_alias=input_data_set_alias,
            ),
        )

    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> SqlDataSet:
        """Builds a resulting data set with all valid conversion events.

        This node takes the conversion and base data set and joins them against an entity and
        a valid time range to get successful conversions. It then deduplicates opportunities
        via the window function `first_value` to take the closest opportunity to the
        corresponding conversion. Then it returns a data set with each row representing a
        successful conversion. Duplication may exist in the result due to a single base event
        being able to link to multiple conversion events.
        """
        base_data_set: SqlDataSet = self.get_output_data_set(node.base_node)
        base_data_set_alias = self._next_unique_table_alias()

        conversion_data_set: SqlDataSet = self.get_output_data_set(node.conversion_node)
        conversion_data_set_alias = self._next_unique_table_alias()

        base_time_dimension_column_name = self._column_association_resolver.resolve_spec(
            node.base_time_dimension_spec
        ).column_name
        conversion_time_dimension_column_name = self._column_association_resolver.resolve_spec(
            node.conversion_time_dimension_spec
        ).column_name
        entity_column_name = self._column_association_resolver.resolve_spec(node.entity_spec).column_name

        constant_property_column_names: List[Tuple[str, str]] = []
        for constant_property in node.constant_properties or []:
            base_property_col_name = self._column_association_resolver.resolve_spec(
                constant_property.base_spec
            ).column_name
            conversion_property_col_name = self._column_association_resolver.resolve_spec(
                constant_property.conversion_spec
            ).column_name
            constant_property_column_names.append((base_property_col_name, conversion_property_col_name))

        # Builds the join conditions that is required for a successful conversion
        sql_join_description = SqlPlanJoinBuilder.make_join_conversion_join_description(
            node=node,
            base_data_set=AnnotatedSqlDataSet(
                data_set=base_data_set,
                alias=base_data_set_alias,
                _metric_time_column_name=base_time_dimension_column_name,
            ),
            conversion_data_set=AnnotatedSqlDataSet(
                data_set=conversion_data_set,
                alias=conversion_data_set_alias,
                _metric_time_column_name=conversion_time_dimension_column_name,
            ),
            column_equality_descriptions=(
                ColumnEqualityDescription(
                    left_column_alias=entity_column_name,
                    right_column_alias=entity_column_name,
                ),
            )
            + tuple(
                ColumnEqualityDescription(left_column_alias=base_col, right_column_alias=conversion_col)
                for base_col, conversion_col in constant_property_column_names
            ),
        )

        # Builds the first_value window function columns
        base_sql_column_references = base_data_set.instance_set.transform(
            CreateSqlColumnReferencesForInstances(base_data_set_alias, self._column_association_resolver)
        )

        unique_conversion_col_names = tuple(
            self._column_association_resolver.resolve_spec(spec).column_name for spec in node.unique_identifier_keys
        )
        partition_by_columns: Tuple[str, ...] = (
            entity_column_name,
            conversion_time_dimension_column_name,
        ) + unique_conversion_col_names
        if node.constant_properties:
            partition_by_columns += tuple(
                conversion_column_name for _, conversion_column_name in constant_property_column_names
            )
        base_sql_select_columns = tuple(
            SqlSelectColumn(
                expr=SqlWindowFunctionExpression.create(
                    sql_function=SqlWindowFunction.FIRST_VALUE,
                    sql_function_args=[
                        SqlColumnReferenceExpression.create(
                            SqlColumnReference(
                                table_alias=base_data_set_alias,
                                column_name=base_sql_column_reference.col_ref.column_name,
                            ),
                        )
                    ],
                    partition_by_args=[
                        SqlColumnReferenceExpression.create(
                            SqlColumnReference(
                                table_alias=conversion_data_set_alias,
                                column_name=column,
                            ),
                        )
                        for column in partition_by_columns
                    ],
                    order_by_args=[
                        SqlWindowOrderByArgument(
                            expr=SqlColumnReferenceExpression.create(
                                SqlColumnReference(
                                    table_alias=base_data_set_alias,
                                    column_name=base_time_dimension_column_name,
                                ),
                            ),
                            descending=True,
                        )
                    ],
                ),
                column_alias=base_sql_column_reference.col_ref.column_name,
            )
            for base_sql_column_reference in base_sql_column_references
        )

        conversion_data_set_output_instance_set = conversion_data_set.instance_set.transform(
            FilterElements(
                include_specs=InstanceSpecSet(simple_metric_input_specs=(node.conversion_input_metric_spec,))
            )
        )

        # Deduplicate the fanout results
        conversion_unique_key_select_columns = tuple(
            SqlSelectColumn(
                expr=SqlColumnReferenceExpression.create(
                    SqlColumnReference(
                        table_alias=conversion_data_set_alias,
                        column_name=column_name,
                    ),
                ),
                column_alias=column_name,
            )
            for column_name in unique_conversion_col_names
        )
        additional_conversion_select_columns = conversion_data_set_output_instance_set.transform(
            CreateSelectColumnsForInstances(conversion_data_set_alias, self._column_association_resolver)
        ).get_columns()
        deduped_sql_select_node = SqlSelectStatementNode.create(
            description=f"Dedupe the fanout with {','.join(spec.dunder_name for spec in node.unique_identifier_keys)} in the conversion data set",
            select_columns=base_sql_select_columns
            + conversion_unique_key_select_columns
            + additional_conversion_select_columns,
            from_source=base_data_set.checked_sql_select_node,
            from_source_alias=base_data_set_alias,
            join_descs=(sql_join_description,),
            distinct=True,
        )

        # Returns the original dataset with all the successful conversion
        output_data_set_alias = self._next_unique_table_alias()
        output_instance_set = ChangeAssociatedColumns(self._column_association_resolver).transform(
            InstanceSet.merge([conversion_data_set_output_instance_set, base_data_set.instance_set])
        )
        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description=node.description,
                select_columns=output_instance_set.transform(
                    CreateSelectColumnsForInstances(output_data_set_alias, self._column_association_resolver)
                ).get_columns(),
                from_source=deduped_sql_select_node,
                from_source_alias=output_data_set_alias,
            ),
        )

    def visit_window_reaggregation_node(self, node: WindowReaggregationNode) -> SqlDataSet:  # noqa: D102
        from_data_set = self.get_output_data_set(node.parent_node)
        parent_instance_set = from_data_set.instance_set
        parent_data_set_alias = self._next_unique_table_alias()

        metric_instance = None
        order_by_instance = None
        partition_by_instances: Tuple[MdoInstance, ...] = ()
        for instance in parent_instance_set.as_tuple:
            if instance.spec == node.metric_spec:
                metric_instance = instance
            elif instance.spec == node.order_by_spec:
                order_by_instance = instance
            elif instance.spec in node.partition_by_specs:
                partition_by_instances += (instance,)
        expected_specs: Set[InstanceSpec] = {node.metric_spec, node.order_by_spec}.union(node.partition_by_specs)
        assert metric_instance and order_by_instance and partition_by_instances, (
            "Did not receive appropriate instances to render SQL for WindowReaggregationNode. Expected instances matching "
            f"specs: {expected_specs}. Got: {parent_instance_set.as_tuple}."
        )

        cumulative_type_params = self._metric_lookup.get_metric(
            metric_instance.spec.reference
        ).type_params.cumulative_type_params
        sql_window_function = SqlWindowFunction.get_window_function_for_period_agg(
            cumulative_type_params.period_agg
            if cumulative_type_params and cumulative_type_params.period_agg
            else PeriodAggregation.FIRST
        )
        order_by_args = []
        if sql_window_function.requires_ordering:
            order_by_args.append(
                SqlWindowOrderByArgument(
                    expr=SqlColumnReferenceExpression.from_column_reference(
                        table_alias=parent_data_set_alias,
                        column_name=order_by_instance.associated_column.column_name,
                    ),
                )
            )
        metric_select_column = SqlSelectColumn(
            expr=SqlWindowFunctionExpression.create(
                sql_function=sql_window_function,
                sql_function_args=[
                    SqlColumnReferenceExpression.from_column_reference(
                        table_alias=parent_data_set_alias, column_name=metric_instance.associated_column.column_name
                    )
                ],
                partition_by_args=[
                    SqlColumnReferenceExpression.from_column_reference(
                        table_alias=parent_data_set_alias,
                        column_name=partition_by_instance.associated_column.column_name,
                    )
                    for partition_by_instance in partition_by_instances
                ],
                order_by_args=order_by_args,
            ),
            column_alias=metric_instance.associated_column.column_name,
        )

        # Order by instance should not be included in the output dataset because it was not requested in the query.
        output_instance_set = parent_instance_set.transform(
            FilterElements(exclude_specs=InstanceSpecSet(time_dimension_specs=(order_by_instance.spec,)))
        )

        # Can't include window function in a group by, so we use a subquery and apply group by in the outer query.
        subquery_select_columns = output_instance_set.transform(
            FilterElements(exclude_specs=InstanceSpecSet(metric_specs=(metric_instance.spec,)))
        ).transform(
            CreateSelectColumnsForInstances(parent_data_set_alias, self._column_association_resolver)
        ).get_columns() + (
            metric_select_column,
        )
        subquery = SqlSelectStatementNode.create(
            description="Window Function for Metric Re-aggregation",
            select_columns=subquery_select_columns,
            from_source=from_data_set.checked_sql_select_node,
            from_source_alias=parent_data_set_alias,
        )
        subquery_alias = self._next_unique_table_alias()

        outer_query_select_columns = output_instance_set.transform(
            CreateSelectColumnsForInstances(subquery_alias, self._column_association_resolver)
        ).get_columns()
        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode.create(
                description="Re-aggregate Metric via Group By",
                select_columns=outer_query_select_columns,
                from_source=subquery,
                from_source_alias=subquery_alias,
                group_bys=outer_query_select_columns,
            ),
        )

    @staticmethod
    def _make_time_range_comparison_expr(
        table_alias: str, column_alias: str, time_range_constraint: TimeRangeConstraint
    ) -> SqlExpressionNode:
        """Build an expression like "ds BETWEEN CAST('2020-01-01' AS TIMESTAMP) AND CAST('2020-01-02' AS TIMESTAMP).

        If the constraint uses day or larger grain, only render to the date level. Otherwise, render to the timestamp level.
        """

        def strip_time_from_dt(ts: dt.datetime) -> dt.datetime:
            date_obj = ts.date()
            return dt.datetime(date_obj.year, date_obj.month, date_obj.day)

        constraint_uses_day_or_larger_grain = True
        for constraint_input in (time_range_constraint.start_time, time_range_constraint.end_time):
            if strip_time_from_dt(constraint_input) != constraint_input:
                constraint_uses_day_or_larger_grain = False
                break

        time_format_to_render = (
            ISO8601_PYTHON_FORMAT if constraint_uses_day_or_larger_grain else ISO8601_PYTHON_TS_FORMAT
        )

        return SqlBetweenExpression.create(
            column_arg=SqlColumnReferenceExpression.create(
                SqlColumnReference(table_alias=table_alias, column_name=column_alias)
            ),
            start_expr=SqlStringLiteralExpression.create(
                literal_value=time_range_constraint.start_time.strftime(time_format_to_render),
            ),
            end_expr=SqlStringLiteralExpression.create(
                literal_value=time_range_constraint.end_time.strftime(time_format_to_render),
            ),
        )

    def visit_offset_base_grain_by_custom_grain_node(self, node: OffsetBaseGrainByCustomGrainNode) -> SqlDataSet:
        """Builds a query to implement an offset window that uses a custom grain.

        The query implements the offset window by offsetting the custom grain's base grain by the requested number of custom
        grain periods. This offset column will later be joined to the non-offset base grain column in order to offset the
        metric values. The query built by this node will look something like this:

        WITH cte AS (
          SELECT
            ds AS ds__day
            , alien_day AS ds__alien_day
            , FIRST_VALUE(ds) OVER (PARTITION BY alien_day ORDER BY ds) AS ds__alien_day__first_value
            , LAST_VALUE(ds) OVER (PARTITION BY alien_day ORDER BY ds) AS ds__alien_day__last_value
            , ROW_NUMBER() OVER (PARTITION BY alien_day ORDER BY ds) AS ds__day__row_number
          FROM mf_time_spine
        )

        SELECT
          ds__day
          , DATE_TRUNC('day', ds__day__lead) AS booking__ds__day  -- if standard grain was requested
          , EXTRACT(year FROM ds__day__lead) AS metric_time__extract_year  -- if date part was requested
        FROM (
          SELECT
            cte.ds__day AS ds__day
            , CASE
              WHEN ds__alien_day__first_value__offset + INTERVAL (ds__day__row_number - 1) day <= ds__alien_day__last_value__offset
              THEN ds__alien_day__first_value__offset + INTERVAL (ds__day__row_number - 1) day
              ELSE NULL
            END AS ds__day__lead
          FROM cte
          INNER JOIN (
            SELECT
              ds__alien_day
              , LEAD(ds__alien_day__first_value, 1) OVER (ORDER BY ds__alien_day) AS ds__alien_day__first_value__offset
              , LEAD(ds__alien_day__last_value, 1) OVER (ORDER BY ds__alien_day) AS ds__alien_day__last_value__offset
            FROM (
              SELECT
                ds__alien_day
                , ds__alien_day__first_value
                , ds__alien_day__last_value
              FROM cte
              GROUP BY
                ds__alien_day
                , ds__alien_day__first_value
                , ds__alien_day__last_value
            ) a
          ) b
          ON cte.ds__alien_day = b.ds__alien_day
        )
        """
        time_spine_data_set = self.get_output_data_set(node.time_spine_node)
        time_spine_alias = self._next_unique_table_alias()
        offset_window = node.offset_window
        custom_grain_name = offset_window.granularity
        base_grain = ExpandedTimeGranularity.from_time_granularity(
            self._get_time_spine_for_custom_granularity(custom_grain_name).base_granularity
        )
        time_spine = self._get_time_spine_for_custom_granularity(custom_grain_name)
        custom_grain_instance = time_spine_data_set.instance_from_time_dimension_grain_and_date_part(
            time_granularity_name=custom_grain_name, date_part=None
        )
        custom_grain_column_name = custom_grain_instance.associated_column.column_name
        base_grain_instance = time_spine_data_set.instance_from_time_dimension_grain_and_date_part(
            time_granularity_name=time_spine.base_granularity.value, date_part=None
        )
        base_grain_column_name = base_grain_instance.associated_column.column_name

        # Build columns that get start and end of the custom grain period.
        # Ex: FIRST_VALUE(ds) OVER (PARTITION BY fiscal_quarter ORDER BY ds) AS ds__fiscal_quarter__first_value
        new_select_columns: Tuple[SqlSelectColumn, ...] = ()
        bounds_columns: Tuple[SqlSelectColumn, ...] = ()
        bounds_instances: Tuple[TimeDimensionInstance, ...] = ()
        custom_column_expr = SqlColumnReferenceExpression.from_column_reference(
            table_alias=time_spine_alias, column_name=custom_grain_column_name
        )
        base_column_expr = SqlColumnReferenceExpression.from_column_reference(
            table_alias=time_spine_alias, column_name=base_grain_column_name
        )
        for window_func in (SqlWindowFunction.FIRST_VALUE, SqlWindowFunction.LAST_VALUE):
            bounds_instance = custom_grain_instance.with_new_spec(
                new_spec=custom_grain_instance.spec.with_window_functions((window_func,)),
                column_association_resolver=self._column_association_resolver,
            )
            select_column = SqlSelectColumn(
                expr=SqlWindowFunctionExpression.create(
                    sql_function=window_func,
                    sql_function_args=(base_column_expr,),
                    partition_by_args=(custom_column_expr,),
                    order_by_args=(SqlWindowOrderByArgument(base_column_expr),),
                ),
                column_alias=bounds_instance.associated_column.column_name,
            )
            bounds_instances += (bounds_instance,)
            bounds_columns += (select_column,)
            new_select_columns += (select_column,)

        # Build a column that tracks the row number for the base grain column within the custom grain period.
        # This will be offset by 1 to represent the number of base grain periods since the start of the custom grain period.
        # Ex: ROW_NUMBER() OVER (PARTITION BY fiscal_quarter ORDER BY ds) AS ds__day__row_number
        row_number_column = SqlSelectColumn(
            expr=SqlWindowFunctionExpression.create(
                sql_function=SqlWindowFunction.ROW_NUMBER,
                partition_by_args=(custom_column_expr,),
                order_by_args=(SqlWindowOrderByArgument(base_column_expr),),
            ),
            column_alias=self._column_association_resolver.resolve_spec(
                base_grain_instance.spec.with_window_functions((SqlWindowFunction.ROW_NUMBER,))
            ).column_name,
        )
        new_select_columns += (row_number_column,)

        # Built a CTE for the new select statement.
        cte_alias = self._next_unique_cte_alias()
        cte = SqlCteNode.create(
            SqlSelectStatementNode.create(
                description="Get Custom Granularity Bounds",
                select_columns=time_spine_data_set.checked_sql_select_node.select_columns + new_select_columns,
                from_source=time_spine_data_set.checked_sql_select_node,
                from_source_alias=time_spine_alias,
            ),
            cte_alias=cte_alias,
        )

        # Create a subquery that gets unique rows for the custom grain and bounds columns.
        # Ex: SELECT first_value, last_value, custom_grain FROM cte GROUP BY first_value, last_value, custom_grain
        unique_rows_columns = tuple(
            SqlSelectColumn.from_column_reference(column_name=column_name, table_alias=cte_alias)
            for column_name in ([custom_grain_column_name] + [col.column_alias for col in bounds_columns])
        )
        unique_rows_alias = self._next_unique_table_alias()
        unique_rows_subquery = SqlSelectStatementNode.create(
            description="Get Unique Rows for Custom Granularity Bounds",
            select_columns=unique_rows_columns,
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name=None, table_name=cte_alias)),
            from_source_alias=cte_alias,
            group_bys=unique_rows_columns,
        )

        # Build a subquery that offsets the first and last value columns.
        # Ex: LEAD(ds__fiscal_quarter__first_value, 1) OVER (ORDER BY ds__fiscal_quarter) AS ds__fiscal_quarter__first_value__offset
        custom_grain_column = SqlSelectColumn.from_column_reference(
            column_name=custom_grain_column_name, table_alias=unique_rows_alias
        )
        offset_bounds_columns: Tuple[SqlSelectColumn, ...] = ()
        for i in range(len(bounds_columns)):
            bounds_instance = bounds_instances[i]
            bounds_column = bounds_columns[i]
            offset_bounds_instance = bounds_instance.with_new_spec(
                bounds_instance.spec.with_window_functions(
                    (bounds_instance.spec.window_functions + (SqlWindowFunction.LEAD,))
                ),
                column_association_resolver=self._column_association_resolver,
            )
            offset_bounds_columns += (
                SqlSelectColumn(
                    expr=SqlWindowFunctionExpression.create(
                        sql_function=SqlWindowFunction.LEAD,
                        sql_function_args=(
                            bounds_column.reference_from(unique_rows_alias),
                            SqlIntegerExpression.create(node.offset_window.count),
                        ),
                        order_by_args=(SqlWindowOrderByArgument(custom_grain_column.expr),),
                    ),
                    column_alias=offset_bounds_instance.associated_column.column_name,
                ),
            )
        first_value_offset_column, last_value_offset_column = offset_bounds_columns
        offset_bounds_subquery_alias = self._next_unique_table_alias()
        offset_bounds_subquery = SqlSelectStatementNode.create(
            description="Offset Custom Granularity Bounds",
            select_columns=(custom_grain_column, first_value_offset_column, last_value_offset_column),
            from_source=unique_rows_subquery,
            from_source_alias=unique_rows_alias,
        )

        # Offset the base column by the requested window. If offset date is not within the offset grain period, use NULL. Ex:
        # CASE
        #   WHEN DATEADD(day, (ds__day__row_number - 1), ds__fiscal_quarter__first_value__offset)
        #     <= ds__fiscal_quarter__last_value__offset
        #   THEN DATEADD(day, (ds__day__row_number - 1), ds__fiscal_quarter__first_value__offset)
        # ELSE NULL
        offset_base_grain_expr = SqlAddTimeExpression.create(
            arg=first_value_offset_column.reference_from(offset_bounds_subquery_alias),
            count_expr=SqlArithmeticExpression.create(
                left_expr=row_number_column.reference_from(cte_alias),
                operator=SqlArithmeticOperator.SUBTRACT,
                right_expr=SqlIntegerExpression.create(1),
            ),
            granularity=base_grain.base_granularity,
        )
        is_below_last_value_expr = SqlComparisonExpression.create(
            left_expr=offset_base_grain_expr,
            comparison=SqlComparison.LESS_THAN_OR_EQUALS,
            right_expr=last_value_offset_column.reference_from(offset_bounds_subquery_alias),
        )
        # LEAD isn't quite accurate here, but this will differentiate the offset instance (and column) from the original one.
        offset_base_column_name = self._column_association_resolver.resolve_spec(
            base_grain_instance.spec.with_window_functions((SqlWindowFunction.LEAD,))
        ).column_name
        offset_base_column = SqlSelectColumn(
            expr=SqlCaseExpression.create(
                when_to_then_exprs={is_below_last_value_expr: offset_base_grain_expr},
                else_expr=SqlNullExpression.create(),
            ),
            column_alias=offset_base_column_name,
        )
        original_base_grain_column = SqlSelectColumn.from_column_reference(
            column_name=base_grain_column_name, table_alias=cte_alias
        )
        join_desc = SqlJoinDescription(
            right_source=offset_bounds_subquery,
            right_source_alias=offset_bounds_subquery_alias,
            join_type=SqlJoinType.INNER,
            on_condition=SqlComparisonExpression.create(
                left_expr=custom_grain_column.reference_from(cte_alias),
                comparison=SqlComparison.EQUALS,
                right_expr=custom_grain_column.reference_from(offset_bounds_subquery_alias),
            ),
        )
        offset_base_grain_subquery = SqlSelectStatementNode.create(
            description=node.description,
            select_columns=(original_base_grain_column, offset_base_column),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name=None, table_name=cte_alias)),
            cte_sources=(cte,),
            from_source_alias=cte_alias,
            join_descs=(join_desc,),
        )
        offset_base_grain_subquery_alias = self._next_unique_table_alias()

        # Apply standard grains & date parts requested in the query. Use base grain for any custom grains.
        requested_instances: Tuple[TimeDimensionInstance, ...] = ()
        requested_columns: Tuple[SqlSelectColumn, ...] = ()
        offset_base_column_ref = offset_base_column.reference_from(offset_base_grain_subquery_alias)
        for spec in node.required_time_spine_specs:
            new_instance = base_grain_instance.with_new_spec(
                new_spec=spec, column_association_resolver=self._column_association_resolver
            )
            if spec.date_part:
                expr: SqlExpressionNode = SqlExtractExpression.create(
                    date_part=spec.date_part, arg=offset_base_column_ref
                )
            else:
                assert (
                    spec.time_granularity is not None
                ), "Got no time granularity or date part for required time spine spec."
                if spec.time_granularity.base_granularity == base_grain.base_granularity:
                    expr = offset_base_column_ref
                else:
                    expr = SqlDateTruncExpression.create(
                        time_granularity=spec.time_granularity.base_granularity, arg=offset_base_column_ref
                    )
            requested_columns += (SqlSelectColumn(expr=expr, column_alias=new_instance.associated_column.column_name),)
            requested_instances += (new_instance,)

        # Need to keep the non-offset base grain column in the output. This will be used to join to the metric source data set.
        non_offset_base_grain_column = SqlSelectColumn.from_column_reference(
            column_name=base_grain_column_name, table_alias=offset_base_grain_subquery_alias
        )

        return SqlDataSet(
            instance_set=InstanceSet(time_dimension_instances=(base_grain_instance,) + requested_instances),
            sql_select_node=SqlSelectStatementNode.create(
                description="Apply Requested Granularities",
                select_columns=(non_offset_base_grain_column,) + requested_columns,
                from_source=offset_base_grain_subquery,
                from_source_alias=offset_base_grain_subquery_alias,
            ),
        )

    def visit_offset_custom_granularity_node(self, node: OffsetCustomGranularityNode) -> SqlDataSet:  # noqa: D102
        """Build a SQL query that offsets the given custom grain by the requested number of periods.

        The base grain is also needed in the output query in order to join to the metric source.
        To offset 1 fiscal_quarter, the SQL would look like this:

        SELECT
            date_day,
            fiscal_quarter__lead AS metric_time__fiscal_quarter
        FROM ANALYTICS_DEV.DBT_JSTEIN.ALL_DAYS a
        INNER JOIN (
            SELECT
                fiscal_quarter,
                LEAD(fiscal_quarter, 1) OVER (ORDER BY fiscal_quarter) AS fiscal_quarter__lead
            FROM ANALYTICS_DEV.DBT_JSTEIN.ALL_DAYS
            GROUP BY fiscal_quarter
        ) b ON b.fiscal_quarter = a.fiscal_quarter
        """
        cte_data_set = self.get_output_data_set(node.time_spine_node)
        cte_alias = self._next_unique_cte_alias()
        cte_node = SqlCteNode.create(select_statement=cte_data_set.checked_sql_select_node, cte_alias=cte_alias)
        custom_grain_name = node.offset_window.granularity
        custom_grain_instance = cte_data_set.instance_from_time_dimension_grain_and_date_part(
            time_granularity_name=custom_grain_name, date_part=None
        )

        # Build subquery with LEAD column.
        custom_grain_column = SqlSelectColumn.from_column_reference(
            table_alias=cte_alias, column_name=custom_grain_instance.associated_column.column_name
        )
        offset_instance = custom_grain_instance.with_new_spec(
            new_spec=custom_grain_instance.spec.with_window_functions((SqlWindowFunction.LEAD,)),
            column_association_resolver=self._column_association_resolver,
        )
        offset_column = SqlSelectColumn(
            expr=SqlWindowFunctionExpression.create(
                sql_function=SqlWindowFunction.LEAD,
                sql_function_args=(custom_grain_column.expr, SqlIntegerExpression.create(node.offset_window.count)),
                order_by_args=(SqlWindowOrderByArgument(custom_grain_column.expr),),
            ),
            column_alias=offset_instance.associated_column.column_name,
        )
        subquery = SqlSelectStatementNode.create(
            description="Offset Custom Granularity",
            select_columns=(custom_grain_column, offset_column),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name=None, table_name=cte_alias)),
            from_source_alias=cte_alias,
            group_bys=(custom_grain_column,),
        )
        subquery_alias = self._next_unique_table_alias()

        # Build join condition.
        join_desc = SqlJoinDescription(
            right_source=subquery,
            right_source_alias=subquery_alias,
            join_type=SqlJoinType.INNER,
            on_condition=SqlComparisonExpression.create(
                left_expr=custom_grain_column.expr,
                comparison=SqlComparison.EQUALS,
                right_expr=custom_grain_column.reference_from(subquery_alias),
            ),
        )

        # Build outer query select columns.
        base_grain = ExpandedTimeGranularity.from_time_granularity(
            self._get_time_spine_for_custom_granularity(custom_grain_name).base_granularity
        )
        base_grain_instance = cte_data_set.instance_from_time_dimension_grain_and_date_part(
            time_granularity_name=base_grain.name
        )
        base_grain_column = SqlSelectColumn.from_column_reference(
            table_alias=cte_alias, column_name=base_grain_instance.associated_column.column_name
        )
        offset_column_ref_expr = offset_column.reference_from(subquery_alias)
        requested_columns: Tuple[SqlSelectColumn, ...] = ()
        requested_instances: Tuple[TimeDimensionInstance, ...] = ()
        for required_spec in node.required_time_spine_specs:
            new_instance = offset_instance.with_new_spec(
                new_spec=required_spec, column_association_resolver=self._column_association_resolver
            )
            requested_instances += (new_instance,)
            requested_columns += (
                SqlSelectColumn(expr=offset_column_ref_expr, column_alias=new_instance.associated_column.column_name),
            )
        return SqlDataSet(
            instance_set=InstanceSet(time_dimension_instances=(base_grain_instance,) + requested_instances),
            sql_select_node=SqlSelectStatementNode.create(
                description="Join Offset Custom Granularity to Base Granularity",
                select_columns=(base_grain_column,) + requested_columns,
                from_source=SqlTableNode.create(sql_table=SqlTable(schema_name=None, table_name=cte_alias)),
                from_source_alias=cte_alias,
                join_descs=(join_desc,),
                cte_sources=(cte_node,),
            ),
        )
