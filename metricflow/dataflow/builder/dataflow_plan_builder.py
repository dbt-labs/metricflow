from __future__ import annotations

import collections
import logging
import time
from dataclasses import dataclass
from typing import DefaultDict, Dict, List, Optional, Sequence, Set, Tuple, Union

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from dbt_semantic_interfaces.protocols.metric import MetricTimeWindow, MetricType
from dbt_semantic_interfaces.references import TimeDimensionReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.assert_one_arg import assert_exactly_one_arg_set
from metricflow.dag.id_generation import DATAFLOW_PLAN_PREFIX, IdGeneratorRegistry
from metricflow.dataflow.builder.costing import DataflowPlanNodeCostFunction, DefaultCostFunction
from metricflow.dataflow.builder.measure_additiveness import group_measure_specs_by_additiveness
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.node_evaluator import (
    JoinLinkableInstancesRecipe,
    LinkableInstanceSatisfiabilityEvaluation,
    NodeEvaluatorForLinkableInstances,
)
from metricflow.dataflow.dataflow_plan import (
    AggregateMeasuresNode,
    BaseOutput,
    CombineMetricsNode,
    ComputeMetricsNode,
    ConstrainTimeRangeNode,
    DataflowPlan,
    FilterElementsNode,
    JoinAggregatedMeasuresByGroupByColumnsNode,
    JoinDescription,
    JoinOverTimeRangeNode,
    JoinToBaseOutputNode,
    JoinToTimeSpineNode,
    OrderByLimitNode,
    SemiAdditiveJoinNode,
    SinkOutput,
    WhereConstraintNode,
    WriteToResultDataframeNode,
    WriteToResultTableNode,
)
from metricflow.dataflow.dataflow_plan_to_text import dataflow_dag_as_text
from metricflow.dataflow.optimizer.dataflow_plan_optimizer import DataflowPlanOptimizer
from metricflow.dataflow.sql_table import SqlTable
from metricflow.dataset.dataset import DataSet
from metricflow.errors.errors import UnableToSatisfyQueryError
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.node_processor import PreDimensionJoinNodeProcessor
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import (
    DimensionSpec,
    EntitySpec,
    InstanceSpecSet,
    LinkableInstanceSpec,
    LinkableSpecSet,
    LinklessEntitySpec,
    MeasureSpec,
    MetricFlowQuerySpec,
    MetricInputMeasureSpec,
    MetricSpec,
    NonAdditiveDimensionSpec,
    OrderBySpec,
    TimeDimensionSpec,
    WhereFilterSpec,
)
from metricflow.sql.sql_plan import SqlJoinType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MeasureRecipe:
    """Get a recipe for how to build a dataflow plan node that outputs measures and the needed linkable instances.

    The recipe involves filtering the measure node so that it only outputs the measures and the instances associated with
    required_local_linkable_specs, then joining the nodes containing the linkable instances according to the recipes
    in join_linkable_instances_recipes.
    """

    measure_node: BaseOutput
    required_local_linkable_specs: Tuple[LinkableInstanceSpec, ...]
    join_linkable_instances_recipes: Tuple[JoinLinkableInstancesRecipe, ...]


@dataclass(frozen=True)
class MeasureSpecProperties:
    """Input dataclass for grouping properties of a sequence of MeasureSpecs."""

    measure_specs: Sequence[MeasureSpec]
    semantic_model_name: str
    agg_time_dimension: TimeDimensionReference
    non_additive_dimension_spec: Optional[NonAdditiveDimensionSpec] = None


class DataflowPlanBuilder:
    """Builds a dataflow plan to satisfy a given query."""

    def __init__(  # noqa: D
        self,
        source_nodes: Sequence[BaseOutput],
        semantic_manifest_lookup: SemanticManifestLookup,
        cost_function: DataflowPlanNodeCostFunction = DefaultCostFunction(),
        node_output_resolver: Optional[DataflowPlanNodeOutputDataSetResolver] = None,
        column_association_resolver: Optional[ColumnAssociationResolver] = None,
    ) -> None:
        self._semantic_model_lookup = semantic_manifest_lookup.semantic_model_lookup
        self._metric_lookup = semantic_manifest_lookup.metric_lookup
        self._metric_time_dimension_reference = DataSet.metric_time_dimension_reference()
        self._cost_function = cost_function
        self._source_nodes = source_nodes
        self._column_association_resolver = (
            DunderColumnAssociationResolver(semantic_manifest_lookup)
            if not column_association_resolver
            else column_association_resolver
        )
        self._node_data_set_resolver = (
            DataflowPlanNodeOutputDataSetResolver(
                column_association_resolver=(
                    DunderColumnAssociationResolver(semantic_manifest_lookup)
                    if not column_association_resolver
                    else column_association_resolver
                ),
                semantic_manifest_lookup=semantic_manifest_lookup,
            )
            if not node_output_resolver
            else node_output_resolver
        )

    def build_plan(
        self,
        query_spec: MetricFlowQuerySpec,
        output_sql_table: Optional[SqlTable] = None,
        output_selection_specs: Optional[InstanceSpecSet] = None,
        optimizers: Sequence[DataflowPlanOptimizer] = (),
    ) -> DataflowPlan:
        """Generate a plan for reading the results of a query with the given spec into a dataframe or table."""
        metrics_output_node = self._build_metrics_output_node(
            metric_specs=query_spec.metric_specs,
            queried_linkable_specs=query_spec.linkable_specs,
            where_constraint=query_spec.where_constraint,
            time_range_constraint=query_spec.time_range_constraint,
        )

        sink_node = DataflowPlanBuilder.build_sink_node_from_metrics_output_node(
            computed_metrics_output=metrics_output_node,
            order_by_specs=query_spec.order_by_specs,
            output_sql_table=output_sql_table,
            limit=query_spec.limit,
            output_selection_specs=output_selection_specs,
        )

        plan_id = IdGeneratorRegistry.for_class(DataflowPlanBuilder).create_id(DATAFLOW_PLAN_PREFIX)

        plan = DataflowPlan(plan_id=plan_id, sink_output_nodes=[sink_node])
        for optimizer in optimizers:
            logger.info(f"Applying {optimizer.__class__.__name__}")
            try:
                plan = optimizer.optimize(plan)
            except Exception:
                logger.exception(f"Got an exception applying {optimizer.__class__.__name__}")

        return plan

    def _build_metrics_output_node(
        self,
        metric_specs: Sequence[MetricSpec],
        queried_linkable_specs: LinkableSpecSet,
        where_constraint: Optional[WhereFilterSpec] = None,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
        combine_metrics_join_type: SqlJoinType = SqlJoinType.FULL_OUTER,
    ) -> BaseOutput:
        """Builds a computed metrics output node.

        Args:
            metric_specs: Specs for metrics to compute.
            queried_linkable_specs: Dimensions/entities that were queried for.
            where_constraint: Where constraint used to compute the metric.
            time_range_constraint: Time range constraint used to compute the metric.
            combine_metrics_join_type: The join used when combining the computed metrics.
        """
        output_nodes: List[BaseOutput] = []
        compute_metrics_node: Optional[ComputeMetricsNode] = None

        for metric_spec in metric_specs:
            logger.info(f"Generating compute metrics node for {metric_spec}")
            metric_reference = metric_spec.as_reference
            metric = self._metric_lookup.get_metric(metric_reference)

            if metric.type is MetricType.DERIVED or metric.type is MetricType.RATIO:
                metric_input_specs = self._metric_lookup.metric_input_specs_for_metric(
                    metric_reference=metric_reference,
                    column_association_resolver=self._column_association_resolver,
                )
                logger.info(
                    f"For {metric.type} metric: {metric_spec}, needed metrics are:\n"
                    f"{pformat_big_objects(metric_input_specs=metric_input_specs)}"
                )

                compute_metrics_node = ComputeMetricsNode(
                    parent_node=self._build_metrics_output_node(
                        metric_specs=metric_input_specs,
                        queried_linkable_specs=queried_linkable_specs,
                        where_constraint=where_constraint,
                        time_range_constraint=time_range_constraint,
                        combine_metrics_join_type=SqlJoinType.INNER,
                    ),
                    metric_specs=[metric_spec],
                )
            elif metric.type is MetricType.SIMPLE or MetricType.CUMULATIVE:
                metric_input_measure_specs = self._metric_lookup.measures_for_metric(
                    metric_reference=metric_reference,
                    column_association_resolver=self._column_association_resolver,
                )

                logger.info(
                    f"For {metric_spec}, needed measures are:\n"
                    f"{pformat_big_objects(metric_input_measure_specs=metric_input_measure_specs)}"
                )
                combined_where = where_constraint
                if metric_spec.constraint:
                    combined_where = (
                        combined_where.combine(metric_spec.constraint) if combined_where else metric_spec.constraint
                    )
                aggregated_measures_node = self.build_aggregated_measures(
                    metric_input_measure_specs=metric_input_measure_specs,
                    metric_spec=metric_spec,
                    queried_linkable_specs=queried_linkable_specs,
                    where_constraint=combined_where,
                    time_range_constraint=time_range_constraint,
                    cumulative=metric.type == MetricType.CUMULATIVE,
                    cumulative_window=metric.type_params.window if metric.type == MetricType.CUMULATIVE else None,
                    cumulative_grain_to_date=(
                        metric.type_params.grain_to_date if metric.type == MetricType.CUMULATIVE else None
                    ),
                )
                compute_metrics_node = self.build_computed_metrics_node(
                    metric_spec=metric_spec,
                    aggregated_measures_node=aggregated_measures_node,
                )
            else:
                assert_values_exhausted(metric.type)

            assert compute_metrics_node is not None

            output_nodes.append(compute_metrics_node)

        assert len(output_nodes) > 0, "ComputeMetricsNode was not properly constructed"

        if len(output_nodes) == 1:
            return output_nodes[0]

        return CombineMetricsNode(
            parent_nodes=output_nodes,
            join_type=combine_metrics_join_type,
        )

    def build_plan_for_distinct_values(
        self,
        metric_specs: Sequence[MetricSpec],
        dimension_spec: Optional[DimensionSpec] = None,
        time_dimension_spec: Optional[TimeDimensionSpec] = None,
        entity_spec: Optional[EntitySpec] = None,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
        limit: Optional[int] = None,
    ) -> DataflowPlan:
        """Generate a plan that would get the distinct values of a linkable instance.

        e.g. distinct listing__country_latest for bookings by listing__country_latest
        """
        assert_exactly_one_arg_set(
            dimension_spec=dimension_spec, time_dimension_spec=time_dimension_spec, entity_spec=entity_spec
        )

        # Doing this to keep the type checker happy, but assert_exactly_one_arg_set should ensure this.
        linkable_spec: Optional[LinkableInstanceSpec] = dimension_spec or time_dimension_spec or entity_spec
        assert linkable_spec

        query_spec = MetricFlowQuerySpec(
            metric_specs=tuple(metric_specs),
            dimension_specs=(dimension_spec,) if dimension_spec else (),
            time_dimension_specs=(time_dimension_spec,) if time_dimension_spec else (),
            entity_specs=(entity_spec,) if entity_spec else (),
            time_range_constraint=time_range_constraint,
        )
        metrics_output_node = self._build_metrics_output_node(
            metric_specs=query_spec.metric_specs,
            queried_linkable_specs=query_spec.linkable_specs,
            where_constraint=query_spec.where_constraint,
            time_range_constraint=query_spec.time_range_constraint,
        )

        distinct_values_node = FilterElementsNode(
            parent_node=metrics_output_node,
            include_specs=InstanceSpecSet.create_from_linkable_specs((linkable_spec,)),
        )

        sink_node = self.build_sink_node_from_metrics_output_node(
            computed_metrics_output=distinct_values_node,
            order_by_specs=(
                OrderBySpec(
                    instance_spec=linkable_spec,
                    descending=False,
                ),
            ),
            limit=limit,
        )

        plan_id = IdGeneratorRegistry.for_class(DataflowPlanBuilder).create_id(DATAFLOW_PLAN_PREFIX)

        return DataflowPlan(
            plan_id=plan_id,
            sink_output_nodes=[sink_node],
        )

    @staticmethod
    def build_sink_node_from_metrics_output_node(
        computed_metrics_output: BaseOutput,
        order_by_specs: Sequence[OrderBySpec],
        output_sql_table: Optional[SqlTable] = None,
        limit: Optional[int] = None,
        output_selection_specs: Optional[InstanceSpecSet] = None,
    ) -> SinkOutput:
        """Adds order by / limit / write nodes."""
        pre_result_node: Optional[BaseOutput] = None

        if order_by_specs or limit:
            pre_result_node = OrderByLimitNode(
                order_by_specs=list(order_by_specs),
                limit=limit,
                parent_node=computed_metrics_output,
            )

        if output_selection_specs:
            pre_result_node = FilterElementsNode(
                parent_node=pre_result_node or computed_metrics_output,
                include_specs=output_selection_specs,
            )

        write_result_node: SinkOutput
        if not output_sql_table:
            write_result_node = WriteToResultDataframeNode(
                parent_node=pre_result_node or computed_metrics_output,
            )
        else:
            write_result_node = WriteToResultTableNode(
                parent_node=pre_result_node or computed_metrics_output,
                output_sql_table=output_sql_table,
            )

        return write_result_node

    @staticmethod
    def _contains_multihop_linkables(linkable_specs: Sequence[LinkableInstanceSpec]) -> bool:
        """Returns true if any of the linkable specs requires a multi-hop join to realize."""
        return any(len(x.entity_links) > 1 for x in linkable_specs)

    def _get_semantic_model_names_for_measures(self, measure_names: Sequence[MeasureSpec]) -> Set[str]:
        """Return the names of the semantic models needed to compute the input measures.

        This is a temporary method for use in assertion boundaries while we implement support for multiple semantic models
        """
        semantic_model_names: Set[str] = set()
        for measure_name in measure_names:
            semantic_model_names = semantic_model_names.union(
                {d.name for d in self._semantic_model_lookup.get_semantic_models_for_measure(measure_name.as_reference)}
            )
        return semantic_model_names

    def _sort_by_suitability(self, nodes: Sequence[BaseOutput]) -> Sequence[BaseOutput]:
        """Sort nodes by the cost, then by the number of linkable specs.

        Lower cost nodes will result in faster queries, and the lower the number of linkable specs means less
        aggregation required.
        """

        def sort_function(node: BaseOutput) -> Tuple[int, int]:
            data_set = self._node_data_set_resolver.get_output_data_set(node)
            return self._cost_function.calculate_cost(node).as_int, len(data_set.instance_set.spec_set.linkable_specs)

        return sorted(nodes, key=sort_function)

    def _select_source_nodes_with_measures(
        self, measure_specs: Set[MeasureSpec], source_nodes: Sequence[BaseOutput]
    ) -> Sequence[BaseOutput]:
        nodes = []
        measure_specs_set = set(measure_specs)
        for source_node in source_nodes:
            measure_specs_in_node = self._node_data_set_resolver.get_output_data_set(
                source_node
            ).instance_set.spec_set.measure_specs
            if measure_specs_set.intersection(set(measure_specs_in_node)) == measure_specs_set:
                nodes.append(source_node)
        return nodes

    def _find_non_additive_dimension_in_linkable_specs(
        self,
        agg_time_dimension: TimeDimensionReference,
        linkable_specs: Sequence[LinkableInstanceSpec],
        non_additive_dimension_spec: NonAdditiveDimensionSpec,
    ) -> Optional[TimeDimensionSpec]:
        """Finds the TimeDimensionSpec matching the non_additive_dimension_spec, if any."""
        queried_time_dimension_spec: Optional[LinkableInstanceSpec] = None
        for linkable_spec in linkable_specs:
            dimension_name_match = linkable_spec.element_name == non_additive_dimension_spec.name
            metric_time_match = (
                non_additive_dimension_spec.name == agg_time_dimension.element_name
                and linkable_spec.element_name == self._metric_time_dimension_reference.element_name
            )
            if dimension_name_match or metric_time_match:
                queried_time_dimension_spec = linkable_spec
                break
        assert queried_time_dimension_spec is None or isinstance(
            queried_time_dimension_spec, TimeDimensionSpec
        ), "Non-additive dimension can only be a time dimension, if specified."
        return queried_time_dimension_spec

    def _build_measure_spec_properties(self, measure_specs: Sequence[MeasureSpec]) -> MeasureSpecProperties:
        """Ensures that the group of MeasureSpecs has the same non_additive_dimension_spec and agg_time_dimension."""
        if len(measure_specs) == 0:
            raise ValueError("Cannot build MeasureParametersForRecipe when given an empty sequence of measure_specs.")
        semantic_models = self._get_semantic_model_names_for_measures(measure_specs)
        if len(semantic_models) > 1:
            raise ValueError(
                f"Cannot find common properties for measures {measure_specs} coming from multiple "
                f"semantic models: {semantic_models}. This suggests the measure_specs were not correctly filtered."
            )

        agg_time_dimension = agg_time_dimension = self._semantic_model_lookup.get_agg_time_dimension_for_measure(
            measure_specs[0].as_reference
        )
        non_additive_dimension_spec = measure_specs[0].non_additive_dimension_spec
        for measure_spec in measure_specs:
            if non_additive_dimension_spec != measure_spec.non_additive_dimension_spec:
                raise ValueError(f"measure_specs {measure_specs} do not have the same non_additive_dimension_spec.")
            measure_agg_time_dimension = self._semantic_model_lookup.get_agg_time_dimension_for_measure(
                measure_spec.as_reference
            )
            if measure_agg_time_dimension != agg_time_dimension:
                raise ValueError(f"measure_specs {measure_specs} do not have the same agg_time_dimension.")
        return MeasureSpecProperties(
            measure_specs=measure_specs,
            semantic_model_name=semantic_models.pop(),
            agg_time_dimension=agg_time_dimension,
            non_additive_dimension_spec=non_additive_dimension_spec,
        )

    def _find_measure_recipe(
        self,
        measure_spec_properties: MeasureSpecProperties,
        linkable_specs: Sequence[LinkableInstanceSpec],
        time_range_constraint: Optional[TimeRangeConstraint] = None,
    ) -> Optional[MeasureRecipe]:
        """Find a recipe for getting measure_specs along with the linkable specs.

        Prior to calling this method we should always be checking that all input measure specs come from
        the same base semantic model, otherwise the internal conditions here will be impossible to satisfy
        """
        measure_specs = measure_spec_properties.measure_specs
        node_processor = PreDimensionJoinNodeProcessor(
            semantic_model_lookup=self._semantic_model_lookup,
            node_data_set_resolver=self._node_data_set_resolver,
        )

        source_nodes: Sequence[BaseOutput] = self._source_nodes

        # We only care about nodes that have all required measures
        potential_measure_nodes: Sequence[BaseOutput] = self._select_source_nodes_with_measures(
            measure_specs=set(measure_specs), source_nodes=source_nodes
        )

        logger.info(f"There are {len(potential_measure_nodes)} potential measure source nodes")

        logger.info(f"Starting search with {len(source_nodes)} source nodes")
        start_time = time.time()
        # Only apply the time constraint to nodes that will be used for measures because some dimensional sources have
        # measures in them, and time constraining those would result in incomplete joins.
        if time_range_constraint:
            potential_measure_nodes = node_processor.add_time_range_constraint(
                source_nodes=potential_measure_nodes,
                metric_time_dimension_reference=self._metric_time_dimension_reference,
                time_range_constraint=time_range_constraint,
            )

        nodes_available_for_joins = node_processor.remove_unnecessary_nodes(
            desired_linkable_specs=linkable_specs,
            nodes=source_nodes,
            metric_time_dimension_reference=self._metric_time_dimension_reference,
        )
        logger.info(
            f"After removing unnecessary nodes, there are {len(nodes_available_for_joins)} nodes available for joins"
        )
        if DataflowPlanBuilder._contains_multihop_linkables(linkable_specs):
            nodes_available_for_joins = node_processor.add_multi_hop_joins(linkable_specs, source_nodes)
            logger.info(
                f"After adding multi-hop nodes, there are {len(nodes_available_for_joins)} nodes available for joins:\n"
                f"{pformat_big_objects(nodes_available_for_joins)}"
            )

        logger.info(f"Processing nodes took: {time.time()-start_time:.2f}s")

        node_evaluator = NodeEvaluatorForLinkableInstances(
            semantic_model_lookup=self._semantic_model_lookup,
            nodes_available_for_joins=self._sort_by_suitability(nodes_available_for_joins),
            node_data_set_resolver=self._node_data_set_resolver,
        )

        # Dict from the node that contains the measure spec to the evaluation results.
        node_to_evaluation: Dict[BaseOutput, LinkableInstanceSatisfiabilityEvaluation] = {}

        for node in self._sort_by_suitability(potential_measure_nodes):
            data_set = self._node_data_set_resolver.get_output_data_set(node)

            missing_specs = [spec for spec in measure_specs if spec not in data_set.instance_set.spec_set.measure_specs]
            if missing_specs:
                logger.debug(
                    f"Skipping evaluation for node since it does not have all of the measure specs {missing_specs}:"
                    f"\n\n{dataflow_dag_as_text(node)}"
                )
                continue

            logger.debug(f"Evaluating measure node:\n{pformat_big_objects(measure_node=dataflow_dag_as_text(node))}")

            start_time = time.time()
            evaluation = node_evaluator.evaluate_node(
                start_node=node,
                required_linkable_specs=list(linkable_specs),
            )
            logger.info(f"Evaluation of {node} took {time.time() - start_time:.2f}s")

            logger.debug(
                f"Evaluation for measure node is:\n"
                f"{pformat_big_objects(node=dataflow_dag_as_text(node), evaluation=evaluation)}"
            )

            if len(evaluation.unjoinable_linkable_specs) > 0:
                logger.debug(
                    f"Skipping {node.node_id} since it contains un-joinable specs: "
                    f"{evaluation.unjoinable_linkable_specs}"
                )
                continue

            num_joins_required = len(evaluation.join_recipes)
            logger.info(f"Found candidate with node ID '{node.node_id}' with {num_joins_required} joins required.")

            node_to_evaluation[node] = evaluation

            # Since are evaluating nodes with the lowest cost first, if we find one without requiring any joins, then
            # this is going to be the lowest cost solution.
            if len(evaluation.join_recipes) == 0:
                logger.info("Not evaluating other nodes since we found one that doesn't require joins")

        logger.info(f"Found {len(node_to_evaluation)} candidate measure nodes.")

        if len(node_to_evaluation) > 0:
            cost_function = DefaultCostFunction()

            node_with_lowest_cost = min(node_to_evaluation, key=cost_function.calculate_cost)
            evaluation = node_to_evaluation[node_with_lowest_cost]
            logger.info(
                "Lowest cost node is:\n"
                + pformat_big_objects(
                    lowest_cost_node=dataflow_dag_as_text(node_with_lowest_cost),
                    evaluation=evaluation,
                    cost=cost_function.calculate_cost(node_with_lowest_cost),
                )
            )

            # Nodes containing the linkable instances will be joined to the node containing the measure, so these
            # entities will need to be present in the measure node.
            required_local_entity_specs = tuple(x.join_on_entity for x in evaluation.join_recipes)
            # Same thing with partitions.
            required_local_dimension_specs = tuple(
                y.start_node_dimension_spec for x in evaluation.join_recipes for y in x.join_on_partition_dimensions
            )
            required_local_time_dimension_specs = tuple(
                y.start_node_time_dimension_spec
                for x in evaluation.join_recipes
                for y in x.join_on_partition_time_dimensions
            )

            return MeasureRecipe(
                measure_node=node_with_lowest_cost,
                required_local_linkable_specs=(
                    evaluation.local_linkable_specs
                    + required_local_entity_specs
                    + required_local_dimension_specs
                    + required_local_time_dimension_specs
                ),
                join_linkable_instances_recipes=node_to_evaluation[node_with_lowest_cost].join_recipes,
            )

        logger.error("No recipe could be constructed.")
        return None

    def build_computed_metrics_node(
        self,
        metric_spec: MetricSpec,
        aggregated_measures_node: Union[AggregateMeasuresNode, BaseOutput],
    ) -> ComputeMetricsNode:
        """Builds a ComputeMetricsNode from aggregated measures."""
        return ComputeMetricsNode(
            parent_node=aggregated_measures_node,
            metric_specs=[metric_spec],
        )

    def build_aggregated_measures(
        self,
        metric_input_measure_specs: Sequence[MetricInputMeasureSpec],
        metric_spec: MetricSpec,
        queried_linkable_specs: LinkableSpecSet,
        where_constraint: Optional[WhereFilterSpec] = None,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
        cumulative: Optional[bool] = False,
        cumulative_window: Optional[MetricTimeWindow] = None,
        cumulative_grain_to_date: Optional[TimeGranularity] = None,
    ) -> BaseOutput:
        """Returns a node where the measures are aggregated by the linkable specs and constrained appropriately.

        This might be a node representing a single aggregation over one semantic model, or a node representing
        a composite set of aggregations originating from multiple semantic models, and joined into a single
        aggregated set of measures.
        """
        output_nodes: List[BaseOutput] = []
        semantic_models_and_constraints_to_measures: DefaultDict[
            tuple[str, Optional[WhereFilterSpec]], List[MetricInputMeasureSpec]
        ] = collections.defaultdict(list)
        for input_spec in metric_input_measure_specs:
            semantic_model_names = [
                dsource.name
                for dsource in self._semantic_model_lookup.get_semantic_models_for_measure(
                    measure_reference=input_spec.measure_spec.as_reference
                )
            ]
            assert (
                len(semantic_model_names) == 1
            ), f"Validation should enforce one semantic model per measure, but found {semantic_model_names} for {input_spec}!"
            semantic_models_and_constraints_to_measures[(semantic_model_names[0], input_spec.constraint)].append(
                input_spec
            )

        for (semantic_model, measure_constraint), measures in semantic_models_and_constraints_to_measures.items():
            logger.info(
                f"Building aggregated measures for {semantic_model}. "
                f" Input measures: {measures} with constraints: {measure_constraint}"
            )
            if measure_constraint is None:
                node_where_constraint = where_constraint
            elif where_constraint is None:
                node_where_constraint = measure_constraint
            else:
                node_where_constraint = where_constraint.combine(measure_constraint)

            input_specs_by_measure_spec = {spec.measure_spec: spec for spec in measures}
            grouped_measures_by_additiveness = group_measure_specs_by_additiveness(
                tuple(input_specs_by_measure_spec.keys())
            )
            measures_by_additiveness = grouped_measures_by_additiveness.measures_by_additiveness

            # Build output nodes for each distinct non-additive dimension spec, including the None case
            for non_additive_spec, measure_specs in measures_by_additiveness.items():
                non_additive_message = ""
                if non_additive_spec is not None:
                    non_additive_message = f" with non-additive dimension spec: {non_additive_spec}"

                logger.info(f"Building aggregated measures for {semantic_model}{non_additive_message}")
                input_specs = tuple(input_specs_by_measure_spec[measure_spec] for measure_spec in measure_specs)
                output_nodes.append(
                    self._build_aggregated_measures_from_measure_source_node(
                        metric_input_measure_specs=input_specs,
                        metric_spec=metric_spec,
                        queried_linkable_specs=queried_linkable_specs,
                        where_constraint=node_where_constraint,
                        time_range_constraint=time_range_constraint,
                        cumulative=cumulative,
                        cumulative_window=cumulative_window,
                        cumulative_grain_to_date=cumulative_grain_to_date,
                    )
                )

        if len(output_nodes) == 1:
            return output_nodes[0]
        else:
            return FilterElementsNode(
                parent_node=JoinAggregatedMeasuresByGroupByColumnsNode(parent_nodes=output_nodes),
                include_specs=InstanceSpecSet.merge(
                    (
                        queried_linkable_specs.as_spec_set,
                        InstanceSpecSet(
                            measure_specs=tuple(x.post_aggregation_spec for x in metric_input_measure_specs)
                        ),
                    )
                ),
            )

    def _build_aggregated_measures_from_measure_source_node(
        self,
        metric_input_measure_specs: Sequence[MetricInputMeasureSpec],
        metric_spec: MetricSpec,
        queried_linkable_specs: LinkableSpecSet,
        where_constraint: Optional[WhereFilterSpec] = None,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
        cumulative: Optional[bool] = False,
        cumulative_window: Optional[MetricTimeWindow] = None,
        cumulative_grain_to_date: Optional[TimeGranularity] = None,
    ) -> BaseOutput:
        metric_time_dimension_specs = [
            time_dimension_spec
            for time_dimension_spec in queried_linkable_specs.time_dimension_specs
            if time_dimension_spec.element_name == self._metric_time_dimension_reference.element_name
        ]
        metric_time_dimension_requested = len(metric_time_dimension_specs) > 0
        measure_specs = tuple(x.measure_spec for x in metric_input_measure_specs)
        measure_properties = self._build_measure_spec_properties(measure_specs)
        non_additive_dimension_spec = measure_properties.non_additive_dimension_spec

        cumulative_metric_adjusted_time_constraint: Optional[TimeRangeConstraint] = None
        if cumulative and time_range_constraint is not None:
            logger.info(f"Time range constraint before adjustment is {time_range_constraint}")
            granularity: Optional[TimeGranularity] = None
            count = 0
            if cumulative_window is not None:
                granularity = cumulative_window.granularity
                count = cumulative_window.count
            elif cumulative_grain_to_date is not None:
                count = 1
                granularity = cumulative_grain_to_date

            cumulative_metric_adjusted_time_constraint = (
                time_range_constraint.adjust_time_constraint_for_cumulative_metric(granularity, count)
            )
            logger.info(f"Adjusted time range constraint {cumulative_metric_adjusted_time_constraint}")

        # Extraneous linkable specs are specs that are used in this phase that should not show up in the final result
        # unless it was already a requested spec in the query
        extraneous_linkable_specs = LinkableSpecSet()
        if where_constraint:
            extraneous_linkable_specs = LinkableSpecSet.merge(
                (extraneous_linkable_specs, where_constraint.linkable_spec_set)
            )
        if non_additive_dimension_spec:
            extraneous_linkable_specs = LinkableSpecSet.merge(
                (extraneous_linkable_specs, non_additive_dimension_spec.linkable_specs)
            )

        required_linkable_specs = LinkableSpecSet.merge((queried_linkable_specs, extraneous_linkable_specs))
        logger.info(
            f"Looking for a recipe to get:\n"
            f"{pformat_big_objects(measure_specs=measure_specs, required_linkable_set=required_linkable_specs)}"
        )

        find_recipe_start_time = time.time()
        measure_recipe = self._find_measure_recipe(
            measure_spec_properties=measure_properties,
            time_range_constraint=cumulative_metric_adjusted_time_constraint or time_range_constraint,
            linkable_specs=required_linkable_specs.as_tuple,
        )
        logger.info(
            f"With {len(self._source_nodes)} source nodes, finding a recipe took "
            f"{time.time() - find_recipe_start_time:.2f}s"
        )

        logger.info(f"Using recipe:\n{pformat_big_objects(measure_recipe=measure_recipe)}")

        if not measure_recipe:
            # TODO: Improve for better user understandability.
            raise UnableToSatisfyQueryError(
                f"Recipe not found for measure specs: {measure_specs} and linkable specs: {required_linkable_specs}"
            )

        # If a cumulative metric is queried with metric_time, join over time range.
        # Otherwise, the measure will be aggregated over all time.
        time_range_node: Optional[JoinOverTimeRangeNode] = None
        if cumulative and metric_time_dimension_requested:
            time_range_node = JoinOverTimeRangeNode(
                parent_node=measure_recipe.measure_node,
                window=cumulative_window,
                grain_to_date=cumulative_grain_to_date,
                time_range_constraint=time_range_constraint,
            )

        # If querying an offset metric, join to time spine.
        join_to_time_spine_node: Optional[JoinToTimeSpineNode] = None
        if metric_spec.offset_window or metric_spec.offset_to_grain:
            assert metric_time_dimension_specs, "Joining to time spine requires querying with metric time."
            join_to_time_spine_node = JoinToTimeSpineNode(
                parent_node=time_range_node or measure_recipe.measure_node,
                metric_time_dimension_specs=metric_time_dimension_specs,
                time_range_constraint=time_range_constraint,
                offset_window=metric_spec.offset_window,
                offset_to_grain=metric_spec.offset_to_grain,
            )

        # Only get the required measure and the local linkable instances so that aggregations work correctly.
        filtered_measure_source_node = FilterElementsNode(
            parent_node=join_to_time_spine_node or time_range_node or measure_recipe.measure_node,
            include_specs=InstanceSpecSet.merge(
                (
                    InstanceSpecSet(measure_specs=measure_specs),
                    InstanceSpecSet.create_from_linkable_specs(measure_recipe.required_local_linkable_specs),
                )
            ),
        )

        join_targets = []
        for join_recipe in measure_recipe.join_linkable_instances_recipes:
            # Figure out what elements to filter from the joined node.

            # Sanity check - all linkable specs should have a link, or else why would we be joining them.
            assert all([len(x.entity_links) > 0 for x in join_recipe.satisfiable_linkable_specs])

            # If we're joining something in, then we need the associated entity, partitions, and time dimension
            # specs defining the validity window (if necessary)
            include_specs: List[LinkableInstanceSpec] = [
                LinklessEntitySpec.from_reference(x.entity_links[0]) for x in join_recipe.satisfiable_linkable_specs
            ]
            include_specs.extend([x.node_to_join_dimension_spec for x in join_recipe.join_on_partition_dimensions])
            include_specs.extend(
                [x.node_to_join_time_dimension_spec for x in join_recipe.join_on_partition_time_dimensions]
            )
            if join_recipe.validity_window:
                include_specs.extend(
                    [
                        join_recipe.validity_window.window_start_dimension,
                        join_recipe.validity_window.window_end_dimension,
                    ]
                )

            # satisfiable_linkable_specs describes what can be satisfied after the join, so remove the entity
            # link when filtering before the join.
            # e.g. if the node is used to satisfy "user_id__country", then the node must have the entity
            # "user_id" and the "country" dimension so that it can be joined to the measure node.
            include_specs.extend([x.without_first_entity_link for x in join_recipe.satisfiable_linkable_specs])
            filtered_node_to_join = FilterElementsNode(
                parent_node=join_recipe.node_to_join,
                include_specs=InstanceSpecSet.create_from_linkable_specs(include_specs),
            )
            join_targets.append(
                JoinDescription(
                    join_node=filtered_node_to_join,
                    join_on_entity=join_recipe.join_on_entity,
                    join_on_partition_dimensions=join_recipe.join_on_partition_dimensions,
                    join_on_partition_time_dimensions=join_recipe.join_on_partition_time_dimensions,
                    validity_window=join_recipe.validity_window,
                )
            )

        unaggregated_measure_node: BaseOutput
        if len(join_targets) > 0:
            filtered_measures_with_joined_elements = JoinToBaseOutputNode(
                left_node=filtered_measure_source_node,
                join_targets=join_targets,
            )

            specs_to_keep_after_join = InstanceSpecSet.merge(
                (
                    InstanceSpecSet(measure_specs=measure_specs),
                    required_linkable_specs.as_spec_set,
                )
            )

            after_join_filtered_node = FilterElementsNode(
                parent_node=filtered_measures_with_joined_elements, include_specs=specs_to_keep_after_join
            )
            unaggregated_measure_node = after_join_filtered_node
        else:
            unaggregated_measure_node = filtered_measure_source_node

        cumulative_metric_constrained_node: Optional[ConstrainTimeRangeNode] = None
        if (
            cumulative_metric_adjusted_time_constraint is not None
            and time_range_constraint is not None
            and metric_time_dimension_requested
        ):
            cumulative_metric_constrained_node = ConstrainTimeRangeNode(
                unaggregated_measure_node, time_range_constraint
            )

        pre_aggregate_node: BaseOutput = cumulative_metric_constrained_node or unaggregated_measure_node
        if where_constraint:
            # Apply where constraint on the node
            pre_aggregate_node = WhereConstraintNode(
                parent_node=pre_aggregate_node,
                where_constraint=where_constraint,
            )

        if non_additive_dimension_spec is not None:
            # Apply semi additive join on the node
            agg_time_dimension = measure_properties.agg_time_dimension
            queried_time_dimension_spec: Optional[
                TimeDimensionSpec
            ] = self._find_non_additive_dimension_in_linkable_specs(
                agg_time_dimension=agg_time_dimension,
                linkable_specs=queried_linkable_specs.as_tuple,
                non_additive_dimension_spec=non_additive_dimension_spec,
            )
            time_dimension_spec = TimeDimensionSpec.from_name(non_additive_dimension_spec.name)
            window_groupings = tuple(
                LinklessEntitySpec.from_element_name(name) for name in non_additive_dimension_spec.window_groupings
            )
            pre_aggregate_node = SemiAdditiveJoinNode(
                parent_node=pre_aggregate_node,
                entity_specs=window_groupings,
                time_dimension_spec=time_dimension_spec,
                agg_by_function=non_additive_dimension_spec.window_choice,
                queried_time_dimension_spec=queried_time_dimension_spec,
            )

        if not extraneous_linkable_specs.is_subset_of(queried_linkable_specs):
            # At this point, it's the case that the linkable specs in the extraneous specs are not a subset of the queried
            # linkable specs. A filter is needed after, say, a where clause so that the linkable specs in the where clause don't
            # show up in the final result.
            #
            # e.g. for "bookings" by "ds" where "is_instant", "is_instant" should not be in the results.
            pre_aggregate_node = FilterElementsNode(
                parent_node=pre_aggregate_node,
                include_specs=InstanceSpecSet.merge(
                    (InstanceSpecSet(measure_specs=measure_specs), queried_linkable_specs.as_spec_set)
                ),
            )
        return AggregateMeasuresNode(
            parent_node=pre_aggregate_node,
            metric_input_measure_specs=tuple(metric_input_measure_specs),
        )
