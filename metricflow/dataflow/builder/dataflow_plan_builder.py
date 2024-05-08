from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple, Union

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.metric import PydanticMetricTimeWindow
from dbt_semantic_interfaces.protocols.metric import (
    ConstantPropertyInput,
    ConversionTypeParams,
    Metric,
    MetricInputMeasure,
    MetricTimeWindow,
    MetricType,
)
from dbt_semantic_interfaces.references import MetricReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from dbt_semantic_interfaces.validations.unique_valid_name import MetricFlowReservedKeywords
from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.errors.error_classes import UnableToSatisfyQueryError
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.mf_logging.runtime import log_runtime
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolutionLookUp,
)
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.spec_classes import (
    ConstantPropertySpec,
    CumulativeMeasureDescription,
    EntitySpec,
    JoinToTimeSpineDescription,
    LinkableInstanceSpec,
    LinklessEntitySpec,
    MeasureSpec,
    MetadataSpec,
    MetricInputMeasureSpec,
    MetricSpec,
    NonAdditiveDimensionSpec,
    OrderBySpec,
    TimeDimensionSpec,
    WhereFilterSpec,
)
from metricflow_semantics.specs.spec_set import InstanceSpecSet, group_specs_by_type
from metricflow_semantics.specs.where_filter_transform import WhereSpecFactory
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.time.dateutil_adjuster import DateutilTimePeriodAdjuster

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.node_evaluator import (
    JoinLinkableInstancesRecipe,
    LinkableInstanceSatisfiabilityEvaluation,
    NodeEvaluatorForLinkableInstances,
)
from metricflow.dataflow.builder.source_node import SourceNodeBuilder, SourceNodeSet
from metricflow.dataflow.dataflow_plan import (
    DataflowPlan,
    DataflowPlanNode,
)
from metricflow.dataflow.nodes.add_generated_uuid import AddGeneratedUuidColumnNode
from metricflow.dataflow.nodes.aggregate_measures import AggregateMeasuresNode
from metricflow.dataflow.nodes.combine_aggregated_outputs import CombineAggregatedOutputsNode
from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode
from metricflow.dataflow.nodes.constrain_time import ConstrainTimeRangeNode
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.dataflow.nodes.join_conversion_events import JoinConversionEventsNode
from metricflow.dataflow.nodes.join_over_time import JoinOverTimeRangeNode
from metricflow.dataflow.nodes.join_to_base import JoinDescription, JoinOnEntitiesNode
from metricflow.dataflow.nodes.join_to_time_spine import JoinToTimeSpineNode
from metricflow.dataflow.nodes.min_max import MinMaxNode
from metricflow.dataflow.nodes.order_by_limit import OrderByLimitNode
from metricflow.dataflow.nodes.semi_additive_join import SemiAdditiveJoinNode
from metricflow.dataflow.nodes.where_filter import WhereConstraintNode
from metricflow.dataflow.nodes.write_to_data_table import WriteToResultDataTableNode
from metricflow.dataflow.nodes.write_to_table import WriteToResultTableNode
from metricflow.dataflow.optimizer.dataflow_plan_optimizer import DataflowPlanOptimizer
from metricflow.dataset.dataset_classes import DataSet
from metricflow.plan_conversion.node_processor import (
    PredicateInputType,
    PredicatePushdownState,
    PreJoinNodeProcessor,
)
from metricflow.sql.sql_table import SqlTable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataflowRecipe:
    """Get a recipe for how to build a dataflow plan node that outputs measures and linkable instances as needed."""

    source_node: DataflowPlanNode
    required_local_linkable_specs: Tuple[LinkableInstanceSpec, ...]
    join_linkable_instances_recipes: Tuple[JoinLinkableInstancesRecipe, ...]

    @property
    def join_targets(self) -> List[JoinDescription]:
        """Joins to be made to source node."""
        return [join_recipe.join_description for join_recipe in self.join_linkable_instances_recipes]


@dataclass(frozen=True)
class MeasureSpecProperties:
    """Input dataclass for grouping properties of a sequence of MeasureSpecs."""

    measure_specs: Sequence[MeasureSpec]
    semantic_model_name: str
    agg_time_dimension: TimeDimensionReference
    non_additive_dimension_spec: Optional[NonAdditiveDimensionSpec] = None


class DataflowPlanBuilder:
    """Builds a dataflow plan to satisfy a given query."""

    def __init__(  # noqa: D107
        self,
        source_node_set: SourceNodeSet,
        semantic_manifest_lookup: SemanticManifestLookup,
        node_output_resolver: DataflowPlanNodeOutputDataSetResolver,
        column_association_resolver: ColumnAssociationResolver,
        source_node_builder: SourceNodeBuilder,
    ) -> None:
        self._semantic_model_lookup = semantic_manifest_lookup.semantic_model_lookup
        self._metric_lookup = semantic_manifest_lookup.metric_lookup
        self._metric_time_dimension_reference = DataSet.metric_time_dimension_reference()
        self._source_node_set = source_node_set
        self._column_association_resolver = column_association_resolver
        self._node_data_set_resolver = node_output_resolver
        self._source_node_builder = source_node_builder
        self._time_period_adjuster = DateutilTimePeriodAdjuster()

    def build_plan(
        self,
        query_spec: MetricFlowQuerySpec,
        output_sql_table: Optional[SqlTable] = None,
        output_selection_specs: Optional[InstanceSpecSet] = None,
        optimizers: Sequence[DataflowPlanOptimizer] = (),
    ) -> DataflowPlan:
        """Generate a plan for reading the results of a query with the given spec into a data_table or table."""
        # Workaround for a Pycharm type inspection issue with decorators.
        # noinspection PyArgumentList
        return self._build_plan(
            query_spec=query_spec,
            output_sql_table=output_sql_table,
            output_selection_specs=output_selection_specs,
            optimizers=optimizers,
        )

    def _build_query_output_node(
        self, query_spec: MetricFlowQuerySpec, for_group_by_source_node: bool = False
    ) -> DataflowPlanNode:
        """Build SQL output node from query inputs. May be used to build query DFP or source node."""
        for metric_spec in query_spec.metric_specs:
            if (
                len(metric_spec.filter_specs) > 0
                or metric_spec.offset_to_grain is not None
                or metric_spec.offset_window is not None
                or metric_spec.alias is not None
            ):
                raise ValueError(
                    f"The metric specs in the query spec should not contain any metric modifiers. Got: {metric_spec}"
                )

        filter_spec_factory = WhereSpecFactory(
            column_association_resolver=self._column_association_resolver,
            spec_resolution_lookup=query_spec.filter_spec_resolution_lookup,
        )

        query_level_filter_specs = tuple(
            filter_spec_factory.create_from_where_filter_intersection(
                filter_location=WhereFilterLocation.for_query(
                    tuple(metric_spec.reference for metric_spec in query_spec.metric_specs)
                ),
                filter_intersection=query_spec.filter_intersection,
            )
        )

        predicate_pushdown_state = PredicatePushdownState(time_range_constraint=query_spec.time_range_constraint)

        return self._build_metrics_output_node(
            metric_specs=tuple(
                MetricSpec(
                    element_name=metric_spec.element_name,
                    filter_specs=query_level_filter_specs,
                )
                for metric_spec in query_spec.metric_specs
            ),
            queried_linkable_specs=query_spec.linkable_specs,
            filter_spec_factory=filter_spec_factory,
            predicate_pushdown_state=predicate_pushdown_state,
            for_group_by_source_node=for_group_by_source_node,
        )

    @log_runtime()
    def _build_plan(
        self,
        query_spec: MetricFlowQuerySpec,
        output_sql_table: Optional[SqlTable],
        output_selection_specs: Optional[InstanceSpecSet],
        optimizers: Sequence[DataflowPlanOptimizer],
    ) -> DataflowPlan:
        metrics_output_node = self._build_query_output_node(query_spec=query_spec)

        sink_node = DataflowPlanBuilder.build_sink_node(
            parent_node=metrics_output_node,
            order_by_specs=query_spec.order_by_specs,
            output_sql_table=output_sql_table,
            limit=query_spec.limit,
            output_selection_specs=output_selection_specs,
        )

        plan_id = DagId.from_id_prefix(StaticIdPrefix.DATAFLOW_PLAN_PREFIX)
        plan = DataflowPlan(sink_nodes=[sink_node], plan_id=plan_id)
        for optimizer in optimizers:
            logger.info(f"Applying {optimizer.__class__.__name__}")
            try:
                plan = optimizer.optimize(plan)
                logger.info(
                    f"After applying {optimizer.__class__.__name__}, the dataflow plan is:\n"
                    f"{indent(plan.structure_text())}"
                )
            except Exception:
                logger.exception(f"Got an exception applying {optimizer.__class__.__name__}")

        return plan

    def _build_aggregated_conversion_node(
        self,
        base_measure_spec: MetricInputMeasureSpec,
        conversion_measure_spec: MetricInputMeasureSpec,
        entity_spec: EntitySpec,
        window: Optional[MetricTimeWindow],
        queried_linkable_specs: LinkableSpecSet,
        predicate_pushdown_state: PredicatePushdownState,
        constant_properties: Optional[Sequence[ConstantPropertyInput]] = None,
    ) -> DataflowPlanNode:
        """Builds a node that contains aggregated values of conversions and opportunities."""
        # Pushdown parameters vary with conversion metrics due to the way the time joins are applied.
        # Due to other outstanding issues with conversion metric filters, we disable predicate
        # pushdown for any filter parameter set that is not part of the original time range constraint
        # implementation.
        disabled_pushdown_state = PredicatePushdownState.with_pushdown_disabled()
        time_range_only_pushdown_state = PredicatePushdownState(
            time_range_constraint=predicate_pushdown_state.time_range_constraint,
            pushdown_enabled_types=frozenset([PredicateInputType.TIME_RANGE_CONSTRAINT]),
        )

        # Build measure recipes
        base_required_linkable_specs, _ = self.__get_required_and_extraneous_linkable_specs(
            queried_linkable_specs=queried_linkable_specs,
            filter_specs=base_measure_spec.filter_specs,
        )
        base_measure_recipe = self._find_dataflow_recipe(
            measure_spec_properties=self._build_measure_spec_properties([base_measure_spec.measure_spec]),
            predicate_pushdown_state=time_range_only_pushdown_state,
            linkable_spec_set=base_required_linkable_specs,
        )
        logger.info(f"Recipe for base measure aggregation:\n{mf_pformat(base_measure_recipe)}")
        conversion_measure_recipe = self._find_dataflow_recipe(
            measure_spec_properties=self._build_measure_spec_properties([conversion_measure_spec.measure_spec]),
            predicate_pushdown_state=disabled_pushdown_state,
            linkable_spec_set=LinkableSpecSet(),
        )
        logger.info(f"Recipe for conversion measure aggregation:\n{mf_pformat(conversion_measure_recipe)}")
        if base_measure_recipe is None:
            raise UnableToSatisfyQueryError(
                f"Unable to join all items in request. Measure: {base_measure_spec.measure_spec}; Specs to join: {base_required_linkable_specs}"
            )
        if conversion_measure_recipe is None:
            raise UnableToSatisfyQueryError(
                f"Unable to build dataflow plan for conversion measure: {conversion_measure_spec.measure_spec}"
            )

        # Gets the aggregated opportunities
        aggregated_base_measure_node = self.build_aggregated_measure(
            metric_input_measure_spec=base_measure_spec,
            queried_linkable_specs=queried_linkable_specs,
            predicate_pushdown_state=time_range_only_pushdown_state,
        )

        # Build unaggregated conversions source node
        # Generate UUID column for conversion source to uniquely identify each row
        unaggregated_conversion_measure_node = AddGeneratedUuidColumnNode(
            parent_node=conversion_measure_recipe.source_node
        )

        # Get the agg time dimension for each measure used for matching conversion time windows
        base_time_dimension_spec = TimeDimensionSpec.from_reference(
            self._semantic_model_lookup.get_agg_time_dimension_for_measure(base_measure_spec.measure_spec.reference)
        )
        conversion_time_dimension_spec = TimeDimensionSpec.from_reference(
            self._semantic_model_lookup.get_agg_time_dimension_for_measure(
                conversion_measure_spec.measure_spec.reference
            )
        )

        # Filter the source nodes with only the required specs needed for the calculation
        constant_property_specs = []
        required_local_specs = [base_measure_spec.measure_spec, entity_spec, base_time_dimension_spec] + list(
            base_measure_recipe.required_local_linkable_specs
        )
        for constant_property in constant_properties or []:
            base_property_spec = self._semantic_model_lookup.get_element_spec_for_name(constant_property.base_property)
            conversion_property_spec = self._semantic_model_lookup.get_element_spec_for_name(
                constant_property.conversion_property
            )
            required_local_specs.append(base_property_spec)
            constant_property_specs.append(
                ConstantPropertySpec(base_spec=base_property_spec, conversion_spec=conversion_property_spec)
            )

        # Build the unaggregated base measure node for computing conversions
        unaggregated_base_measure_node = base_measure_recipe.source_node
        if base_measure_recipe.join_targets:
            unaggregated_base_measure_node = JoinOnEntitiesNode(
                left_node=unaggregated_base_measure_node, join_targets=base_measure_recipe.join_targets
            )
        filtered_unaggregated_base_node = FilterElementsNode(
            parent_node=unaggregated_base_measure_node,
            include_specs=group_specs_by_type(required_local_specs)
            .merge(base_required_linkable_specs.as_spec_set)
            .dedupe(),
        )

        # Gets the successful conversions using JoinConversionEventsNode
        # The conversion events are joined by the base events which are already time constrained. However, this could
        # be still be constrained, where we adjust the time range to the window size similar to cumulative, but
        # adjusted in the opposite direction.
        join_conversion_node = JoinConversionEventsNode(
            base_node=filtered_unaggregated_base_node,
            base_time_dimension_spec=base_time_dimension_spec,
            conversion_node=unaggregated_conversion_measure_node,
            conversion_measure_spec=conversion_measure_spec.measure_spec,
            conversion_time_dimension_spec=conversion_time_dimension_spec,
            unique_identifier_keys=(MetadataSpec.from_name(MetricFlowReservedKeywords.MF_INTERNAL_UUID.value),),
            entity_spec=entity_spec,
            window=window,
            constant_properties=constant_property_specs,
        )

        # Aggregate the conversion events with the JoinConversionEventsNode as the source node
        recipe_with_join_conversion_source_node = DataflowRecipe(
            source_node=join_conversion_node,
            required_local_linkable_specs=base_measure_recipe.required_local_linkable_specs,
            join_linkable_instances_recipes=base_measure_recipe.join_linkable_instances_recipes,
        )
        # TODO: Refine conversion metric configuration to fit into the standard dataflow plan building model
        # In this case we override the measure recipe, which currently results in us bypassing predicate pushdown
        # Rather than relying on happenstance in the way the code is laid out we also explicitly disable
        # predicate pushdwon until we are ready to fully support it for conversion metrics
        aggregated_conversions_node = self.build_aggregated_measure(
            metric_input_measure_spec=conversion_measure_spec,
            queried_linkable_specs=queried_linkable_specs,
            predicate_pushdown_state=disabled_pushdown_state,
            measure_recipe=recipe_with_join_conversion_source_node,
        )

        # Combine the aggregated opportunities and conversion data sets
        return CombineAggregatedOutputsNode(parent_nodes=(aggregated_base_measure_node, aggregated_conversions_node))

    def _build_conversion_metric_output_node(
        self,
        metric_spec: MetricSpec,
        queried_linkable_specs: LinkableSpecSet,
        filter_spec_factory: WhereSpecFactory,
        predicate_pushdown_state: PredicatePushdownState,
        for_group_by_source_node: bool = False,
    ) -> ComputeMetricsNode:
        """Builds a compute metric node for a conversion metric."""
        metric_reference = metric_spec.reference
        metric = self._metric_lookup.get_metric(metric_reference)
        conversion_type_params = metric.type_params.conversion_type_params
        assert conversion_type_params, "A conversion metric should have type_params.conversion_type_params defined."
        base_measure, conversion_measure = self._build_input_measure_specs_for_conversion_metric(
            metric_reference=metric_spec.reference,
            conversion_type_params=conversion_type_params,
            filter_spec_factory=filter_spec_factory,
            descendent_filter_specs=metric_spec.filter_specs,
            queried_linkable_specs=queried_linkable_specs,
        )
        entity_spec = EntitySpec.from_name(conversion_type_params.entity)
        logger.info(
            f"For conversion metric {metric_spec},\n"
            f"base_measure is:\n{mf_pformat(base_measure)}\n"
            f"conversion_measure is:\n{mf_pformat(conversion_measure)}\n"
            f"entity is:\n{mf_pformat(entity_spec)}"
        )

        aggregated_measures_node = self._build_aggregated_conversion_node(
            base_measure_spec=base_measure,
            conversion_measure_spec=conversion_measure,
            queried_linkable_specs=queried_linkable_specs,
            predicate_pushdown_state=predicate_pushdown_state,
            entity_spec=entity_spec,
            window=conversion_type_params.window,
            constant_properties=conversion_type_params.constant_properties,
        )

        return self.build_computed_metrics_node(
            metric_spec=metric_spec,
            aggregated_measures_node=aggregated_measures_node,
            for_group_by_source_node=for_group_by_source_node,
            aggregated_to_elements=set(queried_linkable_specs.as_tuple),
        )

    def _build_base_metric_output_node(
        self,
        metric_spec: MetricSpec,
        queried_linkable_specs: LinkableSpecSet,
        filter_spec_factory: WhereSpecFactory,
        predicate_pushdown_state: PredicatePushdownState,
        for_group_by_source_node: bool = False,
    ) -> ComputeMetricsNode:
        """Builds a node to compute a metric that is not defined from other metrics."""
        metric_reference = metric_spec.reference
        metric = self._metric_lookup.get_metric(metric_reference)
        if metric.type is MetricType.SIMPLE or metric.type is MetricType.CUMULATIVE:
            pass
        elif (
            metric.type is MetricType.RATIO or metric.type is MetricType.DERIVED or metric.type is MetricType.CONVERSION
        ):
            raise ValueError(f"This should only be called for base metrics (simple or cumulative). Got: {metric.type}")
        else:
            assert_values_exhausted(metric.type)
        assert (
            len(metric.input_measures) == 1
        ), f"A base metric should not have multiple measures. Got {metric.input_measures}"

        metric_input_measure_spec = self._build_input_measure_spec(
            filter_spec_factory=filter_spec_factory,
            metric=metric,
            input_measure=metric.input_measures[0],
            queried_linkable_specs=queried_linkable_specs,
            child_metric_offset_window=metric_spec.offset_window,
            child_metric_offset_to_grain=metric_spec.offset_to_grain,
            cumulative_description=(
                CumulativeMeasureDescription(
                    cumulative_window=metric.type_params.window,
                    cumulative_grain_to_date=metric.type_params.grain_to_date,
                )
                if metric.type is MetricType.CUMULATIVE
                else None
            ),
            descendent_filter_specs=metric_spec.filter_specs,
        )

        logger.info(
            f"For\n{indent(mf_pformat(metric_spec))}"
            f"\nneeded measure is:"
            f"\n{indent(mf_pformat(metric_input_measure_spec))}"
        )

        aggregated_measures_node = self.build_aggregated_measure(
            metric_input_measure_spec=metric_input_measure_spec,
            queried_linkable_specs=queried_linkable_specs,
            predicate_pushdown_state=predicate_pushdown_state,
        )
        return self.build_computed_metrics_node(
            metric_spec=metric_spec,
            aggregated_measures_node=aggregated_measures_node,
            for_group_by_source_node=for_group_by_source_node,
            aggregated_to_elements=set(queried_linkable_specs.as_tuple),
        )

    def _build_derived_metric_output_node(
        self,
        metric_spec: MetricSpec,
        queried_linkable_specs: LinkableSpecSet,
        filter_spec_factory: WhereSpecFactory,
        predicate_pushdown_state: PredicatePushdownState,
        for_group_by_source_node: bool = False,
    ) -> DataflowPlanNode:
        """Builds a node to compute a metric defined from other metrics."""
        metric = self._metric_lookup.get_metric(metric_spec.reference)
        metric_input_specs = self._build_input_metric_specs_for_derived_metric(
            metric_reference=metric_spec.reference,
            filter_spec_factory=filter_spec_factory,
        )
        logger.info(
            f"For {metric.type} metric: {metric_spec}, needed metrics are:\n" f"{mf_pformat(metric_input_specs)}"
        )

        required_linkable_specs, extraneous_linkable_specs = self.__get_required_and_extraneous_linkable_specs(
            queried_linkable_specs=queried_linkable_specs, filter_specs=metric_spec.filter_specs
        )

        parent_nodes: List[DataflowPlanNode] = []

        # This is the filter that's defined for the metric in the configs.
        metric_definition_filter_specs = filter_spec_factory.create_from_where_filter_intersection(
            filter_location=WhereFilterLocation.for_metric(metric_spec.reference),
            filter_intersection=metric.filter,
        )

        for metric_input_spec in metric_input_specs:
            filter_specs: List[WhereFilterSpec] = []
            filter_specs.extend(metric_definition_filter_specs)
            # These are the filters that's defined as part of the input metric.
            filter_specs.extend(metric_input_spec.filter_specs)

            # If metric is offset, we'll apply where constraint after offset to avoid removing values
            # unexpectedly. Time constraint will be applied by INNER JOINing to time spine.
            if not metric_spec.has_time_offset:
                filter_specs.extend(metric_spec.filter_specs)

            metric_pushdown_state = (
                predicate_pushdown_state
                if not metric_spec.has_time_offset
                else PredicatePushdownState.with_pushdown_disabled()
            )

            parent_nodes.append(
                self._build_any_metric_output_node(
                    metric_spec=MetricSpec(
                        element_name=metric_input_spec.element_name,
                        filter_specs=tuple(filter_specs),
                        alias=metric_input_spec.alias,
                        offset_window=metric_input_spec.offset_window,
                        offset_to_grain=metric_input_spec.offset_to_grain,
                    ),
                    queried_linkable_specs=(
                        queried_linkable_specs if not metric_spec.has_time_offset else required_linkable_specs
                    ),
                    filter_spec_factory=filter_spec_factory,
                    predicate_pushdown_state=metric_pushdown_state,
                )
            )

        parent_node = (
            parent_nodes[0] if len(parent_nodes) == 1 else CombineAggregatedOutputsNode(parent_nodes=parent_nodes)
        )
        output_node: DataflowPlanNode = ComputeMetricsNode(
            parent_node=parent_node,
            metric_specs=[metric_spec],
            for_group_by_source_node=for_group_by_source_node,
            aggregated_to_elements=set(queried_linkable_specs.as_tuple),
        )

        # For ratio / derived metrics with time offset, apply offset & where constraint after metric computation.
        if metric_spec.has_time_offset:
            queried_agg_time_dimension_specs = queried_linkable_specs.included_agg_time_dimension_specs_for_metric(
                metric_reference=metric_spec.reference, metric_lookup=self._metric_lookup
            )
            assert (
                queried_agg_time_dimension_specs
            ), "Joining to time spine requires querying with metric_time or the appropriate agg_time_dimension."
            output_node = JoinToTimeSpineNode(
                parent_node=output_node,
                requested_agg_time_dimension_specs=queried_agg_time_dimension_specs,
                use_custom_agg_time_dimension=not queried_linkable_specs.contains_metric_time,
                time_range_constraint=predicate_pushdown_state.time_range_constraint,
                offset_window=metric_spec.offset_window,
                offset_to_grain=metric_spec.offset_to_grain,
                join_type=SqlJoinType.INNER,
            )

            if len(metric_spec.filter_specs) > 0:
                merged_where_filter = WhereFilterSpec.merge_iterable(metric_spec.filter_specs)
                output_node = WhereConstraintNode(parent_node=output_node, where_constraint=merged_where_filter)
            if not extraneous_linkable_specs.is_subset_of(queried_linkable_specs):
                output_node = FilterElementsNode(
                    parent_node=output_node,
                    include_specs=InstanceSpecSet(metric_specs=(metric_spec,)).merge(
                        queried_linkable_specs.as_spec_set
                    ),
                )
        return output_node

    def _build_any_metric_output_node(
        self,
        metric_spec: MetricSpec,
        queried_linkable_specs: LinkableSpecSet,
        filter_spec_factory: WhereSpecFactory,
        predicate_pushdown_state: PredicatePushdownState,
        for_group_by_source_node: bool = False,
    ) -> DataflowPlanNode:
        """Builds a node to compute a metric of any type."""
        metric = self._metric_lookup.get_metric(metric_spec.reference)

        if metric.type is MetricType.SIMPLE or metric.type is MetricType.CUMULATIVE:
            return self._build_base_metric_output_node(
                metric_spec=metric_spec,
                queried_linkable_specs=queried_linkable_specs,
                filter_spec_factory=filter_spec_factory,
                predicate_pushdown_state=predicate_pushdown_state,
                for_group_by_source_node=for_group_by_source_node,
            )

        elif metric.type is MetricType.RATIO or metric.type is MetricType.DERIVED:
            return self._build_derived_metric_output_node(
                metric_spec=metric_spec,
                queried_linkable_specs=queried_linkable_specs,
                filter_spec_factory=filter_spec_factory,
                predicate_pushdown_state=predicate_pushdown_state,
                for_group_by_source_node=for_group_by_source_node,
            )
        elif metric.type is MetricType.CONVERSION:
            return self._build_conversion_metric_output_node(
                metric_spec=metric_spec,
                queried_linkable_specs=queried_linkable_specs,
                filter_spec_factory=filter_spec_factory,
                predicate_pushdown_state=predicate_pushdown_state,
                for_group_by_source_node=for_group_by_source_node,
            )

        assert_values_exhausted(metric.type)

    def _build_metrics_output_node(
        self,
        metric_specs: Sequence[MetricSpec],
        queried_linkable_specs: LinkableSpecSet,
        filter_spec_factory: WhereSpecFactory,
        predicate_pushdown_state: PredicatePushdownState,
        for_group_by_source_node: bool = False,
    ) -> DataflowPlanNode:
        """Builds a node that computes all requested metrics.

        Args:
            metric_specs: Specs for metrics to compute. Contains modifications to the metric defined in the model to
            include offsets and filters.
            queried_linkable_specs: Dimensions/entities that were queried.
            filter_spec_factory: Constructs WhereFilterSpecs with the resolved ambiguous group-by-items in the filter.
            predicate_pushdown_state: Parameters for evaluating and applying filter predicate pushdown, e.g., for
            applying time constraints prior to other dimension joins.
        """
        output_nodes: List[DataflowPlanNode] = []

        for metric_spec in metric_specs:
            logger.info(f"Generating compute metrics node for:\n{indent(mf_pformat(metric_spec))}")
            self._metric_lookup.get_metric(metric_spec.reference)

            output_nodes.append(
                self._build_any_metric_output_node(
                    metric_spec=metric_spec,
                    queried_linkable_specs=queried_linkable_specs,
                    filter_spec_factory=filter_spec_factory,
                    predicate_pushdown_state=predicate_pushdown_state,
                    for_group_by_source_node=for_group_by_source_node,
                )
            )

        assert len(output_nodes) > 0, "ComputeMetricsNode was not properly constructed"

        if len(output_nodes) == 1:
            return output_nodes[0]

        return CombineAggregatedOutputsNode(parent_nodes=output_nodes)

    def build_plan_for_distinct_values(self, query_spec: MetricFlowQuerySpec) -> DataflowPlan:
        """Generate a plan that would get the distinct values of a linkable instance.

        e.g. distinct listing__country_latest for bookings by listing__country_latest
        """
        # Workaround for a Pycharm type inspection issue with decorators.
        # noinspection PyArgumentList
        return self._build_plan_for_distinct_values(query_spec)

    @log_runtime()
    def _build_plan_for_distinct_values(self, query_spec: MetricFlowQuerySpec) -> DataflowPlan:
        assert not query_spec.metric_specs, "Can't build distinct values plan with metrics."
        query_level_filter_specs: Sequence[WhereFilterSpec] = ()
        if query_spec.filter_intersection is not None and len(query_spec.filter_intersection.where_filters) > 0:
            filter_spec_factory = WhereSpecFactory(
                column_association_resolver=self._column_association_resolver,
                spec_resolution_lookup=query_spec.filter_spec_resolution_lookup
                or FilterSpecResolutionLookUp.empty_instance(),
            )

            query_level_filter_specs = filter_spec_factory.create_from_where_filter_intersection(
                filter_location=WhereFilterLocation.for_query(metric_references=tuple()),
                filter_intersection=query_spec.filter_intersection,
            )

        required_linkable_specs, _ = self.__get_required_and_extraneous_linkable_specs(
            queried_linkable_specs=query_spec.linkable_specs, filter_specs=query_level_filter_specs
        )
        predicate_pushdown_state = PredicatePushdownState(time_range_constraint=query_spec.time_range_constraint)
        dataflow_recipe = self._find_dataflow_recipe(
            linkable_spec_set=required_linkable_specs, predicate_pushdown_state=predicate_pushdown_state
        )
        if not dataflow_recipe:
            raise UnableToSatisfyQueryError(f"Unable to join all items in request: {required_linkable_specs}")

        output_node = dataflow_recipe.source_node
        if dataflow_recipe.join_targets:
            output_node = JoinOnEntitiesNode(left_node=output_node, join_targets=dataflow_recipe.join_targets)

        if len(query_level_filter_specs) > 0:
            output_node = WhereConstraintNode(
                parent_node=output_node, where_constraint=WhereFilterSpec.merge_iterable(query_level_filter_specs)
            )
        if query_spec.time_range_constraint:
            output_node = ConstrainTimeRangeNode(
                parent_node=output_node, time_range_constraint=query_spec.time_range_constraint
            )

        output_node = FilterElementsNode(
            parent_node=output_node, include_specs=query_spec.linkable_specs.as_spec_set, distinct=True
        )

        if query_spec.min_max_only:
            output_node = MinMaxNode(parent_node=output_node)

        sink_node = self.build_sink_node(
            parent_node=output_node, order_by_specs=query_spec.order_by_specs, limit=query_spec.limit
        )

        return DataflowPlan(sink_nodes=[sink_node])

    @staticmethod
    def build_sink_node(
        parent_node: DataflowPlanNode,
        order_by_specs: Sequence[OrderBySpec],
        output_sql_table: Optional[SqlTable] = None,
        limit: Optional[int] = None,
        output_selection_specs: Optional[InstanceSpecSet] = None,
    ) -> DataflowPlanNode:
        """Adds order by / limit / write nodes."""
        pre_result_node: Optional[DataflowPlanNode] = None

        if order_by_specs or limit:
            pre_result_node = OrderByLimitNode(
                order_by_specs=list(order_by_specs), limit=limit, parent_node=parent_node
            )

        if output_selection_specs:
            pre_result_node = FilterElementsNode(
                parent_node=pre_result_node or parent_node, include_specs=output_selection_specs
            )

        write_result_node: DataflowPlanNode
        if not output_sql_table:
            write_result_node = WriteToResultDataTableNode(parent_node=pre_result_node or parent_node)
        else:
            write_result_node = WriteToResultTableNode(
                parent_node=pre_result_node or parent_node, output_sql_table=output_sql_table
            )

        return write_result_node

    @staticmethod
    def _contains_multihop_linkables(linkable_specs: Sequence[LinkableInstanceSpec]) -> bool:
        """Returns true if any of the linkable specs requires a multi-hop join to realize."""
        return any(len(x.entity_links) > 1 for x in linkable_specs)

    def _sort_by_suitability(self, nodes: Sequence[DataflowPlanNode]) -> Sequence[DataflowPlanNode]:
        """Sort nodes by the number of linkable specs.

        The lower the number of linkable specs means less aggregation required.
        """

        def sort_function(node: DataflowPlanNode) -> int:
            data_set = self._node_data_set_resolver.get_output_data_set(node)
            return len(data_set.instance_set.spec_set.linkable_specs)

        return sorted(nodes, key=sort_function)

    def _select_source_nodes_with_measures(
        self, measure_specs: Set[MeasureSpec], source_nodes: Sequence[DataflowPlanNode]
    ) -> Sequence[DataflowPlanNode]:
        nodes = []
        measure_specs_set = set(measure_specs)
        for source_node in source_nodes:
            measure_specs_in_node = self._node_data_set_resolver.get_output_data_set(
                source_node
            ).instance_set.spec_set.measure_specs
            if measure_specs_set.intersection(set(measure_specs_in_node)) == measure_specs_set:
                nodes.append(source_node)
        return nodes

    def _select_source_nodes_with_linkable_specs(
        self, linkable_specs: LinkableSpecSet, source_nodes: Sequence[DataflowPlanNode]
    ) -> Sequence[DataflowPlanNode]:
        """Find source nodes with requested linkable specs and no measures."""
        # Use a dictionary to dedupe for consistent ordering.
        selected_nodes: Dict[DataflowPlanNode, None] = {}
        requested_linkable_specs_set = set(linkable_specs.as_tuple)
        for source_node in source_nodes:
            output_spec_set = self._node_data_set_resolver.get_output_data_set(source_node).instance_set.spec_set
            all_linkable_specs_in_node = set(output_spec_set.linkable_specs)
            requested_linkable_specs_in_node = requested_linkable_specs_set.intersection(all_linkable_specs_in_node)
            if requested_linkable_specs_in_node:
                selected_nodes[source_node] = None

        return tuple(selected_nodes.keys())

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
        semantic_model_names = {
            self._semantic_model_lookup.get_semantic_model_for_measure(measure.reference).name
            for measure in measure_specs
        }
        if len(semantic_model_names) > 1:
            raise ValueError(
                f"Cannot find common properties for measures {measure_specs} coming from multiple "
                f"semantic models: {semantic_model_names}. This suggests the measure_specs were not correctly filtered."
            )
        semantic_model_name = semantic_model_names.pop()

        agg_time_dimension = self._semantic_model_lookup.get_agg_time_dimension_for_measure(measure_specs[0].reference)
        non_additive_dimension_spec = measure_specs[0].non_additive_dimension_spec
        for measure_spec in measure_specs:
            if non_additive_dimension_spec != measure_spec.non_additive_dimension_spec:
                raise ValueError(f"measure_specs {measure_specs} do not have the same non_additive_dimension_spec.")
            measure_agg_time_dimension = self._semantic_model_lookup.get_agg_time_dimension_for_measure(
                measure_spec.reference
            )
            if measure_agg_time_dimension != agg_time_dimension:
                raise ValueError(f"measure_specs {measure_specs} do not have the same agg_time_dimension.")
        return MeasureSpecProperties(
            measure_specs=measure_specs,
            semantic_model_name=semantic_model_name,
            agg_time_dimension=agg_time_dimension,
            non_additive_dimension_spec=non_additive_dimension_spec,
        )

    def _find_dataflow_recipe(
        self,
        linkable_spec_set: LinkableSpecSet,
        predicate_pushdown_state: PredicatePushdownState,
        measure_spec_properties: Optional[MeasureSpecProperties] = None,
    ) -> Optional[DataflowRecipe]:
        linkable_specs = linkable_spec_set.as_tuple
        candidate_nodes_for_left_side_of_join: List[DataflowPlanNode] = []
        candidate_nodes_for_right_side_of_join: List[DataflowPlanNode] = []

        if measure_spec_properties:
            candidate_nodes_for_right_side_of_join += self._source_node_set.source_nodes_for_metric_queries
            candidate_nodes_for_left_side_of_join += self._select_source_nodes_with_measures(
                measure_specs=set(measure_spec_properties.measure_specs),
                source_nodes=self._source_node_set.source_nodes_for_metric_queries,
            )
            default_join_type = SqlJoinType.LEFT_OUTER
        else:
            candidate_nodes_for_right_side_of_join += list(self._source_node_set.source_nodes_for_group_by_item_queries)
            candidate_nodes_for_left_side_of_join += list(
                self._select_source_nodes_with_linkable_specs(
                    linkable_specs=linkable_spec_set,
                    source_nodes=self._source_node_set.source_nodes_for_group_by_item_queries,
                )
            )
            default_join_type = SqlJoinType.FULL_OUTER

        logger.info(
            f"Starting search with {len(candidate_nodes_for_left_side_of_join)} potential source nodes on the left "
            f"side of the join, and {len(candidate_nodes_for_right_side_of_join)} potential nodes on the right side "
            f"of the join."
        )
        start_time = time.time()

        node_processor = PreJoinNodeProcessor(
            semantic_model_lookup=self._semantic_model_lookup,
            node_data_set_resolver=self._node_data_set_resolver,
        )

        if predicate_pushdown_state.has_pushdown_potential:
            candidate_nodes_for_left_side_of_join = list(
                node_processor.apply_matching_filter_predicates(
                    source_nodes=candidate_nodes_for_left_side_of_join,
                    predicate_pushdown_state=predicate_pushdown_state,
                    metric_time_dimension_reference=self._metric_time_dimension_reference,
                )
            )

        candidate_nodes_for_right_side_of_join = node_processor.remove_unnecessary_nodes(
            desired_linkable_specs=linkable_specs,
            nodes=candidate_nodes_for_right_side_of_join,
            metric_time_dimension_reference=self._metric_time_dimension_reference,
            time_spine_node=self._source_node_set.time_spine_node,
        )
        logger.info(
            f"After removing unnecessary nodes, there are {len(candidate_nodes_for_right_side_of_join)} candidate "
            f"nodes for the right side of the join"
        )
        if DataflowPlanBuilder._contains_multihop_linkables(linkable_specs):
            candidate_nodes_for_right_side_of_join = list(
                node_processor.add_multi_hop_joins(
                    desired_linkable_specs=linkable_specs,
                    nodes=candidate_nodes_for_right_side_of_join,
                    join_type=default_join_type,
                )
            )
            logger.info(
                f"After adding multi-hop nodes, there are {len(candidate_nodes_for_right_side_of_join)} candidate "
                f"nodes for the right side of the join:\n"
                f"{mf_pformat(candidate_nodes_for_right_side_of_join)}"
            )

        # If there are MetricGroupBys in the requested linkable specs, build source nodes to satisfy them.
        # We do this at query time instead of during usual source node generation because the number of potential
        # MetricGroupBy source nodes could be extremely large (and potentially slow).
        logger.info(f"Building source nodes for group by metrics: {linkable_spec_set.group_by_metric_specs}")
        candidate_nodes_for_right_side_of_join += [
            self._build_query_output_node(
                query_spec=self._source_node_builder.build_source_node_inputs_for_group_by_metric(group_by_metric_spec),
                for_group_by_source_node=True,
            )
            for group_by_metric_spec in linkable_spec_set.group_by_metric_specs
        ]

        logger.info(f"Processing nodes took: {time.time()-start_time:.2f}s")

        node_evaluator = NodeEvaluatorForLinkableInstances(
            semantic_model_lookup=self._semantic_model_lookup,
            nodes_available_for_joins=self._sort_by_suitability(candidate_nodes_for_right_side_of_join),
            node_data_set_resolver=self._node_data_set_resolver,
            time_spine_node=self._source_node_set.time_spine_node,
        )

        # Dict from the node that contains the source node to the evaluation results.
        node_to_evaluation: Dict[DataflowPlanNode, LinkableInstanceSatisfiabilityEvaluation] = {}

        for node in self._sort_by_suitability(candidate_nodes_for_left_side_of_join):
            data_set = self._node_data_set_resolver.get_output_data_set(node)

            if measure_spec_properties:
                measure_specs = measure_spec_properties.measure_specs
                missing_specs = [
                    spec for spec in measure_specs if spec not in data_set.instance_set.spec_set.measure_specs
                ]
                if missing_specs:
                    logger.debug(
                        f"Skipping evaluation for:\n"
                        f"{indent(node.structure_text())}"
                        f"since it does not have all of the measure specs:\n"
                        f"{indent(mf_pformat(missing_specs))}"
                    )
                    continue

            logger.debug(
                f"Evaluating candidate node for the left side of the join:\n{indent(mf_pformat(node.structure_text()))}"
            )

            start_time = time.time()
            evaluation = node_evaluator.evaluate_node(
                left_node=node,
                required_linkable_specs=list(linkable_specs),
                default_join_type=default_join_type,
            )
            logger.info(f"Evaluation of {node} took {time.time() - start_time:.2f}s")

            logger.info(
                "Evaluation for source node:"
                + indent(f"\nnode:\n{indent(node.structure_text())}")
                + indent(f"\nevaluation:\n{indent(mf_pformat(evaluation))}")
            )

            if len(evaluation.unjoinable_linkable_specs) > 0:
                logger.info(
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
                break

        logger.info(f"Found {len(node_to_evaluation)} candidate source nodes.")

        if len(node_to_evaluation) > 0:
            # Find evaluation with lowest number of joins.
            node_with_lowest_cost_plan = min(
                node_to_evaluation, key=lambda node: len(node_to_evaluation[node].join_recipes)
            )
            evaluation = node_to_evaluation[node_with_lowest_cost_plan]

            logger.info(
                "Lowest cost plan is:"
                + indent(f"\nnode:\n{indent(node_with_lowest_cost_plan.structure_text())}")
                + indent(f"\nevaluation:\n{indent(mf_pformat(evaluation))}")
                + indent(f"\njoins: {len(node_to_evaluation[node_with_lowest_cost_plan].join_recipes)}")
            )

            # Nodes containing the linkable instances will be joined to the source node, so these
            # entities will need to be present in the source node.
            required_local_entity_specs = tuple(x.join_on_entity for x in evaluation.join_recipes if x.join_on_entity)
            # Same thing with partitions.
            required_local_dimension_specs = tuple(
                y.start_node_dimension_spec for x in evaluation.join_recipes for y in x.join_on_partition_dimensions
            )
            required_local_time_dimension_specs = tuple(
                y.start_node_time_dimension_spec
                for x in evaluation.join_recipes
                for y in x.join_on_partition_time_dimensions
            )
            return DataflowRecipe(
                source_node=node_with_lowest_cost_plan,
                required_local_linkable_specs=(
                    evaluation.local_linkable_specs
                    + required_local_entity_specs
                    + required_local_dimension_specs
                    + required_local_time_dimension_specs
                ),
                join_linkable_instances_recipes=node_to_evaluation[node_with_lowest_cost_plan].join_recipes,
            )

        logger.error("No recipe could be constructed.")
        return None

    def build_computed_metrics_node(
        self,
        metric_spec: MetricSpec,
        aggregated_measures_node: Union[AggregateMeasuresNode, DataflowPlanNode],
        aggregated_to_elements: Set[LinkableInstanceSpec],
        for_group_by_source_node: bool = False,
    ) -> ComputeMetricsNode:
        """Builds a ComputeMetricsNode from aggregated measures."""
        return ComputeMetricsNode(
            parent_node=aggregated_measures_node,
            metric_specs=[metric_spec],
            for_group_by_source_node=for_group_by_source_node,
            aggregated_to_elements=aggregated_to_elements,
        )

    def _build_input_measure_specs_for_conversion_metric(
        self,
        metric_reference: MetricReference,
        conversion_type_params: ConversionTypeParams,
        filter_spec_factory: WhereSpecFactory,
        descendent_filter_specs: Sequence[WhereFilterSpec],
        queried_linkable_specs: LinkableSpecSet,
    ) -> Tuple[MetricInputMeasureSpec, MetricInputMeasureSpec]:
        """Return [base_measure_input, conversion_measure_input] for computing a conversion metric."""
        metric = self._metric_lookup.get_metric(metric_reference)
        if metric.type is not MetricType.CONVERSION:
            raise ValueError("This should only be called for conversion metrics.")

        base_input_measure, conversion_input_measure = [
            self._build_input_measure_spec(
                filter_spec_factory=filter_spec_factory,
                metric=metric,
                input_measure=input_measure,
                queried_linkable_specs=queried_linkable_specs,
                descendent_filter_specs=descendent_filter_specs,
                include_filters=include_filters,
            )
            # Filters should only be applied to base measures.
            for input_measure, include_filters in [
                (conversion_type_params.base_measure, True),
                (conversion_type_params.conversion_measure, False),
            ]
        ]

        return base_input_measure, conversion_input_measure

    def _build_input_measure_spec(
        self,
        filter_spec_factory: WhereSpecFactory,
        metric: Metric,
        input_measure: MetricInputMeasure,
        descendent_filter_specs: Sequence[WhereFilterSpec],
        queried_linkable_specs: LinkableSpecSet,
        include_filters: bool = True,
        child_metric_offset_window: Optional[MetricTimeWindow] = None,
        child_metric_offset_to_grain: Optional[TimeGranularity] = None,
        cumulative_description: Optional[CumulativeMeasureDescription] = None,
    ) -> MetricInputMeasureSpec:
        """Return the input measure spec required to compute the base metric.

        "child" refers to the derived metric that uses the metric specified by metric_reference in the definition.
        descendent_filter_specs includes all filter specs required to compute the metric in the query. This includes the
        filters in the query and any filter in the definition of metrics in between.
        """
        filter_specs: Tuple[WhereFilterSpec, ...] = ()
        if include_filters:
            filter_specs = self._build_filter_specs_for_input_measure(
                filter_spec_factory=filter_spec_factory,
                metric=metric,
                input_measure=input_measure,
                descendent_filter_specs=descendent_filter_specs,
            )

        measure_spec = MeasureSpec(
            element_name=input_measure.name,
            non_additive_dimension_spec=self._semantic_model_lookup.non_additive_dimension_specs_by_measure.get(
                input_measure.measure_reference
            ),
        )

        before_aggregation_time_spine_join_description = None
        # If querying an offset metric, join to time spine.
        if child_metric_offset_window is not None or child_metric_offset_to_grain is not None:
            before_aggregation_time_spine_join_description = JoinToTimeSpineDescription(
                join_type=SqlJoinType.INNER,
                offset_window=child_metric_offset_window,
                offset_to_grain=child_metric_offset_to_grain,
            )

        # Even if the measure is configured to join to time spine, if there's no agg_time_dimension in the query,
        # there's no need to join to the time spine since all time will be aggregated.
        after_aggregation_time_spine_join_description = None
        if input_measure.join_to_timespine:
            if (
                len(
                    queried_linkable_specs.included_agg_time_dimension_specs_for_measure(
                        measure_reference=measure_spec.reference, semantic_model_lookup=self._semantic_model_lookup
                    )
                )
                > 0
            ):
                after_aggregation_time_spine_join_description = JoinToTimeSpineDescription(
                    join_type=SqlJoinType.LEFT_OUTER,
                    offset_window=None,
                    offset_to_grain=None,
                )

        return MetricInputMeasureSpec(
            measure_spec=measure_spec,
            fill_nulls_with=input_measure.fill_nulls_with,
            offset_window=child_metric_offset_window,
            offset_to_grain=child_metric_offset_to_grain,
            cumulative_description=cumulative_description,
            filter_specs=filter_specs,
            alias=input_measure.alias,
            before_aggregation_time_spine_join_description=before_aggregation_time_spine_join_description,
            after_aggregation_time_spine_join_description=after_aggregation_time_spine_join_description,
        )

    def _build_filter_specs_for_input_measure(
        self,
        filter_spec_factory: WhereSpecFactory,
        metric: Metric,
        input_measure: MetricInputMeasure,
        descendent_filter_specs: Sequence[WhereFilterSpec],
    ) -> Tuple[WhereFilterSpec, ...]:
        metric_reference = MetricReference(element_name=metric.name)
        filter_specs: List[WhereFilterSpec] = []
        filter_location = WhereFilterLocation.for_metric(metric_reference)
        for filter_ in (input_measure.filter, metric.filter):
            filter_specs.extend(
                filter_spec_factory.create_from_where_filter_intersection(
                    filter_location=filter_location, filter_intersection=filter_
                )
            )
        filter_specs.extend(descendent_filter_specs)
        return tuple(filter_specs)

    def _build_input_metric_specs_for_derived_metric(
        self,
        metric_reference: MetricReference,
        filter_spec_factory: WhereSpecFactory,
    ) -> Sequence[MetricSpec]:
        """Return the metric specs referenced by the metric. Current use case is for derived metrics."""
        metric = self._metric_lookup.get_metric(metric_reference)
        input_metric_specs: List[MetricSpec] = []

        for input_metric in metric.input_metrics:
            filter_specs = filter_spec_factory.create_from_where_filter_intersection(
                filter_location=WhereFilterLocation.for_metric(input_metric.as_reference),
                filter_intersection=input_metric.filter,
            )

            spec = MetricSpec(
                element_name=input_metric.name,
                filter_specs=tuple(filter_specs),
                alias=input_metric.alias,
                offset_window=(
                    PydanticMetricTimeWindow(
                        count=input_metric.offset_window.count,
                        granularity=input_metric.offset_window.granularity,
                    )
                    if input_metric.offset_window
                    else None
                ),
                offset_to_grain=input_metric.offset_to_grain,
            )
            input_metric_specs.append(spec)
        return tuple(input_metric_specs)

    def build_aggregated_measure(
        self,
        metric_input_measure_spec: MetricInputMeasureSpec,
        queried_linkable_specs: LinkableSpecSet,
        predicate_pushdown_state: PredicatePushdownState,
        measure_recipe: Optional[DataflowRecipe] = None,
    ) -> DataflowPlanNode:
        """Returns a node where the measures are aggregated by the linkable specs and constrained appropriately.

        This might be a node representing a single aggregation over one semantic model, or a node representing
        a composite set of aggregations originating from multiple semantic models, and joined into a single
        aggregated set of measures.
        """
        measure_spec = metric_input_measure_spec.measure_spec

        logger.info(
            f"Building aggregated measure: {measure_spec} with input measure filters:\n"
            f"{mf_pformat(metric_input_measure_spec.filter_specs)}\n"
            f"and  filters:\n{mf_pformat(metric_input_measure_spec.filter_specs)}"
        )

        return self._build_aggregated_measure_from_measure_source_node(
            metric_input_measure_spec=metric_input_measure_spec,
            queried_linkable_specs=queried_linkable_specs,
            predicate_pushdown_state=predicate_pushdown_state,
            measure_recipe=measure_recipe,
        )

    def __get_required_and_extraneous_linkable_specs(
        self,
        queried_linkable_specs: LinkableSpecSet,
        filter_specs: Sequence[WhereFilterSpec],
        non_additive_dimension_spec: Optional[NonAdditiveDimensionSpec] = None,
    ) -> Tuple[LinkableSpecSet, LinkableSpecSet]:
        """Get the required and extraneous linkable specs for this query.

        Extraneous linkable specs are specs that are used in this phase that should not show up in the final result
        unless it was already a requested spec in the query (e.g., linkable spec used in where constraint)
        """
        linkable_spec_sets_to_merge: List[LinkableSpecSet] = []
        for filter_spec in filter_specs:
            linkable_spec_sets_to_merge.append(LinkableSpecSet.create_from_specs(filter_spec.linkable_specs))
        if non_additive_dimension_spec:
            linkable_spec_sets_to_merge.append(
                LinkableSpecSet.create_from_specs(non_additive_dimension_spec.linkable_specs)
            )

        extraneous_linkable_specs = LinkableSpecSet.merge_iterable(linkable_spec_sets_to_merge).dedupe()
        required_linkable_specs = queried_linkable_specs.merge(extraneous_linkable_specs).dedupe()

        return required_linkable_specs, extraneous_linkable_specs

    def _build_aggregated_measure_from_measure_source_node(
        self,
        metric_input_measure_spec: MetricInputMeasureSpec,
        queried_linkable_specs: LinkableSpecSet,
        predicate_pushdown_state: PredicatePushdownState,
        measure_recipe: Optional[DataflowRecipe] = None,
    ) -> DataflowPlanNode:
        measure_spec = metric_input_measure_spec.measure_spec
        cumulative = metric_input_measure_spec.cumulative_description is not None
        cumulative_window = (
            metric_input_measure_spec.cumulative_description.cumulative_window
            if metric_input_measure_spec.cumulative_description is not None
            else None
        )
        cumulative_grain_to_date = (
            metric_input_measure_spec.cumulative_description.cumulative_grain_to_date
            if metric_input_measure_spec.cumulative_description
            else None
        )
        measure_properties = self._build_measure_spec_properties([measure_spec])
        non_additive_dimension_spec = measure_properties.non_additive_dimension_spec

        cumulative_metric_adjusted_time_constraint: Optional[TimeRangeConstraint] = None
        if cumulative and predicate_pushdown_state.time_range_constraint is not None:
            logger.info(f"Time range constraint before adjustment is {predicate_pushdown_state.time_range_constraint}")
            granularity: Optional[TimeGranularity] = None
            count = 0
            if cumulative_window is not None:
                granularity = cumulative_window.granularity
                count = cumulative_window.count
            elif cumulative_grain_to_date is not None:
                count = 1
                granularity = cumulative_grain_to_date

            cumulative_metric_adjusted_time_constraint = (
                self._time_period_adjuster.expand_time_constraint_for_cumulative_metric(
                    predicate_pushdown_state.time_range_constraint, granularity, count
                )
            )
            logger.info(f"Adjusted time range constraint to: {cumulative_metric_adjusted_time_constraint}")

        required_linkable_specs, extraneous_linkable_specs = self.__get_required_and_extraneous_linkable_specs(
            queried_linkable_specs=queried_linkable_specs,
            filter_specs=metric_input_measure_spec.filter_specs,
            non_additive_dimension_spec=non_additive_dimension_spec,
        )

        before_aggregation_time_spine_join_description = (
            metric_input_measure_spec.before_aggregation_time_spine_join_description
        )

        if measure_recipe is None:
            logger.info(
                "Looking for a recipe to get:"
                + indent(f"\nmeasure_specs:\n{mf_pformat([measure_spec])}")
                + indent(f"\nevaluation:\n{mf_pformat(required_linkable_specs)}")
            )
            measure_time_constraint = (
                (cumulative_metric_adjusted_time_constraint or predicate_pushdown_state.time_range_constraint)
                # If joining to time spine for time offset, constraints will be applied after that join.
                if not before_aggregation_time_spine_join_description
                else None
            )
            if measure_time_constraint is None:
                measure_pushdown_state = PredicatePushdownState.without_time_range_constraint(predicate_pushdown_state)
            else:
                measure_pushdown_state = PredicatePushdownState.with_time_range_constraint(
                    predicate_pushdown_state, time_range_constraint=measure_time_constraint
                )

            find_recipe_start_time = time.time()
            measure_recipe = self._find_dataflow_recipe(
                measure_spec_properties=measure_properties,
                predicate_pushdown_state=measure_pushdown_state,
                linkable_spec_set=required_linkable_specs,
            )
            logger.info(
                f"With {len(self._source_node_set.source_nodes_for_metric_queries)} source nodes, finding a recipe "
                f"took {time.time() - find_recipe_start_time:.2f}s"
            )

        logger.info(f"Using recipe:\n{indent(mf_pformat(measure_recipe))}")

        if measure_recipe is None:
            raise UnableToSatisfyQueryError(
                f"Unable to join all items in request. Measure: {measure_spec.element_name}; Specs to join: {required_linkable_specs}"
            )

        queried_agg_time_dimension_specs = queried_linkable_specs.included_agg_time_dimension_specs_for_measure(
            measure_reference=measure_spec.reference, semantic_model_lookup=self._semantic_model_lookup
        )

        # If a cumulative metric is queried with metric_time, join over time range.
        # Otherwise, the measure will be aggregated over all time.
        time_range_node: Optional[JoinOverTimeRangeNode] = None
        if cumulative and queried_agg_time_dimension_specs:
            # Use the time dimension spec with the smallest granularity.
            agg_time_dimension_spec_for_join = sorted(
                queried_agg_time_dimension_specs, key=lambda spec: spec.time_granularity.to_int()
            )[0]
            time_range_node = JoinOverTimeRangeNode(
                parent_node=measure_recipe.source_node,
                time_dimension_spec_for_join=agg_time_dimension_spec_for_join,
                window=cumulative_window,
                grain_to_date=cumulative_grain_to_date,
                # Note: we use the original constraint here because the JoinOverTimeRangeNode will eventually get
                # rendered with an interval that expands the join window
                time_range_constraint=(
                    predicate_pushdown_state.time_range_constraint
                    if not before_aggregation_time_spine_join_description
                    else None
                ),
            )

        # If querying an offset metric, join to time spine before aggregation.
        join_to_time_spine_node: Optional[JoinToTimeSpineNode] = None
        if before_aggregation_time_spine_join_description is not None:
            assert queried_agg_time_dimension_specs, (
                "Joining to time spine requires querying with metric time or the appropriate agg_time_dimension."
                "This should have been caught by validations."
            )
            assert before_aggregation_time_spine_join_description.join_type is SqlJoinType.INNER, (
                f"Expected {SqlJoinType.INNER} for joining to time spine before aggregation. Remove this if there's a "
                f"new use case."
            )
            # This also uses the original time range constraint due to the application of the time window intervals
            # in join rendering
            join_to_time_spine_node = JoinToTimeSpineNode(
                parent_node=time_range_node or measure_recipe.source_node,
                requested_agg_time_dimension_specs=queried_agg_time_dimension_specs,
                use_custom_agg_time_dimension=not queried_linkable_specs.contains_metric_time,
                time_range_constraint=predicate_pushdown_state.time_range_constraint,
                offset_window=before_aggregation_time_spine_join_description.offset_window,
                offset_to_grain=before_aggregation_time_spine_join_description.offset_to_grain,
                join_type=before_aggregation_time_spine_join_description.join_type,
            )

        # Only get the required measure and the local linkable instances so that aggregations work correctly.
        filtered_measure_source_node = FilterElementsNode(
            parent_node=join_to_time_spine_node or time_range_node or measure_recipe.source_node,
            include_specs=InstanceSpecSet(measure_specs=(measure_spec,)).merge(
                group_specs_by_type(measure_recipe.required_local_linkable_specs),
            ),
        )

        join_targets = measure_recipe.join_targets
        unaggregated_measure_node: DataflowPlanNode
        if len(join_targets) > 0:
            filtered_measures_with_joined_elements = JoinOnEntitiesNode(
                left_node=filtered_measure_source_node,
                join_targets=join_targets,
            )

            specs_to_keep_after_join = InstanceSpecSet(measure_specs=(measure_spec,)).merge(
                required_linkable_specs.as_spec_set,
            )

            after_join_filtered_node = FilterElementsNode(
                parent_node=filtered_measures_with_joined_elements, include_specs=specs_to_keep_after_join
            )
            unaggregated_measure_node = after_join_filtered_node
        else:
            unaggregated_measure_node = filtered_measure_source_node

        # If time constraint was previously adjusted for cumulative window or grain, apply original time constraint
        # here. Can skip if metric is being aggregated over all time.
        cumulative_metric_constrained_node: Optional[ConstrainTimeRangeNode] = None
        # TODO - Pushdown: Encapsulate all of this window sliding bookkeeping in the pushdown params object
        if (
            cumulative_metric_adjusted_time_constraint is not None
            and predicate_pushdown_state.time_range_constraint is not None
        ):
            assert (
                queried_linkable_specs.contains_metric_time
            ), "Using time constraints currently requires querying with metric_time."
            cumulative_metric_constrained_node = ConstrainTimeRangeNode(
                unaggregated_measure_node, predicate_pushdown_state.time_range_constraint
            )

        pre_aggregate_node: DataflowPlanNode = cumulative_metric_constrained_node or unaggregated_measure_node
        merged_where_filter_spec = WhereFilterSpec.merge_iterable(metric_input_measure_spec.filter_specs)
        if len(metric_input_measure_spec.filter_specs) > 0:
            # Apply where constraint on the node
            pre_aggregate_node = WhereConstraintNode(
                parent_node=pre_aggregate_node,
                where_constraint=merged_where_filter_spec,
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
            # At this point, it's the case that there are extraneous specs are not a subset of the queried
            # linkable specs. A filter is needed after, say, a where clause so that the linkable specs in the where clause don't
            # show up in the final result.
            #
            # e.g. for "bookings" by "ds" where "is_instant", "is_instant" should not be in the results.
            pre_aggregate_node = FilterElementsNode(
                parent_node=pre_aggregate_node,
                include_specs=InstanceSpecSet(measure_specs=(measure_spec,)).merge(queried_linkable_specs.as_spec_set),
            )

        aggregate_measures_node = AggregateMeasuresNode(
            parent_node=pre_aggregate_node,
            metric_input_measure_specs=(metric_input_measure_spec,),
        )

        # Joining to time spine after aggregation is for measures that specify `join_to_timespine` in the YAML spec.
        after_aggregation_time_spine_join_description = (
            metric_input_measure_spec.after_aggregation_time_spine_join_description
        )
        if after_aggregation_time_spine_join_description is not None:
            assert after_aggregation_time_spine_join_description.join_type is SqlJoinType.LEFT_OUTER, (
                f"Expected {SqlJoinType.LEFT_OUTER} for joining to time spine after aggregation. Remove this if "
                f"there's a new use case."
            )
            output_node: DataflowPlanNode = JoinToTimeSpineNode(
                parent_node=aggregate_measures_node,
                requested_agg_time_dimension_specs=queried_agg_time_dimension_specs,
                use_custom_agg_time_dimension=not queried_linkable_specs.contains_metric_time,
                join_type=after_aggregation_time_spine_join_description.join_type,
                time_range_constraint=predicate_pushdown_state.time_range_constraint,
                offset_window=after_aggregation_time_spine_join_description.offset_window,
                offset_to_grain=after_aggregation_time_spine_join_description.offset_to_grain,
            )

            # Since new rows might have been added due to time spine join, re-apply constraints here. Only re-apply filters
            # for specs that are also in the queried specs, since those are the only ones that might have changed after the
            # time spine join.
            queried_filter_specs = [
                filter_spec
                for filter_spec in metric_input_measure_spec.filter_specs
                if set(filter_spec.linkable_specs).issubset(set(queried_linkable_specs.as_tuple))
            ]
            if len(queried_filter_specs) > 0:
                output_node = WhereConstraintNode(
                    parent_node=output_node, where_constraint=WhereFilterSpec.merge_iterable(queried_filter_specs)
                )

            # TODO: this will break if you query by agg_time_dimension but apply a time constraint on metric_time.
            # To fix when enabling time range constraints for agg_time_dimension.
            if queried_agg_time_dimension_specs and predicate_pushdown_state.time_range_constraint is not None:
                output_node = ConstrainTimeRangeNode(
                    parent_node=output_node, time_range_constraint=predicate_pushdown_state.time_range_constraint
                )
            return output_node

        return aggregate_measures_node
