from __future__ import annotations

import copy
import logging
import time
from typing import Dict, FrozenSet, List, Optional, Sequence, Set, Tuple, Union

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
from dbt_semantic_interfaces.references import MeasureReference, MetricReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from dbt_semantic_interfaces.validations.unique_valid_name import MetricFlowReservedKeywords
from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.errors.custom_grain_not_supported import error_if_not_standard_grain
from metricflow_semantics.errors.error_classes import UnableToSatisfyQueryError
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.mf_logging.formatting import indent
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.mf_logging.runtime import log_runtime
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolutionLookUp,
)
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.constant_property_spec import ConstantPropertySpec
from metricflow_semantics.specs.entity_spec import EntitySpec, LinklessEntitySpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.measure_spec import (
    CumulativeMeasureDescription,
    JoinToTimeSpineDescription,
    MeasureSpec,
    MetricInputMeasureSpec,
)
from metricflow_semantics.specs.metadata_spec import MetadataSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.non_additive_dimension_spec import NonAdditiveDimensionSpec
from metricflow_semantics.specs.order_by_spec import OrderBySpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet, group_specs_by_type
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.specs.where_filter.where_filter_spec_set import WhereFilterSpecSet
from metricflow_semantics.specs.where_filter.where_filter_transform import WhereSpecFactory
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.time.dateutil_adjuster import DateutilTimePeriodAdjuster
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.time.time_spine_source import TimeSpineSource

from metricflow.dataflow.builder.builder_cache import (
    BuildAnyMetricOutputNodeParameterSet,
    DataflowPlanBuilderCache,
    FindSourceNodeRecipeParameterSet,
    FindSourceNodeRecipeResult,
)
from metricflow.dataflow.builder.measure_spec_properties import MeasureSpecProperties
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.node_evaluator import (
    LinkableInstanceSatisfiabilityEvaluation,
    NodeEvaluatorForLinkableInstances,
)
from metricflow.dataflow.builder.source_node import SourceNodeBuilder, SourceNodeSet
from metricflow.dataflow.builder.source_node_recipe import SourceNodeRecipe
from metricflow.dataflow.dataflow_plan import (
    DataflowPlan,
    DataflowPlanNode,
)
from metricflow.dataflow.nodes.add_generated_uuid import AddGeneratedUuidColumnNode
from metricflow.dataflow.nodes.aggregate_measures import AggregateMeasuresNode
from metricflow.dataflow.nodes.alias_specs import AliasSpecsNode, SpecToAlias
from metricflow.dataflow.nodes.combine_aggregated_outputs import CombineAggregatedOutputsNode
from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode
from metricflow.dataflow.nodes.constrain_time import ConstrainTimeRangeNode
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.dataflow.nodes.join_conversion_events import JoinConversionEventsNode
from metricflow.dataflow.nodes.join_over_time import JoinOverTimeRangeNode
from metricflow.dataflow.nodes.join_to_base import JoinDescription, JoinOnEntitiesNode
from metricflow.dataflow.nodes.join_to_custom_granularity import JoinToCustomGranularityNode
from metricflow.dataflow.nodes.join_to_time_spine import JoinToTimeSpineNode
from metricflow.dataflow.nodes.metric_time_transform import MetricTimeDimensionTransformNode
from metricflow.dataflow.nodes.min_max import MinMaxNode
from metricflow.dataflow.nodes.order_by_limit import OrderByLimitNode
from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
from metricflow.dataflow.nodes.semi_additive_join import SemiAdditiveJoinNode
from metricflow.dataflow.nodes.where_filter import WhereConstraintNode
from metricflow.dataflow.nodes.window_reaggregation_node import WindowReaggregationNode
from metricflow.dataflow.nodes.write_to_data_table import WriteToResultDataTableNode
from metricflow.dataflow.nodes.write_to_table import WriteToResultTableNode
from metricflow.dataflow.optimizer.dataflow_optimizer_factory import (
    DataflowPlanOptimization,
    DataflowPlanOptimizerFactory,
)
from metricflow.dataset.dataset_classes import DataSet
from metricflow.plan_conversion.node_processor import (
    PredicateInputType,
    PredicatePushdownState,
    PreJoinNodeProcessor,
)

logger = logging.getLogger(__name__)


class DataflowPlanBuilder:
    """Builds a dataflow plan to satisfy a given query."""

    def __init__(  # noqa: D107
        self,
        source_node_set: SourceNodeSet,
        semantic_manifest_lookup: SemanticManifestLookup,
        node_output_resolver: DataflowPlanNodeOutputDataSetResolver,
        column_association_resolver: ColumnAssociationResolver,
        source_node_builder: SourceNodeBuilder,
        dataflow_plan_builder_cache: Optional[DataflowPlanBuilderCache] = None,
    ) -> None:
        self._semantic_model_lookup = semantic_manifest_lookup.semantic_model_lookup
        self._metric_lookup = semantic_manifest_lookup.metric_lookup
        self._metric_time_dimension_reference = DataSet.metric_time_dimension_reference()
        self._source_node_set = source_node_set
        self._column_association_resolver = column_association_resolver
        self._node_data_set_resolver = node_output_resolver
        self._source_node_builder = source_node_builder
        self._time_period_adjuster = DateutilTimePeriodAdjuster()
        self._cache = dataflow_plan_builder_cache or DataflowPlanBuilderCache()

    def build_plan(
        self,
        query_spec: MetricFlowQuerySpec,
        output_sql_table: Optional[SqlTable] = None,
        output_selection_specs: Optional[InstanceSpecSet] = None,
        optimizations: FrozenSet[DataflowPlanOptimization] = frozenset(),
    ) -> DataflowPlan:
        """Generate a plan for reading the results of a query with the given spec into a data_table or table."""
        # Workaround for a Pycharm type inspection issue with decorators.
        # noinspection PyArgumentList
        return self._build_plan(
            query_spec=query_spec,
            output_sql_table=output_sql_table,
            output_selection_specs=output_selection_specs,
            optimizations=optimizations,
        )

    def _build_query_output_node(
        self, query_spec: MetricFlowQuerySpec, for_group_by_source_node: bool = False
    ) -> DataflowPlanNode:
        """Build SQL output node from query inputs. May be used to build query DFP or source node."""
        metric_specs: Tuple[MetricSpec, ...] = ()
        for metric_spec in query_spec.metric_specs:
            if (
                len(metric_spec.filter_spec_set.all_filter_specs) > 0
                or metric_spec.offset_to_grain is not None
                or metric_spec.offset_window is not None
            ):
                # Remove aliases here. They will be added back at the very end of the query.
                metric_specs += (metric_spec.with_alias(None),) if metric_spec.alias else (metric_spec,)
                raise ValueError(
                    f"The metric specs in the query spec should not contain any metric modifiers. Got: {metric_spec}"
                )

        filter_spec_factory = WhereSpecFactory(
            column_association_resolver=self._column_association_resolver,
            spec_resolution_lookup=query_spec.filter_spec_resolution_lookup,
            semantic_model_lookup=self._semantic_model_lookup,
        )

        query_level_filter_specs = tuple(
            filter_spec_factory.create_from_where_filter_intersection(
                filter_location=WhereFilterLocation.for_query(
                    tuple(metric_spec.reference for metric_spec in query_spec.metric_specs)
                ),
                filter_intersection=query_spec.filter_intersection,
            )
        )

        predicate_pushdown_state = PredicatePushdownState(
            time_range_constraint=query_spec.time_range_constraint,
            where_filter_specs=(),
            pushdown_enabled_types=frozenset({PredicateInputType.TIME_RANGE_CONSTRAINT}),
        )
        return self._build_metrics_output_node(
            metric_specs=tuple(
                MetricSpec(
                    element_name=metric_spec.element_name,
                    filter_spec_set=WhereFilterSpecSet(query_level_filter_specs=query_level_filter_specs),
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
        optimizations: FrozenSet[DataflowPlanOptimization],
    ) -> DataflowPlan:
        metrics_output_node = self._build_query_output_node(query_spec=query_spec)

        sink_node = DataflowPlanBuilder.build_sink_node(
            parent_node=metrics_output_node,
            metric_specs=query_spec.metric_specs,
            order_by_specs=query_spec.order_by_specs,
            output_sql_table=output_sql_table,
            limit=query_spec.limit,
            output_selection_specs=output_selection_specs,
        )

        plan_id = DagId.from_id_prefix(StaticIdPrefix.DATAFLOW_PLAN_PREFIX)
        plan = DataflowPlan(sink_nodes=[sink_node], plan_id=plan_id)
        return self._optimize_plan(plan, optimizations)

    def _optimize_plan(self, plan: DataflowPlan, optimizations: FrozenSet[DataflowPlanOptimization]) -> DataflowPlan:
        optimizer_factory = DataflowPlanOptimizerFactory(self._node_data_set_resolver)
        for optimizer in optimizer_factory.get_optimizers(optimizations):
            logger.debug(LazyFormat(lambda: f"Applying optimizer: {optimizer.__class__.__name__}"))
            try:
                plan = optimizer.optimize(plan)
                logger.debug(
                    LazyFormat(
                        lambda: f"After applying optimizer {optimizer.__class__.__name__}, the dataflow plan is:\n"
                        f"{indent(plan.structure_text())}"
                    )
                )
            except Exception:
                logger.exception(f"Got an exception applying {optimizer.__class__.__name__}")

        return plan

    def _get_minimum_metric_time_spec_for_metric(self, metric_reference: MetricReference) -> TimeDimensionSpec:
        """Gets the minimum metric time spec for the given metric reference."""
        min_granularity = ExpandedTimeGranularity.from_time_granularity(
            self._metric_lookup.get_min_queryable_time_granularity(metric_reference)
        )
        return DataSet.metric_time_dimension_spec(min_granularity)

    def _build_aggregated_conversion_node(
        self,
        metric_spec: MetricSpec,
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
            where_filter_specs=tuple(),
            pushdown_enabled_types=frozenset([PredicateInputType.TIME_RANGE_CONSTRAINT]),
        )

        # Build measure recipes
        base_required_linkable_specs = self.__get_required_linkable_specs(
            queried_linkable_specs=queried_linkable_specs,
            filter_specs=base_measure_spec.filter_spec_set.all_filter_specs,
        )
        base_measure_recipe = self._find_source_node_recipe(
            FindSourceNodeRecipeParameterSet(
                measure_spec_properties=self._build_measure_spec_properties([base_measure_spec.measure_spec]),
                predicate_pushdown_state=time_range_only_pushdown_state,
                linkable_spec_set=base_required_linkable_specs,
            )
        )
        logger.debug(LazyFormat(lambda: f"Recipe for base measure aggregation:\n{mf_pformat(base_measure_recipe)}"))
        conversion_measure_recipe = self._find_source_node_recipe(
            FindSourceNodeRecipeParameterSet(
                measure_spec_properties=self._build_measure_spec_properties([conversion_measure_spec.measure_spec]),
                predicate_pushdown_state=disabled_pushdown_state,
                linkable_spec_set=LinkableSpecSet(),
            )
        )
        logger.debug(
            LazyFormat(lambda: f"Recipe for conversion measure aggregation:\n{mf_pformat(conversion_measure_recipe)}")
        )
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
        unaggregated_conversion_measure_node = AddGeneratedUuidColumnNode.create(
            parent_node=conversion_measure_recipe.source_node
        )

        # Get the time dimension used to calculate the conversion window
        # Currently, both the base/conversion measure uses metric_time as it's the default agg time dimension.
        # However, eventually, there can be user-specified time dimensions used for this calculation.
        min_metric_time_spec = self._get_minimum_metric_time_spec_for_metric(metric_spec.reference)

        # Filter the source nodes with only the required specs needed for the calculation
        constant_property_specs = []
        required_local_specs = [base_measure_spec.measure_spec, entity_spec, min_metric_time_spec] + list(
            base_measure_recipe.required_local_linkable_specs.as_tuple
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
        unaggregated_base_measure_node = self._build_pre_aggregation_plan(
            source_node=base_measure_recipe.source_node,
            join_targets=base_measure_recipe.join_targets,
            filter_to_specs=group_specs_by_type(required_local_specs)
            .merge(base_required_linkable_specs.as_instance_spec_set)
            .dedupe(),
            custom_granularity_specs=base_required_linkable_specs.time_dimension_specs_with_custom_grain,
            where_filter_specs=base_measure_spec.filter_spec_set.all_filter_specs,
        )

        # Gets the successful conversions using JoinConversionEventsNode
        # The conversion events are joined by the base events which are already time constrained. However, this could
        # be still be constrained, where we adjust the time range to the window size similar to cumulative, but
        # adjusted in the opposite direction.
        join_conversion_node = JoinConversionEventsNode.create(
            base_node=unaggregated_base_measure_node,
            base_time_dimension_spec=min_metric_time_spec,
            conversion_node=unaggregated_conversion_measure_node,
            conversion_measure_spec=conversion_measure_spec.measure_spec,
            conversion_time_dimension_spec=min_metric_time_spec,
            unique_identifier_keys=(MetadataSpec(MetricFlowReservedKeywords.MF_INTERNAL_UUID.value),),
            entity_spec=entity_spec,
            window=window,
            constant_properties=constant_property_specs,
        )

        # Aggregate the conversion events with the JoinConversionEventsNode as the source node.
        recipe_with_join_conversion_source_node = SourceNodeRecipe(
            source_node=join_conversion_node,
            required_local_linkable_specs=queried_linkable_specs,
            join_linkable_instances_recipes=(),
            all_linkable_specs_required_for_source_nodes=queried_linkable_specs,
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
        return CombineAggregatedOutputsNode.create(
            parent_nodes=(aggregated_base_measure_node, aggregated_conversions_node)
        )

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
            descendent_filter_spec_set=metric_spec.filter_spec_set,
            queried_linkable_specs=queried_linkable_specs,
        )
        # TODO: [custom granularity] change this to an assertion once we're sure there aren't exceptions
        if not StructuredLinkableSpecName.from_name(
            qualified_name=conversion_type_params.entity, custom_granularity_names=()
        ).is_element_name:
            logger.warning(
                LazyFormat(
                    lambda: f"Found additional annotations in type param entity name `{conversion_type_params.entity}`, which "
                    "should be a simple element name reference. This should have been blocked by model validation!"
                )
            )
        entity_spec = EntitySpec(element_name=conversion_type_params.entity, entity_links=())
        logger.debug(
            LazyFormat(
                lambda: f"For conversion metric {metric_spec},\n"
                f"base_measure is:\n{mf_pformat(base_measure)}\n"
                f"conversion_measure is:\n{mf_pformat(conversion_measure)}\n"
                f"entity is:\n{mf_pformat(entity_spec)}"
            )
        )

        aggregated_measures_node = self._build_aggregated_conversion_node(
            metric_spec=metric_spec,
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

    def _build_cumulative_metric_output_node(
        self,
        metric_spec: MetricSpec,
        queried_linkable_specs: LinkableSpecSet,
        filter_spec_factory: WhereSpecFactory,
        predicate_pushdown_state: PredicatePushdownState,
        for_group_by_source_node: bool = False,
    ) -> DataflowPlanNode:
        min_metric_time_spec = self._get_minimum_metric_time_spec_for_metric(metric_spec.reference)
        min_granularity = min_metric_time_spec.time_granularity

        queried_agg_time_dimensions = queried_linkable_specs.included_agg_time_dimension_specs_for_metric(
            metric_reference=metric_spec.reference, metric_lookup=self._metric_lookup
        )
        query_includes_agg_time_dimension_with_min_granularity = False
        for time_dimension_spec in queried_agg_time_dimensions:
            if time_dimension_spec.time_granularity == min_granularity:
                query_includes_agg_time_dimension_with_min_granularity = True
                break

        if query_includes_agg_time_dimension_with_min_granularity or len(queried_agg_time_dimensions) == 0:
            return self._build_base_metric_output_node(
                metric_spec=metric_spec,
                queried_linkable_specs=queried_linkable_specs,
                filter_spec_factory=filter_spec_factory,
                predicate_pushdown_state=predicate_pushdown_state,
                for_group_by_source_node=for_group_by_source_node,
            )

        # If a cumulative metric is queried without its minimum granularity, it will need to be aggregated twice:
        # once as a normal metric, and again using a window function to narrow down to one row per granularity period.
        # In this case, add metric time at the default granularity to the linkable specs. It will be used in the order by
        # clause of the window function and later excluded from the output selections.
        include_linkable_specs = queried_linkable_specs.add_specs(time_dimension_specs=(min_metric_time_spec,))
        compute_metrics_node = self._build_base_metric_output_node(
            metric_spec=metric_spec,
            queried_linkable_specs=include_linkable_specs,
            filter_spec_factory=filter_spec_factory,
            predicate_pushdown_state=predicate_pushdown_state,
            for_group_by_source_node=for_group_by_source_node,
        )
        return WindowReaggregationNode.create(
            parent_node=compute_metrics_node,
            metric_spec=metric_spec,
            order_by_spec=min_metric_time_spec,
            partition_by_specs=queried_linkable_specs.as_tuple,
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

        cumulative_grain_to_date: Optional[TimeGranularity] = None
        if (
            metric.type_params.cumulative_type_params
            and metric.type_params.cumulative_type_params.grain_to_date is not None
        ):
            cumulative_grain_to_date = error_if_not_standard_grain(
                context=f"CumulativeMetric({metric_spec.element_name}).grain_to_date",
                input_granularity=metric.type_params.cumulative_type_params.grain_to_date,
            )

        metric_input_measure_spec = self._build_input_measure_spec(
            filter_spec_factory=filter_spec_factory,
            metric=metric,
            input_measure=metric.input_measures[0],
            queried_linkable_specs=queried_linkable_specs,
            child_metric_offset_window=metric_spec.offset_window,
            child_metric_offset_to_grain=metric_spec.offset_to_grain,
            cumulative_description=(
                CumulativeMeasureDescription(
                    cumulative_window=(
                        metric.type_params.cumulative_type_params.window
                        if metric.type_params.cumulative_type_params
                        else None
                    ),
                    cumulative_grain_to_date=cumulative_grain_to_date,
                )
                if metric.type is MetricType.CUMULATIVE
                else None
            ),
            descendent_filter_spec_set=metric_spec.filter_spec_set,
        )
        logger.debug(
            LazyFormat(
                lambda: f"For\n{indent(mf_pformat(metric_spec))}"
                f"\nneeded measure is:"
                f"\n{indent(mf_pformat(metric_input_measure_spec))}"
            )
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
        logger.debug(
            LazyFormat(
                lambda: f"For {metric.type} metric: {metric_spec}, needed metrics are:\n"
                f"{mf_pformat(metric_input_specs)}"
            )
        )

        required_linkable_specs = self.__get_required_linkable_specs(
            queried_linkable_specs=queried_linkable_specs, filter_specs=metric_spec.filter_spec_set.all_filter_specs
        )

        parent_nodes: List[DataflowPlanNode] = []

        # This is the filter that's defined for the metric in the configs.
        metric_definition_filter_specs = filter_spec_factory.create_from_where_filter_intersection(
            filter_location=WhereFilterLocation.for_metric(metric_spec.reference),
            filter_intersection=metric.filter,
        )

        for metric_input_spec in metric_input_specs:
            where_filter_spec_set = WhereFilterSpecSet(
                metric_level_filter_specs=tuple(metric_definition_filter_specs),
            )

            # These are the filters that's defined as part of the input metric.
            where_filter_spec_set = where_filter_spec_set.merge(metric_input_spec.filter_spec_set)

            # If metric is offset, we'll apply where constraint after offset to avoid removing values
            # unexpectedly. Time constraint will be applied by INNER JOINing to time spine.
            # We may consider encapsulating this in pushdown state later, but as of this moment pushdown
            # is about post-join to pre-join for dimension access, and relies on the builder to collect
            # predicates from query and metric specs and make them available at measure level.
            if not metric_spec.has_time_offset:
                where_filter_spec_set = where_filter_spec_set.merge(metric_spec.filter_spec_set)
            metric_pushdown_state = (
                predicate_pushdown_state
                if not metric_spec.has_time_offset
                else PredicatePushdownState.with_pushdown_disabled()
            )

            parent_nodes.append(
                self._build_any_metric_output_node(
                    BuildAnyMetricOutputNodeParameterSet(
                        metric_spec=MetricSpec(
                            element_name=metric_input_spec.element_name,
                            filter_spec_set=where_filter_spec_set,
                            alias=metric_input_spec.alias,
                            offset_window=metric_input_spec.offset_window,
                            offset_to_grain=metric_input_spec.offset_to_grain,
                        ),
                        queried_linkable_specs=(
                            queried_linkable_specs if not metric_spec.has_time_offset else required_linkable_specs
                        ),
                        filter_spec_factory=filter_spec_factory,
                        predicate_pushdown_state=metric_pushdown_state,
                        for_group_by_source_node=False,
                    )
                )
            )

        parent_node = (
            parent_nodes[0]
            if len(parent_nodes) == 1
            else CombineAggregatedOutputsNode.create(parent_nodes=parent_nodes)
        )
        output_node: DataflowPlanNode = ComputeMetricsNode.create(
            parent_node=parent_node,
            metric_specs=[metric_spec],
            for_group_by_source_node=for_group_by_source_node,
            aggregated_to_elements=set(queried_linkable_specs.as_tuple),
        )

        # For ratio / derived metrics with time offset, apply offset join here. Constraints will be applied after the offset
        # to avoid filtering out values that will be changed.
        queried_agg_time_dimension_specs = queried_linkable_specs.included_agg_time_dimension_specs_for_metric(
            metric_reference=metric_spec.reference, metric_lookup=self._metric_lookup
        )
        if metric_spec.has_time_offset and queried_agg_time_dimension_specs:
            # TODO: move this to a helper method
            time_spine_node = self._build_time_spine_node(queried_agg_time_dimension_specs)
            output_node = JoinToTimeSpineNode.create(
                metric_source_node=output_node,
                time_spine_node=time_spine_node,
                requested_agg_time_dimension_specs=queried_agg_time_dimension_specs,
                join_on_time_dimension_spec=self._sort_by_base_granularity(queried_agg_time_dimension_specs)[0],
                offset_window=metric_spec.offset_window,
                offset_to_grain=metric_spec.offset_to_grain,
                join_type=SqlJoinType.INNER,
            )

            # TODO: fix bug here where filter specs are being included in when aggregating.
            if len(metric_spec.filter_spec_set.all_filter_specs) > 0 or predicate_pushdown_state.time_range_constraint:
                # FilterElementsNode will only be needed if there are where filter specs that were selected in the group by.
                specs_in_filters = set(
                    linkable_spec
                    for filter_spec in metric_spec.filter_spec_set.all_filter_specs
                    for linkable_spec in filter_spec.linkable_specs
                )
                filter_to_specs = None
                if not specs_in_filters.issubset(queried_linkable_specs.as_tuple):
                    filter_to_specs = InstanceSpecSet(metric_specs=(metric_spec,)).merge(
                        InstanceSpecSet.create_from_specs(queried_linkable_specs.as_tuple)
                    )
                output_node = self._build_pre_aggregation_plan(
                    source_node=output_node,
                    where_filter_specs=metric_spec.filter_spec_set.all_filter_specs,
                    time_range_constraint=predicate_pushdown_state.time_range_constraint,
                    filter_to_specs=filter_to_specs,
                )

        return output_node

    def _get_base_agg_time_dimensions(
        self, queried_linkable_specs: LinkableSpecSet, measure_reference: MeasureReference
    ) -> Tuple[TimeDimensionSpec, ...]:
        """Get queried agg_time_dimensios with their base grains, deduped.

        Custom grains are joined right before measure aggregation and after all other pre-aggregation joins,
        so only base grains are needed prior to that point.
        """
        queried_agg_time_dimension_specs = queried_linkable_specs.included_agg_time_dimension_specs_for_measure(
            measure_reference=measure_reference, semantic_model_lookup=self._semantic_model_lookup
        )
        return TimeDimensionSpec.with_base_grains(queried_agg_time_dimension_specs)

    def _build_any_metric_output_node(self, parameter_set: BuildAnyMetricOutputNodeParameterSet) -> DataflowPlanNode:
        """Builds a node to compute a metric of any type."""
        result = self._cache.get_build_any_metric_output_node_result(parameter_set)
        if result is not None:
            return result

        result = self._build_any_metric_output_node_non_cached(parameter_set)
        self._cache.set_build_any_metric_output_node_result(parameter_set, result)
        return result

    def _build_any_metric_output_node_non_cached(
        self, parameter_set: BuildAnyMetricOutputNodeParameterSet
    ) -> DataflowPlanNode:
        """Builds a node to compute a metric of any type."""
        metric_spec = parameter_set.metric_spec
        queried_linkable_specs = parameter_set.queried_linkable_specs
        filter_spec_factory = parameter_set.filter_spec_factory
        predicate_pushdown_state = parameter_set.predicate_pushdown_state
        for_group_by_source_node = parameter_set.for_group_by_source_node

        metric = self._metric_lookup.get_metric(metric_spec.reference)

        if metric.type is MetricType.SIMPLE:
            return self._build_base_metric_output_node(
                metric_spec=metric_spec,
                queried_linkable_specs=queried_linkable_specs,
                filter_spec_factory=filter_spec_factory,
                predicate_pushdown_state=predicate_pushdown_state,
                for_group_by_source_node=for_group_by_source_node,
            )

        elif metric.type is MetricType.CUMULATIVE:
            return self._build_cumulative_metric_output_node(
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
            logger.debug(LazyFormat(lambda: f"Generating compute metrics node for:\n{indent(mf_pformat(metric_spec))}"))
            self._metric_lookup.get_metric(metric_spec.reference)

            output_nodes.append(
                self._build_any_metric_output_node(
                    BuildAnyMetricOutputNodeParameterSet(
                        metric_spec=metric_spec,
                        queried_linkable_specs=queried_linkable_specs,
                        filter_spec_factory=filter_spec_factory,
                        predicate_pushdown_state=predicate_pushdown_state,
                        for_group_by_source_node=for_group_by_source_node,
                    )
                )
            )

        assert len(output_nodes) > 0, "ComputeMetricsNode was not properly constructed"

        if len(output_nodes) == 1:
            return output_nodes[0]

        return CombineAggregatedOutputsNode.create(parent_nodes=output_nodes)

    def build_plan_for_distinct_values(
        self, query_spec: MetricFlowQuerySpec, optimizations: FrozenSet[DataflowPlanOptimization] = frozenset()
    ) -> DataflowPlan:
        """Generate a plan that would get the distinct values of a linkable instance.

        e.g. distinct listing__country_latest for bookings by listing__country_latest
        """
        # Workaround for a Pycharm type inspection issue with decorators.
        # noinspection PyArgumentList
        return self._build_plan_for_distinct_values(query_spec, optimizations=optimizations)

    @log_runtime()
    def _build_plan_for_distinct_values(
        self, query_spec: MetricFlowQuerySpec, optimizations: FrozenSet[DataflowPlanOptimization]
    ) -> DataflowPlan:
        assert not query_spec.metric_specs, "Can't build distinct values plan with metrics."
        query_level_filter_specs: Sequence[WhereFilterSpec] = ()
        if query_spec.filter_intersection is not None and len(query_spec.filter_intersection.where_filters) > 0:
            filter_spec_factory = WhereSpecFactory(
                column_association_resolver=self._column_association_resolver,
                spec_resolution_lookup=query_spec.filter_spec_resolution_lookup
                or FilterSpecResolutionLookUp.empty_instance(),
                semantic_model_lookup=self._semantic_model_lookup,
            )

            query_level_filter_specs = filter_spec_factory.create_from_where_filter_intersection(
                filter_location=WhereFilterLocation.for_query(metric_references=tuple()),
                filter_intersection=query_spec.filter_intersection,
            )

        required_linkable_specs = self.__get_required_linkable_specs(
            queried_linkable_specs=query_spec.linkable_specs, filter_specs=query_level_filter_specs
        )
        predicate_pushdown_state = PredicatePushdownState(
            time_range_constraint=query_spec.time_range_constraint, where_filter_specs=tuple(query_level_filter_specs)
        )
        dataflow_recipe = self._find_source_node_recipe(
            FindSourceNodeRecipeParameterSet(
                linkable_spec_set=required_linkable_specs,
                predicate_pushdown_state=predicate_pushdown_state,
                measure_spec_properties=None,
            )
        )
        if not dataflow_recipe:
            raise UnableToSatisfyQueryError(f"Unable to join all items in request: {required_linkable_specs}")

        output_node = self._build_pre_aggregation_plan(
            source_node=dataflow_recipe.source_node,
            join_targets=dataflow_recipe.join_targets,
            filter_to_specs=InstanceSpecSet.create_from_specs(query_spec.linkable_specs.as_tuple),
            custom_granularity_specs=required_linkable_specs.time_dimension_specs_with_custom_grain,
            where_filter_specs=query_level_filter_specs,
            time_range_constraint=query_spec.time_range_constraint,
            distinct=True,
        )

        if query_spec.min_max_only:
            output_node = MinMaxNode.create(parent_node=output_node)

        sink_node = self.build_sink_node(
            parent_node=output_node,
            metric_specs=query_spec.metric_specs,
            order_by_specs=query_spec.order_by_specs,
            limit=query_spec.limit,
        )

        plan = DataflowPlan(sink_nodes=[sink_node])
        return self._optimize_plan(plan, optimizations)

    @staticmethod
    def build_sink_node(
        parent_node: DataflowPlanNode,
        metric_specs: Sequence[MetricSpec],
        order_by_specs: Sequence[OrderBySpec],
        output_sql_table: Optional[SqlTable] = None,
        limit: Optional[int] = None,
        output_selection_specs: Optional[InstanceSpecSet] = None,
    ) -> DataflowPlanNode:
        """Adds order by / limit / write nodes."""
        pre_result_node: Optional[DataflowPlanNode] = None

        if order_by_specs or limit:
            pre_result_node = OrderByLimitNode.create(
                order_by_specs=list(order_by_specs), limit=limit, parent_node=parent_node
            )

        if output_selection_specs:
            pre_result_node = FilterElementsNode.create(
                parent_node=pre_result_node or parent_node, include_specs=output_selection_specs
            )

        alias_specs = tuple(
            SpecToAlias(MetricSpec(metric.element_name), MetricSpec(metric.element_name, alias=metric.alias))
            for metric in metric_specs
            if metric.alias is not None
        )
        if len(alias_specs) > 0:
            pre_result_node = AliasSpecsNode.create(
                parent_node=pre_result_node or parent_node, change_specs=alias_specs
            )

        write_result_node: DataflowPlanNode
        if not output_sql_table:
            write_result_node = WriteToResultDataTableNode.create(parent_node=pre_result_node or parent_node)
        else:
            write_result_node = WriteToResultTableNode.create(
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

        # TODO: Add support for no-metrics queries for custom grains without a join (i.e., select directly from time spine).
        linkable_specs_set = set(linkable_specs.as_tuple)
        for source_node in source_nodes:
            output_spec_set = self._node_data_set_resolver.get_output_data_set(source_node).instance_set.spec_set
            all_linkable_specs_in_node = set(output_spec_set.linkable_specs)
            requested_linkable_specs_in_node = linkable_specs_set.intersection(all_linkable_specs_in_node)
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
            self._semantic_model_lookup.measure_lookup.get_properties(
                measure.reference
            ).model_reference.semantic_model_name
            for measure in measure_specs
        }
        if len(semantic_model_names) > 1:
            raise ValueError(
                f"Cannot find common properties for measures {measure_specs} coming from multiple "
                f"semantic models: {semantic_model_names}. This suggests the measure_specs were not correctly filtered."
            )
        semantic_model_name = semantic_model_names.pop()

        measure_reference = measure_specs[0].reference
        measure_properties = self._semantic_model_lookup.measure_lookup.get_properties(measure_reference)
        agg_time_dimension = measure_properties.agg_time_dimension_reference
        agg_time_dimension_grain = measure_properties.agg_time_granularity

        non_additive_dimension_spec = measure_specs[0].non_additive_dimension_spec
        for measure_spec in measure_specs:
            if non_additive_dimension_spec != measure_spec.non_additive_dimension_spec:
                raise ValueError(f"measure_specs {measure_specs} do not have the same non_additive_dimension_spec.")
            measure_agg_time_dimension = self._semantic_model_lookup.measure_lookup.get_properties(
                measure_spec.reference
            ).agg_time_dimension_reference
            if measure_agg_time_dimension != agg_time_dimension:
                raise ValueError(f"measure_specs {measure_specs} do not have the same agg_time_dimension.")
        return MeasureSpecProperties(
            measure_specs=tuple(measure_specs),
            semantic_model_name=semantic_model_name,
            agg_time_dimension=agg_time_dimension,
            agg_time_dimension_grain=agg_time_dimension_grain,
            non_additive_dimension_spec=non_additive_dimension_spec,
        )

    def _find_source_node_recipe(self, parameter_set: FindSourceNodeRecipeParameterSet) -> Optional[SourceNodeRecipe]:
        """Find the most suitable source nodes to satisfy the requested specs, as well as how to join them."""
        result = self._cache.get_find_source_node_recipe_result(parameter_set)
        if result is not None:
            return result.source_node_recipe
        source_node_recipe = self._find_source_node_recipe_non_cached(parameter_set)
        self._cache.set_find_source_node_recipe_result(parameter_set, FindSourceNodeRecipeResult(source_node_recipe))
        return source_node_recipe

    def _find_source_node_recipe_non_cached(
        self, parameter_set: FindSourceNodeRecipeParameterSet
    ) -> Optional[SourceNodeRecipe]:
        linkable_spec_set = parameter_set.linkable_spec_set
        predicate_pushdown_state = parameter_set.predicate_pushdown_state
        measure_spec_properties = parameter_set.measure_spec_properties

        candidate_nodes_for_left_side_of_join: List[DataflowPlanNode] = []
        candidate_nodes_for_right_side_of_join: List[DataflowPlanNode] = []

        # Replace any custom granularities with their base granularities. The custom granularity will be joined in
        # later, since custom granularities cannot be satisfied by source nodes. But we will need the dimension at
        # base granularity from the source node in order to join to the appropriate time spine later.
        linkable_specs_to_satisfy = linkable_spec_set.replace_custom_granularity_with_base_granularity()
        linkable_specs_to_satisfy_tuple = linkable_specs_to_satisfy.as_tuple
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
                    linkable_specs=linkable_specs_to_satisfy,
                    source_nodes=self._source_node_set.source_nodes_for_group_by_item_queries,
                )
            )
            # If metric_time is requested without metrics, choose appropriate time spine node to select those values from.
            if linkable_specs_to_satisfy.metric_time_specs:
                time_spine_node = self._choose_time_spine_metric_time_node(linkable_specs_to_satisfy.metric_time_specs)
                candidate_nodes_for_right_side_of_join += [time_spine_node]
                candidate_nodes_for_left_side_of_join += [time_spine_node]
            default_join_type = SqlJoinType.FULL_OUTER

        logger.debug(
            LazyFormat(
                lambda: f"Starting search with {len(candidate_nodes_for_left_side_of_join)} potential source nodes on the left "
                f"side of the join, and {len(candidate_nodes_for_right_side_of_join)} potential nodes on the right side "
                f"of the join."
            )
        )
        start_time = time.time()

        node_processor = PreJoinNodeProcessor(
            semantic_model_lookup=self._semantic_model_lookup,
            node_data_set_resolver=self._node_data_set_resolver,
        )

        if predicate_pushdown_state.has_pushdown_potential and default_join_type is not SqlJoinType.FULL_OUTER:
            # TODO: encapsulate join type and distinct values state and eventually move this to a DataflowPlanOptimizer
            # This works today because all of our subsequent join configuration operations preserve the join type
            # as-is, or else switch it to a CROSS JOIN or INNER JOIN type, both of which are safe for predicate
            # pushdown. However, there is currently no way to enforce that invariant, so we will need to move
            # to a model where we evaluate the join nodes themselves and decide on whether or not to push down
            # the predicate. This will be much more straightforward once we finish encapsulating our existing
            # time range constraint pushdown controls into this mechanism.
            candidate_nodes_for_left_side_of_join = list(
                node_processor.apply_matching_filter_predicates(
                    source_nodes=candidate_nodes_for_left_side_of_join,
                    predicate_pushdown_state=predicate_pushdown_state,
                    metric_time_dimension_reference=self._metric_time_dimension_reference,
                )
            )

        candidate_nodes_for_right_side_of_join = node_processor.remove_unnecessary_nodes(
            desired_linkable_specs=linkable_specs_to_satisfy_tuple,
            nodes=candidate_nodes_for_right_side_of_join,
            metric_time_dimension_reference=self._metric_time_dimension_reference,
            time_spine_metric_time_nodes=self._source_node_set.time_spine_metric_time_nodes_tuple,
        )
        logger.debug(
            LazyFormat(
                lambda: f"After removing unnecessary nodes, there are {len(candidate_nodes_for_right_side_of_join)} candidate "
                f"nodes for the right side of the join"
            )
        )
        # TODO: test multi-hop with custom grains
        if DataflowPlanBuilder._contains_multihop_linkables(linkable_specs_to_satisfy_tuple):
            candidate_nodes_for_right_side_of_join = list(
                node_processor.add_multi_hop_joins(
                    desired_linkable_specs=linkable_specs_to_satisfy_tuple,
                    nodes=candidate_nodes_for_right_side_of_join,
                    join_type=default_join_type,
                )
            )
            logger.debug(
                LazyFormat(
                    lambda: f"After adding multi-hop nodes, there are {len(candidate_nodes_for_right_side_of_join)} candidate "
                    f"nodes for the right side of the join:\n"
                    f"{mf_pformat(candidate_nodes_for_right_side_of_join)}"
                )
            )

        # If there are MetricGroupBys in the requested linkable specs, build source nodes to satisfy them.
        # We do this at query time instead of during usual source node generation because the number of potential
        # MetricGroupBy source nodes could be extremely large (and potentially slow).
        logger.debug(
            LazyFormat(
                lambda: f"Building source nodes for group by metrics: {linkable_specs_to_satisfy.group_by_metric_specs}"
            )
        )
        candidate_nodes_for_right_side_of_join += [
            self._build_query_output_node(
                query_spec=self._source_node_builder.build_source_node_inputs_for_group_by_metric(group_by_metric_spec),
                for_group_by_source_node=True,
            )
            for group_by_metric_spec in linkable_specs_to_satisfy.group_by_metric_specs
        ]

        logger.debug(LazyFormat(lambda: f"Processing nodes took: {time.time()-start_time:.2f}s"))

        node_evaluator = NodeEvaluatorForLinkableInstances(
            semantic_model_lookup=self._semantic_model_lookup,
            nodes_available_for_joins=self._sort_by_suitability(candidate_nodes_for_right_side_of_join),
            node_data_set_resolver=self._node_data_set_resolver,
            time_spine_metric_time_nodes=self._source_node_set.time_spine_metric_time_nodes_tuple,
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
                        LazyFormat(
                            lambda: f"Skipping evaluation for:\n"
                            f"{indent(node.structure_text())}"
                            f"since it does not have all of the measure specs:\n"
                            f"{indent(mf_pformat(missing_specs))}"
                        )
                    )
                    continue

            logger.debug(
                LazyFormat(
                    lambda: f"Evaluating candidate node for the left side of the join:\n{indent(mf_pformat(node.structure_text()))}"
                )
            )

            start_time = time.time()
            evaluation = node_evaluator.evaluate_node(
                left_node=node,
                required_linkable_specs=list(linkable_specs_to_satisfy_tuple),
                default_join_type=default_join_type,
            )
            logger.debug(LazyFormat(lambda: f"Evaluation of {node} took {time.time() - start_time:.2f}s"))

            logger.debug(
                LazyFormat(
                    lambda: "Evaluation for source node:"
                    + indent(f"\nnode:\n{indent(node.structure_text())}")
                    + indent(f"\nevaluation:\n{indent(mf_pformat(evaluation))}")
                )
            )

            if len(evaluation.unjoinable_linkable_specs) > 0:
                logger.debug(
                    LazyFormat(
                        lambda: f"Skipping {node.node_id} since it contains un-joinable specs: "
                        f"{evaluation.unjoinable_linkable_specs}"
                    )
                )
                continue

            num_joins_required = len(evaluation.join_recipes)
            logger.debug(
                LazyFormat(
                    lambda: f"Found candidate with node ID '{node.node_id}' with {num_joins_required} joins required."
                )
            )

            node_to_evaluation[node] = evaluation

            # Since are evaluating nodes with the lowest cost first, if we find one without requiring any joins, then
            # this is going to be the lowest cost solution.
            if len(evaluation.join_recipes) == 0:
                logger.debug(
                    LazyFormat(lambda: "Not evaluating other nodes since we found one that doesn't require joins")
                )
                break

        logger.debug(LazyFormat(lambda: f"Found {len(node_to_evaluation)} candidate source nodes."))

        if len(node_to_evaluation) > 0:
            # Find evaluation with lowest number of joins.
            node_with_lowest_cost_plan = min(
                node_to_evaluation, key=lambda node: len(node_to_evaluation[node].join_recipes)
            )
            evaluation = node_to_evaluation[node_with_lowest_cost_plan]

            logger.debug(
                LazyFormat(
                    lambda: "Lowest cost plan is:"
                    + indent(f"\nnode:\n{indent(node_with_lowest_cost_plan.structure_text())}")
                    + indent(f"\nevaluation:\n{indent(mf_pformat(evaluation))}")
                    + indent(f"\njoins: {len(node_to_evaluation[node_with_lowest_cost_plan].join_recipes)}")
                )
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
            return SourceNodeRecipe(
                source_node=node_with_lowest_cost_plan,
                required_local_linkable_specs=LinkableSpecSet.create_from_specs(
                    evaluation.local_linkable_specs
                    + required_local_entity_specs
                    + required_local_dimension_specs
                    + required_local_time_dimension_specs
                ),
                join_linkable_instances_recipes=node_to_evaluation[node_with_lowest_cost_plan].join_recipes,
                all_linkable_specs_required_for_source_nodes=linkable_specs_to_satisfy,
            )

        logger.error(LazyFormat(lambda: "No recipe could be constructed."))
        return None

    def build_computed_metrics_node(
        self,
        metric_spec: MetricSpec,
        aggregated_measures_node: Union[AggregateMeasuresNode, DataflowPlanNode],
        aggregated_to_elements: Set[LinkableInstanceSpec],
        for_group_by_source_node: bool = False,
    ) -> ComputeMetricsNode:
        """Builds a ComputeMetricsNode from aggregated measures."""
        return ComputeMetricsNode.create(
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
        descendent_filter_spec_set: WhereFilterSpecSet,
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
                descendent_filter_spec_set=descendent_filter_spec_set,
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
        descendent_filter_spec_set: WhereFilterSpecSet,
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
        filter_spec_set: WhereFilterSpecSet = WhereFilterSpecSet()
        if include_filters:
            filter_spec_set = self._build_filter_specs_for_input_measure(
                filter_spec_factory=filter_spec_factory,
                metric=metric,
                input_measure=input_measure,
                descendent_filter_spec_set=descendent_filter_spec_set,
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
            filter_spec_set=filter_spec_set,
            alias=input_measure.alias,
            before_aggregation_time_spine_join_description=before_aggregation_time_spine_join_description,
            after_aggregation_time_spine_join_description=after_aggregation_time_spine_join_description,
        )

    def _build_filter_specs_for_input_measure(
        self,
        filter_spec_factory: WhereSpecFactory,
        metric: Metric,
        input_measure: MetricInputMeasure,
        descendent_filter_spec_set: WhereFilterSpecSet,
    ) -> WhereFilterSpecSet:
        metric_reference = MetricReference(element_name=metric.name)
        filter_location = WhereFilterLocation.for_metric(metric_reference)

        filter_spec_set = WhereFilterSpecSet(
            measure_level_filter_specs=tuple(
                filter_spec_factory.create_from_where_filter_intersection(
                    filter_location=filter_location, filter_intersection=input_measure.filter
                )
            ),
            metric_level_filter_specs=tuple(
                filter_spec_factory.create_from_where_filter_intersection(
                    filter_location=filter_location, filter_intersection=metric.filter
                )
            ),
        )
        return filter_spec_set.merge(descendent_filter_spec_set)

    def _build_input_metric_specs_for_derived_metric(
        self,
        metric_reference: MetricReference,
        filter_spec_factory: WhereSpecFactory,
    ) -> Sequence[MetricSpec]:
        """Return the metric specs referenced by the metric. Current use case is for derived metrics."""
        metric = self._metric_lookup.get_metric(metric_reference)
        input_metric_specs: List[MetricSpec] = []

        for input_metric in metric.input_metrics:
            filter_spec_set = WhereFilterSpecSet(
                metric_level_filter_specs=tuple(
                    filter_spec_factory.create_from_where_filter_intersection(
                        filter_location=WhereFilterLocation.for_input_metric(
                            input_metric_reference=input_metric.as_reference
                        ),
                        filter_intersection=input_metric.filter,
                    )
                ),
            )

            input_metric_offset_to_grain: Optional[TimeGranularity] = None
            if input_metric.offset_to_grain is not None:
                input_metric_offset_to_grain = error_if_not_standard_grain(
                    context=f"Metric({metric.name}).InputMetric({input_metric.name}).offset_to_grain",
                    input_granularity=input_metric.offset_to_grain,
                )

            spec = MetricSpec(
                element_name=input_metric.name,
                filter_spec_set=filter_spec_set,
                alias=input_metric.alias,
                offset_window=(
                    PydanticMetricTimeWindow(
                        count=input_metric.offset_window.count,
                        granularity=input_metric.offset_window.granularity,
                    )
                    if input_metric.offset_window
                    else None
                ),
                offset_to_grain=input_metric_offset_to_grain,
            )
            input_metric_specs.append(spec)
        return tuple(input_metric_specs)

    def build_aggregated_measure(
        self,
        metric_input_measure_spec: MetricInputMeasureSpec,
        queried_linkable_specs: LinkableSpecSet,
        predicate_pushdown_state: PredicatePushdownState,
        measure_recipe: Optional[SourceNodeRecipe] = None,
    ) -> DataflowPlanNode:
        """Returns a node where the measures are aggregated by the linkable specs and constrained appropriately.

        This might be a node representing a single aggregation over one semantic model, or a node representing
        a composite set of aggregations originating from multiple semantic models, and joined into a single
        aggregated set of measures.
        """
        measure_spec = metric_input_measure_spec.measure_spec

        logger.debug(
            LazyFormat(
                lambda: f"Building aggregated measure: {measure_spec} with input measure filters:\n"
                f"{mf_pformat(metric_input_measure_spec.filter_spec_set)}\n"
                f"and  filters:\n{mf_pformat(metric_input_measure_spec.filter_spec_set)}"
            )
        )

        return self._build_aggregated_measure_from_measure_source_node(
            metric_input_measure_spec=metric_input_measure_spec,
            queried_linkable_specs=queried_linkable_specs,
            predicate_pushdown_state=predicate_pushdown_state,
            measure_recipe=measure_recipe,
        )

    def __get_required_linkable_specs(
        self,
        queried_linkable_specs: LinkableSpecSet,
        filter_specs: Sequence[WhereFilterSpec],
        measure_spec_properties: Optional[MeasureSpecProperties] = None,
    ) -> LinkableSpecSet:
        """Get all required linkable specs for this query, including extraneous linkable specs.

        Extraneous linkable specs are specs that are used in this phase that should not show up in the final result
        unless it was already a requested spec in the query, e.g., a linkable spec used in where constraint is extraneous.
        """
        linkable_spec_sets_to_merge: List[LinkableSpecSet] = []
        for filter_spec in filter_specs:
            linkable_spec_sets_to_merge.append(LinkableSpecSet.create_from_specs(filter_spec.linkable_specs))

        if measure_spec_properties is not None:
            non_additive_dimension_spec = (
                measure_spec_properties.non_additive_dimension_spec if measure_spec_properties else None
            )
            if non_additive_dimension_spec is not None:
                agg_time_dimension_grain = measure_spec_properties.agg_time_dimension_grain
                linkable_spec_sets_to_merge.append(
                    LinkableSpecSet.create_from_specs(
                        non_additive_dimension_spec.linkable_specs(agg_time_dimension_grain)
                    )
                )

        extraneous_linkable_specs = LinkableSpecSet.merge_iterable(linkable_spec_sets_to_merge).dedupe()
        required_linkable_specs = queried_linkable_specs.merge(extraneous_linkable_specs).dedupe()

        # Custom grains require joining to their base grain, so add base grain to extraneous specs.
        if required_linkable_specs.time_dimension_specs_with_custom_grain:
            base_grain_set = LinkableSpecSet.create_from_specs(
                [spec.with_base_grain() for spec in required_linkable_specs.time_dimension_specs_with_custom_grain]
            )
            extraneous_linkable_specs = extraneous_linkable_specs.merge(base_grain_set).dedupe()
            required_linkable_specs = required_linkable_specs.merge(extraneous_linkable_specs).dedupe()

        return required_linkable_specs

    def _build_aggregated_measure_from_measure_source_node(
        self,
        metric_input_measure_spec: MetricInputMeasureSpec,
        queried_linkable_specs: LinkableSpecSet,
        predicate_pushdown_state: PredicatePushdownState,
        measure_recipe: Optional[SourceNodeRecipe] = None,
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
        cumulative_metric_adjusted_time_constraint: Optional[TimeRangeConstraint] = None
        if cumulative and predicate_pushdown_state.time_range_constraint is not None:
            logger.debug(
                LazyFormat(
                    lambda: f"Time range constraint before adjustment is {predicate_pushdown_state.time_range_constraint}"
                )
            )
            granularity: Optional[TimeGranularity] = None
            count = 0
            if cumulative_window is not None:
                granularity = error_if_not_standard_grain(
                    context="CumulativeMetric.window.granularity", input_granularity=cumulative_window.granularity
                )
                count = cumulative_window.count
            elif cumulative_grain_to_date is not None:
                count = 1
                granularity = cumulative_grain_to_date

            cumulative_metric_adjusted_time_constraint = (
                self._time_period_adjuster.expand_time_constraint_for_cumulative_metric(
                    predicate_pushdown_state.time_range_constraint, granularity, count
                )
            )
            logger.debug(
                LazyFormat(lambda: f"Adjusted time range constraint to: {cumulative_metric_adjusted_time_constraint}")
            )

        required_linkable_specs = self.__get_required_linkable_specs(
            queried_linkable_specs=queried_linkable_specs,
            filter_specs=metric_input_measure_spec.filter_spec_set.all_filter_specs,
            measure_spec_properties=measure_properties,
        )

        before_aggregation_time_spine_join_description = (
            metric_input_measure_spec.before_aggregation_time_spine_join_description
        )

        if measure_recipe is None:
            logger.debug(
                LazyFormat(
                    lambda: "Looking for a recipe to get:"
                    + indent(f"\nmeasure_specs:\n{mf_pformat([measure_spec])}")
                    + indent(f"\nevaluation:\n{mf_pformat(required_linkable_specs)}")
                )
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
            measure_recipe = self._find_source_node_recipe(
                FindSourceNodeRecipeParameterSet(
                    measure_spec_properties=measure_properties,
                    predicate_pushdown_state=measure_pushdown_state,
                    linkable_spec_set=required_linkable_specs,
                )
            )
            logger.debug(
                LazyFormat(
                    lambda: f"With {len(self._source_node_set.source_nodes_for_metric_queries)} source nodes, finding a recipe "
                    f"took {time.time() - find_recipe_start_time:.2f}s"
                )
            )

        logger.debug(LazyFormat(lambda: f"Using recipe:\n{indent(mf_pformat(measure_recipe))}"))

        if measure_recipe is None:
            raise UnableToSatisfyQueryError(
                f"Unable to join all items in request. Measure: {measure_spec.element_name}; Specs to join: {required_linkable_specs}"
            )

        base_queried_agg_time_dimension_specs = self._get_base_agg_time_dimensions(
            queried_linkable_specs=queried_linkable_specs, measure_reference=measure_spec.reference
        )

        # If a cumulative metric is queried with metric_time / agg_time_dimension, join over time range.
        # Otherwise, the measure will be aggregated over all time.
        unaggregated_measure_node: DataflowPlanNode = measure_recipe.source_node
        if cumulative and base_queried_agg_time_dimension_specs:
            unaggregated_measure_node = JoinOverTimeRangeNode.create(
                parent_node=unaggregated_measure_node,
                queried_agg_time_dimension_specs=base_queried_agg_time_dimension_specs,
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
        if before_aggregation_time_spine_join_description and base_queried_agg_time_dimension_specs:
            # TODO: move all of this to a helper function
            assert before_aggregation_time_spine_join_description.join_type is SqlJoinType.INNER, (
                f"Expected {SqlJoinType.INNER} for joining to time spine before aggregation. Remove this if there's a "
                f"new use case."
            )

            join_on_time_dimension_spec = self._determine_time_spine_join_spec(
                measure_properties=measure_properties, required_time_spine_specs=base_queried_agg_time_dimension_specs
            )
            required_time_spine_specs = base_queried_agg_time_dimension_specs
            if join_on_time_dimension_spec not in required_time_spine_specs:
                required_time_spine_specs = (join_on_time_dimension_spec,) + required_time_spine_specs
            time_spine_node = self._build_time_spine_node(required_time_spine_specs)
            unaggregated_measure_node = JoinToTimeSpineNode.create(
                metric_source_node=unaggregated_measure_node,
                time_spine_node=time_spine_node,
                requested_agg_time_dimension_specs=base_queried_agg_time_dimension_specs,
                join_on_time_dimension_spec=join_on_time_dimension_spec,
                offset_window=before_aggregation_time_spine_join_description.offset_window,
                offset_to_grain=before_aggregation_time_spine_join_description.offset_to_grain,
                join_type=before_aggregation_time_spine_join_description.join_type,
            )

        custom_granularity_specs_to_join = [
            spec
            for spec in required_linkable_specs.time_dimension_specs_with_custom_grain
            # If this is the second layer of aggregation for a conversion metric, we have already joined the custom granularity.
            if spec not in measure_recipe.all_linkable_specs_required_for_source_nodes.as_tuple
        ]
        # Apply original time constraint if it wasn't applied to the source node recipe. For cumulative metrics, the constraint
        # may have been expanded and needs to be narrowed here. For offsets, the constraint was deferred to after the offset.
        # TODO - Pushdown: Encapsulate all of this window sliding bookkeeping in the pushdown params object
        time_range_constraint_to_apply = None
        if cumulative_metric_adjusted_time_constraint or before_aggregation_time_spine_join_description:
            time_range_constraint_to_apply = predicate_pushdown_state.time_range_constraint
        unaggregated_measure_node = self._build_pre_aggregation_plan(
            source_node=unaggregated_measure_node,
            join_targets=measure_recipe.join_targets,
            custom_granularity_specs=custom_granularity_specs_to_join,
            where_filter_specs=metric_input_measure_spec.filter_spec_set.all_filter_specs,
            time_range_constraint=time_range_constraint_to_apply,
            filter_to_specs=InstanceSpecSet(measure_specs=(measure_spec,)).merge(
                InstanceSpecSet.create_from_specs(queried_linkable_specs.as_tuple)
            ),
            measure_properties=measure_properties,
            queried_linkable_specs_for_semi_additive_join=queried_linkable_specs,
        )

        aggregate_measures_node = AggregateMeasuresNode.create(
            parent_node=unaggregated_measure_node, metric_input_measure_specs=(metric_input_measure_spec,)
        )

        # Joining to time spine after aggregation is for measures that specify `join_to_timespine: true` in the YAML spec.
        after_aggregation_time_spine_join_description = (
            metric_input_measure_spec.after_aggregation_time_spine_join_description
        )
        queried_agg_time_dimension_specs = queried_linkable_specs.included_agg_time_dimension_specs_for_measure(
            measure_reference=measure_spec.reference, semantic_model_lookup=self._semantic_model_lookup
        )
        if after_aggregation_time_spine_join_description and queried_agg_time_dimension_specs:
            # TODO: move all of this to a helper function
            assert after_aggregation_time_spine_join_description.join_type is SqlJoinType.LEFT_OUTER, (
                f"Expected {SqlJoinType.LEFT_OUTER} for joining to time spine after aggregation. Remove this if "
                f"there's a new use case."
            )
            time_spine_required_specs = copy.deepcopy(queried_agg_time_dimension_specs)

            # Find filters that contain only metric_time or agg_time_dimension. They will be applied to the time spine table.
            agg_time_only_filters: List[WhereFilterSpec] = []
            non_agg_time_filters: List[WhereFilterSpec] = []
            for filter_spec in metric_input_measure_spec.filter_spec_set.after_measure_aggregation_filter_specs:
                included_agg_time_specs = filter_spec.linkable_spec_set.included_agg_time_dimension_specs_for_measure(
                    measure_reference=measure_spec.reference, semantic_model_lookup=self._semantic_model_lookup
                )
                if set(included_agg_time_specs) == set(filter_spec.linkable_spec_set.as_tuple):
                    agg_time_only_filters.append(filter_spec)
                    for agg_time_spec in included_agg_time_specs:
                        if agg_time_spec not in time_spine_required_specs:
                            time_spine_required_specs.append(agg_time_spec)
                else:
                    non_agg_time_filters.append(filter_spec)

            time_spine_node = self._build_time_spine_node(
                queried_time_spine_specs=queried_agg_time_dimension_specs,
                time_range_constraint=predicate_pushdown_state.time_range_constraint,
                where_filter_specs=agg_time_only_filters,
            )
            output_node: DataflowPlanNode = JoinToTimeSpineNode.create(
                metric_source_node=aggregate_measures_node,
                time_spine_node=time_spine_node,
                requested_agg_time_dimension_specs=queried_agg_time_dimension_specs,
                join_on_time_dimension_spec=self._sort_by_base_granularity(queried_agg_time_dimension_specs)[0],
                join_type=after_aggregation_time_spine_join_description.join_type,
            )

            # Since new rows might have been added due to time spine join, re-apply constraints here. Only re-apply filters
            # for specs that are also in the queried specs, since those are the only ones that could change after the time
            # spine join. Exclude filters that contain only metric_time or an agg time dimension, since were already applied
            # to the time spine table.
            queried_non_agg_time_filter_specs = [
                filter_spec
                for filter_spec in non_agg_time_filters
                if set(filter_spec.linkable_specs).issubset(set(queried_linkable_specs.as_tuple))
            ]
            if len(queried_non_agg_time_filter_specs) > 0:
                output_node = WhereConstraintNode.create(
                    parent_node=output_node, where_specs=queried_non_agg_time_filter_specs, always_apply=True
                )

            # TODO: this will break if you query by agg_time_dimension but apply a time constraint on metric_time.
            # To fix when enabling time range constraints for agg_time_dimension.
            if queried_agg_time_dimension_specs and predicate_pushdown_state.time_range_constraint is not None:
                output_node = ConstrainTimeRangeNode.create(
                    parent_node=output_node, time_range_constraint=predicate_pushdown_state.time_range_constraint
                )
            return output_node

        return aggregate_measures_node

    def _build_pre_aggregation_plan(
        self,
        source_node: DataflowPlanNode,
        filter_to_specs: Optional[InstanceSpecSet] = None,
        join_targets: List[JoinDescription] = [],
        custom_granularity_specs: Sequence[TimeDimensionSpec] = (),
        where_filter_specs: Sequence[WhereFilterSpec] = (),
        time_range_constraint: Optional[TimeRangeConstraint] = None,
        measure_properties: Optional[MeasureSpecProperties] = None,
        queried_linkable_specs_for_semi_additive_join: Optional[LinkableSpecSet] = None,
        distinct: bool = False,
    ) -> DataflowPlanNode:
        """Adds standard pre-aggegation steps after building source node and before aggregation."""
        output_node = source_node
        if join_targets:
            output_node = JoinOnEntitiesNode.create(left_node=output_node, join_targets=join_targets)

        for custom_granularity_spec in custom_granularity_specs:
            output_node = JoinToCustomGranularityNode.create(
                parent_node=output_node, time_dimension_spec=custom_granularity_spec
            )

        if len(where_filter_specs) > 0:
            output_node = WhereConstraintNode.create(parent_node=output_node, where_specs=where_filter_specs)

        if time_range_constraint:
            output_node = ConstrainTimeRangeNode.create(
                parent_node=output_node, time_range_constraint=time_range_constraint
            )

        if measure_properties and measure_properties.non_additive_dimension_spec:
            if queried_linkable_specs_for_semi_additive_join is None:
                raise ValueError(
                    "`queried_linkable_specs_for_semi_additive_join` must be provided in when building pre-aggregation plan "
                    "if `non_additive_dimension_spec` is present."
                )
            output_node = self._build_semi_additive_join_node(
                measure_properties=measure_properties,
                queried_linkable_specs=queried_linkable_specs_for_semi_additive_join,
                parent_node=output_node,
            )
        if filter_to_specs:
            output_node = FilterElementsNode.create(
                parent_node=output_node, include_specs=filter_to_specs, distinct=distinct
            )

        return output_node

    def _build_semi_additive_join_node(
        self,
        measure_properties: MeasureSpecProperties,
        queried_linkable_specs: LinkableSpecSet,
        parent_node: DataflowPlanNode,
    ) -> SemiAdditiveJoinNode:
        non_additive_dimension_spec = measure_properties.non_additive_dimension_spec
        assert (
            non_additive_dimension_spec
        ), "_build_semi_additive_join_node() should only be called if there is a non_additive_dimension_spec."
        # Apply semi additive join on the node
        agg_time_dimension = measure_properties.agg_time_dimension
        non_additive_dimension_grain = measure_properties.agg_time_dimension_grain
        queried_time_dimension_spec: Optional[TimeDimensionSpec] = self._find_non_additive_dimension_in_linkable_specs(
            agg_time_dimension=agg_time_dimension,
            linkable_specs=queried_linkable_specs.as_tuple,
            non_additive_dimension_spec=non_additive_dimension_spec,
        )
        time_dimension_spec = TimeDimensionSpec(
            # The NonAdditiveDimensionSpec name property is a plain element name
            element_name=non_additive_dimension_spec.name,
            entity_links=(),
            time_granularity=ExpandedTimeGranularity.from_time_granularity(non_additive_dimension_grain),
        )
        window_groupings = tuple(
            LinklessEntitySpec.from_element_name(name) for name in non_additive_dimension_spec.window_groupings
        )
        return SemiAdditiveJoinNode.create(
            parent_node=parent_node,
            entity_specs=window_groupings,
            time_dimension_spec=time_dimension_spec,
            agg_by_function=non_additive_dimension_spec.window_choice,
            queried_time_dimension_spec=queried_time_dimension_spec,
        )

    def _choose_time_spine_source(self, required_time_spine_specs: Sequence[TimeDimensionSpec]) -> TimeSpineSource:
        """Choose the time spine source that can satisfy the required time spine specs."""
        return TimeSpineSource.choose_time_spine_source(
            required_time_spine_specs=required_time_spine_specs,
            time_spine_sources=self._source_node_builder.time_spine_sources,
        )

    def _choose_time_spine_metric_time_node(
        self, required_time_spine_specs: Sequence[TimeDimensionSpec]
    ) -> MetricTimeDimensionTransformNode:
        """Return the MetricTimeDimensionTransform time spine node needed to satisfy the specs."""
        time_spine_source = self._choose_time_spine_source(required_time_spine_specs)
        return self._source_node_set.time_spine_metric_time_nodes[time_spine_source.base_granularity]

    def _choose_time_spine_read_node(self, time_spine_source: TimeSpineSource) -> ReadSqlSourceNode:
        """Return the MetricTimeDimensionTransform time spine node needed to satisfy the specs."""
        return self._source_node_set.time_spine_read_nodes[time_spine_source.base_granularity]

    def _build_time_spine_node(
        self,
        queried_time_spine_specs: Sequence[TimeDimensionSpec],
        where_filter_specs: Sequence[WhereFilterSpec] = (),
        time_range_constraint: Optional[TimeRangeConstraint] = None,
    ) -> DataflowPlanNode:
        """Return the time spine node needed to satisfy the specs."""
        required_time_spine_spec_set = self.__get_required_linkable_specs(
            queried_linkable_specs=LinkableSpecSet(time_dimension_specs=tuple(queried_time_spine_specs)),
            filter_specs=where_filter_specs,
        )
        required_time_spine_specs = required_time_spine_spec_set.time_dimension_specs

        # TODO: support multiple time spines here. Build node on the one with the smallest base grain.
        # Then, pass custom_granularity_specs into _build_pre_aggregation_plan if they aren't satisfied by smallest time spine.
        time_spine_source = self._choose_time_spine_source(required_time_spine_specs)
        read_node = self._choose_time_spine_read_node(time_spine_source)
        time_spine_data_set = self._node_data_set_resolver.get_output_data_set(read_node)

        # Change the column aliases to match the specs that were requested in the query.
        time_spine_node = AliasSpecsNode.create(
            parent_node=read_node,
            change_specs=tuple(
                SpecToAlias(
                    input_spec=time_spine_data_set.instance_from_time_dimension_grain_and_date_part(required_spec).spec,
                    output_spec=required_spec,
                )
                for required_spec in required_time_spine_specs
            ),
        )

        # If the base grain of the time spine isn't selected, it will have duplicate rows that need deduping.
        should_dedupe = ExpandedTimeGranularity.from_time_granularity(time_spine_source.base_granularity) not in {
            spec.time_granularity for spec in queried_time_spine_specs
        }

        return self._build_pre_aggregation_plan(
            source_node=time_spine_node,
            filter_to_specs=InstanceSpecSet(time_dimension_specs=tuple(queried_time_spine_specs)),
            time_range_constraint=time_range_constraint,
            where_filter_specs=where_filter_specs,
            distinct=should_dedupe,
        )

    def _sort_by_base_granularity(self, time_dimension_specs: Sequence[TimeDimensionSpec]) -> List[TimeDimensionSpec]:
        """Sort the time dimensions by their base granularity.

        Specs with date part will come after specs without it. Standard grains will come before custom.
        """
        return sorted(
            time_dimension_specs,
            key=lambda spec: (
                spec.date_part is not None,
                spec.time_granularity.is_custom_granularity,
                spec.time_granularity.base_granularity.to_int(),
            ),
        )

    def _determine_time_spine_join_spec(
        self, measure_properties: MeasureSpecProperties, required_time_spine_specs: Tuple[TimeDimensionSpec, ...]
    ) -> TimeDimensionSpec:
        """Determine the spec to join on for a time spine join.

        Defaults to metric_time if it is included in the request, else the agg_time_dimension.
        Will use the smallest available grain for the meeasure.
        """
        join_spec_grain = ExpandedTimeGranularity.from_time_granularity(measure_properties.agg_time_dimension_grain)
        join_on_time_dimension_spec = DataSet.metric_time_dimension_spec(time_granularity=join_spec_grain)
        if not LinkableSpecSet(time_dimension_specs=required_time_spine_specs).contains_metric_time:
            sample_agg_time_dimension_spec = required_time_spine_specs[0]
            join_on_time_dimension_spec = sample_agg_time_dimension_spec.with_grain_and_date_part(
                time_granularity=join_spec_grain, date_part=None
            )
        return join_on_time_dimension_spec
