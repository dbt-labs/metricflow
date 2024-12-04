from __future__ import annotations

import dataclasses
import logging
from enum import Enum
from typing import Dict, FrozenSet, List, Optional, Sequence, Set, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.references import EntityReference, SemanticModelReference, TimeDimensionReference
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow_semantics.specs.entity_spec import LinklessEntitySpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.spec_set import group_specs_by_type
from metricflow_semantics.specs.spec_set_transforms import ToElementNameSet
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.sql.sql_join_type import SqlJoinType

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.partitions import PartitionJoinResolver
from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)
from metricflow.dataflow.nodes.constrain_time import ConstrainTimeRangeNode
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.dataflow.nodes.join_to_base import JoinDescription, JoinOnEntitiesNode
from metricflow.dataflow.nodes.metric_time_transform import MetricTimeDimensionTransformNode
from metricflow.dataflow.nodes.where_filter import WhereConstraintNode
from metricflow.validation.dataflow_join_validator import JoinDataflowOutputValidator

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class MultiHopJoinCandidateLineage:
    """Describes how the multi-hop join candidate was formed.

    For example, if
    * bridge_source has the primary entity account_id and the foreign entity customer_id
    * customers_source has the primary entity customer_id and dimension country
    * transactions_source has the transactions measure and the account_id foreign entity

    Then the candidate lineage can be describe as
        first_node_to_join = bridge_source
        second_node_to_join = customers_source
    to get the country dimension.
    """

    first_node_to_join: DataflowPlanNode
    second_node_to_join: DataflowPlanNode
    join_second_node_by_entity: LinklessEntitySpec


@dataclasses.dataclass(frozen=True)
class MultiHopJoinCandidate:
    """A candidate node containing linkable specs that is join of other nodes. It's used to resolve multi-hop queries.

    Also see MultiHopJoinCandidateLineage.
    """

    node_with_multi_hop_elements: DataflowPlanNode
    lineage: MultiHopJoinCandidateLineage


class PredicateInputType(Enum):
    """Enumeration of predicate input types we may encounter in where filters.

    This is primarily used for describing when predicate pushdown may operate in a dataflow plan, and is necessary
    for holistic checks against the set of potentially enabled pushdown operations. For example, in the scenario
    scenario where we only allow time range updates, we must do careful overriding of other pushdown properties.

    This also allows us to disable pushdown for things like time dimension filters in cases where we might
    accidentally censor input data.
    """

    CATEGORICAL_DIMENSION = "categorical_dimension"
    ENTITY = "entity"
    TIME_DIMENSION = "time_dimension"
    TIME_RANGE_CONSTRAINT = "time_range_constraint"


@dataclasses.dataclass(frozen=True)
class PredicatePushdownState:
    """Container class for maintaining state information relevant for predicate pushdown.

    This broadly tracks three related items:
    1. Filter predicates collected during the process of constructing a dataflow plan
    2. Predicate types eligible for pushdown
    3. Filters which have been applied already

    The first may be updated as things like time constraints get altered or metric and measure filters are
    added to the query filters.
    The second may be updated based on query configuration, like if a cumulative metric is added to the plan
    there may be changes to what sort of predicate pushdown operations are supported.
    The last will be updated as filters are applied via pushdown or by the original WhereConstraintNode.

    Finally, the time_range_constraint property holds the time window for setting up a time range filter expression.
    """

    time_range_constraint: Optional[TimeRangeConstraint]
    # TODO: Deduplicate where_filter_specs
    where_filter_specs: Tuple[WhereFilterSpec, ...]
    applied_where_filter_specs: FrozenSet[WhereFilterSpec] = frozenset()
    pushdown_enabled_types: FrozenSet[PredicateInputType] = frozenset(
        [PredicateInputType.TIME_RANGE_CONSTRAINT, PredicateInputType.CATEGORICAL_DIMENSION]
    )

    def __post_init__(self) -> None:
        """Validation to ensure pushdown states are configured correctly.

        In particular, this asserts that cases where pushdown is disabled cannot leak pushdown operations via
        outside property access - if pushdown is disabled, no further pushdown operations of any kind are allowed
        on that particular code branch. It also asserts that unsupported pushdown scenarios are not configured.
        """
        invalid_types: Set[PredicateInputType] = set()

        for input_type in self.pushdown_enabled_types:
            if input_type is PredicateInputType.ENTITY or input_type is PredicateInputType.TIME_DIMENSION:
                invalid_types.add(input_type)
            elif (
                input_type is PredicateInputType.CATEGORICAL_DIMENSION
                or input_type is PredicateInputType.TIME_RANGE_CONSTRAINT
            ):
                continue
            else:
                assert_values_exhausted(input_type)

        assert len(invalid_types) == 0, (
            "Unsupported predicate input type found in pushdown state configuration! We currently only support "
            "predicate pushdown for a subset of possible predicate input types (i.e., types of semantic manifest "
            "elements, such as entities and time dimensions, referenced in filter predicates), but this was enabled "
            f"for {self.pushdown_enabled_types}, which includes the following invalid types: {invalid_types}."
        )

        time_range_constraint_is_valid = (
            self.time_range_constraint is None
            or PredicateInputType.TIME_RANGE_CONSTRAINT in self.pushdown_enabled_types
        )
        where_filter_specs_are_valid = len(self.where_filter_specs) == 0 or self.where_filter_pushdown_enabled
        assert time_range_constraint_is_valid and where_filter_specs_are_valid, (
            "Invalid pushdown state configuration! Disabled pushdown state objects cannot have properties "
            "set that may lead to improper access and use in other contexts, as that can lead to unintended "
            "filtering operations in cases where these properties are accessed without appropriate checks against "
            "pushdown configuration. The following properties should be None or empty:\n"
            f"time_range_constraint: {self.time_range_constraint}\n"
            f"where_filter_specs: {self.where_filter_specs}"
        )

    @property
    def has_pushdown_potential(self) -> bool:
        """Returns whether or not pushdown is enabled for a type with predicate candidates in place."""
        return self.has_time_range_constraint_to_push_down or self.has_where_filters_to_push_down

    @property
    def has_time_range_constraint_to_push_down(self) -> bool:
        """Convenience accessor for checking if there is a time range constraint that can be pushed down.

        Note: this time range enabled state is a backwards compatibility shim for use with conversion metrics while
        we determine how best to support predicate pushdown for conversion metrics. It may have longer term utility,
        but ideally we'd collapse this with the more general time dimension filter input scenarios.
        """
        return (
            PredicateInputType.TIME_RANGE_CONSTRAINT in self.pushdown_enabled_types
            and self.time_range_constraint is not None
        )

    @property
    def has_where_filters_to_push_down(self) -> bool:
        """Convenience accessor for checking if there are any where filters to push down."""
        return self.where_filter_pushdown_enabled and len(self.where_filter_specs) > 0

    @property
    def where_filter_pushdown_enabled(self) -> bool:
        """Indicates whether or not pushdown is enabled for where filters."""
        return (
            PredicateInputType.CATEGORICAL_DIMENSION in self.pushdown_enabled_types
            or PredicateInputType.ENTITY in self.pushdown_enabled_types
            or PredicateInputType.TIME_DIMENSION in self.pushdown_enabled_types
        )

    @property
    def pushdown_eligible_element_types(self) -> FrozenSet[LinkableElementType]:
        """Set of linkable element types eligible for predicate pushdown.

        This converts from enabled PushdownInputTypes for checking if linkable elements in where filter specs are
        eligible for pushdown.
        """
        eligible_types: List[LinkableElementType] = []
        for enabled_type in self.pushdown_enabled_types:
            if enabled_type is PredicateInputType.TIME_RANGE_CONSTRAINT:
                pass
            elif enabled_type is PredicateInputType.CATEGORICAL_DIMENSION:
                eligible_types.append(LinkableElementType.DIMENSION)
            elif enabled_type is PredicateInputType.TIME_DIMENSION or enabled_type is PredicateInputType.ENTITY:
                # TODO: Remove as support for time dimensions and entities becomes available
                raise NotImplementedError(
                    "Predicate pushdown is not currently supported for where filter predicates with time dimension or "
                    f"entity references, but this pushdown state is enabled for {enabled_type}."
                )
            else:
                assert_values_exhausted(enabled_type)

        return frozenset(eligible_types)

    @staticmethod
    def with_time_range_constraint(
        original_pushdown_state: PredicatePushdownState, time_range_constraint: TimeRangeConstraint
    ) -> PredicatePushdownState:
        """Factory method for updating a pushdown state with a time range constraint.

        This allows for temporarily overriding a time range constraint with an adjusted one, or enabling a time
        range constraint filter if one becomes available mid-stream during dataflow plan construction.
        """
        pushdown_enabled_types = original_pushdown_state.pushdown_enabled_types.union(
            {PredicateInputType.TIME_RANGE_CONSTRAINT}
        )
        return PredicatePushdownState(
            time_range_constraint=time_range_constraint,
            pushdown_enabled_types=pushdown_enabled_types,
            where_filter_specs=original_pushdown_state.where_filter_specs,
            applied_where_filter_specs=original_pushdown_state.applied_where_filter_specs,
        )

    @staticmethod
    def without_time_range_constraint(
        original_pushdown_state: PredicatePushdownState,
    ) -> PredicatePushdownState:
        """Factory method for updating pushdown state to bypass time range constraints.

        This eliminates time range constraint pushdown as an option, since the only reason to remove
        time range constraint metadata is to turn it off, so we avoid potential issues where
        a second ConstrainTimeRange node might update the pushdown state.

        TODO: replace or rename this method.
        """
        pushdown_enabled_types = original_pushdown_state.pushdown_enabled_types.difference(
            {PredicateInputType.TIME_RANGE_CONSTRAINT}
        )
        return PredicatePushdownState(
            time_range_constraint=None,
            pushdown_enabled_types=pushdown_enabled_types,
            where_filter_specs=original_pushdown_state.where_filter_specs,
            applied_where_filter_specs=original_pushdown_state.applied_where_filter_specs,
        )

    @staticmethod
    def without_where_filter_specs(
        original_pushdown_state: PredicatePushdownState,
    ) -> PredicatePushdownState:
        """Factory method for updating pushdown state to remove existing where filter specs.

        This simply blanks out the where filter specs without altering which types of pushdown are available.
        """
        return PredicatePushdownState.with_where_filter_specs(
            original_pushdown_state=original_pushdown_state,
            where_filter_specs=tuple(),
        )

    @staticmethod
    def with_where_filter_specs(
        original_pushdown_state: PredicatePushdownState, where_filter_specs: Sequence[WhereFilterSpec]
    ) -> PredicatePushdownState:
        """Factory method for replacing WhereFilterSpecs in pushdown operations.

        This requires that the PushdownState allow for where filters - time range only or disabled states will raise
        an exception, and must be checked externally.
        """
        return PredicatePushdownState(
            time_range_constraint=original_pushdown_state.time_range_constraint,
            where_filter_specs=tuple(where_filter_specs),
            pushdown_enabled_types=original_pushdown_state.pushdown_enabled_types,
            applied_where_filter_specs=original_pushdown_state.applied_where_filter_specs,
        )

    @staticmethod
    def with_pushdown_applied_where_filter_specs(
        original_pushdown_state: PredicatePushdownState, pushdown_applied_where_filter_specs: FrozenSet[WhereFilterSpec]
    ) -> PredicatePushdownState:
        """Factory method for replacing pushdown applied where filter specs in pushdown operations.

        This is useful for managing propagation - both forwards and backwards - of where filter specs that have been
        applied via a pushdown operation.
        """
        return PredicatePushdownState(
            time_range_constraint=original_pushdown_state.time_range_constraint,
            pushdown_enabled_types=original_pushdown_state.pushdown_enabled_types,
            where_filter_specs=original_pushdown_state.where_filter_specs,
            applied_where_filter_specs=pushdown_applied_where_filter_specs,
        )

    @staticmethod
    def with_pushdown_disabled() -> PredicatePushdownState:
        """Factory method for configuring a disabled predicate pushdown state.

        This is useful in cases where there is a branched path where pushdown should be disabled in one branch while the
        other may remain eligible. For example, a join linkage where one side of the join contains an unsupported
        configuration might send a disabled copy of the pushdown parameters down that path while retaining the potential
        for using another path.
        """
        return PredicatePushdownState(
            time_range_constraint=None,
            pushdown_enabled_types=frozenset(),
            where_filter_specs=tuple(),
        )


class PreJoinNodeProcessor:
    """Processes source nodes before other nodes are joined.

    Generally, the source nodes will be combined with other dataflow plan nodes to produce a new set of nodes to realize
    a condition of the query. For example, to realize a time range constraint, a ConstrainTimeRangeNode will be added
    to the source nodes.

    e.g.

    <SomeDataflowPlanNode/>

    ->

    <ConstrainTimeRangeNode>
        <SomeDataflowPlanNode/>
    </ConstrainTimeRangeNode>

    """

    def __init__(  # noqa: D107
        self,
        semantic_model_lookup: SemanticModelLookup,
        node_data_set_resolver: DataflowPlanNodeOutputDataSetResolver,
    ):
        self._node_data_set_resolver = node_data_set_resolver
        self._partition_resolver = PartitionJoinResolver(semantic_model_lookup)
        self._semantic_model_lookup = semantic_model_lookup
        self._join_evaluator = JoinDataflowOutputValidator(semantic_model_lookup)

    def apply_matching_filter_predicates(
        self,
        source_nodes: Sequence[DataflowPlanNode],
        predicate_pushdown_state: PredicatePushdownState,
        metric_time_dimension_reference: TimeDimensionReference,
    ) -> Sequence[DataflowPlanNode]:
        """Adds filter predicate nodes to the input nodes as appropriate."""
        if predicate_pushdown_state.has_time_range_constraint_to_push_down:
            source_nodes = self._add_time_range_constraint(
                source_nodes=source_nodes,
                metric_time_dimension_reference=metric_time_dimension_reference,
                time_range_constraint=predicate_pushdown_state.time_range_constraint,
            )

        if predicate_pushdown_state.has_where_filters_to_push_down:
            source_nodes = self._add_where_constraint(
                source_nodes=source_nodes,
                where_filter_specs=predicate_pushdown_state.where_filter_specs,
                enabled_element_types=predicate_pushdown_state.pushdown_eligible_element_types,
            )

        return source_nodes

    def _add_time_range_constraint(
        self,
        source_nodes: Sequence[DataflowPlanNode],
        metric_time_dimension_reference: TimeDimensionReference,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
    ) -> Sequence[DataflowPlanNode]:
        """Adds a time range constraint node to the input nodes."""
        processed_nodes: List[DataflowPlanNode] = []
        for source_node in source_nodes:
            # Constrain the time range if specified.
            if time_range_constraint:
                node_output_data_set = self._node_data_set_resolver.get_output_data_set(source_node)
                constrain_time = False
                for time_dimension_instance in node_output_data_set.instance_set.time_dimension_instances:
                    if (
                        time_dimension_instance.spec.reference == metric_time_dimension_reference
                        and len(time_dimension_instance.spec.entity_links) == 0
                    ):
                        constrain_time = True
                        break
                if constrain_time:
                    processed_nodes.append(
                        ConstrainTimeRangeNode.create(
                            parent_node=source_node, time_range_constraint=time_range_constraint
                        )
                    )
                else:
                    processed_nodes.append(source_node)
            else:
                processed_nodes.append(source_node)
        return processed_nodes

    def _add_where_constraint(
        self,
        source_nodes: Sequence[DataflowPlanNode],
        where_filter_specs: Sequence[WhereFilterSpec],
        enabled_element_types: FrozenSet[LinkableElementType],
    ) -> Sequence[DataflowPlanNode]:
        """Processes where filter specs and evaluates their fitness for pushdown against the provided node set."""
        eligible_filter_specs_by_model: Dict[SemanticModelReference, Sequence[WhereFilterSpec]] = {}
        for spec in where_filter_specs:
            semantic_models = set(element.semantic_model_origin for element in spec.linkable_elements)
            invalid_element_types = [
                element for element in spec.linkable_elements if element.element_type not in enabled_element_types
            ]
            if len(semantic_models) == 1 and len(invalid_element_types) == 0:
                model = semantic_models.pop()
                eligible_filter_specs_by_model[model] = tuple(eligible_filter_specs_by_model.get(model, tuple())) + (
                    spec,
                )

        filtered_nodes: List[DataflowPlanNode] = []
        for source_node in source_nodes:
            node_semantic_models = tuple(source_node.as_plan().source_semantic_models)
            if len(node_semantic_models) == 1 and node_semantic_models[0] in eligible_filter_specs_by_model:
                eligible_filter_specs = eligible_filter_specs_by_model[node_semantic_models[0]]
                source_node_specs = self._node_data_set_resolver.get_output_data_set(source_node).instance_set.spec_set
                matching_filter_specs = [
                    filter_spec
                    for filter_spec in eligible_filter_specs
                    if all([spec in source_node_specs.linkable_specs for spec in filter_spec.linkable_specs])
                ]
                if len(matching_filter_specs) == 0:
                    filtered_nodes.append(source_node)
                else:
                    filtered_nodes.append(
                        WhereConstraintNode.create(parent_node=source_node, where_specs=matching_filter_specs)
                    )
            else:
                filtered_nodes.append(source_node)

        return filtered_nodes

    def _node_contains_entity(
        self,
        node: DataflowPlanNode,
        entity_reference: EntityReference,
    ) -> bool:
        """Returns true if the output of the node contains an entity of the given types."""
        data_set = self._node_data_set_resolver.get_output_data_set(node)

        for entity_instance_in_first_node in data_set.instance_set.entity_instances:
            entity_spec_in_first_node = entity_instance_in_first_node.spec

            if entity_spec_in_first_node.reference != entity_reference:
                continue

            assert (
                len(entity_instance_in_first_node.defined_from) == 1
            ), "Multiple items in defined_from not yet supported"

            entity = self._semantic_model_lookup.get_entity_in_semantic_model(
                entity_instance_in_first_node.defined_from[0]
            )
            if entity is None:
                raise RuntimeError(
                    f"Invalid SemanticModelElementReference {entity_instance_in_first_node.defined_from[0]}"
                )

            return True

        return False

    def _get_candidates_nodes_for_multi_hop(
        self, desired_linkable_spec: LinkableInstanceSpec, nodes: Sequence[DataflowPlanNode], join_type: SqlJoinType
    ) -> Sequence[MultiHopJoinCandidate]:
        """Assemble nodes representing all possible one-hop joins."""
        if len(desired_linkable_spec.entity_links) > MAX_JOIN_HOPS:
            raise NotImplementedError(
                f"Multi-hop joins with more than {MAX_JOIN_HOPS} entity links not yet supported. "
                f"Got: {desired_linkable_spec}"
            )
        if len(desired_linkable_spec.entity_links) != 2:
            return ()

        multi_hop_join_candidates: List[MultiHopJoinCandidate] = []
        logger.debug(LazyFormat(lambda: f"Creating nodes for {desired_linkable_spec}"))

        for first_node_that_could_be_joined in nodes:
            data_set_of_first_node_that_could_be_joined = self._node_data_set_resolver.get_output_data_set(
                first_node_that_could_be_joined
            )

            # When joining on the entity, the first node needs the first and second entity links.
            if not (
                self._node_contains_entity(
                    node=first_node_that_could_be_joined,
                    entity_reference=desired_linkable_spec.entity_links[0],
                )
                and self._node_contains_entity(
                    node=first_node_that_could_be_joined,
                    entity_reference=desired_linkable_spec.entity_links[1],
                )
            ):
                continue

            for second_node_that_could_be_joined in nodes:
                if not (
                    self._node_contains_entity(
                        node=second_node_that_could_be_joined,
                        entity_reference=desired_linkable_spec.entity_links[1],
                    )
                ):
                    continue

                # Avoid loops between the same semantic models.
                if second_node_that_could_be_joined.node_id == first_node_that_could_be_joined.node_id:
                    continue

                data_set_of_second_node_that_can_be_joined = self._node_data_set_resolver.get_output_data_set(
                    second_node_that_could_be_joined
                )

                # If the element name of the linkable spec doesn't exist in the joined data set, then it can't be
                # useful for obtaining that linkable spec.
                element_names_in_data_set = ToElementNameSet().transform(
                    data_set_of_second_node_that_can_be_joined.instance_set.spec_set
                )

                if desired_linkable_spec.element_name not in element_names_in_data_set:
                    continue

                # The first and second nodes are joined by this entity
                entity_reference_to_join_first_and_second_nodes = desired_linkable_spec.entity_links[1]

                if not self._join_evaluator.is_valid_instance_set_join(
                    left_instance_set=data_set_of_first_node_that_could_be_joined.instance_set,
                    right_instance_set=data_set_of_second_node_that_can_be_joined.instance_set,
                    on_entity_reference=entity_reference_to_join_first_and_second_nodes,
                    right_node_is_aggregated_to_entity=second_node_that_could_be_joined.aggregated_to_elements
                    == {entity_reference_to_join_first_and_second_nodes},
                ):
                    continue

                # filter measures out of joinable_node
                specs = data_set_of_second_node_that_can_be_joined.instance_set.spec_set
                filtered_joinable_node = FilterElementsNode.create(
                    parent_node=second_node_that_could_be_joined,
                    include_specs=group_specs_by_type(
                        specs.dimension_specs
                        + specs.entity_specs
                        + specs.time_dimension_specs
                        + specs.group_by_metric_specs
                    ),
                )

                join_on_partition_dimensions = self._partition_resolver.resolve_partition_dimension_joins(
                    left_node_spec_set=data_set_of_first_node_that_could_be_joined.instance_set.spec_set,
                    node_to_join_spec_set=data_set_of_second_node_that_can_be_joined.instance_set.spec_set,
                )
                join_on_partition_time_dimensions = self._partition_resolver.resolve_partition_time_dimension_joins(
                    left_node_spec_set=data_set_of_first_node_that_could_be_joined.instance_set.spec_set,
                    node_to_join_spec_set=data_set_of_second_node_that_can_be_joined.instance_set.spec_set,
                )

                multi_hop_join_candidates.append(
                    MultiHopJoinCandidate(
                        node_with_multi_hop_elements=JoinOnEntitiesNode.create(
                            left_node=first_node_that_could_be_joined,
                            join_targets=[
                                JoinDescription(
                                    join_node=filtered_joinable_node,
                                    join_on_entity=LinklessEntitySpec.from_reference(
                                        desired_linkable_spec.entity_links[1]
                                    ),
                                    join_on_partition_dimensions=join_on_partition_dimensions,
                                    join_on_partition_time_dimensions=join_on_partition_time_dimensions,
                                    join_type=join_type,
                                )
                            ],
                        ),
                        lineage=MultiHopJoinCandidateLineage(
                            first_node_to_join=first_node_that_could_be_joined,
                            second_node_to_join=second_node_that_could_be_joined,
                            # entity_spec_in_first_node should already not have entity links since we checked
                            # for that, but using this method for type checking.
                            join_second_node_by_entity=LinklessEntitySpec.from_reference(
                                desired_linkable_spec.entity_links[1]
                            ),
                        ),
                    )
                )

        for multi_hop_join_candidate in multi_hop_join_candidates:
            output_data_set = self._node_data_set_resolver.get_output_data_set(
                multi_hop_join_candidate.node_with_multi_hop_elements
            )
            logger.debug(
                LazyFormat(
                    lambda: f"Node {multi_hop_join_candidate.node_with_multi_hop_elements} has spec set:\n"
                    f"{mf_pformat(output_data_set.instance_set.spec_set)}"
                )
            )

        return multi_hop_join_candidates

    def add_multi_hop_joins(
        self,
        desired_linkable_specs: Sequence[LinkableInstanceSpec],
        nodes: Sequence[DataflowPlanNode],
        join_type: SqlJoinType,
    ) -> Sequence[DataflowPlanNode]:
        """Assemble nodes representing all possible one-hop joins."""
        all_multi_hop_join_candidates: List[MultiHopJoinCandidate] = []
        lineage_for_all_multi_hop_join_candidates: Set[MultiHopJoinCandidateLineage] = set()

        for desired_linkable_spec in desired_linkable_specs:
            for multi_hop_join_candidate in self._get_candidates_nodes_for_multi_hop(
                desired_linkable_spec=desired_linkable_spec, nodes=nodes, join_type=join_type
            ):
                # Dedupe candidates that are the same join.
                if multi_hop_join_candidate.lineage not in lineage_for_all_multi_hop_join_candidates:
                    all_multi_hop_join_candidates.append(multi_hop_join_candidate)
                    lineage_for_all_multi_hop_join_candidates.add(multi_hop_join_candidate.lineage)

        return list(x.node_with_multi_hop_elements for x in all_multi_hop_join_candidates) + list(nodes)

    def remove_unnecessary_nodes(
        self,
        desired_linkable_specs: Sequence[LinkableInstanceSpec],
        nodes: Sequence[DataflowPlanNode],
        metric_time_dimension_reference: TimeDimensionReference,
        time_spine_metric_time_nodes: Sequence[MetricTimeDimensionTransformNode],
    ) -> List[DataflowPlanNode]:
        """Filters out many of the nodes that can't possibly be useful for joins to obtain the desired linkable specs.

        A simple filter is to remove any nodes that don't share a common element with the query. Having a common element
        doesn't mean that the node will be useful, but not having common elements definitely means it's not useful.
        """
        relevant_element_names = {x.element_name for x in desired_linkable_specs}.union(
            {y.element_name for x in desired_linkable_specs for y in x.entity_links}
        )

        # The metric time dimension is used everywhere, so don't count it unless specifically desired in linkable spec
        # that has entity links.
        metric_time_dimension_used_in_linked_spec = any(
            [
                len(linkable_spec.entity_links) > 0
                and linkable_spec.element_name == metric_time_dimension_reference.element_name
                for linkable_spec in desired_linkable_specs
            ]
        )

        if (
            metric_time_dimension_reference.element_name in relevant_element_names
            and not metric_time_dimension_used_in_linked_spec
        ):
            relevant_element_names.remove(metric_time_dimension_reference.element_name)

        logger.debug(LazyFormat(lambda: f"Relevant names are: {relevant_element_names}"))

        relevant_nodes = []

        for node in nodes:
            logger.debug(LazyFormat(lambda: f"Examining {node} for pruning"))
            data_set = self._node_data_set_resolver.get_output_data_set(node)
            element_names_in_data_set = ToElementNameSet().transform(data_set.instance_set.spec_set)
            element_names_intersection = element_names_in_data_set.intersection(relevant_element_names)
            if len(element_names_intersection) > 0:
                logger.debug(
                    LazyFormat(
                        lambda: f"Including {node} since `element_names_intersection` is {element_names_intersection}"
                    )
                )
                relevant_nodes.append(node)
                continue

            # Used for group-by-item-values queries.
            if node in time_spine_metric_time_nodes:
                logger.debug(LazyFormat(lambda: f"Including {node} since it matches `time_spine_node`"))
                relevant_nodes.append(node)
                continue

        return relevant_nodes
