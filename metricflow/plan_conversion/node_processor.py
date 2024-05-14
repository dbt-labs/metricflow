from __future__ import annotations

import dataclasses
import logging
from enum import Enum
from typing import List, Optional, Sequence, Set

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.references import EntityReference, TimeDimensionReference
from metricflow_semantics.filters.time_constraint import TimeRangeConstraint
from metricflow_semantics.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS
from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow_semantics.specs.spec_classes import LinkableInstanceSpec, LinklessEntitySpec
from metricflow_semantics.specs.spec_set import group_specs_by_type
from metricflow_semantics.specs.spec_set_transforms import ToElementNameSet
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


class PredicatePushdownState(Enum):
    """Enumeration of constraint states describing when predicate pushdown may operate in a dataflow plan.

    This is necessary for holistic checks against the set of potentially enabled pushdown operations, because the
    scenario where we only allow time range updates requires careful overriding of other pushdown properties.

    Note: the time_range_only state is a backwards compatibility shim for use with conversion metrics while
    we determine how best to support predicate pushdown for conversion metrics. It may have longer term utility,
    but ideally we'd collapse this into a single enabled/disabled boolean property.
    """

    DISABLED = "disabled"
    FULLY_ENABLED = "fully_enabled"
    ENABLED_FOR_TIME_RANGE_ONLY = "time_range_only"


@dataclasses.dataclass(frozen=True)
class PredicatePushdownParameters:
    """Container class for managing information about whether and how to do filter predicate pushdown.

    The time_range_constraint property holds the time window for setting up a time range filter expression.
    """

    _PREDICATE_METADATA_KEY = "is_predicate_property"

    time_range_constraint: Optional[TimeRangeConstraint] = dataclasses.field(metadata={_PREDICATE_METADATA_KEY: True})
    pushdown_state: PredicatePushdownState = PredicatePushdownState.FULLY_ENABLED

    def __post_init__(self) -> None:
        """Validation to ensure pushdown properties are configured correctly.

        In particular, this asserts that cases where pushdown is disabled cannot leak pushdown operations via
        outside property access - if pushdown is disabled, no further pushdown operations of any kind are allowed
        on that particular code branch.
        """
        if self.pushdown_state is PredicatePushdownState.FULLY_ENABLED:
            return

        invalid_predicate_property_names = {
            field.name for field in dataclasses.fields(self) if field.metadata.get(self._PREDICATE_METADATA_KEY)
        }

        if self.pushdown_state is PredicatePushdownState.ENABLED_FOR_TIME_RANGE_ONLY:
            # We don't do validation for time range constraint configuration - having None set in this state is the
            # equivalent of disabling predicate pushdown, but that time constraint value might be updated later,
            # and so we do not block overrides to (or from) None to avoid meaningless bookkeeping at callsites.
            # Also, we keep the magic string name Python uses hidden in here instead of expanding access to it.
            invalid_predicate_property_names.remove("time_range_constraint")

        instance_configuration = dataclasses.asdict(self)
        invalid_disabled_properties = {
            property_name: value
            for property_name, value in instance_configuration.items()
            if property_name in invalid_predicate_property_names and value is not None
        }

        assert not invalid_disabled_properties, (
            "Invalid pushdown parameter configuration! Disabled pushdown parameters cannot have properties "
            "set that may lead to improper access and use in other contexts, as that can lead to unintended "
            "filtering operations in cases where these properties are accessed without appropriate checks against "
            "pushdown configuration. This indicates that pushdown is configured for limited scope operations, but "
            f"other predicate properties are set to non-None values.\nFull configuration: {instance_configuration}\n"
            f"Invalid predicate properties: {invalid_disabled_properties}"
        )

    @property
    def is_pushdown_enabled(self) -> bool:
        """Convenience accessor for checking that no pushdown constraints exist."""
        pushdown_state = self.pushdown_state
        if pushdown_state is PredicatePushdownState.DISABLED:
            return False
        elif pushdown_state is PredicatePushdownState.FULLY_ENABLED:
            return True
        elif pushdown_state is PredicatePushdownState.ENABLED_FOR_TIME_RANGE_ONLY:
            return True
        else:
            return assert_values_exhausted(pushdown_state)

    @staticmethod
    def with_time_range_constraint(
        original_pushdown_params: PredicatePushdownParameters, time_range_constraint: Optional[TimeRangeConstraint]
    ) -> PredicatePushdownParameters:
        """Factory method for overriding the time range constraint value in a given set of pushdown parameters.

        This allows for crude updates to the core time range constraint, including selectively disabling time range
        predicate pushdown in certain sub-branches of the dataflow plan, such as in complex cases involving time spine
        joins and cumulative metrics.
        """
        if original_pushdown_params.is_pushdown_enabled:
            return PredicatePushdownParameters(
                time_range_constraint=time_range_constraint,
            )
        else:
            return original_pushdown_params

    @staticmethod
    def with_pushdown_disabled() -> PredicatePushdownParameters:
        """Factory method for disabling predicate pushdown for all parameter types.

        This is useful in cases where there is a branched path where pushdown should be disabled in one branch while the
        other may remain eligible. For example, a join linkage where one side of the join contains an unsupported
        configuration might send a disabled copy of the pushdown parameters down that path while retaining the potential
        for using another path.
        """
        return PredicatePushdownParameters(time_range_constraint=None, pushdown_state=PredicatePushdownState.DISABLED)


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

    def add_time_range_constraint(
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
                        ConstrainTimeRangeNode(parent_node=source_node, time_range_constraint=time_range_constraint)
                    )
                else:
                    processed_nodes.append(source_node)
            else:
                processed_nodes.append(source_node)
        return processed_nodes

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
        logger.info(f"Creating nodes for {desired_linkable_spec}")

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
                filtered_joinable_node = FilterElementsNode(
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
                        node_with_multi_hop_elements=JoinOnEntitiesNode(
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
                f"Node {multi_hop_join_candidate.node_with_multi_hop_elements} has spec set:\n"
                f"{mf_pformat(output_data_set.instance_set.spec_set)}"
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
        time_spine_node: MetricTimeDimensionTransformNode,
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

        logger.info(f"Relevant names are: {relevant_element_names}")

        relevant_nodes = []

        for node in nodes:
            logger.debug(f"Examining {node} for pruning")
            data_set = self._node_data_set_resolver.get_output_data_set(node)
            element_names_in_data_set = ToElementNameSet().transform(data_set.instance_set.spec_set)
            element_names_intersection = element_names_in_data_set.intersection(relevant_element_names)
            if len(element_names_intersection) > 0:
                logger.debug(f"Including {node} since `element_names_intersection` is {element_names_intersection}")
                relevant_nodes.append(node)
                continue

            # Used for group-by-item-values queries.
            if node == time_spine_node:
                logger.debug(f"Including {node} since it matches `time_spine_node`")
                relevant_nodes.append(node)
                continue

        return relevant_nodes
