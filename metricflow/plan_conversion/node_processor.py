from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Sequence, Set

from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from dbt_semantic_interfaces.references import EntityReference, TimeDimensionReference

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.partitions import PartitionJoinResolver
from metricflow.dataflow.dataflow_plan import (
    BaseOutput,
    ConstrainTimeRangeNode,
    FilterElementsNode,
    JoinDescription,
    JoinToBaseOutputNode,
)
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS, SemanticModelJoinEvaluator
from metricflow.protocols.semantics import SemanticModelAccessor
from metricflow.specs.spec_set_transforms import ToElementNameSet
from metricflow.specs.specs import InstanceSpecSet, LinkableInstanceSpec, LinklessEntitySpec

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
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

    first_node_to_join: BaseOutput
    second_node_to_join: BaseOutput
    join_second_node_by_entity: LinklessEntitySpec


@dataclass(frozen=True)
class MultiHopJoinCandidate:
    """A candidate node containing linkable specs that is join of other nodes. It's used to resolve multi-hop queries.

    Also see MultiHopJoinCandidateLineage.
    """

    node_with_multi_hop_elements: BaseOutput
    lineage: MultiHopJoinCandidateLineage


class PreDimensionJoinNodeProcessor:
    """Processes source nodes before measures are joined to dimensions.

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

    def __init__(  # noqa: D
        self,
        semantic_model_lookup: SemanticModelAccessor,
        node_data_set_resolver: DataflowPlanNodeOutputDataSetResolver,
    ):
        self._node_data_set_resolver = node_data_set_resolver
        self._partition_resolver = PartitionJoinResolver(semantic_model_lookup)
        self._semantic_model_lookup = semantic_model_lookup
        self._join_evaluator = SemanticModelJoinEvaluator(semantic_model_lookup)

    def add_time_range_constraint(
        self,
        source_nodes: Sequence[BaseOutput],
        metric_time_dimension_reference: TimeDimensionReference,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
    ) -> Sequence[BaseOutput]:
        """Adds a time range constraint node to the input nodes."""
        processed_nodes: List[BaseOutput] = []
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
        node: BaseOutput,
        entity_reference: EntityReference,
    ) -> bool:
        """Returns true if the output of the node contains an entity of the given types."""
        data_set = self._node_data_set_resolver.get_output_data_set(node)

        for entity_instance_in_first_node in data_set.instance_set.entity_instances:
            entity_spec_in_first_node = entity_instance_in_first_node.spec

            if entity_spec_in_first_node.reference != entity_reference:
                continue

            if len(entity_spec_in_first_node.entity_links) > 0:
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
        self,
        desired_linkable_spec: LinkableInstanceSpec,
        nodes: Sequence[BaseOutput],
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
                ):
                    continue

                # filter measures out of joinable_node
                specs = data_set_of_second_node_that_can_be_joined.instance_set.spec_set
                filtered_joinable_node = FilterElementsNode(
                    parent_node=second_node_that_could_be_joined,
                    include_specs=InstanceSpecSet.create_from_linkable_specs(
                        specs.dimension_specs + specs.entity_specs + specs.time_dimension_specs
                    ),
                )

                join_on_partition_dimensions = self._partition_resolver.resolve_partition_dimension_joins(
                    start_node_spec_set=data_set_of_first_node_that_could_be_joined.instance_set.spec_set,
                    node_to_join_spec_set=data_set_of_second_node_that_can_be_joined.instance_set.spec_set,
                )
                join_on_partition_time_dimensions = self._partition_resolver.resolve_partition_time_dimension_joins(
                    start_node_spec_set=data_set_of_first_node_that_could_be_joined.instance_set.spec_set,
                    node_to_join_spec_set=data_set_of_second_node_that_can_be_joined.instance_set.spec_set,
                )

                multi_hop_join_candidates.append(
                    MultiHopJoinCandidate(
                        node_with_multi_hop_elements=JoinToBaseOutputNode(
                            left_node=first_node_that_could_be_joined,
                            join_targets=[
                                JoinDescription(
                                    join_node=filtered_joinable_node,
                                    join_on_entity=LinklessEntitySpec.from_reference(
                                        desired_linkable_spec.entity_links[1]
                                    ),
                                    join_on_partition_dimensions=join_on_partition_dimensions,
                                    join_on_partition_time_dimensions=join_on_partition_time_dimensions,
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
                f"{pformat_big_objects(output_data_set.instance_set.spec_set)}"
            )

        return multi_hop_join_candidates

    def add_multi_hop_joins(
        self, desired_linkable_specs: Sequence[LinkableInstanceSpec], nodes: Sequence[BaseOutput]
    ) -> Sequence[BaseOutput]:
        """Assemble nodes representing all possible one-hop joins."""
        all_multi_hop_join_candidates: List[MultiHopJoinCandidate] = []
        lineage_for_all_multi_hop_join_candidates: Set[MultiHopJoinCandidateLineage] = set()

        for desired_linkable_spec in desired_linkable_specs:
            for multi_hop_join_candidate in self._get_candidates_nodes_for_multi_hop(
                desired_linkable_spec=desired_linkable_spec,
                nodes=nodes,
            ):
                # Dedupe candidates that are the same join.
                if multi_hop_join_candidate.lineage not in lineage_for_all_multi_hop_join_candidates:
                    all_multi_hop_join_candidates.append(multi_hop_join_candidate)
                    lineage_for_all_multi_hop_join_candidates.add(multi_hop_join_candidate.lineage)

        return list(x.node_with_multi_hop_elements for x in all_multi_hop_join_candidates) + list(nodes)

    def remove_unnecessary_nodes(
        self,
        desired_linkable_specs: Sequence[LinkableInstanceSpec],
        nodes: Sequence[BaseOutput],
        metric_time_dimension_reference: TimeDimensionReference,
    ) -> Sequence[BaseOutput]:
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
            data_set = self._node_data_set_resolver.get_output_data_set(node)
            element_names_in_data_set = ToElementNameSet().transform(data_set.instance_set.spec_set)

            if len(element_names_in_data_set.intersection(relevant_element_names)) > 0:
                relevant_nodes.append(node)

        return relevant_nodes
