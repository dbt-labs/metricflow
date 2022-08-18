import logging
from dataclasses import dataclass
from typing import Generic, Sequence, List, TypeVar, Optional, Set

from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.partitions import PartitionJoinResolver
from metricflow.dataflow.dataflow_plan import (
    ConstrainTimeRangeNode,
    BaseOutput,
    JoinToBaseOutputNode,
    FilterElementsNode,
    JoinDescription,
)
from metricflow.model.objects.elements.identifier import IdentifierType
from metricflow.model.semantics.semantic_containers import MAX_JOIN_HOPS
from metricflow.object_utils import pformat_big_objects
from metricflow.plan_conversion.sql_dataset import SqlDataSet
from metricflow.protocols.semantics import DataSourceSemanticsAccessor
from metricflow.references import TimeDimensionReference, IdentifierReference
from metricflow.spec_set_transforms import ToElementNameSet
from metricflow.specs import InstanceSpec, LinkableInstanceSpec, LinklessIdentifierSpec

SqlDataSetT = TypeVar("SqlDataSetT", bound=SqlDataSet)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MultiHopJoinCandidateLineage(Generic[SqlDataSetT]):
    """Describes how the multi-hop join candidate was formed.

    For example, if
    * bridge_source has the primary identifier account_id and the foreign identifier customer_id
    * customers_source has the primary identifier customer_id and dimension country
    * transactions_source has the transactions measure and the account_id foreign identifier

    Then the candidate lineage can be describe as
        first_node_to_join = bridge_source
        second_node_to_join = customers_source
    to get the country dimension.
    """

    first_node_to_join: BaseOutput[SqlDataSetT]
    second_node_to_join: BaseOutput[SqlDataSetT]
    join_second_node_by_identifier: LinklessIdentifierSpec


@dataclass(frozen=True)
class MultiHopJoinCandidate(Generic[SqlDataSetT]):
    """A candidate node containing linkable specs that is join of other nodes. It's used to resolve multi-hop queries.

    Also see MultiHopJoinCandidateLineage.
    """

    node_with_multi_hop_elements: BaseOutput[SqlDataSetT]
    lineage: MultiHopJoinCandidateLineage[SqlDataSetT]


class PreDimensionJoinNodeProcessor(Generic[SqlDataSetT]):
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
        data_source_semantics: DataSourceSemanticsAccessor,
        node_data_set_resolver: DataflowPlanNodeOutputDataSetResolver[SqlDataSetT],
    ):
        self._node_data_set_resolver = node_data_set_resolver
        self._partition_resolver = PartitionJoinResolver(data_source_semantics)
        self._data_source_semantics = data_source_semantics

    def add_time_range_constraint(
        self,
        source_nodes: Sequence[BaseOutput[SqlDataSetT]],
        metric_time_dimension_reference: TimeDimensionReference,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
    ) -> Sequence[BaseOutput[SqlDataSetT]]:
        """Adds a time range constraint node to the input nodes."""
        processed_nodes: List[BaseOutput[SqlDataSetT]] = []
        for source_node in source_nodes:

            # Constrain the time range if specified.
            if time_range_constraint:
                node_output_data_set = self._node_data_set_resolver.get_output_data_set(source_node)
                constrain_time = False
                for time_dimension_instance in node_output_data_set.instance_set.time_dimension_instances:
                    if (
                        time_dimension_instance.spec.reference == metric_time_dimension_reference
                        and len(time_dimension_instance.spec.identifier_links) == 0
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

    def _node_contains_identifier(
        self,
        node: BaseOutput[SqlDataSetT],
        identifier_reference: IdentifierReference,
        valid_types: Set[IdentifierType],
    ) -> bool:
        """Returns true if the output of the node contains an identifier of the given types."""
        data_set = self._node_data_set_resolver.get_output_data_set(node)

        for identifier_instance_in_first_node in data_set.instance_set.identifier_instances:
            identifier_spec_in_first_node = identifier_instance_in_first_node.spec

            if identifier_spec_in_first_node.reference != identifier_reference:
                continue

            if len(identifier_spec_in_first_node.identifier_links) > 0:
                continue

            assert (
                len(identifier_instance_in_first_node.defined_from) == 1
            ), "Multiple items in defined_from not yet supported"

            identifier = self._data_source_semantics.get_identifier_in_data_source(
                identifier_instance_in_first_node.defined_from[0]
            )
            if identifier is None:
                raise RuntimeError(
                    f"Invalid DataSourceElementReference {identifier_instance_in_first_node.defined_from[0]}"
                )

            if identifier.type in valid_types:
                return True

        return False

    def _get_candidates_nodes_for_multi_hop(
        self,
        desired_linkable_spec: LinkableInstanceSpec,
        nodes: Sequence[BaseOutput[SqlDataSetT]],
    ) -> Sequence[MultiHopJoinCandidate]:
        """Assemble nodes representing all possible one-hop joins"""

        if len(desired_linkable_spec.identifier_links) > MAX_JOIN_HOPS:
            raise NotImplementedError(
                f"Multi-hop joins with more than {MAX_JOIN_HOPS} identifier links not yet supported. "
                f"Got: {desired_linkable_spec}"
            )
        if len(desired_linkable_spec.identifier_links) != 2:
            return ()

        multi_hop_join_candidates: List[MultiHopJoinCandidate] = []
        logger.info(f"Creating nodes for {desired_linkable_spec}")

        for first_node_that_could_be_joined in nodes:
            data_set = self._node_data_set_resolver.get_output_data_set(first_node_that_could_be_joined)

            # Fan-out joins aren't supported, so the identifiers must be of these types to join.
            if not (
                self._node_contains_identifier(
                    node=first_node_that_could_be_joined,
                    identifier_reference=desired_linkable_spec.identifier_links[0],
                    valid_types={IdentifierType.PRIMARY, IdentifierType.UNIQUE},
                )
                and self._node_contains_identifier(
                    node=first_node_that_could_be_joined,
                    identifier_reference=desired_linkable_spec.identifier_links[1],
                    valid_types={IdentifierType.PRIMARY, IdentifierType.UNIQUE, IdentifierType.FOREIGN},
                )
            ):
                continue

            for second_node_that_could_be_joined in nodes:
                if not (
                    self._node_contains_identifier(
                        node=second_node_that_could_be_joined,
                        identifier_reference=desired_linkable_spec.identifier_links[1],
                        valid_types={IdentifierType.PRIMARY, IdentifierType.UNIQUE},
                    )
                ):
                    continue

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

                # filter measures out of joinable_node
                specs = data_set_of_second_node_that_can_be_joined.instance_set.spec_set
                pass_specs = InstanceSpec.merge(
                    specs.dimension_specs + specs.identifier_specs + specs.time_dimension_specs
                )
                filtered_joinable_node = FilterElementsNode(second_node_that_could_be_joined, pass_specs)

                join_on_partition_dimensions = self._partition_resolver.resolve_partition_dimension_joins(
                    start_node_spec_set=data_set.instance_set.spec_set,
                    node_to_join_spec_set=data_set_of_second_node_that_can_be_joined.instance_set.spec_set,
                )
                join_on_partition_time_dimensions = self._partition_resolver.resolve_partition_time_dimension_joins(
                    start_node_spec_set=data_set.instance_set.spec_set,
                    node_to_join_spec_set=data_set_of_second_node_that_can_be_joined.instance_set.spec_set,
                )

                multi_hop_join_candidates.append(
                    MultiHopJoinCandidate(
                        node_with_multi_hop_elements=JoinToBaseOutputNode(
                            parent_node=first_node_that_could_be_joined,
                            join_targets=[
                                JoinDescription(
                                    join_node=filtered_joinable_node,
                                    join_on_identifier=LinklessIdentifierSpec.from_reference(
                                        desired_linkable_spec.identifier_links[1]
                                    ),
                                    join_on_partition_dimensions=join_on_partition_dimensions,
                                    join_on_partition_time_dimensions=join_on_partition_time_dimensions,
                                )
                            ],
                        ),
                        lineage=MultiHopJoinCandidateLineage(
                            first_node_to_join=first_node_that_could_be_joined,
                            second_node_to_join=second_node_that_could_be_joined,
                            # identifier_spec_in_first_node should already not have identifier links since we checked
                            # for that, but using this method for type checking.
                            join_second_node_by_identifier=LinklessIdentifierSpec.from_reference(
                                desired_linkable_spec.identifier_links[1]
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
        self, desired_linkable_specs: Sequence[LinkableInstanceSpec], nodes: Sequence[BaseOutput[SqlDataSetT]]
    ) -> Sequence[BaseOutput[SqlDataSetT]]:
        """Assemble nodes representing all possible one-hop joins"""

        all_multi_hop_join_candidates: List[MultiHopJoinCandidate[SqlDataSetT]] = []
        lineage_for_all_multi_hop_join_candidates: Set[MultiHopJoinCandidateLineage[SqlDataSetT]] = set()

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
        nodes: Sequence[BaseOutput[SqlDataSetT]],
        metric_time_dimension_reference: TimeDimensionReference,
    ) -> Sequence[BaseOutput[SqlDataSetT]]:
        """Filters out many of the nodes that can't possibly be useful for joins to obtain the desired linkable specs.

        A simple filter is to remove any nodes that don't share a common element with the query. Having a common element
        doesn't mean that the node will be useful, but not having common elements definitely means it's not useful.
        """
        relevant_element_names = {x.element_name for x in desired_linkable_specs}.union(
            {y.element_name for x in desired_linkable_specs for y in x.identifier_links}
        )

        # The metric time dimension is used everywhere, so don't count it unless specifically desired in linkable spec
        # that has identifier links.
        metric_time_dimension_used_in_linked_spec = any(
            [
                len(linkable_spec.identifier_links) > 0
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
