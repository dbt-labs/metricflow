import logging
from collections import defaultdict
from typing import Generic, Sequence, List, TypeVar, Optional, Dict

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
from metricflow.model.semantics.semantic_containers import DataSourceSemantics, MAX_JOIN_HOPS
from metricflow.plan_conversion.sql_dataset import SqlDataSet
from metricflow.spec_set_transforms import ToElementNameSet
from metricflow.specs import IdentifierSpec, InstanceSpec, LinkableInstanceSpec, TimeDimensionReference

SqlDataSetT = TypeVar("SqlDataSetT", bound=SqlDataSet)


logger = logging.getLogger(__name__)


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
        data_source_semantics: DataSourceSemantics,
        node_data_set_resolver: DataflowPlanNodeOutputDataSetResolver[SqlDataSetT],
    ):
        self._node_data_set_resolver = node_data_set_resolver
        self._partition_resolver = PartitionJoinResolver(data_source_semantics)

    def add_time_range_constraint(
        self,
        source_nodes: Sequence[BaseOutput[SqlDataSetT]],
        primary_time_dimension_name: str,
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
                        time_dimension_instance.spec.element_name == primary_time_dimension_name
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

    def add_multi_hop_joins(
        self, desired_linkable_specs: Sequence[LinkableInstanceSpec], nodes: Sequence[BaseOutput[SqlDataSetT]]
    ) -> Sequence[BaseOutput[SqlDataSetT]]:
        """Assemble nodes representing all possible one-hop joins"""

        if max(len(x.identifier_links) for x in desired_linkable_specs) > MAX_JOIN_HOPS:
            raise NotImplementedError("Multi-hop joins with more than 2 identifier links not yet supported")

        join_nodes: List[BaseOutput[SqlDataSetT]] = []
        identifier_node_index: Dict[IdentifierSpec, List[BaseOutput]] = defaultdict(list)

        for node in nodes:
            data_set: SqlDataSet = self._node_data_set_resolver.get_output_data_set(node)
            for identifier in data_set.instance_set.spec_set.identifier_specs:
                identifier_node_index[identifier].append(node)

        # Relevant identifier element names are the identifiers listed in the identifier links of the desired linkable
        # specs.
        relevant_identifier_element_names = set()
        # One of the element names in linkable_element_names must exist in the right data set for it to be useful in
        # satisfying the desired linkable specs.
        linkable_element_names = {x.element_name for x in desired_linkable_specs}
        for linkable_spec in desired_linkable_specs:
            if len(linkable_spec.identifier_links) == 2:
                relevant_identifier_element_names.add(linkable_spec.identifier_links[1].element_name)

        for node in nodes:
            data_set = self._node_data_set_resolver.get_output_data_set(node)

            for identifier in data_set.instance_set.spec_set.identifier_specs:

                # No need to create a join node using an identifier that can't be used to satisfy the query.
                if identifier.element_name not in relevant_identifier_element_names:
                    continue

                if len(identifier.identifier_links) > 0:
                    logger.warning(
                        f"Not constructing multihop joinable sources for identifier ({identifier}) - it has > 0 identifier_links"
                    )
                    continue

                for joinable_node in identifier_node_index[identifier]:
                    if joinable_node.node_id == node.node_id:
                        continue

                    joinable_node_data_set = self._node_data_set_resolver.get_output_data_set(joinable_node)

                    # If the element name of the linkable spec doesn't exist in the joined data set, then it can't be
                    # useful for obtaining that linkable spec.
                    if linkable_element_names is not None:
                        element_names_in_data_set = ToElementNameSet().transform(
                            joinable_node_data_set.instance_set.spec_set
                        )

                        if not element_names_in_data_set.intersection(linkable_element_names):
                            continue
                    # filter measures out of joinable_node
                    specs = joinable_node_data_set.instance_set.spec_set
                    pass_specs = InstanceSpec.merge(
                        specs.dimension_specs + specs.identifier_specs + specs.time_dimension_specs
                    )
                    filtered_joinable_node = FilterElementsNode(joinable_node, pass_specs)

                    join_on_partition_dimensions = self._partition_resolver.resolve_partition_dimension_joins(
                        start_node_spec_set=data_set.instance_set.spec_set,
                        node_to_join_spec_set=joinable_node_data_set.instance_set.spec_set,
                    )
                    join_on_partition_time_dimensions = self._partition_resolver.resolve_partition_time_dimension_joins(
                        start_node_spec_set=data_set.instance_set.spec_set,
                        node_to_join_spec_set=joinable_node_data_set.instance_set.spec_set,
                    )

                    join_nodes.append(
                        JoinToBaseOutputNode(
                            parent_node=node,
                            join_targets=[
                                JoinDescription(
                                    join_node=filtered_joinable_node,
                                    join_on_identifier=identifier.without_identifier_links(),
                                    join_on_partition_dimensions=join_on_partition_dimensions,
                                    join_on_partition_time_dimensions=join_on_partition_time_dimensions,
                                )
                            ],
                        )
                    )
        return list(nodes) + join_nodes

    def remove_unnecessary_nodes(
        self,
        desired_linkable_specs: Sequence[LinkableInstanceSpec],
        nodes: Sequence[BaseOutput[SqlDataSetT]],
        primary_time_dimension_reference: TimeDimensionReference,
    ) -> Sequence[BaseOutput[SqlDataSetT]]:
        """Filters out many of the nodes that can't possibly be useful for joins to obtain the desired linkable specs.

        A simple filter is to remove any nodes that don't share a common element with the query. Having a common element
        doesn't mean that the node will be useful, but not having common elements definitely means it's not useful.
        """
        relevant_element_names = {x.element_name for x in desired_linkable_specs}.union(
            {y.element_name for x in desired_linkable_specs for y in x.identifier_links}
        )

        # The primary time dimension is used everywhere, so don't count it unless specifically desired in linkable spec
        # that has identifier links.
        primary_time_dimension_used_in_linked_spec = any(
            [
                len(linkable_spec.identifier_links) > 0
                and linkable_spec.element_name == primary_time_dimension_reference.element_name
                for linkable_spec in desired_linkable_specs
            ]
        )

        if (
            primary_time_dimension_reference.element_name in relevant_element_names
            and not primary_time_dimension_used_in_linked_spec
        ):
            relevant_element_names.remove(primary_time_dimension_reference.element_name)

        logger.info(f"Relevant names are: {relevant_element_names}")

        relevant_nodes = []

        for node in nodes:
            data_set = self._node_data_set_resolver.get_output_data_set(node)
            element_names_in_data_set = ToElementNameSet().transform(data_set.instance_set.spec_set)

            if len(element_names_in_data_set.intersection(relevant_element_names)) > 0:
                relevant_nodes.append(node)

        return relevant_nodes
