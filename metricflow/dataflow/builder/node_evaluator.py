"""This module aims to help figure out the answer to the following scenario:

I have a node containing a measure that's needed for a metric, and I also have a list of dimensions that I need for that
measure.

Can I join that node with other nodes using a common identifier to get those dimensions? If so, what are those nodes,
what identifier should I join on, and if I do that join, what dimensions can be retrieved?

Note: the term "dimension" is used below, but it actually refers to any LinkableInstance. Also, when you see the term
"start_node", think "node containing the measure". Using the term "start_node" as the scenario of joining measure nodes
to dimension nodes is an easy case to explain, but there are non-measure nodes that can be joined with dimension nodes
to realize other planned features.
"""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass
from typing import List, Sequence, Generic, TypeVar, Tuple

from metricflow.model.objects.elements.identifier import IdentifierType
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.partitions import PartitionJoinResolver
from metricflow.dataflow.dataflow_plan import (
    JoinDescription,
    BaseOutput,
    PartitionDimensionJoinDescription,
    PartitionTimeDimensionJoinDescription,
)
from metricflow.instances import InstanceSet
from metricflow.object_utils import pformat_big_objects
from metricflow.plan_conversion.sql_dataset import SqlDataSet
from metricflow.specs import (
    LinkableInstanceSpec,
    LinklessIdentifierSpec,
    InstanceSpecSet,
)
from metricflow.model.semantics.semantic_containers import DataSourceSemantics

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JoinLinkableInstancesRecipe:
    """A recipe for how to join a node containing linkable instances to the "start_node".

    node_to_join contains the linkable instances that are needed - it should be filtered so that the output data set of
    that node only includes the identifier instance for the join and the instances associated with
    satisfiable_linkable_specs.
    """

    node_to_join: BaseOutput
    # The identifier to join "node_to_join" on.
    join_on_identifier: LinklessIdentifierSpec
    # The linkable instances from the query that can be satisfied if we join this node. Note that this is different from
    # the linkable specs in the node that can help to satisfy the query. e.g. "user_id__country" might be one of the
    # "satisfiable_linkable_specs", but "country" is the linkable spec in the node.
    satisfiable_linkable_specs: List[LinkableInstanceSpec]

    # The partitions to join on, if there are matching partitions between the start_node and node_to_join.
    join_on_partition_dimensions: Tuple[PartitionDimensionJoinDescription, ...]
    join_on_partition_time_dimensions: Tuple[PartitionTimeDimensionJoinDescription, ...]

    @property
    def join_description(self) -> JoinDescription:
        """The recipe as a join description to use in the dataflow plan node."""
        return JoinDescription(
            join_node=self.node_to_join,
            join_on_identifier=self.join_on_identifier,
            join_on_partition_dimensions=self.join_on_partition_dimensions,
            join_on_partition_time_dimensions=self.join_on_partition_time_dimensions,
        )


@dataclass(frozen=True)
class LinkableInstanceSatisfiabilityEvaluation:
    """Evaluation for a node on the ability to get linkable instances that we need to resolve a query.

    Includes whether the node contains the linkable instances that we need (local_linkable_specs), or if it can have
    them joined in (joinable_linkable_specs). Linkable instances that can't be satisfied either locally or via joins are
    listed in unjoinable_linkable_specs.
    """

    local_linkable_specs: Tuple[LinkableInstanceSpec, ...]
    joinable_linkable_specs: Tuple[LinkableInstanceSpec, ...]
    join_recipes: Tuple[JoinLinkableInstancesRecipe, ...]
    unjoinable_linkable_specs: Tuple[LinkableInstanceSpec, ...]


SourceDataSetT = TypeVar("SourceDataSetT", bound=SqlDataSet)


class NodeEvaluatorForLinkableInstances(Generic[SourceDataSetT]):
    """Helps to evaluate if linkable instances can be obtained using the given node, with joins if necessary.

    For example, consider a "start_node" containing the "bookings" measure, "is_instant" dimension, and "listing_id"
    identifier with nodes_available_for_joins including a node with the "listing_id" identifier, and the "country"
    dimension.

    We want to know if we can get "bookings", "is_instant", "listing_id__country" using the start_node. The result
    should be that we know: "is_instant" is available locally (i.e. in the same node), and if we join another node
    containing "listing_id" and "country" by "listing_id", we can get "listing_id__country".
    """

    def __init__(
        self,
        data_source_semantics: DataSourceSemantics,
        nodes_available_for_joins: Sequence[BaseOutput[SourceDataSetT]],
        node_data_set_resolver: DataflowPlanNodeOutputDataSetResolver[SourceDataSetT],
    ) -> None:
        """Constructor

        Args:
            data_source_semantics: Needed to resolve partition dimensions.
            nodes_available_for_joins: Nodes that contain linkable instances and may be joined with the "start_node"
            (e.g. the node containing a desired measure) to retrieve the needed linkable instances.
            node_data_set_resolver: Figures out what data set is output by a node.
        """
        self._data_source_semantics = data_source_semantics
        self._nodes_available_for_joins = nodes_available_for_joins
        self._node_data_set_resolver = node_data_set_resolver
        self._partition_resolver = PartitionJoinResolver(self._data_source_semantics)

    def _find_joinable_candidate_nodes_that_can_satisfy_linkable_specs(
        self,
        start_node_spec_set: InstanceSpecSet,
        needed_linkable_specs: List[LinkableInstanceSpec],
    ) -> List[JoinLinkableInstancesRecipe]:
        """Get nodes that can be joined to get 1 or more of the "needed_linkable_specs".

        The returned list is ordered by the number of "needed_linkable_specs" that it can satisfy.
        """
        candidates_for_join: List[JoinLinkableInstancesRecipe] = []

        for node in self._nodes_available_for_joins:
            data_set: SqlDataSet = self._node_data_set_resolver.get_output_data_set(node)
            linkable_specs_in_node = data_set.instance_set.spec_set.linkable_specs
            identifier_specs_in_node = data_set.instance_set.spec_set.identifier_specs

            # For each unlinked identifier in the data set, create a candidate for joining.
            # For a data set to be useful for satisfying a linkable spec, it needs to have the identifier
            # and the linkable spec without the identifier. This allows joining based on the identifier, which will
            # then produce the linkable spec. See comments further below for more details.

            for identifier_spec_in_node in identifier_specs_in_node:
                # If an identifier has links, what that means and whether it can be used is unclear at the moment,
                # so skip it.
                if len(identifier_spec_in_node.identifier_links) > 0:
                    continue

                identifier_instance = None
                for instance in data_set.instance_set.identifier_instances:
                    if instance.spec == identifier_spec_in_node:
                        identifier_instance = instance
                        break

                if identifier_instance is None:
                    raise RuntimeError(f"Could not find identifier instance with name ({identifier_spec_in_node})")
                ident = self._data_source_semantics.get_data_source_element(identifier_instance.defined_from[0])
                if ident is None:
                    raise RuntimeError(f"Invalid DataSourceElementReference {identifier_instance.defined_from[0]}")
                if ident.type not in (IdentifierType.PRIMARY, IdentifierType.UNIQUE):
                    continue

                linkless_identifier_spec_in_node = LinklessIdentifierSpec.from_element_name(
                    identifier_spec_in_node.element_name
                )

                satisfiable_linkable_specs = []
                for needed_linkable_spec in needed_linkable_specs:
                    assert (
                        len(needed_linkable_spec.identifier_links) != 0
                    ), f"Invalid needed linkable spec passed in {needed_linkable_spec}"

                    # If the identifier in the data set matches the link, then it can be used for joins. For example,
                    # if the node has the identifier "user_id", and dimension "country" then it can be used for
                    # satisfying "user_id__country".
                    #
                    # Multi-hop example:
                    # required_linkable_spec = "user_id__device_id__platform"
                    # identifier_spec_in_data_set = "user_id"
                    #
                    # Then the data set must contain "device_id__platform", which is realized with
                    #
                    # required_linkable_spec.remove_first_identifier_link()
                    #
                    # We might also need to check the identifier type and see if it's the type of join we're allowing,
                    # but since we're doing all left joins now, it's been left out.

                    required_identifier_matches_data_set_identifier = (
                        needed_linkable_spec.identifier_links[0] == linkless_identifier_spec_in_node
                    )
                    needed_linkable_spec_in_node = (
                        needed_linkable_spec.without_first_identifier_link() in linkable_specs_in_node
                    )
                    if required_identifier_matches_data_set_identifier and needed_linkable_spec_in_node:
                        satisfiable_linkable_specs.append(needed_linkable_spec)

                # If this node can satisfy some linkable specs, it could be useful to join on, so add it to the
                # candidate list.
                if len(satisfiable_linkable_specs) > 0:
                    join_on_partition_dimensions = self._partition_resolver.resolve_partition_dimension_joins(
                        start_node_spec_set=start_node_spec_set,
                        node_to_join_spec_set=data_set.instance_set.spec_set,
                    )
                    join_on_partition_time_dimensions = self._partition_resolver.resolve_partition_time_dimension_joins(
                        start_node_spec_set=start_node_spec_set,
                        node_to_join_spec_set=data_set.instance_set.spec_set,
                    )
                    candidates_for_join.append(
                        JoinLinkableInstancesRecipe(
                            node_to_join=node,
                            join_on_identifier=linkless_identifier_spec_in_node,
                            satisfiable_linkable_specs=satisfiable_linkable_specs,
                            join_on_partition_dimensions=join_on_partition_dimensions,
                            join_on_partition_time_dimensions=join_on_partition_time_dimensions,
                        )
                    )

        # Return with the candidate set that can satisfy the most linkable specs at the front.
        return sorted(
            candidates_for_join,
            key=lambda x: len(x.satisfiable_linkable_specs),
            reverse=True,
        )

    @staticmethod
    def _update_candidates_that_can_satisfy_linkable_specs(
        candidates_for_join: List[JoinLinkableInstancesRecipe],
        already_satisfisfied_linkable_specs: List[LinkableInstanceSpec],
    ) -> List[JoinLinkableInstancesRecipe]:
        """Update / filter candidates_for_join based on linkable instance specs that we have already satisfied.

        Those linkable instances are no longer needed because those were satisfied previously by joining a node
        containing some needed linkable instances to the "start_node". This method will then remove the satisfied
        linkable instances from the candidate recipes, and if a candidate recipe doesn't have any linkable instances
        that can help satisfy the query, it is removed.
        """
        updated_candidate_data_sets: List[JoinLinkableInstancesRecipe] = []
        for candidate_for_join in candidates_for_join:
            updated_satisfiable_linkable_specs = list(
                set(candidate_for_join.satisfiable_linkable_specs) - set(already_satisfisfied_linkable_specs)
            )

            if len(updated_satisfiable_linkable_specs) > 0:
                updated_candidate_data_sets.append(
                    JoinLinkableInstancesRecipe(
                        node_to_join=candidate_for_join.node_to_join,
                        join_on_identifier=candidate_for_join.join_on_identifier,
                        satisfiable_linkable_specs=updated_satisfiable_linkable_specs,
                        join_on_partition_dimensions=candidate_for_join.join_on_partition_dimensions,
                        join_on_partition_time_dimensions=candidate_for_join.join_on_partition_time_dimensions,
                    )
                )
        return sorted(
            updated_candidate_data_sets,
            key=lambda x: len(x.satisfiable_linkable_specs),
            reverse=True,
        )

    def evaluate_node(
        self,
        start_node: BaseOutput[SourceDataSetT],
        required_linkable_specs: List[LinkableInstanceSpec],
    ) -> LinkableInstanceSatisfiabilityEvaluation:
        """Evaluates if the "required_linkable_specs" can be realized by joining the "start_node" with other nodes.

        In other words, given the data set associated with "start_node":

        * Can all "required_linkable_specs" be retrieved from the start_node? (These would be considered "local").
        * If not, can they be retrieved by joining an available node though a common identifier?
        * If so, return all possible ways (by joining different nodes) that can be done.
        """
        candidate_instance_set: InstanceSet = self._node_data_set_resolver.get_output_data_set(start_node).instance_set
        candidate_spec_set = candidate_instance_set.spec_set

        logger.debug(f"Candidate spec set is:\n{pformat_big_objects(candidate_spec_set)}")

        data_set_linkable_specs = candidate_spec_set.linkable_specs

        # These are linkable specs in the same data set as the measure. Those are considered "local".
        local_linkable_specs = []

        # These are linkable specs that aren't in the data set, but they might be able to be joined in.
        possibly_joinable_linkable_specs = []

        # Group required_linkable_specs into local / un-joinable / or possibly joinable.
        unjoinable_linkable_specs = []
        for required_linkable_spec in required_linkable_specs:
            is_local = required_linkable_spec in data_set_linkable_specs
            is_unjoinable = (
                len(required_linkable_spec.identifier_links) == 0
                or required_linkable_spec.identifier_links[0] not in data_set_linkable_specs
            )
            if is_local:
                local_linkable_specs.append(required_linkable_spec)
            elif is_unjoinable:
                unjoinable_linkable_specs.append(required_linkable_spec)
            else:
                possibly_joinable_linkable_specs.append(required_linkable_spec)

        candidates_for_join = self._find_joinable_candidate_nodes_that_can_satisfy_linkable_specs(
            start_node_spec_set=candidate_spec_set, needed_linkable_specs=possibly_joinable_linkable_specs
        )
        join_candidates: List[JoinLinkableInstancesRecipe] = []

        logger.info("Looping over nodes that can be joined to get the required linkable specs")

        # Using a greedy approach, try to get the "possibly_joinable_linkable_specs" by iteratively joining nodes with
        # the most matching linkable specs. We try to join nodes with the most matching specs to minimize the number of
        # joins that we have to do to. A knapsack solution is ideal, but punting on that for simplicity.
        while len(possibly_joinable_linkable_specs) > 0:
            logger.info(f"Looking for linkable specs:\n{pformat_big_objects(possibly_joinable_linkable_specs)}")

            # We've run out of candidate data sets, but there are more linkable specs that we need. That means the
            # rest of the linkable specs can't be joined in, and we're left with unjoinable specs remaining.
            if len(candidates_for_join) == 0:
                logger.info(
                    "There are no more candidate nodes that can be joined, but not all linkable specs have "
                    "been acquired."
                )
                unjoinable_linkable_specs.extend(possibly_joinable_linkable_specs)
                break

            # Join the best candidate to realize the linkable specs
            next_candidate = candidates_for_join.pop(0)
            logger.info(f"The next candidate node to be joined is:\n{pformat_big_objects(next_candidate)}")
            join_candidates.append(next_candidate)

            # Update the candidates. Since we'll be joined/ing the previously selected candidate, we no longer need
            # to get those linkable specs from the remaining candidates, and we also need to re-rank based on the new
            # requirements.
            candidates_for_join = self._update_candidates_that_can_satisfy_linkable_specs(
                candidates_for_join=candidates_for_join,
                already_satisfisfied_linkable_specs=next_candidate.satisfiable_linkable_specs,
            )

            # The once possibly joinable specs are definitely joinable and no longer need to be searched for.
            # Remove from "possibly_joinable_linkable_specs"
            possibly_joinable_linkable_specs = [
                x for x in possibly_joinable_linkable_specs if x not in next_candidate.satisfiable_linkable_specs
            ]

        logger.info("Done evaluating possible joins")
        return LinkableInstanceSatisfiabilityEvaluation(
            local_linkable_specs=tuple(local_linkable_specs),
            joinable_linkable_specs=tuple(
                itertools.chain.from_iterable([x.satisfiable_linkable_specs for x in join_candidates])
            ),
            join_recipes=tuple(join_candidates),
            unjoinable_linkable_specs=tuple(unjoinable_linkable_specs),
        )
