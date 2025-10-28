from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass
from typing import Sequence, Tuple

from dbt_semantic_interfaces.references import SemanticModelReference
from typing_extensions import override

from metricflow_semantics.model.semantic_model_derivation import SemanticModelDerivation
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseGroupByItemSet
from metricflow_semantics.query.group_by_item.path_prefixable import PathPrefixable
from metricflow_semantics.query.group_by_item.resolution_path import MetricFlowQueryResolutionPath
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import (
    GroupByItemSet,
)
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GroupByItemCandidateSet(PathPrefixable, SemanticModelDerivation):
    """The set of candidate specs that could match a given spec pattern.

    This candidate set is refined as it is passed from the root node (representing a measure) to the leaf node
    (representing a query) in the resolution DAG.

    For logging / error reporting:

    * This includes the path from the leaf node to the measure node where the specs
    originated. There can be multiple paths as when candidate sets are merged, there can be multiple simple-metric inputs were a
    spec could have come from.

    * This includes the path from the leaf node to the node where this set was generated since during debugging or in
    error messages, you start analyzing from the leaf node.
    """

    linkable_element_set: BaseGroupByItemSet
    simple_metric_input_paths: Tuple[MetricFlowQueryResolutionPath, ...]
    path_from_leaf_node: MetricFlowQueryResolutionPath

    def __post_init__(self) -> None:  # noqa: D105
        # If there are no specs, there shouldn't be any simple-metric input paths.
        assert (len(self.specs) > 0 and len(self.simple_metric_input_paths) > 0) or (
            len(self.specs) == 0 and len(self.simple_metric_input_paths) == 0
        )

    @property
    def specs(self) -> Sequence[LinkableInstanceSpec]:  # noqa: D102
        return self.linkable_element_set.specs

    @staticmethod
    def intersection(
        path_from_leaf_node: MetricFlowQueryResolutionPath, candidate_sets: Sequence[GroupByItemCandidateSet]
    ) -> GroupByItemCandidateSet:
        """Create a new candidate set that is the intersection of the given candidate sets.

        The intersection is defined as the specs common to all candidate sets. path_from_leaf_node is used to indicate
        where the new candidate set was created.
        """
        if len(candidate_sets) == 0:
            return GroupByItemCandidateSet.empty_instance()
        elif len(candidate_sets) == 1:
            return GroupByItemCandidateSet(
                linkable_element_set=candidate_sets[0].linkable_element_set,
                simple_metric_input_paths=candidate_sets[0].simple_metric_input_paths,
                path_from_leaf_node=path_from_leaf_node,
            )
        linkable_element_set_candidates = tuple(candidate_set.linkable_element_set for candidate_set in candidate_sets)

        candidates_count = len(linkable_element_set_candidates)

        if candidates_count == 0:
            return GroupByItemCandidateSet.empty_instance()

        intersection_result = linkable_element_set_candidates[0].intersection(*linkable_element_set_candidates[1:])

        if intersection_result.is_empty:
            return GroupByItemCandidateSet.empty_instance()

        measure_paths = tuple(
            itertools.chain.from_iterable(candidate_set.simple_metric_input_paths for candidate_set in candidate_sets)
        )

        return GroupByItemCandidateSet(
            linkable_element_set=intersection_result,
            simple_metric_input_paths=measure_paths,
            path_from_leaf_node=path_from_leaf_node,
        )

    @property
    def is_empty(self) -> bool:  # noqa: D102
        return len(self.specs) == 0

    @property
    def num_candidates(self) -> int:  # noqa: D102
        return len(self.specs)

    @staticmethod
    def empty_instance() -> GroupByItemCandidateSet:  # noqa: D102
        return GroupByItemCandidateSet(
            linkable_element_set=GroupByItemSet(),
            simple_metric_input_paths=(),
            path_from_leaf_node=MetricFlowQueryResolutionPath.empty_instance(),
        )

    def filter_candidates_by_pattern(
        self,
        spec_pattern: SpecPattern,
    ) -> GroupByItemCandidateSet:
        """Return a new candidate set that only contains specs that match the given pattern."""
        filtered_element_set = self.linkable_element_set.filter_by_spec_patterns((spec_pattern,))
        if filtered_element_set.is_empty:
            return GroupByItemCandidateSet.empty_instance()
        return GroupByItemCandidateSet(
            linkable_element_set=filtered_element_set,
            simple_metric_input_paths=self.simple_metric_input_paths,
            path_from_leaf_node=self.path_from_leaf_node,
        )

    @override
    def with_path_prefix(self, path_prefix: MetricFlowQueryResolutionPath) -> GroupByItemCandidateSet:
        return GroupByItemCandidateSet(
            linkable_element_set=self.linkable_element_set,
            simple_metric_input_paths=tuple(
                path.with_path_prefix(path_prefix) for path in self.simple_metric_input_paths
            ),
            path_from_leaf_node=self.path_from_leaf_node.with_path_prefix(path_prefix),
        )

    @property
    @override
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        return self.linkable_element_set.derived_from_semantic_models
