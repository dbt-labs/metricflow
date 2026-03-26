from __future__ import annotations

import logging
from collections import defaultdict
from typing import Dict, Mapping

from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.dataflow.dataflow_plan import DataflowPlan, DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitorWithDefaultHandler

logger = logging.getLogger(__name__)


class DataflowPlanAnalyzer:
    """Analyze higher-order properties of a dataflow plan."""

    # Guardrail against non-converging common branches.
    MAX_COMMON_BRANCH_ITERATIONS = 10

    @staticmethod
    def find_common_branches(dataflow_plan: DataflowPlan) -> OrderedSet[DataflowPlanNode]:
        """Find nodes that represent common branches in the plan DAG.

        Traversal starts from `sink_node` and repeatedly finds shared upstream branches.
        If a common branch has deeper common sub-branches, those are included as well.

        Returns:
            Nodes that are the "largest" shared branch roots for each level of shared
            structure discovered from the sink toward source nodes.
        """
        all_common_branches: MutableOrderedSet[DataflowPlanNode] = MutableOrderedSet()
        branch_roots_to_scan: OrderedSet[DataflowPlanNode] = MutableOrderedSet([dataflow_plan.sink_node])

        for _ in range(DataflowPlanAnalyzer.MAX_COMMON_BRANCH_ITERATIONS):
            largest_common_branches = DataflowPlanAnalyzer._find_largest_common_branches(branch_roots_to_scan)
            # No more common branches in the recursive search.
            if not largest_common_branches:
                return all_common_branches

            # The largest common branches might share deeper sub-branches, so keep iterating.
            all_common_branches.update(largest_common_branches)
            branch_roots_to_scan = largest_common_branches

        logger.warning(
            LazyFormat(
                "Reached max iterations while finding common branches. Returning partial results. "
                "Consider updating the limit.",
                max_iterations=DataflowPlanAnalyzer.MAX_COMMON_BRANCH_ITERATIONS,
                final_batch_count=len(branch_roots_to_scan),
            )
        )
        return all_common_branches

    @staticmethod
    def _find_largest_common_branches(nodes: OrderedSet[DataflowPlanNode]) -> OrderedSet[DataflowPlanNode]:
        """Return shared nodes that are closest to the sink among `nodes` and their parents."""
        counting_visitor = _CountDataflowNodeVisitor()
        for node in nodes:
            node.accept(counting_visitor)

        node_to_common_count = counting_visitor.get_node_counts()

        common_nodes: MutableOrderedSet[DataflowPlanNode] = MutableOrderedSet()
        for node, count in node_to_common_count.items():
            if count > 1:
                common_nodes.add(node)

        common_branches_visitor = _FindLargestCommonBranchesVisitor(common_nodes)

        common_branches: MutableOrderedSet[DataflowPlanNode] = MutableOrderedSet()
        for node in nodes:
            common_branches.update(node.accept(common_branches_visitor))

        return common_branches


class _CountDataflowNodeVisitor(DataflowPlanNodeVisitorWithDefaultHandler[None]):
    """Count how many times each node is reached while traversing upstream.

    This counts path reachability, not unique node identity. For example, if a node can be reached via two
    parent paths from the sink, it is counted twice.
    """

    def __init__(self) -> None:
        self._node_to_count: Dict[DataflowPlanNode, int] = defaultdict(int)

    def get_node_counts(self) -> Mapping[DataflowPlanNode, int]:
        """Return node reachability counts collected during traversal."""
        return self._node_to_count

    @override
    def _default_handler(self, node: DataflowPlanNode) -> None:
        for parent_node in node.parent_nodes:
            parent_node.accept(self)
        self._node_to_count[node] += 1


class _FindLargestCommonBranchesVisitor(DataflowPlanNodeVisitorWithDefaultHandler[OrderedSet[DataflowPlanNode]]):
    """Given common nodes, return only the largest common branches.

    For `A -> B -> C -> D` and `B -> C -> D`, both `B -> C -> D` and `C -> D` are  common. This visitor
    returns `B` by stopping at the first common node on each path.
    """

    def __init__(self, common_nodes: OrderedSet[DataflowPlanNode]) -> None:
        self._common_nodes = common_nodes

    @override
    def _default_handler(self, node: DataflowPlanNode) -> OrderedSet[DataflowPlanNode]:
        # Traversal starts from the sink and moves to parents (upstream). Checking
        # membership before recursion ensures we return the largest common branch root
        # on the current path, not a smaller nested branch.
        if node in self._common_nodes:
            return FrozenOrderedSet([node])

        largest_common_branch_roots: MutableOrderedSet[DataflowPlanNode] = MutableOrderedSet()

        for parent_node in node.parent_nodes:
            largest_common_branch_roots.update(parent_node.accept(self))

        return largest_common_branch_roots
