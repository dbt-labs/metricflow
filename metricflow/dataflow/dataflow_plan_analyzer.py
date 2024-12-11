from __future__ import annotations

import logging
from collections import defaultdict
from typing import Dict, FrozenSet, Mapping, Sequence, Set

from typing_extensions import override

from metricflow.dataflow.dataflow_plan import DataflowPlan, DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitorWithDefaultHandler

logger = logging.getLogger(__name__)


class DataflowPlanAnalyzer:
    """Class to determine more complex properties of the dataflow plan.

    These could also be made as member methods of the dataflow plan, but this requires resolving some circular
    dependency issues to break out the functionality into separate files.
    """

    @staticmethod
    def find_common_branches(dataflow_plan: DataflowPlan) -> Sequence[DataflowPlanNode]:
        """Starting from the sink node, find the common branches that exist in the associated DAG.

        Returns a sorted sequence for reproducibility.
        """
        counting_visitor = _CountDataflowNodeVisitor()
        dataflow_plan.sink_node.accept(counting_visitor)

        node_to_common_count = counting_visitor.get_node_counts()

        common_nodes = []
        for node, count in node_to_common_count.items():
            if count > 1:
                common_nodes.append(node)

        common_branches_visitor = _FindLargestCommonBranchesVisitor(frozenset(common_nodes))

        return tuple(sorted(dataflow_plan.sink_node.accept(common_branches_visitor)))


class _CountDataflowNodeVisitor(DataflowPlanNodeVisitorWithDefaultHandler[None]):
    """Helper visitor to build a dict from a node in the plan to the number of times it appears in the plan."""

    def __init__(self) -> None:
        self._node_to_count: Dict[DataflowPlanNode, int] = defaultdict(int)

    def get_node_counts(self) -> Mapping[DataflowPlanNode, int]:
        return self._node_to_count

    @override
    def _default_handler(self, node: DataflowPlanNode) -> None:
        for parent_node in node.parent_nodes:
            parent_node.accept(self)
        self._node_to_count[node] += 1


class _FindLargestCommonBranchesVisitor(DataflowPlanNodeVisitorWithDefaultHandler[FrozenSet[DataflowPlanNode]]):
    """Given the nodes that are known to appear in the DAG multiple times, find the common branches.

    To get the largest common branches, (e.g. for `A -> B -> C -> D` and `B -> C -> D`, both `B -> C -> D`
    and `C -> D` can be considered common branches, and we want the largest one), this uses preorder traversal and
    returns the first common node that is seen.
    """

    def __init__(self, common_nodes: FrozenSet[DataflowPlanNode]) -> None:
        self._common_nodes = common_nodes

    @override
    def _default_handler(self, node: DataflowPlanNode) -> FrozenSet[DataflowPlanNode]:
        # Traversal starts from the leaf node and then goes to the parent branches. By doing this check first, we don't
        # return smaller common branches that are a part of a larger common branch.
        if node in self._common_nodes:
            return frozenset({node})

        common_branch_leaf_nodes: Set[DataflowPlanNode] = set()

        for parent_node in node.parent_nodes:
            common_branch_leaf_nodes.update(parent_node.accept(self))

        return frozenset(common_branch_leaf_nodes)
