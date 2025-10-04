from __future__ import annotations

from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId, MetricFlowDag
from metricflow_semantics.query.group_by_item.resolution_dag.resolution_nodes.base_node import GroupByItemResolutionNode


class GroupByItemResolutionDag(MetricFlowDag[GroupByItemResolutionNode]):
    """A DAG that represents how valid group-by-items are resolved.

    In this representation, group-by-item candidates flow from the root / source nodes to the sink / leaf node. The
    source nodes represent the measures, and the sink node is usually the node that represents the metric query.

    The candidates that reach the sink node should be the valid ones. Generally, the nodes intersect the candidates
    from the parents and pass the intersection down to the child node. If the intersection produces an empty set, an
    issue can be produced with appropriate context and passed down to the child node. This allows generation of specific
    reasons for why a group-by item isn't valid for a given configuration. The nodes can also filter the candidates to
    realize limitations appropriate to that node.
    """

    def __init__(self, sink_node: GroupByItemResolutionNode) -> None:  # noqa: D107
        super().__init__(
            dag_id=DagId.from_id_prefix(StaticIdPrefix.GROUP_BY_ITEM_RESOLUTION_DAG),
            sink_nodes=[sink_node],
        )
        self._sink_node = sink_node

    @property
    def sink_node(self) -> GroupByItemResolutionNode:  # noqa: D102
        return self._sink_node
