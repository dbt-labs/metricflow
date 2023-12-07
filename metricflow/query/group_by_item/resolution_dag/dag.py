from __future__ import annotations

from typing import Union

from metricflow.dag.id_generation import IdGeneratorRegistry
from metricflow.dag.id_prefix import IdPrefix
from metricflow.dag.mf_dag import DagId, MetricFlowDag
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.base_node import GroupByItemResolutionNode
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.metric_resolution_node import (
    MetricGroupByItemResolutionNode,
)
from metricflow.query.group_by_item.resolution_dag.resolution_nodes.query_resolution_node import (
    QueryGroupByItemResolutionNode,
)

ResolutionDagSinkNode = Union[QueryGroupByItemResolutionNode, MetricGroupByItemResolutionNode]


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

    def __init__(self, sink_node: ResolutionDagSinkNode) -> None:  # noqa: D
        super().__init__(
            dag_id=DagId.from_str(
                IdGeneratorRegistry.for_class(self.__class__).create_id(IdPrefix.GROUP_BY_ITEM_RESOLUTION_DAG.value)
            ),
            sink_nodes=[sink_node],
        )
        self._sink_node = sink_node

    @property
    def sink_node(self) -> ResolutionDagSinkNode:  # noqa: D
        return self._sink_node
