from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional, Sequence

from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.sequential_id import SequentialIdGenerator
from metricflow_semantics.mf_logging.runtime import log_block_runtime
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from typing_extensions import override

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)
from metricflow.dataset.sql_dataset import SqlDataSet
from metricflow.plan_conversion.dataflow_to_sql import (
    DataflowNodeToSqlSubqueryVisitor,
)

if TYPE_CHECKING:
    from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup


class DataflowPlanNodeOutputDataSetResolver:
    """Given a node in a dataflow plan, figure out what is the data set output by that node.

    Recall that in the dataflow plan, the nodes represent computation, and the inputs and outputs of the nodes are
    data sets containing measures, dimensions, etc.

    Note: the term "dimension" is used below, but it actually refers to any LinkableInstance.

    This class is useful when computing the joins required to retrieve the dimensions associated with a measure. The
    object that figures out the joins (NodeEvaluatorForLinkableInstances) is given a set of dataflow plan nodes that
    output different dimensions (this information is not necessarily in the node itself).

    NodeEvaluatorForLinkableInstances needs the data set associated with a node so that it knows what dimensions and
    entities are in the node. This information is used to figure out whether that node is useful to join to in order
    to retrieve a particular dimension.

    In the simple case, if the input nodes are all ReadSqlSourceNodes, then the data set is a member variable of the
    node, and there would be no need to traverse the dataflow node hierarchy with this class. However, if it's not a
    single node, that information is not available as the dataflow plan nodes contain little metadata. In that case,
    this class is needed to traverse the dataflow plan and figure that out.

    The reason why the input nodes to NodeEvaluatorForLinkableInstances wouldn't always be all ReadSqlSourceNodes is to
    realize an easy way to handle multi-hop joins without making changes to NodeEvaluatorForLinkableInstances. If we
    generate a set of nodes that already include the multi-hop dimensions available, the join resolution logic becomes
    much simpler. For example, a node like:

    <JoinOnEntitiesNode>
        <!-- Join dim_users and dim_devices by device_id -->
        <ReadSqlSourceNode>
          <!-- Read from dim_users to get user_id, device_id -->
        <ReadSqlSourceNodes>
          <!-- Read from dim_devices device_id, platform -->
    </JoinOnEntitiesNode>

    would have the dimension user_id__device_id__platform available, so to NodeEvaluatorForLinkableInstances,
    it's the same problem as doing a single-hop join. This simplifies the join resolution logic, though now the input
    to NodeEvaluatorForLinkableInstances needs to contain nodes that include all possible multi-hop joins.

    The logic to figure the dataset output by a node is the same as DataflowToSqlQueryPlanConverter because the same
    information is needed for generating SQL queries, so inheriting from that. We may want to look later at making
    another class to have better separation of concerns.
    """

    def __init__(  # noqa: D107
        self,
        column_association_resolver: ColumnAssociationResolver,
        semantic_manifest_lookup: SemanticManifestLookup,
        _node_to_output_data_set: Optional[Dict[DataflowPlanNode, SqlDataSet]] = None,
    ) -> None:
        self._node_to_output_data_set: Dict[DataflowPlanNode, SqlDataSet] = _node_to_output_data_set or {}
        self._column_association_resolver = column_association_resolver
        self._semantic_manifest_lookup = semantic_manifest_lookup
        self._to_data_set_visitor = _NodeDataSetVisitor(
            column_association_resolver=self._column_association_resolver,
            semantic_manifest_lookup=self._semantic_manifest_lookup,
        )

    def get_output_data_set(self, node: DataflowPlanNode) -> SqlDataSet:
        """Cached since this will be called repeatedly during the computation of multiple metrics.

        # TODO: The cache needs to be pruned, but has not yet been an issue.
        """
        if node not in self._node_to_output_data_set:
            self._node_to_output_data_set[node] = node.accept(self._to_data_set_visitor)

        return self._node_to_output_data_set[node]

    def cache_output_data_sets(self, nodes: Sequence[DataflowPlanNode]) -> None:
        """Cache the output of the given nodes for consistent retrieval with `get_output_data_set`."""
        with log_block_runtime(f"cache_output_data_sets for {len(nodes)} nodes"):
            for node in nodes:
                self.get_output_data_set(node)

    def copy(self) -> DataflowPlanNodeOutputDataSetResolver:
        """Return a copy of this with the same nodes cached."""
        return DataflowPlanNodeOutputDataSetResolver(
            column_association_resolver=self._column_association_resolver,
            semantic_manifest_lookup=self._semantic_manifest_lookup,
            _node_to_output_data_set=dict(self._node_to_output_data_set),
        )


class _NodeDataSetVisitor(DataflowNodeToSqlSubqueryVisitor):
    @override
    def _next_unique_table_alias(self) -> str:
        """Return the next unique table alias to use in generating queries.

        This uses a separate prefix in order to minimize subquery ID thrash.
        """
        return SequentialIdGenerator.create_next_id(StaticIdPrefix.NODE_RESOLVER_SUB_QUERY).str_value
