from typing import Dict, TypeVar, Generic

from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.plan_conversion.sql_dataset import SqlDataSet
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.specs import ColumnAssociationResolver
from metricflow.model.semantic_model import SemanticModel

SourceDataSetT = TypeVar("SourceDataSetT", bound=SqlDataSet)


class DataflowPlanNodeOutputDataSetResolver(Generic[SourceDataSetT], DataflowToSqlQueryPlanConverter[SourceDataSetT]):
    """Given a node in a dataflow plan, figure out what is the data set output by that node.

    Recall that in the dataflow plan, the nodes represent computation, and the inputs and outputs of the nodes are
    data sets containing measures, dimensions, etc.

    Note: the term "dimension" is used below, but it actually refers to any LinkableInstance.

    This class is useful when computing the joins required to retrieve the dimensions associated with a measure. The
    object that figures out the joins (NodeEvaluatorForLinkableInstances) is given a set of dataflow plan nodes that
    output different dimensions (this information is not necessarily in the node itself).

    NodeEvaluatorForLinkableInstances needs the data set associated with a node so that it knows what dimensions and
    identifiers are in the node. This information is used to figure out whether that node is useful to join to in order
    to retrieve a particular dimension.

    In the simple case, if the input nodes are all ReadSqlSourceNodes, then the data set is a member variable of the
    node, and there would be no need to traverse the dataflow node hierarchy with this class. However, if it's not a
    single node, that information is not available as the dataflow plan nodes contain little metadata. In that case,
    this class is needed to traverse the dataflow plan and figure that out.

    The reason why the input nodes to NodeEvaluatorForLinkableInstances wouldn't always be all ReadSqlSourceNodes is to
    realize an easy way to handle multi-hop joins without making changes to NodeEvaluatorForLinkableInstances. If we
    generate a set of nodes that already include the multi-hop dimensions available, the join resolution logic becomes
    much simpler. For example, a node like:

    <JoinToBaseOutputNode>
        <!-- Join dim_users and dim_devices by device_id -->
        <ReadSqlSourceNode>
          <!-- Read from dim_users to get user_id, device_id -->
        <ReadSqlSourceNodes>
          <!-- Read from dim_devices device_id, platform -->
    </JoinToBaseOutputNode>

    would have the dimension user_id__device_id__platform available, so to NodeEvaluatorForLinkableInstances,
    it's the same problem as doing a single-hop join. This simplifies the join resolution logic, though now the input
    to NodeEvaluatorForLinkableInstances needs to contain nodes that include all possible multi-hop joins.

    The logic to figure the dataset output by a node is the same as DataflowToSqlQueryPlanConverter because the same
    information is needed for generating SQL queries, so inheriting from that. We may want to look later at making
    another class to have better separation of concerns.
    """

    def __init__(  # noqa: D
        self,
        column_association_resolver: ColumnAssociationResolver,
        semantic_model: SemanticModel,
        time_spine_source: TimeSpineSource,
    ) -> None:
        self._node_to_output_data_set: Dict[DataflowPlanNode[SourceDataSetT], SqlDataSet] = {}
        super().__init__(
            column_association_resolver=column_association_resolver,
            semantic_model=semantic_model,
            time_spine_source=time_spine_source,
        )

    def get_output_data_set(self, node: DataflowPlanNode[SourceDataSetT]) -> SqlDataSet:  # noqa: D
        """Cached since this will be called repeatedly during the computation of multiple metrics."""

        if node not in self._node_to_output_data_set:
            self._node_to_output_data_set[node] = node.accept(self)

        return self._node_to_output_data_set[node]
