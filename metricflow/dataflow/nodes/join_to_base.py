from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty, NodeId
from metricflow_semantics.specs.spec_classes import LinklessEntitySpec, TimeDimensionSpec
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.builder.partitions import (
    PartitionDimensionJoinDescription,
    PartitionTimeDimensionJoinDescription,
)
from metricflow.dataflow.dataflow_plan import DataflowPlanNode, DataflowPlanNodeVisitor


@dataclass(frozen=True)
class ValidityWindowJoinDescription:
    """Encapsulates details about join constraints around validity windows."""

    window_start_dimension: TimeDimensionSpec
    window_end_dimension: TimeDimensionSpec


@dataclass(frozen=True)
class JoinDescription:
    """Describes how data from a node should be joined to data from another node."""

    join_node: DataflowPlanNode
    join_on_entity: Optional[LinklessEntitySpec]
    join_type: SqlJoinType

    join_on_partition_dimensions: Tuple[PartitionDimensionJoinDescription, ...]
    join_on_partition_time_dimensions: Tuple[PartitionTimeDimensionJoinDescription, ...]

    validity_window: Optional[ValidityWindowJoinDescription] = None

    def __post_init__(self) -> None:  # noqa: D105
        if self.join_on_entity is None and self.join_type != SqlJoinType.CROSS_JOIN:
            raise RuntimeError("`join_on_entity` is required unless using CROSS JOIN.")


class JoinOnEntitiesNode(DataflowPlanNode):
    """A node that joins data from other nodes via the entities in the inputs."""

    def __init__(
        self,
        left_node: DataflowPlanNode,
        join_targets: Sequence[JoinDescription],
        node_id: Optional[NodeId] = None,
    ) -> None:
        """Constructor.

        Args:
            left_node: node with standard output
            join_targets: other sources that should be joined to this node.
            node_id: Override the node ID with this value
        """
        self._left_node = left_node
        self._join_targets = tuple(join_targets)

        # Doing a list comprehension throws a type error, so doing it this way.
        parent_nodes: List[DataflowPlanNode] = [self._left_node]
        for join_target in self._join_targets:
            parent_nodes.append(join_target.join_node)
        super().__init__(node_id=node_id or self.create_unique_id(), parent_nodes=parent_nodes)

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_JOIN_TO_STANDARD_OUTPUT_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_join_on_entities_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return """Join Standard Outputs"""

    @property
    def left_node(self) -> DataflowPlanNode:  # noqa: D102
        return self._left_node

    @property
    def join_targets(self) -> Sequence[JoinDescription]:  # noqa: D102
        return self._join_targets

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + tuple(
            DisplayedProperty(f"join{i}_for_node_id_{join_description.join_node.node_id}", join_description)
            for i, join_description in enumerate(self._join_targets)
        )

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        if not isinstance(other_node, self.__class__) or len(self.join_targets) != len(other_node.join_targets):
            return False

        for i in range(len(self.join_targets)):
            if (
                self.join_targets[i].join_on_entity != other_node.join_targets[i].join_on_entity
                or self.join_targets[i].join_on_partition_dimensions
                != other_node.join_targets[i].join_on_partition_dimensions
                or self.join_targets[i].join_on_partition_time_dimensions
                != other_node.join_targets[i].join_on_partition_time_dimensions
                or self.join_targets[i].validity_window != other_node.join_targets[i].validity_window
            ):
                return False
        return True

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> JoinOnEntitiesNode:  # noqa: D102
        assert len(new_parent_nodes) > 1
        new_left_node = new_parent_nodes[0]
        new_join_nodes = new_parent_nodes[1:]
        assert len(new_join_nodes) == len(self._join_targets)

        return JoinOnEntitiesNode(
            left_node=new_left_node,
            join_targets=[
                JoinDescription(
                    join_node=new_join_nodes[i],
                    join_on_entity=old_join_target.join_on_entity,
                    join_on_partition_dimensions=old_join_target.join_on_partition_dimensions,
                    join_on_partition_time_dimensions=old_join_target.join_on_partition_time_dimensions,
                    validity_window=old_join_target.validity_window,
                    join_type=old_join_target.join_type,
                )
                for i, old_join_target in enumerate(self._join_targets)
            ],
        )
