from __future__ import annotations

import textwrap
from typing import Sequence

import jinja2

from metricflow.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow.dag.mf_dag import DisplayedProperty
from metricflow.dataflow.dataflow_plan import BaseOutput, DataflowPlanNode, DataflowPlanNodeVisitor
from metricflow.dataset.sql_dataset import SqlDataSet
from metricflow.visitor import VisitorOutputT


class ReadSqlSourceNode(BaseOutput):
    """A source node where data from an SQL table or SQL query is read and output."""

    def __init__(self, data_set: SqlDataSet) -> None:
        """Constructor.

        Args:
            data_set: dataset describing the SQL table / SQL query
        """
        self._dataset = data_set
        super().__init__(node_id=self.create_unique_id(), parent_nodes=())

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_READ_SQL_SOURCE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_source_node(self)

    @property
    def data_set(self) -> SqlDataSet:
        """Return the data set that this source represents and is passed to the child nodes."""
        return self._dataset

    def __str__(self) -> str:  # noqa: D105
        return jinja2.Template(
            textwrap.dedent(
                """\
                <{{ class_name }} data_set={{ data_set }} />
                """
            )
        ).render(class_name=self.__class__.__name__, data_set=str(self.data_set))

    @property
    def description(self) -> str:  # noqa: D102
        return f"""Read From {self.data_set}"""

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return tuple(super().displayed_properties) + (DisplayedProperty("data_set", self.data_set),)

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        return isinstance(other_node, self.__class__) and other_node.data_set == self.data_set

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> ReadSqlSourceNode:  # noqa: D102
        assert len(new_parent_nodes) == 0
        return ReadSqlSourceNode(data_set=self.data_set)
