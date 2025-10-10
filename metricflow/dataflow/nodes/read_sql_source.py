from __future__ import annotations

import textwrap
from dataclasses import dataclass
from typing import Optional, Sequence

import jinja2
from dbt_semantic_interfaces.references import SemanticModelReference
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.toolkit.visitor import VisitorOutputT
from typing_extensions import override

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor
from metricflow.dataset.sql_dataset import SqlDataSet


@dataclass(frozen=True, eq=False)
class ReadSqlSourceNode(DataflowPlanNode):
    """A source node where data from an SQL table or SQL query is read and output.

    Attributes:
        data_set: Dataset describing the SQL table / SQL query.
    """

    data_set: SqlDataSet

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 0

    @staticmethod
    def create(  # noqa: D102
        data_set: SqlDataSet,
    ) -> ReadSqlSourceNode:
        return ReadSqlSourceNode(
            parent_nodes=(),
            data_set=data_set,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_READ_SQL_SOURCE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_source_node(self)

    @override
    @property
    def _input_semantic_model(self) -> Optional[SemanticModelReference]:
        """Return the semantic model serving as direct input for this node, if one exists."""
        return self.data_set.semantic_model_reference

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

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> ReadSqlSourceNode:  # noqa: D102
        assert len(new_parent_nodes) == 0
        return ReadSqlSourceNode.create(data_set=self.data_set)
