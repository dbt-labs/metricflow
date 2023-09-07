from __future__ import annotations

from dbt_semantic_interfaces.references import SemanticModelReference

from metricflow.dataset.sql_dataset import SqlDataSet
from metricflow.instances import InstanceSet
from metricflow.sql.sql_plan import SqlSelectStatementNode


class SemanticModelDataSet(SqlDataSet):
    """Similar to SqlDataSet, but contains metadata on the semantic model that was used to create this."""

    def __init__(  # noqa: D
        self,
        semantic_model_reference: SemanticModelReference,
        instance_set: InstanceSet,
        sql_select_node: SqlSelectStatementNode,
    ) -> None:
        self._semantic_model_reference = semantic_model_reference
        super().__init__(instance_set=instance_set, sql_select_node=sql_select_node)

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}({self._semantic_model_reference})"

    @property
    def semantic_model_reference(self) -> SemanticModelReference:  # noqa: D
        return self._semantic_model_reference
