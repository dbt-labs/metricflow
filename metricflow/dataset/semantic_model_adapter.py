from __future__ import annotations

from dbt_semantic_interfaces.references import SemanticModelReference
from metricflow_semantics.instances import InstanceSet
from typing_extensions import override

from metricflow.dataset.sql_dataset import SqlDataSet
from metricflow.sql.sql_select_node import SqlSelectStatementNode


class SemanticModelDataSet(SqlDataSet):
    """Similar to SqlDataSet, but contains metadata on the semantic model that was used to create this."""

    def __init__(  # noqa: D107
        self,
        semantic_model_reference: SemanticModelReference,
        instance_set: InstanceSet,
        sql_select_node: SqlSelectStatementNode,
    ) -> None:
        self._semantic_model_reference = semantic_model_reference
        super().__init__(instance_set=instance_set, sql_select_node=sql_select_node)

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}({repr(self._semantic_model_reference.semantic_model_name)})"

    @property
    @override
    def semantic_model_reference(self) -> SemanticModelReference:
        return self._semantic_model_reference
