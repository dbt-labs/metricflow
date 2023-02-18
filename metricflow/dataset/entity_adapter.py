from metricflow.instances import MetricFlowEntityReference, InstanceSet
from metricflow.plan_conversion.sql_dataset import SqlDataSet
from metricflow.sql.sql_plan import SqlSelectStatementNode


class MetricFlowEntityDataSet(SqlDataSet):
    """Similar to SqlDataSet, but contains metadata on the entity that was used to create this."""

    def __init__(  # noqa: D
        self,
        entity_reference: MetricFlowEntityReference,
        instance_set: InstanceSet,
        sql_select_node: SqlSelectStatementNode,
    ) -> None:
        self._entity_reference = entity_reference
        super().__init__(instance_set=instance_set, sql_select_node=sql_select_node)

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}({self._entity_reference})"

    @property
    def entity_reference(self) -> MetricFlowEntityReference:  # noqa: D
        return self._entity_reference
