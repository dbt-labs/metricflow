from metricflow.instances import DataSourceReference, InstanceSet
from metricflow.plan_conversion.sql_dataset import SqlDataSet
from metricflow.sql.sql_plan import SqlSelectStatementNode


class DataSourceDataSet(SqlDataSet):
    """Similar to SqlDataSet, but contains metadata on the data source that was used to create this."""

    def __init__(  # noqa: D
        self,
        data_source_reference: DataSourceReference,
        instance_set: InstanceSet,
        sql_select_node: SqlSelectStatementNode,
    ) -> None:
        self._data_source_reference = data_source_reference
        super().__init__(instance_set=instance_set, sql_select_node=sql_select_node)

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}({self._data_source_reference})"

    @property
    def data_source_reference(self) -> DataSourceReference:  # noqa: D
        return self._data_source_reference
