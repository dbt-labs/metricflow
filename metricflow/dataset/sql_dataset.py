from __future__ import annotations

from typing import Optional, Sequence

from dbt_semantic_interfaces.references import SemanticModelReference
from typing_extensions import override

from metricflow.assert_one_arg import assert_exactly_one_arg_set
from metricflow.dataset.dataset import DataSet
from metricflow.instances import (
    InstanceSet,
)
from metricflow.specs.column_assoc import ColumnAssociation
from metricflow.specs.specs import DimensionSpec, EntitySpec, TimeDimensionSpec
from metricflow.sql.sql_plan import (
    SqlQueryPlanNode,
    SqlSelectStatementNode,
)


class SqlDataSet(DataSet):
    """A metric data set along with the associated SQL query node that can be rendered to get those values."""

    def __init__(
        self,
        instance_set: InstanceSet,
        sql_select_node: Optional[SqlSelectStatementNode] = None,
        sql_node: Optional[SqlQueryPlanNode] = None,
    ) -> None:
        """Constructor.

        Args:
            instance_set: Describes the instances in the SQL.
            sql_select_node: The SQL that can be rendered to realize the instance set
        """
        self._sql_select_node = sql_select_node
        self._sql_node = sql_node
        assert_exactly_one_arg_set(sql_select_node=sql_select_node, sql_node=sql_node)
        super().__init__(instance_set=instance_set)

    @property
    def sql_node(self) -> SqlQueryPlanNode:  # noqa: D102
        node_to_return = self._sql_select_node or self._sql_node
        if node_to_return is None:
            raise RuntimeError("This node was not created with a SQL node.")
        return node_to_return

    @property
    def checked_sql_select_node(self) -> SqlSelectStatementNode:
        """If applicable, return a SELECT node that can be used to read data from the given SQL table or SQL query.

        Otherwise, an exception is thrown.
        """
        if self._sql_select_node is None:
            raise RuntimeError(f"{self} was created with a SQL node that is not a {SqlSelectStatementNode}")
        return self._sql_select_node

    def column_associations_for_entity(
        self,
        entity_spec: EntitySpec,
    ) -> Sequence[ColumnAssociation]:
        """Given the name of the entity, return the set of columns associated with it in the data set."""
        matching_instances = 0
        column_associations_to_return = None
        for linkable_instance in self.instance_set.entity_instances:
            if (
                entity_spec.element_name == linkable_instance.spec.element_name
                and entity_spec.entity_links == linkable_instance.spec.entity_links
            ):
                column_associations_to_return = linkable_instance.associated_columns
                matching_instances += 1

        if matching_instances > 1:
            raise RuntimeError(
                f"More than one instance with spec {entity_spec} in " f"instance set: {self.instance_set}"
            )

        if not column_associations_to_return:
            raise RuntimeError(f"No instances with spec {entity_spec} in instance set: {self.instance_set}")

        return column_associations_to_return

    def column_association_for_dimension(
        self,
        dimension_spec: DimensionSpec,
    ) -> ColumnAssociation:
        """Given the name of the dimension, return the set of columns associated with it in the data set."""
        matching_instances = 0
        column_associations_to_return = None
        for dimension_instance in self.instance_set.dimension_instances:
            if dimension_instance.spec == dimension_spec:
                column_associations_to_return = dimension_instance.associated_columns
                matching_instances += 1

        if matching_instances > 1:
            raise RuntimeError(
                f"More than one dimension instance with spec {dimension_spec} in " f"instance set: {self.instance_set}"
            )

        if not column_associations_to_return:
            raise RuntimeError(
                f"No dimension instances with spec {dimension_spec} in instance set: {self.instance_set}"
            )

        return column_associations_to_return[0]

    def column_association_for_time_dimension(
        self,
        time_dimension_spec: TimeDimensionSpec,
    ) -> ColumnAssociation:
        """Given the name of the time dimension, return the set of columns associated with it in the data set."""
        matching_instances = 0
        column_associations_to_return = None
        for time_dimension_instance in self.instance_set.time_dimension_instances:
            if time_dimension_instance.spec == time_dimension_spec:
                column_associations_to_return = time_dimension_instance.associated_columns
                matching_instances += 1

        if matching_instances > 1:
            raise RuntimeError(
                f"More than one time dimension instance with spec {time_dimension_spec} in "
                f"instance set: {self.instance_set}"
            )

        if not column_associations_to_return:
            raise RuntimeError(
                f"No time dimension instances with spec {time_dimension_spec} in instance set: {self.instance_set}"
            )

        return column_associations_to_return[0]

    @property
    @override
    def semantic_model_reference(self) -> Optional[SemanticModelReference]:
        return None
