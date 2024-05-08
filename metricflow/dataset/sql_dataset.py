from __future__ import annotations

from typing import List, Optional, Sequence

from dbt_semantic_interfaces.references import SemanticModelReference
from metricflow_semantics.assert_one_arg import assert_exactly_one_arg_set
from metricflow_semantics.instances import EntityInstance, InstanceSet
from metricflow_semantics.specs.column_assoc import ColumnAssociation
from metricflow_semantics.specs.spec_classes import DimensionSpec, EntitySpec, TimeDimensionSpec
from typing_extensions import override

from metricflow.dataset.dataset_classes import DataSet
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
        matching_instances_with_same_entity_links: List[EntityInstance] = []
        matching_instances_with_different_entity_links: List[EntityInstance] = []
        for linkable_instance in self.instance_set.entity_instances:
            if entity_spec.element_name == linkable_instance.spec.element_name:
                if entity_spec.entity_links == linkable_instance.spec.entity_links:
                    matching_instances_with_same_entity_links.append(linkable_instance)
                else:
                    matching_instances_with_different_entity_links.append(linkable_instance)

        # Prioritize instances with matching entity links, but use mismatched links if matching links not found.
        # Semantic model source data sets might have multiple instances of the same entity, in which case we want the one without
        # links. But group by metric source data sets might only have an instance of the entity with links, and we can join to that.
        matching_instances = matching_instances_with_same_entity_links or matching_instances_with_different_entity_links

        if len(matching_instances) != 1:
            raise RuntimeError(
                f"Expected exactly one matching instance for {entity_spec} in instance set, but found: {matching_instances}. "
                f"All entity instances: {self.instance_set.entity_instances}"
            )
        matching_instance = matching_instances[0]
        if not matching_instance.associated_columns:
            raise RuntimeError(
                f"No associated columns for entity instance {matching_instance} in data set."
                "This indicates internal misconfiguration."
            )

        return matching_instance.associated_columns

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
