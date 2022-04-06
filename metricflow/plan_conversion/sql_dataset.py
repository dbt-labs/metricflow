from typing import Sequence, List

from metricflow.column_assoc import ColumnAssociation
from metricflow.dataset.dataset import DataSet
from metricflow.instances import (
    InstanceSet,
    DataSourceReference,
    DataSourceElementInstance,
    InstanceSetTransform,
)
from metricflow.object_utils import flatten_nested_sequence
from metricflow.specs import DimensionSpec, TimeDimensionSpec, IdentifierSpec
from metricflow.sql.sql_plan import (
    SqlSelectStatementNode,
)


class SqlDataSet(DataSet):
    """A metric data set along with the associated SQL query node that can be rendered to get those values."""

    def __init__(self, instance_set: InstanceSet, sql_select_node: SqlSelectStatementNode) -> None:
        """Constructor.

        Args:
            instance_set: Describes the instances in the SQL.
            sql_select_node: The SQL that can be rendered to realize the instance set
        """
        self._sql_select_node = sql_select_node
        super().__init__(instance_set=instance_set)

    @property
    def sql_select_node(self) -> SqlSelectStatementNode:
        """Return a SELECT node that can be used to read data from the given SQL table or SQL query"""
        return self._sql_select_node

    def column_associations_for_identifier(
        self,
        identifier_spec: IdentifierSpec,
    ) -> Sequence[ColumnAssociation]:
        """Given the name of the identifier, return the set of columns associated with it in the data set."""
        matching_instances = 0
        column_associations_to_return = None
        for linkable_instance in self.instance_set.identifier_instances:
            if (
                identifier_spec.element_name == linkable_instance.spec.element_name
                and identifier_spec.identifier_links == linkable_instance.spec.identifier_links
            ):
                column_associations_to_return = linkable_instance.associated_columns
                matching_instances += 1

        if matching_instances > 1:
            raise RuntimeError(
                f"More than one instance with spec {identifier_spec} in " f"instance set: {self.instance_set}"
            )

        if not column_associations_to_return:
            raise RuntimeError(f"No instances with spec {identifier_spec} in instance set: {self.instance_set}")

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
    def groupable_column_associations(self) -> Sequence[ColumnAssociation]:
        """Return a flattened iterable with all groupable column associations for the current data set"""
        instances = (
            self.instance_set.identifier_instances
            + self.instance_set.dimension_instances
            + self.instance_set.time_dimension_instances
        )
        return flatten_nested_sequence([instance.associated_columns for instance in instances])


class SameDataSourceReferenceChecker(InstanceSetTransform[bool]):
    """Checks to see that all elements in the instance set come from the same data source."""

    def __init__(self, data_source_reference: DataSourceReference) -> None:  # noqa: D
        self._data_source_reference = data_source_reference

    def transform(self, instance_set: InstanceSet) -> bool:  # noqa: D
        combined: List[DataSourceElementInstance] = []
        combined.extend(instance_set.measure_instances)
        combined.extend(instance_set.dimension_instances)
        combined.extend(instance_set.time_dimension_instances)
        combined.extend(instance_set.identifier_instances)

        return all([all([y.is_from(self._data_source_reference) for y in x.defined_from]) for x in combined])
