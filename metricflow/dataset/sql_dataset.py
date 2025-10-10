from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.references import SemanticModelReference
from dbt_semantic_interfaces.type_enums import DatePart
from metricflow_semantics.instances import EntityInstance, InstanceSet, MdoInstance, TimeDimensionInstance
from metricflow_semantics.specs.column_assoc import ColumnAssociation
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.toolkit.assert_one_arg import assert_exactly_one_arg_set
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import override

from metricflow.dataset.dataset_classes import DataSet
from metricflow.sql.sql_plan import (
    SqlPlanNode,
)
from metricflow.sql.sql_select_node import SqlSelectStatementNode


class SqlDataSet(DataSet):
    """A metric data set along with the associated SQL query node that can be rendered to get those values."""

    def __init__(
        self,
        instance_set: InstanceSet,
        sql_select_node: Optional[SqlSelectStatementNode] = None,
        sql_node: Optional[SqlPlanNode] = None,
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
    def sql_node(self) -> SqlPlanNode:  # noqa: D102
        node_to_return = self._sql_select_node or self._sql_node
        if node_to_return is None:
            raise RuntimeError(
                "This node was not created with a SQL node. This should have been prevented by the initializer."
            )
        return node_to_return

    def with_copied_sql_node(self) -> SqlDataSet:
        """Return a new instance of the dataset a copy of the SQL node."""
        sql_select_node = self._sql_select_node
        if sql_select_node:
            sql_select_node = sql_select_node.copy()
        sql_node = self._sql_node
        if sql_node:
            sql_node = sql_node.copy()
        return SqlDataSet(instance_set=self.instance_set, sql_select_node=sql_select_node, sql_node=sql_node)

    @property
    def checked_sql_select_node(self) -> SqlSelectStatementNode:
        """If applicable, return a SELECT node that can be used to read data from the given SQL table or SQL query.

        Otherwise, an exception is thrown.
        """
        if self._sql_select_node is None:
            raise RuntimeError(
                LazyFormat(
                    f"{self.__class__.__name__} was created with a SQL node that is not a {SqlSelectStatementNode.__name__}",
                    sql_node=self.sql_node.structure_text(),
                )
            )
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

    def instances_for_time_dimensions(
        self, time_dimension_specs: Sequence[TimeDimensionSpec]
    ) -> Tuple[TimeDimensionInstance, ...]:
        """Return the instances associated with these specs in the data set."""
        time_dimension_specs_set = set(time_dimension_specs)
        matching_instances = 0
        instances_to_return: Tuple[TimeDimensionInstance, ...] = ()
        for time_dimension_instance in self.instance_set.time_dimension_instances:
            if time_dimension_instance.spec in time_dimension_specs_set:
                instances_to_return += (time_dimension_instance,)
                matching_instances += 1

        if matching_instances != len(time_dimension_specs_set):
            raise RuntimeError(
                f"Unexpected number of time dimension instances found matching specs.\nSpecs: {time_dimension_specs_set}\n"
                f"Instances: {instances_to_return}"
            )

        return instances_to_return

    def instance_for_time_dimension(self, time_dimension_spec: TimeDimensionSpec) -> TimeDimensionInstance:
        """Given a time dimension spec, return the instance associated with it in the data set."""
        instances = self.instances_for_time_dimensions((time_dimension_spec,))
        if not len(instances) == 1:
            raise RuntimeError(
                f"Unexpected number of time dimension instances found matching specs.\nSpecs: {time_dimension_spec}\n"
                f"Instances: {instances}"
            )
        return instances[0]

    def instance_for_spec(self, spec: InstanceSpec) -> MdoInstance:
        """Given a spec, return the instance associated with it in the data set."""
        instances = self.instance_set.as_tuple
        for instance in instances:
            if instance.spec == spec:
                return instance
        raise RuntimeError(
            LazyFormat("Did not find instance matching spec in dataset.", spec=spec, instances=instances)
        )

    def instance_for_column_name(self, column_name: str) -> MdoInstance:
        """Given a spec, return the instance associated with it in the data set."""
        instances = self.instance_set.as_tuple
        for instance in instances:
            if instance.associated_column.column_name == column_name:
                return instance
        raise RuntimeError(
            LazyFormat(
                "Did not find instance matching column name in dataset.",
                column_name=column_name,
                instances=instances,
            )
        )

    def instance_from_time_dimension_grain_and_date_part(
        self, time_granularity_name: Optional[str] = None, date_part: Optional[DatePart] = None
    ) -> TimeDimensionInstance:
        """Find instance in dataset that matches the given grain and date part."""
        for time_dimension_instance in self.instance_set.time_dimension_instances:
            if (
                time_dimension_instance.spec.time_granularity_name == time_granularity_name
                and time_dimension_instance.spec.date_part == date_part
                and not time_dimension_instance.spec.window_functions
            ):
                return time_dimension_instance

        raise RuntimeError(
            LazyFormat(
                "Did not find a time dimension instance with grain and date part in dataset.",
                time_granularity_name=time_granularity_name,
                date_part=date_part,
                instances_available=self.instance_set.time_dimension_instances,
            )
        )

    def column_association_for_time_dimension(self, time_dimension_spec: TimeDimensionSpec) -> ColumnAssociation:
        """Given the name of the time dimension, return the set of columns associated with it in the data set."""
        return self.instance_for_time_dimension(time_dimension_spec).associated_column

    @property
    @override
    def semantic_model_reference(self) -> Optional[SemanticModelReference]:
        return None

    def annotate(self, alias: str, metric_time_spec: TimeDimensionSpec) -> AnnotatedSqlDataSet:
        """Convert to an AnnotatedSqlDataSet with specified metadata."""
        metric_time_column_name = self.column_association_for_time_dimension(metric_time_spec).column_name
        return AnnotatedSqlDataSet(data_set=self, alias=alias, _metric_time_column_name=metric_time_column_name)


@dataclass(frozen=True)
class AnnotatedSqlDataSet:
    """Class to bind a DataSet to transient properties associated with it at a given point in the SqlQueryPlan."""

    data_set: SqlDataSet
    alias: str
    _metric_time_column_name: Optional[str] = None

    @property
    def metric_time_column_name(self) -> str:
        """Direct accessor for the optional metric time name, only safe to call when we know that value is set."""
        assert (
            self._metric_time_column_name
        ), "Expected a valid metric time dimension name to be associated with this dataset, but did not get one!"
        return self._metric_time_column_name
