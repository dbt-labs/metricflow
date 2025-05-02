from __future__ import annotations

from collections import OrderedDict
from itertools import chain
from typing import Dict, List, Optional, Sequence, Tuple, Union

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.instances import InstanceSet, InstanceSetTransform, MdoInstance
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.sql.sql_exprs import SqlColumnReference, SqlColumnReferenceExpression

from metricflow.plan_conversion.instance_set_transforms.instance_converters import InstanceT, logger
from metricflow.plan_conversion.select_column_gen import SelectColumnSet
from metricflow.sql.sql_plan import SqlSelectColumn


class CreateSelectColumnsForInstances(InstanceSetTransform[SelectColumnSet]):
    """Create select column expressions that will express all instances in the set.

    It assumes that the column names of the instances are represented by the supplied column association resolver and
    come from the given table alias.
    """

    def __init__(
        self,
        table_alias: str,
        column_resolver: ColumnAssociationResolver,
        spec_output_order: Sequence[InstanceSpec] = (),
        output_to_input_column_mapping: Optional[OrderedDict[str, str]] = None,
    ) -> None:
        """Initializer.

        Args:
            table_alias: the table alias to select columns from
            column_resolver: resolver to name columns.
            spec_output_order: If specified, order the output columns for instances according to the given order of the
            instances' specs.
            output_to_input_column_mapping: if specified, use these columns in the input for the given output columns.
        """
        self._table_alias = table_alias
        self._column_resolver = column_resolver
        self._output_to_input_column_mapping = output_to_input_column_mapping or OrderedDict()
        # Map the instance spec to a key that can be used to sort the order of output columns.
        self._spec_to_output_column_sort_key = {spec: i for i, spec in enumerate(spec_output_order)}

    def _sort_instances_by_output_order(self, instances: Sequence[InstanceT]) -> Sequence[InstanceT]:
        """Sort instances in the same order as `spec_output_order` / `_spec_to_output_column_sort_key`."""
        if len(self._spec_to_output_column_sort_key) == 0:
            return instances

        def _sort_key_function(instance: InstanceT) -> Union[int, float]:
            key = self._spec_to_output_column_sort_key.get(instance.spec)
            if key is None:
                logger.error(
                    LazyFormat(
                        "Bug: Missing sort key for an instance so returning a sentinel value to put the instance at the"
                        " end. This should result in a query that still runs, but the output columns may not be in the"
                        " expected order.",
                        instance=instance,
                        spec_to_sort_key=self._spec_to_output_column_sort_key,
                    )
                )
                return float("inf")

            return key

        return sorted(instances, key=_sort_key_function)

    def transform(self, instance_set: InstanceSet) -> SelectColumnSet:  # noqa: D102
        metric_cols = tuple(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.metric_instances])
        )
        measure_cols = tuple(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.measure_instances])
        )
        dimension_cols = tuple(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.dimension_instances])
        )
        time_dimension_cols = tuple(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.time_dimension_instances])
        )
        entity_cols = tuple(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.entity_instances])
        )
        metadata_cols = tuple(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.metadata_instances])
        )
        group_by_metric_cols = tuple(
            chain.from_iterable([self._make_sql_column_expression(x) for x in instance_set.group_by_metric_instances])
        )
        columns_in_order: Optional[AnyLengthTuple[SqlSelectColumn]] = None

        if len(self._spec_to_output_column_sort_key) > 0:
            group_by_item_columns = tuple(
                chain.from_iterable(
                    [
                        self._make_sql_column_expression(instance)
                        for instance in self._sort_instances_by_output_order(
                            instance_set.time_dimension_instances
                            + instance_set.entity_instances
                            + instance_set.dimension_instances
                            + instance_set.group_by_metric_instances
                        )
                    ]
                )
            )
            columns_in_order = group_by_item_columns + metric_cols + measure_cols + metadata_cols
            logger.debug(
                LazyFormat(
                    "Generated columns using the specified order",
                    spec_to_sort_key=self._spec_to_output_column_sort_key,
                    columns_in_order=columns_in_order,
                )
            )

        return SelectColumnSet.create(
            metric_columns=metric_cols,
            measure_columns=measure_cols,
            dimension_columns=dimension_cols,
            time_dimension_columns=time_dimension_cols,
            entity_columns=entity_cols,
            group_by_metric_columns=group_by_metric_cols,
            metadata_columns=metadata_cols,
            columns_in_order=columns_in_order,
        )

    def _make_sql_column_expression(
        self,
        element_instance: MdoInstance,
    ) -> List[SqlSelectColumn]:
        """Convert one element instance into a SQL column."""
        # Do a sanity check to make sure that there's a 1:1 mapping between the columns associations generated by the
        # column resolver based on the spec, and the columns that are already associated with the instance.
        expected_column_associations = (self._column_resolver.resolve_spec(element_instance.spec),)
        existing_column_associations = element_instance.associated_columns

        # Dict between the expected column name and the corresponding column in the existing columns
        column_matches: Dict[str, List[str]] = {
            expected_column.column_name: [
                col.column_name
                for col in existing_column_associations
                if col.column_correlation_key == expected_column.column_correlation_key
            ]
            for expected_column in expected_column_associations
        }

        # Assert a 1:1 mapping between expected and existing
        assert all([len(x) == 1 for x in column_matches.values()]), (
            f"Did not find exactly one match for each expected column associations.  "
            f"Expected -> existing mappings: {column_matches}"
        )
        existing_names = set([col.column_name for col in existing_column_associations])
        mapped_names = set()
        mapped_cols: List[str] = []
        for mapped_cols in column_matches.values():
            mapped_names.update([col_name for col_name in mapped_cols])
        assert existing_names == mapped_names, (
            f"Not all existing columns were mapped. Existing: {existing_names}.  Mapped: {mapped_cols}, "
            f"{expected_column_associations} -- {existing_column_associations}"
        )

        select_columns = []
        for expected_name, mapped_cols in column_matches.items():
            input_column_name = mapped_cols[0]
            output_column_name = expected_name

            if output_column_name in self._output_to_input_column_mapping:
                input_column_name = self._output_to_input_column_mapping[output_column_name]
            select_columns.append(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(SqlColumnReference(self._table_alias, input_column_name)),
                    column_alias=output_column_name,
                )
            )
        return select_columns


def create_simple_select_columns_for_instance_sets(
    column_resolver: ColumnAssociationResolver,
    table_alias_to_instance_set: OrderedDict[str, InstanceSet],
) -> Tuple[SqlSelectColumn, ...]:
    """Creates select columns for instance sets coming from multiple table as defined in table_alias_to_instance_set.

    Used in cases where you join multiple tables and need to render select columns to access all of those.
    """
    column_set = SelectColumnSet.create()
    for table_alias, instance_set in table_alias_to_instance_set.items():
        column_set = column_set.merge(
            instance_set.transform(
                CreateSelectColumnsForInstances(
                    table_alias=table_alias,
                    column_resolver=column_resolver,
                )
            )
        )

    return column_set.columns_in_order
