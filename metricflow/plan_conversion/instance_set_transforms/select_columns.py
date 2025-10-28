from __future__ import annotations

import logging
from collections import OrderedDict
from dataclasses import dataclass
from functools import cached_property
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from metricflow_semantics.instances import InstanceSet, InstanceSetTransform, MdoInstance
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.sql.sql_exprs import SqlColumnReference, SqlColumnReferenceExpression
from metricflow_semantics.toolkit.collections.mapping_helpers import mf_items_to_dict
from metricflow_semantics.toolkit.merger import Mergeable
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple, MappingItemsTuple
from typing_extensions import override

from metricflow.plan_conversion.select_column_gen import SelectColumnSet
from metricflow.sql.sql_plan import SqlSelectColumn

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CreateSelectColumnsResult(Mergeable):
    """Result class for `CreateSelectColumnsForInstances`."""

    # Columns that should be in the `SELECT` clause.
    select_column_set: SelectColumnSet
    spec_to_associated_columns_mapping_items: MappingItemsTuple[InstanceSpec, AnyLengthTuple[SqlSelectColumn]]

    @staticmethod
    def create(  # noqa: D102
        select_column_set: SelectColumnSet,
        spec_to_associated_columns_mapping: Mapping[InstanceSpec, Sequence[SqlSelectColumn]],
    ) -> CreateSelectColumnsResult:
        return CreateSelectColumnsResult(
            select_column_set=select_column_set,
            spec_to_associated_columns_mapping_items=tuple(
                (spec, tuple(columns)) for spec, columns in spec_to_associated_columns_mapping.items()
            ),
        )

    @cached_property
    def spec_to_columns_mapping(self) -> Mapping[InstanceSpec, Sequence[SqlSelectColumn]]:  # noqa: D102
        return mf_items_to_dict(self.spec_to_associated_columns_mapping_items)

    @override
    def merge(self, other: CreateSelectColumnsResult) -> CreateSelectColumnsResult:
        return CreateSelectColumnsResult(
            select_column_set=self.select_column_set.merge(other.select_column_set),
            spec_to_associated_columns_mapping_items=self.spec_to_associated_columns_mapping_items
            + other.spec_to_associated_columns_mapping_items,
        )

    @classmethod
    @override
    def empty_instance(cls) -> CreateSelectColumnsResult:
        return CreateSelectColumnsResult(
            select_column_set=SelectColumnSet.empty_instance(), spec_to_associated_columns_mapping_items=()
        )

    def get_columns(self, spec_output_order: Sequence[InstanceSpec] = ()) -> AnyLengthTuple[SqlSelectColumn]:
        """Returns the generated columns.

        If `spec_output_order` is specified, use the relative ordering described to determine the column order.
        """
        if len(spec_output_order) > 0:
            accounted_specs: set[InstanceSpec] = set()
            unknown_specs: set[InstanceSpec] = set()
            output_columns: list[SqlSelectColumn] = []
            for spec in spec_output_order:
                columns = self.spec_to_columns_mapping.get(spec)
                if columns is None:
                    unknown_specs.add(spec)
                    continue
                output_columns.extend(columns)
                accounted_specs.add(spec)

            if len(accounted_specs) != len(spec_output_order):
                unaccounted_specs = set()
                for spec, columns in self.spec_to_columns_mapping.items():
                    if spec not in accounted_specs:
                        unaccounted_specs.add(spec)
                logger.error(
                    LazyFormat(
                        "Mismatch between `spec_output_order` and created specs. This is a bug and should be"
                        " investigated, but returning the default ordering to reduce user-facing errors.",
                        accounted_specs=accounted_specs,
                        unaccounted_specs=unaccounted_specs,
                        unknown_specs=unknown_specs,
                        spec_output_order=spec_output_order,
                        spec_to_columns_mapping=self.spec_to_columns_mapping,
                    )
                )
                return self.select_column_set.columns_in_default_order

            return tuple(output_columns)
        else:
            return self.select_column_set.columns_in_default_order


class CreateSelectColumnsForInstances(InstanceSetTransform[CreateSelectColumnsResult]):
    """Create select column expressions that will express all instances in the set.

    It assumes that the column names of the instances are represented by the supplied column association resolver and
    come from the given table alias.
    """

    def __init__(
        self,
        table_alias: str,
        column_resolver: ColumnAssociationResolver,
        output_to_input_column_mapping: Optional[OrderedDict[str, str]] = None,
    ) -> None:
        """Initializer.

        Args:
            table_alias: the table alias to select columns from
            column_resolver: resolver to name columns.
            output_to_input_column_mapping: if specified, use these columns in the input for the given output columns.
        """
        self._table_alias = table_alias
        self._column_resolver = column_resolver
        self._output_to_input_column_mapping = output_to_input_column_mapping or OrderedDict()

    def transform(self, instance_set: InstanceSet) -> CreateSelectColumnsResult:  # noqa: D102
        spec_to_associated_columns: dict[InstanceSpec, AnyLengthTuple[SqlSelectColumn]] = {}

        metric_cols = []
        for metric_instance in instance_set.metric_instances:
            columns = self._make_sql_column_expression(metric_instance)
            for column in columns:
                metric_cols.append(column)
                metric_spec = metric_instance.spec
                # The metric spec used in the dataflow plan contains additional attributes that are different from
                # the metric spec provided in the query spec (e.g. includes filters), so the mapping should use the
                # simplified form.
                simplified_metric_spec = MetricSpec(element_name=metric_spec.element_name, alias=metric_spec.alias)
                spec_to_associated_columns[simplified_metric_spec] = columns

        simple_metric_input_cols = []
        for simple_metric_input_instance in instance_set.simple_metric_input_instances:
            columns = self._make_sql_column_expression(simple_metric_input_instance)
            for column in columns:
                simple_metric_input_cols.append(column)
                spec_to_associated_columns[simple_metric_input_instance.spec] = columns

        dimension_cols = []
        for dimension_instance in instance_set.dimension_instances:
            columns = self._make_sql_column_expression(dimension_instance)
            for column in columns:
                dimension_cols.append(column)
                spec_to_associated_columns[dimension_instance.spec] = columns

        time_dimension_cols = []
        for time_dimension_instance in instance_set.time_dimension_instances:
            columns = self._make_sql_column_expression(time_dimension_instance)
            for column in columns:
                time_dimension_cols.append(column)
                spec_to_associated_columns[time_dimension_instance.spec] = columns

        entity_cols = []
        for entity_instance in instance_set.entity_instances:
            columns = self._make_sql_column_expression(entity_instance)
            for column in columns:
                entity_cols.append(column)
                spec_to_associated_columns[entity_instance.spec] = columns

        metadata_cols = []
        for metadata_instance in instance_set.metadata_instances:
            columns = self._make_sql_column_expression(metadata_instance)
            for column in columns:
                metadata_cols.append(column)
                spec_to_associated_columns[metadata_instance.spec] = columns

        group_by_metric_cols = []
        for group_metric_instance in instance_set.group_by_metric_instances:
            columns = self._make_sql_column_expression(group_metric_instance)
            for column in columns:
                group_by_metric_cols.append(column)
                spec_to_associated_columns[group_metric_instance.spec] = columns

        return CreateSelectColumnsResult.create(
            SelectColumnSet.create(
                metric_columns=metric_cols,
                simple_metric_input_columns=simple_metric_input_cols,
                dimension_columns=dimension_cols,
                time_dimension_columns=time_dimension_cols,
                entity_columns=entity_cols,
                group_by_metric_columns=group_by_metric_cols,
                metadata_columns=metadata_cols,
            ),
            spec_to_associated_columns_mapping=spec_to_associated_columns,
        )

    def _make_sql_column_expression(
        self,
        element_instance: MdoInstance,
    ) -> AnyLengthTuple[SqlSelectColumn]:
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
        return tuple(select_columns)


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
            ).select_column_set
        )

    return column_set.columns_in_default_order
