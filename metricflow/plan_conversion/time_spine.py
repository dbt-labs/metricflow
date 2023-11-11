from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List

from dbt_semantic_interfaces.references import TimeDimensionReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dag.id_generation import IdGeneratorRegistry
from metricflow.dataflow.dataflow_plan import MetricTimeDimensionTransformNode, ReadSqlSourceNode
from metricflow.dataflow.sql_table import SqlTable
from metricflow.dataset.sql_dataset import SqlDataSet
from metricflow.instances import InstanceSet, TimeDimensionInstance
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.specs.column_assoc import ColumnAssociation, SingleColumnCorrelationKey
from metricflow.specs.specs import TimeDimensionSpec
from metricflow.sql.sql_exprs import SqlColumnReference, SqlColumnReferenceExpression, SqlDateTruncExpression
from metricflow.sql.sql_plan import SqlSelectColumn, SqlSelectStatementNode, SqlTableFromClauseNode

logger = logging.getLogger(__name__)

TIME_SPINE_DATA_SET_DESCRIPTION = "Date Spine"


@dataclass(frozen=True)
class TimeSpineSource:
    """Defines a source table containing all timestamps to use for computing cumulative metrics."""

    schema_name: str
    table_name: str = "mf_time_spine"
    # Name of the column in the table that contains the dates.
    time_column_name: str = "ds"
    # The time granularity of the dates in the spine table.
    time_column_granularity: TimeGranularity = TimeGranularity.DAY

    @property
    def spine_table(self) -> SqlTable:
        """Table containing all dates."""
        return SqlTable(schema_name=self.schema_name, table_name=self.table_name)

    def build_source_node(self) -> MetricTimeDimensionTransformNode:
        """Build data set for time spine."""
        from_source_alias = IdGeneratorRegistry.for_class(self.__class__).create_id("time_spine_src")

        # TODO: add date part to instances & select columns. Can we use the same logic as elsewhere??
        # TODO: add test cases for date part
        time_spine_instances: List[TimeDimensionInstance] = []
        select_columns: List[SqlSelectColumn] = []
        for granularity in TimeGranularity:
            if granularity.to_int() >= self.time_column_granularity.to_int():
                column_alias = StructuredLinkableSpecName(
                    entity_link_names=(),
                    element_name=self.time_column_name,
                    time_granularity=granularity,
                ).qualified_name
                time_spine_instance = TimeDimensionInstance(
                    defined_from=(),
                    associated_columns=(
                        ColumnAssociation(
                            column_name=column_alias,
                            single_column_correlation_key=SingleColumnCorrelationKey(),
                        ),
                    ),
                    spec=TimeDimensionSpec(
                        element_name=self.time_column_name, entity_links=(), time_granularity=granularity
                    ),
                )
                time_spine_instances.append(time_spine_instance)
                select_column = SqlSelectColumn(
                    SqlDateTruncExpression(
                        time_granularity=granularity,
                        arg=SqlColumnReferenceExpression(
                            SqlColumnReference(
                                table_alias=from_source_alias,
                                column_name=self.time_column_name,
                            ),
                        ),
                    ),
                    column_alias=column_alias,
                )
                select_columns.append(select_column)

        time_spine_instance_set = InstanceSet(time_dimension_instances=tuple(time_spine_instances))

        data_set = SqlDataSet(
            instance_set=time_spine_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=TIME_SPINE_DATA_SET_DESCRIPTION,
                select_columns=tuple(select_columns),
                from_source=SqlTableFromClauseNode(sql_table=self.spine_table),
                from_source_alias=from_source_alias,
                joins_descs=(),
                group_bys=(),
                order_bys=(),
            ),
        )

        return MetricTimeDimensionTransformNode(
            parent_node=ReadSqlSourceNode(data_set=data_set),
            aggregation_time_dimension_reference=TimeDimensionReference(element_name=self.time_column_name),
        )
