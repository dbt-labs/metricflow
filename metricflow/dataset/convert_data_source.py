import logging
import re
from dataclasses import dataclass
from typing import Optional, List, Tuple, Sequence

from metricflow.dag.id_generation import IdGeneratorRegistry
from metricflow.dataflow.sql_table import SqlTable
from metricflow.instances import (
    MeasureInstance,
    DataSourceElementReference,
    DimensionInstance,
    TimeDimensionInstance,
    IdentifierInstance,
    InstanceSet,
    DataSourceReference,
    AggregationState,
)
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.dimension import Dimension, DimensionType
from metricflow.model.objects.elements.identifier import Identifier, IdentifierType
from metricflow.model.objects.elements.measure import Measure
from metricflow.specs import (
    TimeDimensionSpec,
    MeasureSpec,
    DimensionSpec,
    IdentifierSpec,
    ColumnAssociationResolver,
    LinklessIdentifierSpec,
)
from metricflow.sql.sql_exprs import (
    SqlStringExpression,
    SqlDateTruncExpression,
    SqlColumnReferenceExpression,
    SqlColumnReference,
    SqlExpressionNode,
)
from metricflow.sql.sql_plan import (
    SqlTableFromClauseNode,
    SqlSelectStatementNode,
    SqlSelectColumn,
    SqlQueryPlanNode,
    SqlSelectQueryFromClauseNode,
)
from metricflow.time.time_granularity import TimeGranularity
from metricflow.dataset.data_source_adapter import DataSourceDataSet

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DimensionConversionResult:
    """Helper class for returning the results of converting dimensions from a data source."""

    dimension_instances: Sequence[DimensionInstance]
    time_dimension_instances: Sequence[TimeDimensionInstance]
    select_columns: Sequence[SqlSelectColumn]


class DataSourceToDataSetConverter:
    """Converts a data source in the model to a data set that can be used with the dataflow plan builder.

    Identifier links generally refer to the identifiers used to join the measure source to the dimension source. For
    example, the dimension name "user_id__device_id__platform" has identifier links "user_id" and "device_id" and would
    mean that the measure source was joined by "user_id" to an intermediate source, and then it was joined by
    "device_id" to the source containing the "platform" dimension.
    """

    # Regex for inferring whether an expression for an element is a column reference.
    _SQL_IDENTIFIER_REGEX = re.compile("^[a-zA-Z_][a-zA-Z_0-9]*$")

    def __init__(self, column_association_resolver: ColumnAssociationResolver) -> None:  # noqa: D
        self._column_association_resolver = column_association_resolver

    def _create_dimension_instance(
        self,
        data_source_name: str,
        dimension: Dimension,
        identifier_links: Tuple[LinklessIdentifierSpec, ...],
    ) -> DimensionInstance:
        """Create a dimension instance from the dimension object in the model."""
        dimension_spec = DimensionSpec(
            element_name=dimension.reference.element_name,
            identifier_links=identifier_links,
        )
        column_associations = dimension_spec.column_associations(self._column_association_resolver)

        return DimensionInstance(
            associated_columns=column_associations,
            spec=dimension_spec,
            defined_from=(
                DataSourceElementReference(
                    data_source_name=data_source_name,
                    element_name=dimension.reference.element_name,
                ),
            ),
        )

    def _create_time_dimension_instance(
        self,
        data_source_name: str,
        time_dimension: Dimension,
        identifier_links: Tuple[LinklessIdentifierSpec, ...],
        time_granularity: Optional[TimeGranularity] = None,
    ) -> TimeDimensionInstance:
        """Create a time dimension instance from the dimension object from a data source in the model."""
        time_dimension_spec = TimeDimensionSpec(
            element_name=time_dimension.reference.element_name,
            identifier_links=identifier_links,
            time_granularity=time_granularity,
        )

        column_associations = time_dimension_spec.column_associations(self._column_association_resolver)

        return TimeDimensionInstance(
            associated_columns=column_associations,
            spec=time_dimension_spec,
            defined_from=(
                DataSourceElementReference(
                    data_source_name=data_source_name,
                    element_name=time_dimension.reference.element_name,
                ),
            ),
        )

    def _create_identifier_instance(
        self,
        data_source_name: str,
        identifier: Identifier,
        identifier_links: Tuple[LinklessIdentifierSpec, ...],
    ) -> IdentifierInstance:
        """Create an identifier instance from the identifier object from a data sourcein the model."""
        identifier_spec = IdentifierSpec(
            element_name=identifier.reference.element_name,
            identifier_links=identifier_links,
        )
        column_associations = identifier_spec.column_associations(self._column_association_resolver)

        return IdentifierInstance(
            associated_columns=column_associations,
            spec=identifier_spec,
            defined_from=(
                DataSourceElementReference(
                    data_source_name=data_source_name,
                    element_name=identifier.reference.element_name,
                ),
            ),
        )

    @staticmethod
    def _make_element_sql_expr(
        table_alias: str, element_name: str, element_expr: Optional[str] = None
    ) -> SqlExpressionNode:
        """Create an expression that can be used for reading the element from the data source's SQL."""
        if element_expr:
            if DataSourceToDataSetConverter._SQL_IDENTIFIER_REGEX.match(element_expr) and element_expr.upper() not in (
                "TRUE",
                "FALSE",
                "NULL",
            ):
                return SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=table_alias,
                        column_name=element_expr,
                    )
                )
            return SqlStringExpression(sql_expr=element_expr)

        return SqlColumnReferenceExpression(
            SqlColumnReference(
                table_alias=table_alias,
                column_name=element_name,
            )
        )

    def _convert_measures(
        self,
        data_source_name: str,
        measures: Sequence[Measure],
        measure_time_dimension_spec: TimeDimensionSpec,
        table_alias: str,
    ) -> Tuple[Sequence[MeasureInstance], Sequence[SqlSelectColumn]]:
        # Convert all elements to instances
        measure_instances = []
        select_columns = []
        for measure in measures or []:
            measure_spec = MeasureSpec(
                element_name=measure.reference.element_name,
            )
            measure_instance = MeasureInstance(
                associated_columns=measure_spec.column_associations(self._column_association_resolver),
                spec=measure_spec,
                defined_from=(
                    DataSourceElementReference(
                        data_source_name=data_source_name,
                        element_name=measure.reference.element_name,
                    ),
                ),
                aggregation_state=AggregationState.NON_AGGREGATED,
            )
            measure_instances.append(measure_instance)
            select_columns.append(
                SqlSelectColumn(
                    expr=DataSourceToDataSetConverter._make_element_sql_expr(
                        table_alias=table_alias,
                        element_name=measure.reference.element_name,
                        element_expr=measure.expr,
                    ),
                    column_alias=measure_instance.associated_column.column_name,
                )
            )

        return measure_instances, select_columns

    def _convert_dimensions(
        self,
        data_source_name: str,
        dimensions: Sequence[Dimension],
        identifier_links: Tuple[LinklessIdentifierSpec, ...],
        table_alias: str,
    ) -> DimensionConversionResult:
        dimension_instances = []
        time_dimension_instances = []
        select_columns = []

        for dimension in dimensions or []:
            if dimension.type == DimensionType.CATEGORICAL:
                dimension_instance = self._create_dimension_instance(
                    data_source_name=data_source_name,
                    dimension=dimension,
                    identifier_links=identifier_links,
                )
                dimension_instances.append(dimension_instance)
                select_columns.append(
                    SqlSelectColumn(
                        expr=DataSourceToDataSetConverter._make_element_sql_expr(
                            table_alias=table_alias,
                            element_name=dimension.reference.element_name,
                            element_expr=dimension.expr,
                        ),
                        column_alias=dimension_instance.associated_column.column_name,
                    )
                )

            elif dimension.type == DimensionType.TIME:
                defined_time_granularity = TimeGranularity.DAY
                if dimension.type_params and dimension.type_params.time_granularity:
                    defined_time_granularity = dimension.type_params.time_granularity

                time_dimension_instance = self._create_time_dimension_instance(
                    data_source_name=data_source_name,
                    time_dimension=dimension,
                    identifier_links=identifier_links,
                    time_granularity=defined_time_granularity,
                )
                time_dimension_instances.append(time_dimension_instance)
                select_columns.append(
                    SqlSelectColumn(
                        expr=DataSourceToDataSetConverter._make_element_sql_expr(
                            table_alias=table_alias,
                            element_name=dimension.reference.element_name,
                            element_expr=dimension.expr,
                        ),
                        column_alias=time_dimension_instance.associated_column.column_name,
                    )
                )

                # Add time dimensions with a smaller granularity for ease in query resolution
                for time_granularity in TimeGranularity:
                    if time_granularity.to_int() > defined_time_granularity.to_int():
                        time_dimension_instance = self._create_time_dimension_instance(
                            data_source_name=data_source_name,
                            time_dimension=dimension,
                            identifier_links=identifier_links,
                            time_granularity=time_granularity,
                        )
                        time_dimension_instances.append(time_dimension_instance)

                        select_columns.append(
                            SqlSelectColumn(
                                expr=SqlDateTruncExpression(
                                    time_granularity=time_granularity,
                                    arg=DataSourceToDataSetConverter._make_element_sql_expr(
                                        table_alias=table_alias,
                                        element_name=dimension.reference.element_name,
                                        element_expr=dimension.expr,
                                    ),
                                ),
                                column_alias=time_dimension_instance.associated_column.column_name,
                            )
                        )
            else:
                assert False, f"Unhandled dimension type: {dimension.type}"

        return DimensionConversionResult(
            dimension_instances=dimension_instances,
            time_dimension_instances=time_dimension_instances,
            select_columns=select_columns,
        )

    def _create_identifier_instances(
        self,
        data_source_name: str,
        identifiers: Sequence[Identifier],
        identifier_links: Tuple[LinklessIdentifierSpec, ...],
        table_alias: str,
    ) -> Tuple[Sequence[IdentifierInstance], Sequence[SqlSelectColumn]]:
        identifier_instances = []
        select_columns = []
        for identifier in identifiers or []:
            # We don't want to create something like user_id__user_id, so skip if the link is the same as the
            # identifier.
            if (
                len(identifier_links) == 1
                and LinklessIdentifierSpec.from_element_name(identifier.reference.element_name) == identifier_links[0]
            ):
                continue

            identifier_instance = self._create_identifier_instance(
                data_source_name=data_source_name,
                identifier=identifier,
                identifier_links=identifier_links,
            )

            identifier_instances.append(identifier_instance)
            if identifier.is_composite:
                for idx in range(len(identifier.identifiers)):
                    sub_id = identifier.identifiers[idx]
                    column_name = identifier_instance.associated_columns[idx].column_name

                    expr = sub_id.expr
                    if expr is None:
                        assert sub_id.name is not None
                        expr = sub_id.name
                    sub_id_name = sub_id.ref or sub_id.name
                    assert sub_id_name, f"Sub-identifier {sub_id} must have 'name' or 'ref' defined"

                    select_columns.append(
                        SqlSelectColumn(
                            expr=DataSourceToDataSetConverter._make_element_sql_expr(
                                table_alias=table_alias,
                                element_name=sub_id_name,
                                element_expr=expr,
                            ),
                            column_alias=column_name,
                        )
                    )
            else:
                select_columns.append(
                    SqlSelectColumn(
                        expr=DataSourceToDataSetConverter._make_element_sql_expr(
                            table_alias=table_alias,
                            element_name=identifier.reference.element_name,
                            element_expr=identifier.expr,
                        ),
                        column_alias=identifier_instance.associated_column.column_name,
                    )
                )
        return identifier_instances, select_columns

    @staticmethod
    def _find_primary_time_dimension(data_source: DataSource) -> TimeDimensionSpec:
        # If a data source has measures, it should have a primary time dimension. Find it and use it to set the time
        # dimension for the measure.
        primary_time_dimensions = [dimension for dimension in data_source.dimensions if dimension.is_primary_time]
        assert len(primary_time_dimensions) == 1, (
            f"Data source ({data_source}) with measures should have exactly 1 primary time dimension, found "
            f"{len(primary_time_dimensions)}: {primary_time_dimensions}."
        )
        primary_time_dimension = primary_time_dimensions[0]
        assert (
            primary_time_dimension.type_params and primary_time_dimension.type_params.time_granularity
        ), f"Primary time dimension missing time granularity: {primary_time_dimension}"

        return TimeDimensionSpec(
            element_name=primary_time_dimension.reference.element_name,
            identifier_links=(),
            time_granularity=primary_time_dimension.type_params.time_granularity,
        )

    def create_sql_source_data_set(self, data_source: DataSource) -> DataSourceDataSet:
        """Create an SQL source data set from a data source in the model."""

        # Gather all instances and columns from all data sources.
        all_measure_instances: List[MeasureInstance] = []
        all_dimension_instances: List[DimensionInstance] = []
        all_time_dimension_instances: List[TimeDimensionInstance] = []
        all_identifier_instances: List[IdentifierInstance] = []

        all_select_columns: List[SqlSelectColumn] = []
        from_source_alias = IdGeneratorRegistry.for_class(self.__class__).create_id(f"{data_source.name}_src")

        # Handle measures
        if len(data_source.measures) > 0:
            primary_time_dimension = self._find_primary_time_dimension(data_source)
            measure_instances, select_columns = self._convert_measures(
                data_source_name=data_source.name,
                measures=data_source.measures,
                measure_time_dimension_spec=primary_time_dimension,
                table_alias=from_source_alias,
            )
            all_measure_instances.extend(measure_instances)
            all_select_columns.extend(select_columns)

        # For dimensions in a data source, we can access them through the local form, or the dundered form.
        # e.g. in the "users" data source, with the "country" dimension and the "user_id" identifier,
        # the dimensions "country" and "user_id__country" both mean the same thing. To make matching easier, create both
        # instances in the instance set. We'll create a different instance for each "possible_identifier_links".
        possible_identifier_links: List[Tuple[LinklessIdentifierSpec, ...]] = [()]
        for identifier in data_source.identifiers:
            if identifier.type in (IdentifierType.PRIMARY, IdentifierType.UNIQUE):
                possible_identifier_links.append(
                    (LinklessIdentifierSpec.from_element_name(element_name=identifier.reference.element_name),)
                )

        # Handle dimensions
        conversion_results = [
            self._convert_dimensions(
                data_source_name=data_source.name,
                dimensions=data_source.dimensions,
                identifier_links=identifier_links,
                table_alias=from_source_alias,
            )
            for identifier_links in possible_identifier_links
        ]

        all_dimension_instances.extend(
            [
                dimension_instance
                for conversion_result in conversion_results
                for dimension_instance in conversion_result.dimension_instances
            ]
        )

        all_time_dimension_instances.extend(
            [
                time_dimension_instance
                for conversion_result in conversion_results
                for time_dimension_instance in conversion_result.time_dimension_instances
            ]
        )

        all_select_columns.extend(
            [
                select_column
                for conversion_result in conversion_results
                for select_column in conversion_result.select_columns
            ]
        )

        # Handle identifiers
        for identifier_links in possible_identifier_links:
            identifier_instances, select_columns = self._create_identifier_instances(
                data_source_name=data_source.name,
                identifiers=data_source.identifiers,
                identifier_links=identifier_links,
                table_alias=from_source_alias,
            )
            all_identifier_instances.extend(identifier_instances)
            all_select_columns.extend(select_columns)

        # Generate the "from" clause depending on whether it's an SQL query or an SQL table.
        from_source: Optional[SqlQueryPlanNode] = None
        if data_source.sql_table:
            from_source = SqlTableFromClauseNode(sql_table=SqlTable.from_string(data_source.sql_table))
        elif data_source.sql_query:
            from_source = SqlSelectQueryFromClauseNode(select_query=data_source.sql_query)
        else:
            raise RuntimeError(f"Data source does not have sql_table or sql_query set: {data_source}")

        select_statement_node = SqlSelectStatementNode(
            description=f"Read Elements From Data Source '{data_source.name}'",
            select_columns=tuple(all_select_columns),
            from_source=from_source,
            from_source_alias=from_source_alias,
            joins_descs=(),
            where=None,
            group_bys=(),
            order_bys=(),
        )

        return DataSourceDataSet(
            data_source_reference=DataSourceReference(data_source_name=data_source.name),
            instance_set=InstanceSet(
                measure_instances=tuple(all_measure_instances),
                dimension_instances=tuple(all_dimension_instances),
                time_dimension_instances=tuple(all_time_dimension_instances),
                identifier_instances=tuple(all_identifier_instances),
                metric_instances=(),
            ),
            sql_select_node=select_statement_node,
        )
