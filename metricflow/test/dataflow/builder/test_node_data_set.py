import logging

from metricflow.column_assoc import ColumnAssociation, SingleColumnCorrelationKey
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.dataflow_plan import ReadSqlSourceNode, JoinToBaseOutputNode, JoinDescription
from metricflow.dataflow.sql_table import SqlTable
from metricflow.instances import (
    InstanceSet,
    MeasureInstance,
    DataSourceElementReference,
    AggregationState,
)
from metricflow.model.semantic_model import SemanticModel
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.sql_dataset import SqlDataSet
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.specs import (
    IdentifierSpec,
    MeasureSpec,
    TimeDimensionSpec,
    LinklessIdentifierSpec,
    DimensionSpec,
    InstanceSpecSet,
)
from metricflow.sql.sql_exprs import SqlColumnReferenceExpression, SqlColumnReference
from metricflow.sql.sql_plan import SqlSelectStatementNode, SqlSelectColumn, SqlTableFromClauseNode
from metricflow.time.time_granularity import TimeGranularity
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository

logger = logging.getLogger(__name__)


def test_no_parent_node_data_set(
    simple_semantic_model: SemanticModel,
    time_spine_source: TimeSpineSource,
) -> None:
    """Tests getting the data set from a single node."""
    resolver: DataflowPlanNodeOutputDataSetResolver = DataflowPlanNodeOutputDataSetResolver(
        column_association_resolver=DefaultColumnAssociationResolver(simple_semantic_model),
        semantic_model=simple_semantic_model,
        time_spine_source=time_spine_source,
    )

    data_set = SqlDataSet(
        instance_set=InstanceSet(
            measure_instances=(
                MeasureInstance(
                    associated_columns=(
                        ColumnAssociation(column_name="bookings", column_correlation_key=SingleColumnCorrelationKey()),
                    ),
                    defined_from=(
                        DataSourceElementReference(
                            data_source_name="fct_bookings_data_source", element_name="bookings"
                        ),
                    ),
                    spec=MeasureSpec(
                        element_name="bookings",
                    ),
                    aggregation_state=AggregationState.NON_AGGREGATED,
                ),
            ),
            dimension_instances=(),
            time_dimension_instances=(),
            identifier_instances=(),
        ),
        sql_select_node=SqlSelectStatementNode(
            description="test0",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression(SqlColumnReference(table_alias="src", column_name="bookings")),
                    column_alias="bookings",
                ),
            ),
            from_source=SqlTableFromClauseNode(sql_table=SqlTable(schema_name="demo", table_name="fct_bookings")),
            from_source_alias="src",
            joins_descs=(),
            where=None,
            group_bys=(),
            order_bys=(),
        ),
    )

    node = ReadSqlSourceNode(data_set=data_set)

    assert resolver.get_output_data_set(node).instance_set == data_set.instance_set


def test_joined_node_data_set(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    simple_semantic_model: SemanticModel,
    time_spine_source: TimeSpineSource,
) -> None:
    """Tests getting the data set from a dataflow plan with a join."""
    resolver: DataflowPlanNodeOutputDataSetResolver = DataflowPlanNodeOutputDataSetResolver(
        column_association_resolver=DefaultColumnAssociationResolver(simple_semantic_model),
        semantic_model=simple_semantic_model,
        time_spine_source=time_spine_source,
    )

    # Join "revenue" with "users_latest" to get "user__home_state_latest"
    revenue_node = ReadSqlSourceNode(consistent_id_object_repository.simple_model_data_sets["revenue"])
    users_node = ReadSqlSourceNode(consistent_id_object_repository.simple_model_data_sets["users_latest"])
    join_node = JoinToBaseOutputNode(
        parent_node=revenue_node,
        join_targets=[
            JoinDescription(
                join_node=users_node,
                join_on_identifier=LinklessIdentifierSpec.from_element_name("user"),
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
            )
        ],
    )

    join_node_output_data_set = resolver.get_output_data_set(join_node)
    assert join_node_output_data_set.instance_set.spec_set == InstanceSpecSet(
        metric_specs=(),
        measure_specs=(
            MeasureSpec(
                element_name="txn_revenue",
            ),
        ),
        dimension_specs=(
            DimensionSpec(
                element_name="home_state_latest",
                identifier_links=(LinklessIdentifierSpec(element_name="user", identifier_links=()),),
            ),
        ),
        identifier_specs=(IdentifierSpec(element_name="user", identifier_links=()),),
        time_dimension_specs=(
            TimeDimensionSpec(element_name="ds", identifier_links=(), time_granularity=TimeGranularity.DAY),
            TimeDimensionSpec(element_name="ds", identifier_links=(), time_granularity=TimeGranularity.WEEK),
            TimeDimensionSpec(element_name="ds", identifier_links=(), time_granularity=TimeGranularity.MONTH),
            TimeDimensionSpec(element_name="ds", identifier_links=(), time_granularity=TimeGranularity.QUARTER),
            TimeDimensionSpec(element_name="ds", identifier_links=(), time_granularity=TimeGranularity.YEAR),
            TimeDimensionSpec(
                element_name="ds",
                identifier_links=(LinklessIdentifierSpec(element_name="user", identifier_links=()),),
                time_granularity=TimeGranularity.DAY,
            ),
            TimeDimensionSpec(
                element_name="ds",
                identifier_links=(LinklessIdentifierSpec(element_name="user", identifier_links=()),),
                time_granularity=TimeGranularity.WEEK,
            ),
            TimeDimensionSpec(
                element_name="ds",
                identifier_links=(LinklessIdentifierSpec(element_name="user", identifier_links=()),),
                time_granularity=TimeGranularity.MONTH,
            ),
            TimeDimensionSpec(
                element_name="ds",
                identifier_links=(LinklessIdentifierSpec(element_name="user", identifier_links=()),),
                time_granularity=TimeGranularity.QUARTER,
            ),
            TimeDimensionSpec(
                element_name="ds",
                identifier_links=(LinklessIdentifierSpec(element_name="user", identifier_links=()),),
                time_granularity=TimeGranularity.YEAR,
            ),
        ),
    )
