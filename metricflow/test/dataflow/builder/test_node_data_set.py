from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.references import SemanticModelElementReference

from metricflow.aggregation_properties import AggregationState
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.dataflow_plan import JoinDescription, JoinToBaseOutputNode, ReadSqlSourceNode
from metricflow.dataflow.sql_table import SqlTable
from metricflow.dataset.sql_dataset import SqlDataSet
from metricflow.instances import (
    InstanceSet,
    MeasureInstance,
)
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.specs.column_assoc import ColumnAssociation, SingleColumnCorrelationKey
from metricflow.specs.specs import (
    LinklessEntitySpec,
    MeasureSpec,
)
from metricflow.sql.sql_exprs import SqlColumnReference, SqlColumnReferenceExpression
from metricflow.sql.sql_plan import SqlSelectColumn, SqlSelectStatementNode, SqlTableFromClauseNode
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.snapshot_utils import assert_spec_set_snapshot_equal

logger = logging.getLogger(__name__)


def test_no_parent_node_data_set(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    time_spine_source: TimeSpineSource,
) -> None:
    """Tests getting the data set from a single node."""
    resolver: DataflowPlanNodeOutputDataSetResolver = DataflowPlanNodeOutputDataSetResolver(
        column_association_resolver=DunderColumnAssociationResolver(simple_semantic_manifest_lookup),
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
    )

    data_set = SqlDataSet(
        instance_set=InstanceSet(
            measure_instances=(
                MeasureInstance(
                    associated_columns=(
                        ColumnAssociation(
                            column_name="bookings", single_column_correlation_key=SingleColumnCorrelationKey()
                        ),
                    ),
                    defined_from=(
                        SemanticModelElementReference(
                            semantic_model_name="fct_bookings_semantic_model", element_name="bookings"
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
            entity_instances=(),
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


def test_joined_node_data_set(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    consistent_id_object_repository: ConsistentIdObjectRepository,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    time_spine_source: TimeSpineSource,
) -> None:
    """Tests getting the data set from a dataflow plan with a join."""
    resolver: DataflowPlanNodeOutputDataSetResolver = DataflowPlanNodeOutputDataSetResolver(
        column_association_resolver=DunderColumnAssociationResolver(simple_semantic_manifest_lookup),
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
    )

    # Join "revenue" with "users_latest" to get "user__home_state_latest"
    revenue_node = consistent_id_object_repository.simple_model_read_nodes["revenue"]
    users_node = consistent_id_object_repository.simple_model_read_nodes["users_latest"]
    join_node = JoinToBaseOutputNode(
        left_node=revenue_node,
        join_targets=[
            JoinDescription(
                join_node=users_node,
                join_on_entity=LinklessEntitySpec.from_element_name("user"),
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
            )
        ],
    )

    join_node_output_data_set = resolver.get_output_data_set(join_node)

    assert_spec_set_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        set_id="result0",
        spec_set=join_node_output_data_set.instance_set.spec_set,
    )
