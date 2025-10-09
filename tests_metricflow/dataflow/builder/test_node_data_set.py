from __future__ import annotations

import logging
from typing import Mapping

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.references import SemanticModelElementReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.aggregation_properties import AggregationState
from metricflow_semantics.instances import (
    InstanceSet,
    SimpleMetricInputInstance,
)
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociation
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.specs.entity_spec import LinklessEntitySpec
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.sql.sql_exprs import SqlColumnReference, SqlColumnReferenceExpression
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_spec_set_snapshot_equal
from metricflow_semantics.time.time_spine_source import TimeSpineSource

from metricflow.dataflow.nodes.join_to_base import JoinDescription, JoinOnEntitiesNode
from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
from metricflow.dataset.sql_dataset import SqlDataSet
from metricflow.plan_conversion.to_sql_plan.dataflow_to_subquery import DataflowNodeToSqlSubqueryVisitor
from metricflow.sql.sql_plan import (
    SqlSelectColumn,
)
from metricflow.sql.sql_select_node import SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup

logger = logging.getLogger(__name__)


def test_no_parent_node_data_set(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    time_spine_sources: Mapping[TimeGranularity, TimeSpineSource],
) -> None:
    """Tests getting the data set from a single node."""
    resolver: DataflowNodeToSqlSubqueryVisitor = DataflowNodeToSqlSubqueryVisitor(
        column_association_resolver=DunderColumnAssociationResolver(),
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
    )

    data_set = SqlDataSet(
        instance_set=InstanceSet(
            simple_metric_input_instances=(
                SimpleMetricInputInstance(
                    associated_columns=(ColumnAssociation("bookings"),),
                    defined_from=(
                        SemanticModelElementReference(
                            semantic_model_name="fct_bookings_semantic_model", element_name="bookings"
                        ),
                    ),
                    spec=SimpleMetricInputSpec(
                        element_name="bookings",
                    ),
                    aggregation_state=AggregationState.NON_AGGREGATED,
                ),
            ),
            dimension_instances=(),
            time_dimension_instances=(),
            entity_instances=(),
        ),
        sql_select_node=SqlSelectStatementNode.create(
            description="test0",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression.create(
                        SqlColumnReference(table_alias="src", column_name="bookings")
                    ),
                    column_alias="bookings",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="demo", table_name="fct_bookings")),
            from_source_alias="src",
        ),
    )

    node = ReadSqlSourceNode.create(data_set=data_set)

    assert resolver.get_output_data_set(node).instance_set == data_set.instance_set


def test_joined_node_data_set(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    time_spine_sources: Mapping[TimeGranularity, TimeSpineSource],
) -> None:
    """Tests getting the data set from a dataflow plan with a join."""
    resolver: DataflowNodeToSqlSubqueryVisitor = DataflowNodeToSqlSubqueryVisitor(
        column_association_resolver=DunderColumnAssociationResolver(),
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
    )

    # Join "revenue" with "users_latest" to get "user__home_state_latest"
    revenue_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping["revenue"]
    users_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping["users_latest"]
    join_node = JoinOnEntitiesNode.create(
        left_node=revenue_node,
        join_targets=[
            JoinDescription(
                join_node=users_node,
                join_on_entity=LinklessEntitySpec.from_element_name("user"),
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
                join_type=SqlJoinType.LEFT_OUTER,
            )
        ],
    )

    join_node_output_data_set = resolver.get_output_data_set(join_node)

    assert_spec_set_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        set_id="result0",
        spec_set=join_node_output_data_set.instance_set.spec_set,
    )
