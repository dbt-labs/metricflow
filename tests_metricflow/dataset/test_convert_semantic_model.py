from __future__ import annotations

import logging
from typing import Mapping

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.references import SemanticModelReference
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_spec_set_snapshot_equal

from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.sql_plan import SqlPlanNode
from metricflow.sql.sql_select_text_node import SqlSelectTextNode
from metricflow.sql.sql_table_node import SqlTableNode
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.sql.compare_sql_plan import assert_rendered_sql_equal

logger = logging.getLogger(__name__)


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_convert_table_semantic_model_without_simple_metrics(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> None:
    """Simple test for converting a table semantic model. Since there are no simple-metric inputs, primary time is not checked."""
    users_data_set = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].data_set_mapping[
        "users_latest"
    ]

    assert_spec_set_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        set_id="result0",
        spec_set=users_data_set.instance_set.spec_set,
    )
    assert users_data_set.semantic_model_reference == SemanticModelReference(semantic_model_name="users_latest")
    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        plan_id="plan0",
        sql_plan_node=users_data_set.checked_sql_select_node,
        sql_client=sql_client,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_convert_table_semantic_model_with_simple_metrics(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> None:
    """Complete test of table semantic model conversion. This includes the full set of simple-metric inputs/entities/dimensions.

    Measures trigger a primary time dimension validation. Additionally, this includes both categorical and time
    dimension types, which should cover most, if not all, of the table source branches in the target class.
    """
    id_verifications_data_set = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].data_set_mapping[
        "id_verifications"
    ]

    assert_spec_set_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        set_id="result0",
        spec_set=id_verifications_data_set.instance_set.spec_set,
    )

    assert id_verifications_data_set.semantic_model_reference == SemanticModelReference(
        semantic_model_name="id_verifications"
    )
    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        plan_id="plan0",
        sql_plan_node=id_verifications_data_set.checked_sql_select_node,
        sql_client=sql_client,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_convert_query_semantic_model(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> None:
    bookings_data_set = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].data_set_mapping[
        "revenue"
    ]

    assert_rendered_sql_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        plan_id="plan0",
        sql_plan_node=bookings_data_set.checked_sql_select_node,
        sql_client=sql_client,
    )


_DSI_HAS_COMPILED_SQL = hasattr(PydanticNodeRelation, "compiled_sql")


@pytest.mark.skipif(not _DSI_HAS_COMPILED_SQL, reason="installed dbt-semantic-interfaces lacks compiled_sql")
def test_from_source_uses_sql_table_node_for_table_models() -> None:
    """When compiled_sql is not set, the from_source should be a SqlTableNode."""
    node_relation = PydanticNodeRelation(schema_name="my_schema", alias="my_table")

    assert getattr(node_relation, "compiled_sql", None) is None
    from_source = SqlTableNode.create(sql_table=SqlTable.from_string(node_relation.relation_name))
    assert isinstance(from_source, SqlTableNode)
    assert from_source.sql_table == SqlTable.from_string("my_schema.my_table")


@pytest.mark.skipif(not _DSI_HAS_COMPILED_SQL, reason="installed dbt-semantic-interfaces lacks compiled_sql")
def test_from_source_uses_sql_select_text_node_for_ephemeral_models() -> None:
    """When compiled_sql is set (ephemeral model), the from_source should be a SqlSelectTextNode."""
    compiled_sql = "SELECT id, name FROM raw.source_table WHERE active = true"
    node_relation = PydanticNodeRelation(
        schema_name="my_schema",
        alias="my_table",
        compiled_sql=compiled_sql,
    )

    assert getattr(node_relation, "compiled_sql", None) is not None
    from_source = SqlSelectTextNode.create(select_query=node_relation.compiled_sql)
    assert isinstance(from_source, SqlSelectTextNode)
    assert from_source.select_query == compiled_sql


@pytest.mark.skipif(not _DSI_HAS_COMPILED_SQL, reason="installed dbt-semantic-interfaces lacks compiled_sql")
def test_from_source_branching_logic() -> None:
    """Test the branching logic used in the converter to select the right from_source node type."""
    # Table-based model (no compiled_sql)
    table_relation = PydanticNodeRelation(schema_name="my_schema", alias="my_table")
    table_compiled_sql = getattr(table_relation, "compiled_sql", None)
    table_from_source: SqlPlanNode
    if table_compiled_sql is not None:
        table_from_source = SqlSelectTextNode.create(select_query=table_compiled_sql)
    else:
        table_from_source = SqlTableNode.create(sql_table=SqlTable.from_string(table_relation.relation_name))
    assert isinstance(table_from_source, SqlTableNode)

    # Ephemeral model (with compiled_sql)
    compiled_sql = "SELECT 1 AS id"
    ephemeral_relation = PydanticNodeRelation(
        schema_name="my_schema",
        alias="my_table",
        compiled_sql=compiled_sql,
    )
    ephemeral_compiled_sql = getattr(ephemeral_relation, "compiled_sql", None)
    ephemeral_from_source: SqlPlanNode
    if ephemeral_compiled_sql is not None:
        ephemeral_from_source = SqlSelectTextNode.create(select_query=ephemeral_compiled_sql)
    else:
        ephemeral_from_source = SqlTableNode.create(sql_table=SqlTable.from_string(ephemeral_relation.relation_name))
    assert isinstance(ephemeral_from_source, SqlSelectTextNode)
    assert ephemeral_from_source.select_query == compiled_sql
