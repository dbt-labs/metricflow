from __future__ import annotations

from collections.abc import Mapping

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.query_rendering.compare_rendered_query import render_and_check


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_metric_name_same_as_dimension_name(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Check a soon-to-be-deprecated use case where a manifest contains a metric with the same name as a dimension."""
    fixture = mf_engine_test_fixture_mapping[SemanticManifestSetup.NAME_EDGE_CASE_MANIFEST]
    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=fixture.dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=fixture.dataflow_plan_builder,
        query_spec=fixture.query_parser.parse_and_validate_query(
            metric_names=["homonymous_metric_and_dimension"],
            group_by_names=["booking__homonymous_metric_and_dimension"],
        ).query_spec,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_homonymous_metric_and_entity(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Check a soon-to-be-deprecated use case where a manifest contains a metric with the same name as an entity."""
    fixture = mf_engine_test_fixture_mapping[SemanticManifestSetup.NAME_EDGE_CASE_MANIFEST]
    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=fixture.dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=fixture.dataflow_plan_builder,
        query_spec=fixture.query_parser.parse_and_validate_query(
            metric_names=["homonymous_metric_and_entity"],
            group_by_names=["metric_time"],
            where_constraint_strs=["homonymous_metric_and_entity IS NOT NULL"],
        ).query_spec,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_filter_by_metric_name_with_2_measures_from_same_source_node(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    sql_client: SqlClient,
) -> None:
    """Check a soon-to-be-deprecated use case of filtering by a metric name with 2 metrics from the same source node."""
    fixture = mf_engine_test_fixture_mapping[SemanticManifestSetup.NAME_EDGE_CASE_MANIFEST]
    render_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=fixture.dataflow_to_sql_converter,
        sql_client=sql_client,
        dataflow_plan_builder=fixture.dataflow_plan_builder,
        query_spec=fixture.query_parser.parse_and_validate_query(
            metric_names=["homonymous_metric_and_entity", "homonymous_metric_and_dimension"],
            group_by_names=["metric_time"],
            where_constraint_strs=["homonymous_metric_and_entity IS NOT NULL"],
        ).query_spec,
    )
