from __future__ import annotations

from typing import Mapping

import pytest

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.test.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.fixtures.sql_client_fixtures import sql_client  # noqa: F401, F403

"""
Using 'function' scope to make ID generation more deterministic for the dataflow plan builder fixtures..

Using 'session' scope can result in other 'session' scope fixtures causing ID consistency issues.
"""


@pytest.fixture(scope="session")
def column_association_resolver(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> ColumnAssociationResolver:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].column_association_resolver


@pytest.fixture
def dataflow_plan_builder(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> DataflowPlanBuilder:
    # Scope needs to be function as the DataflowPlanBuilder contains state.
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].dataflow_plan_builder


@pytest.fixture(scope="session")
def query_parser(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> MetricFlowQueryParser:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].query_parser


@pytest.fixture
def extended_date_dataflow_plan_builder(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> DataflowPlanBuilder:
    # Scope needs to be function as the DataflowPlanBuilder contains state.
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.EXTENDED_DATE_MANIFEST].dataflow_plan_builder


@pytest.fixture
def multihop_dataflow_plan_builder(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> DataflowPlanBuilder:
    # Scope needs to be function as the DataflowPlanBuilder contains state.
    return mf_engine_test_fixture_mapping[
        SemanticManifestSetup.PARTITIONED_MULTI_HOP_JOIN_MANIFEST
    ].dataflow_plan_builder


@pytest.fixture(scope="session")
def scd_column_association_resolver(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> ColumnAssociationResolver:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].column_association_resolver


@pytest.fixture
def scd_dataflow_plan_builder(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> DataflowPlanBuilder:
    # Scope needs to be function as the DataflowPlanBuilder contains state.
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].dataflow_plan_builder


@pytest.fixture(scope="session")
def scd_query_parser(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> MetricFlowQueryParser:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].query_parser


@pytest.fixture(scope="session")
def time_spine_source(  # noqa: D
    sql_client: SqlClient, mf_test_session_state: MetricFlowTestSessionState  # noqa: F811
) -> TimeSpineSource:
    return TimeSpineSource(schema_name=mf_test_session_state.mf_source_schema, table_name="mf_time_spine")
