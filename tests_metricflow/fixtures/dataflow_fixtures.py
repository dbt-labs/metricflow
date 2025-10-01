from __future__ import annotations

from typing import Mapping

import pytest
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.time.time_spine_source import TimeSpineSource

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.fixtures.sql_client_fixtures import sql_client  # noqa: F401, F403

"""
Using 'function' scope to make ID generation more deterministic for the dataflow plan builder fixtures..

Using 'session' scope can result in other 'session' scope fixtures causing ID consistency issues.
"""


@pytest.fixture(scope="session")
def column_association_resolver(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> ColumnAssociationResolver:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].column_association_resolver


@pytest.fixture
def dataflow_plan_builder(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> DataflowPlanBuilder:
    # Scope needs to be function as the DataflowPlanBuilder contains state.
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].dataflow_plan_builder


@pytest.fixture(scope="session")
def query_parser(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> MetricFlowQueryParser:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].query_parser


@pytest.fixture
def extended_date_dataflow_plan_builder(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> DataflowPlanBuilder:
    # Scope needs to be function as the DataflowPlanBuilder contains state.
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.EXTENDED_DATE_MANIFEST].dataflow_plan_builder


@pytest.fixture
def multihop_dataflow_plan_builder(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> DataflowPlanBuilder:
    # Scope needs to be function as the DataflowPlanBuilder contains state.
    return mf_engine_test_fixture_mapping[
        SemanticManifestSetup.PARTITIONED_MULTI_HOP_JOIN_MANIFEST
    ].dataflow_plan_builder


@pytest.fixture(scope="session")
def multihop_query_parser(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> MetricFlowQueryParser:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.PARTITIONED_MULTI_HOP_JOIN_MANIFEST].query_parser


@pytest.fixture(scope="session")
def scd_column_association_resolver(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> ColumnAssociationResolver:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].column_association_resolver


@pytest.fixture
def scd_dataflow_plan_builder(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> DataflowPlanBuilder:
    # Scope needs to be function as the DataflowPlanBuilder contains state.
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].dataflow_plan_builder


@pytest.fixture(scope="session")
def scd_query_parser(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> MetricFlowQueryParser:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].query_parser


@pytest.fixture(scope="session")
def time_spine_sources(  # noqa: D103
    sql_client: SqlClient, mf_test_configuration: MetricFlowTestConfiguration  # noqa: F811
) -> Mapping[TimeGranularity, TimeSpineSource]:
    legacy_time_spine_grain = TimeGranularity.DAY
    time_spine_base_table_name = "mf_time_spine"

    # Legacy time spine
    time_spine_sources = {
        legacy_time_spine_grain: TimeSpineSource(
            sql_table=SqlTable.from_string(f"{mf_test_configuration.mf_source_schema}.{time_spine_base_table_name}"),
            base_column="ts",
            base_granularity=legacy_time_spine_grain,
        )
    }
    # Current time spines
    for granularity in TimeGranularity:
        if (
            granularity in sql_client.sql_engine_type.unsupported_granularities
            or granularity.to_int() >= legacy_time_spine_grain.to_int()
        ):
            continue
        time_spine_sources[granularity] = TimeSpineSource(
            sql_table=SqlTable.from_string(
                f"{mf_test_configuration.mf_source_schema}.{time_spine_base_table_name}_{granularity.value}"
            ),
            base_column="ts",
            base_granularity=granularity,
        )

    return time_spine_sources
