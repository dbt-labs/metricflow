from __future__ import annotations

from typing import Mapping

import pytest

from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer, SqlPlanRenderer
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup


@pytest.fixture
def default_sql_plan_renderer() -> SqlPlanRenderer:  # noqa: D103
    return DefaultSqlPlanRenderer()


@pytest.fixture(scope="session")
def dataflow_to_sql_converter(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> DataflowToSqlPlanConverter:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].dataflow_to_sql_converter


@pytest.fixture(scope="session")
def extended_date_dataflow_to_sql_converter(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> DataflowToSqlPlanConverter:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.EXTENDED_DATE_MANIFEST].dataflow_to_sql_converter


@pytest.fixture(scope="session")
def multihop_dataflow_to_sql_converter(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> DataflowToSqlPlanConverter:
    return mf_engine_test_fixture_mapping[
        SemanticManifestSetup.PARTITIONED_MULTI_HOP_JOIN_MANIFEST
    ].dataflow_to_sql_converter


@pytest.fixture(scope="session")
def scd_dataflow_to_sql_converter(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> DataflowToSqlPlanConverter:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].dataflow_to_sql_converter
