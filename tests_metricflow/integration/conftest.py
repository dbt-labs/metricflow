from __future__ import annotations

from dataclasses import dataclass

import pytest
from dbt_semantic_interfaces.test_utils import as_datetime
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.time_helpers import ConfigurableTimeSource
from metricflow_semantics.time.time_spine_source import TimeSpineSource

from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.protocols.sql_client import SqlClient


@dataclass(frozen=True)
class IntegrationTestHelpers:
    """Holds common objects used for many integration tests."""

    mf_engine: MetricFlowEngine
    mf_system_schema: str
    source_schema: str
    sql_client: SqlClient


@pytest.fixture
def it_helpers(  # noqa: D103
    sql_client: SqlClient,
    create_source_tables: bool,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    time_spine_source: TimeSpineSource,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> IntegrationTestHelpers:
    return IntegrationTestHelpers(
        mf_engine=MetricFlowEngine(
            semantic_manifest_lookup=simple_semantic_manifest_lookup,
            sql_client=sql_client,
            column_association_resolver=DunderColumnAssociationResolver(
                semantic_manifest_lookup=simple_semantic_manifest_lookup
            ),
            time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
        ),
        mf_system_schema=mf_test_configuration.mf_system_schema,
        source_schema=mf_test_configuration.mf_source_schema,
        sql_client=sql_client,
    )
