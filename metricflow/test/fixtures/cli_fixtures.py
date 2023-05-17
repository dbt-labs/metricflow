from __future__ import annotations

from typing import Sequence

import click
import pytest
from click.testing import CliRunner, Result

from metricflow.cli.cli_context import CLIContext
from metricflow.engine.metricflow_engine import MetricFlowEngine
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.async_sql_client import AsyncSqlClient
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from dbt_semantic_interfaces.test_utils import as_datetime
from metricflow.test.time.configurable_time_source import ConfigurableTimeSource


@pytest.fixture
def cli_context(  # noqa: D
    async_sql_client: AsyncSqlClient,
    simple_semantic_manifest: SemanticManifest,
    time_spine_source: TimeSpineSource,
    mf_test_session_state: MetricFlowTestSessionState,
    create_source_tables: bool,
) -> CLIContext:
    semantic_manifest_lookup = SemanticManifestLookup(simple_semantic_manifest)
    mf_engine = MetricFlowEngine(
        semantic_manifest_lookup=semantic_manifest_lookup,
        sql_client=async_sql_client,
        column_association_resolver=DefaultColumnAssociationResolver(semantic_manifest_lookup=semantic_manifest_lookup),
        time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
        time_spine_source=time_spine_source,
        system_schema=mf_test_session_state.mf_system_schema,
    )
    context = CLIContext()
    context._mf = mf_engine
    context._sql_client = async_sql_client
    context._semantic_manifest = simple_semantic_manifest
    context._semantic_manifest_lookup = semantic_manifest_lookup
    context._mf_system_schema = mf_test_session_state.mf_system_schema
    return context


class MetricFlowCliRunner(CliRunner):
    """Custom CliRunner class to handle passing context."""

    def __init__(self, cli_context: CLIContext) -> None:  # noqa: D
        self.cli_context = cli_context
        super().__init__()

    def run(self, cli: click.BaseCommand, args: Sequence[str] = None) -> Result:  # noqa: D
        return super().invoke(cli, args, obj=self.cli_context)


@pytest.fixture
def cli_runner(cli_context: CLIContext) -> MetricFlowCliRunner:  # noqa: D
    return MetricFlowCliRunner(cli_context=cli_context)
