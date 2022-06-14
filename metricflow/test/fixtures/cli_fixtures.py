from __future__ import annotations

import click
import pytest

from click.testing import CliRunner, Result
from typing import Sequence

from metricflow.cli.cli_context import CLIContext
from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.semantic_model import SemanticModel
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.test_utils import as_datetime
from metricflow.test.time.configurable_time_source import ConfigurableTimeSource


@pytest.fixture
def cli_context(  # noqa: D
    sql_client: SqlClient,
    simple_user_configured_model: UserConfiguredModel,
    time_spine_source: TimeSpineSource,
    mf_test_session_state: MetricFlowTestSessionState,
    create_simple_model_tables: bool,
) -> CLIContext:
    semantic_model = SemanticModel(simple_user_configured_model)
    mf_engine = MetricFlowEngine(
        semantic_model=semantic_model,
        sql_client=sql_client,
        column_association_resolver=DefaultColumnAssociationResolver(semantic_model=semantic_model),
        time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
        time_spine_source=time_spine_source,
        system_schema=mf_test_session_state.mf_system_schema,
    )
    context = CLIContext()
    context._mf = mf_engine
    context._sql_client = sql_client
    context._user_configured_model = simple_user_configured_model
    context._semantic_model = semantic_model
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
