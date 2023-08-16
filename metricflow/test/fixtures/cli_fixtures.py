from __future__ import annotations

import pathlib
import tempfile
from typing import Generator, Optional, Sequence

import click
import pytest
from click.testing import CliRunner, Result
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.test_utils import as_datetime
from typing_extensions import override

from metricflow.cli.cli_context import CLIContext
from metricflow.cli.dbt_connectors.dbt_config_accessor import dbtArtifacts, dbtProjectMetadata
from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.protocols.sql_client import SqlClient
from metricflow.test.time.configurable_time_source import ConfigurableTimeSource


class FakeCLIContext(CLIContext):
    """Fake context for testing. Manually initialize or override params used by CLIContext as appropriate.

    Note - this construct should not exist. It bypasses a fundamental initialization process within the CLI which
    can currently only be tested manually. Eventually, we need to refactor the CLI itself to allow for more robust
    testing. This will, however, allow us to test the invocation of CLI commands assuming the required configuration
    components are all in place.
    """

    def __init__(self) -> None:
        """Initializer configures only the two required parameters and leaves the remainder for inline override."""
        self.verbose = False
        self._mf: Optional[MetricFlowEngine] = None
        self._sql_client: Optional[SqlClient] = None
        self._semantic_manifest: Optional[SemanticManifest] = None
        self._semantic_manifest_lookup: Optional[SemanticManifestLookup] = None
        self._log_file_path: Optional[pathlib.Path] = None

    @property
    @override
    def dbt_artifacts(self) -> dbtArtifacts:
        raise NotImplementedError("FakeCLIContext does not load full dbt artifacts!")

    @property
    @override
    def dbt_project_metadata(self) -> dbtProjectMetadata:
        raise NotImplementedError("FakeCLIContext does not load dbt project metadata!")

    @property
    @override
    def log_file_path(self) -> pathlib.Path:
        assert self._log_file_path
        return self._log_file_path

    @property
    @override
    def sql_client(self) -> SqlClient:
        assert self._sql_client, "Must set _sql_client before use!"
        return self._sql_client

    @property
    @override
    def mf(self) -> MetricFlowEngine:
        assert self._mf, "Must set _mf before use!"
        return self._mf

    @property
    @override
    def semantic_manifest(self) -> SemanticManifest:
        assert self._semantic_manifest, "Must set _semantic_manifest before use!"
        return self._semantic_manifest


@pytest.fixture
def cli_context(  # noqa: D
    sql_client: SqlClient,
    simple_semantic_manifest: SemanticManifest,
    create_source_tables: bool,
) -> Generator[CLIContext, None, None]:
    semantic_manifest_lookup = SemanticManifestLookup(simple_semantic_manifest)
    mf_engine = MetricFlowEngine(
        semantic_manifest_lookup=semantic_manifest_lookup,
        sql_client=sql_client,
        column_association_resolver=DunderColumnAssociationResolver(semantic_manifest_lookup=semantic_manifest_lookup),
        time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
    )
    context = FakeCLIContext()
    context._mf = mf_engine
    context._sql_client = sql_client
    context._semantic_manifest = simple_semantic_manifest
    context._semantic_manifest_lookup = semantic_manifest_lookup
    with tempfile.NamedTemporaryFile() as file:
        context._log_file_path = pathlib.Path(file.name)
        yield context


class MetricFlowCliRunner(CliRunner):
    """Custom CliRunner class to handle passing context."""

    def __init__(self, cli_context: CLIContext) -> None:  # noqa: D
        self.cli_context = cli_context
        super().__init__()

    def run(self, cli: click.BaseCommand, args: Optional[Sequence[str]] = None) -> Result:  # noqa: D
        # TODO: configure CLI to use a dbt_project fixture
        dummy_dbt_project = pathlib.Path("dbt_project.yml")
        dummy_dbt_project.touch()
        result = super().invoke(cli, args, obj=self.cli_context)
        dummy_dbt_project.unlink()
        return result


@pytest.fixture
def cli_runner(cli_context: CLIContext) -> MetricFlowCliRunner:  # noqa: D
    return MetricFlowCliRunner(cli_context=cli_context)
