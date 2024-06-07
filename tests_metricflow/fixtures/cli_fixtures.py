from __future__ import annotations

import os
import pathlib
import tempfile
from typing import Generator, Mapping, Optional, Sequence

import click
import pytest
from click.testing import CliRunner, Result
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from typing_extensions import override

from dbt_metricflow.cli.cli_context import CLIContext
from dbt_metricflow.cli.dbt_connectors.dbt_config_accessor import dbtArtifacts, dbtProjectMetadata
from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.fixtures.setup_fixtures import dbt_project_dir


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


@pytest.fixture(scope="session")
def cli_context(  # noqa: D103
    sql_client: SqlClient,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    create_source_tables: bool,
) -> Generator[CLIContext, None, None]:
    engine_test_fixture = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST]
    context = FakeCLIContext()
    context._mf = engine_test_fixture.metricflow_engine
    context._sql_client = sql_client
    context._semantic_manifest = engine_test_fixture.semantic_manifest
    context._semantic_manifest_lookup = engine_test_fixture.semantic_manifest_lookup
    with tempfile.NamedTemporaryFile() as file:
        context._log_file_path = pathlib.Path(file.name)
        yield context


class MetricFlowCliRunner(CliRunner):
    """Custom CliRunner class to handle passing context."""

    def __init__(self, cli_context: CLIContext, project_path: str) -> None:  # noqa: D107
        self.cli_context = cli_context
        self.project_path = project_path
        super().__init__()

    def run(self, cli: click.BaseCommand, args: Optional[Sequence[str]] = None) -> Result:  # noqa: D102
        current_dir = os.getcwd()
        os.chdir(self.project_path)
        result = super().invoke(cli, args, obj=self.cli_context)
        os.chdir(current_dir)
        return result


@pytest.fixture(scope="session")
def cli_runner(cli_context: CLIContext) -> MetricFlowCliRunner:  # noqa: D103
    return MetricFlowCliRunner(cli_context=cli_context, project_path=dbt_project_dir())
