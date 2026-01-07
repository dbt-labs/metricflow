from __future__ import annotations

import logging
from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods
from tests_metricflow.release_validation.explain_results_snapshot import assert_explain_tester_results_equal
from tests_metricflow.release_validation.explain_runner import ExplainQueryStatus
from tests_metricflow.release_validation.explain_tester import DuckDbExplainTester
from tests_metricflow.release_validation.manifest_setup.external_manifest import ExternalManifestSetupSource
from tests_metricflow.release_validation.request_generation.saved_query import SavedQueryRequestGenerator
from tests_metricflow.table_snapshot.table_snapshots import SqlTableSnapshotRepository

logger = logging.getLogger(__name__)


@pytest.mark.slow
@pytest.mark.duckdb_only
def test_explain_all_saved_queries_from_external_manifest(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    ddl_sql_client: SqlClientWithDDLMethods,
    sql_client: SqlClient,
    simple_semantic_manifest: PydanticSemanticManifest,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
    tmp_path: Path,
) -> None:
    """Test generated SQL for all saved queries in a JSON-serialized manifest."""
    manifest_name = "simple_semantic_manifest"
    semantic_manifest = simple_semantic_manifest
    manifest_directory = tmp_path.joinpath("manifest_json")
    manifest_path = DuckDbExplainTester.serialize_manifest_to_json_file(
        manifest_name, semantic_manifest, manifest_directory
    )
    logger.debug(LazyFormat("Wrote manifest to JSON file", manifest_path=manifest_path))
    result_file_directory = tmp_path.joinpath("results")
    explain_tester = DuckDbExplainTester(
        manifest_setup_source=ExternalManifestSetupSource(manifest_directory),
        result_file_directory=result_file_directory,
        request_generator=SavedQueryRequestGenerator(),
    )

    results = explain_tester.run()
    assert all(result.status is ExplainQueryStatus.PASS for result in results)
    assert_explain_tester_results_equal(request=request, mf_test_configuration=mf_test_configuration, results=results)
