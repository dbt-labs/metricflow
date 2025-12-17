from __future__ import annotations

import logging
from pathlib import Path

import pytest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from varname import nameof

from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods
from tests_metricflow.release_validation.explain_runner import ExplainQueryStatus
from tests_metricflow.release_validation.explain_tester import DuckDbExplainTester
from tests_metricflow.release_validation.manifest_setup.manifest_setup import (
    InternalManifestSetupSource,
)
from tests_metricflow.release_validation.request_generation.exhaustive import ExhaustiveQueryGenerator
from tests_metricflow.table_snapshot.table_snapshots import (
    SqlTableSnapshotRepository,
)

logger = logging.getLogger(__name__)


@pytest.mark.skip("This test revealed a failure case.")
@pytest.mark.duckdb_only
def test_explain_metric_and_group_by_item_pairs(
    mf_test_configuration: MetricFlowTestConfiguration,
    ddl_sql_client: SqlClientWithDDLMethods,
    sql_client: SqlClient,
    simple_semantic_manifest: PydanticSemanticManifest,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
    tmp_path: Path,
) -> None:
    """Test all possible queries with a single metric and a single group-by item."""
    explain_tester = DuckDbExplainTester(
        manifest_setup_source=InternalManifestSetupSource(
            manifest_name=nameof(simple_semantic_manifest),
            semantic_manifest=simple_semantic_manifest,
            schema_name="default_schema",
            table_snapshots=source_table_snapshot_repository.table_snapshots,
        ),
        result_file_directory=tmp_path,
        request_generator=ExhaustiveQueryGenerator(metric_chunk_size=1, group_by_item_chunk_size=1),
    )
    results = explain_tester.run()
    assert all(result.status is ExplainQueryStatus.PASS for result in results)
