from __future__ import annotations

import copy
import logging
from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.implementations.saved_query import PydanticSavedQuery, PydanticSavedQueryQueryParams
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from varname import nameof

from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods
from tests_metricflow.release_validation.explain_results_snapshot import assert_explain_tester_results_equal
from tests_metricflow.release_validation.explain_runner import ExplainQueryStatus
from tests_metricflow.release_validation.explain_tester import DuckDbExplainTester
from tests_metricflow.release_validation.manifest_setup.manifest_setup import (
    InternalManifestSetupSource,
)
from tests_metricflow.release_validation.request_generation.saved_query import SavedQueryRequestGenerator
from tests_metricflow.table_snapshot.table_snapshots import (
    SqlTableSnapshotRepository,
)

logger = logging.getLogger(__name__)


@pytest.mark.slow
@pytest.mark.duckdb_only
def test_explain_all_saved_queries(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    ddl_sql_client: SqlClientWithDDLMethods,
    sql_client: SqlClient,
    simple_semantic_manifest: PydanticSemanticManifest,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
    tmp_path: Path,
) -> None:
    """Test SQL generated for all saved queries in the manifest."""
    explain_tester = DuckDbExplainTester(
        manifest_setup_source=InternalManifestSetupSource(
            manifest_name=nameof(simple_semantic_manifest),
            semantic_manifest=simple_semantic_manifest,
            schema_name="default_schema",
            table_snapshots=source_table_snapshot_repository.table_snapshots,
        ),
        result_file_directory=tmp_path,
        request_generator=SavedQueryRequestGenerator(),
    )
    results = explain_tester.run()
    assert all(result.status is ExplainQueryStatus.PASS for result in results)
    assert_explain_tester_results_equal(request=request, mf_test_configuration=mf_test_configuration, results=results)


@pytest.mark.slow
@pytest.mark.duckdb_only
def test_invalid_filter_sql(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    ddl_sql_client: SqlClientWithDDLMethods,
    sql_client: SqlClient,
    simple_semantic_manifest: PydanticSemanticManifest,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
    tmp_path: Path,
) -> None:
    """Test that invalid SQL in a filter is detected."""
    invalid_saved_query_manifest = copy.deepcopy(simple_semantic_manifest)

    invalid_saved_query = PydanticSavedQuery(
        name="invalid_saved_query_sql",
        query_params=PydanticSavedQueryQueryParams(
            metrics=["bookings"],
            group_by=["metric_time"],
            where=PydanticWhereFilterIntersection(
                where_filters=[PydanticWhereFilter(where_sql_template="invalid_column > 1")]
            ),
        ),
    )
    invalid_saved_query_manifest.saved_queries = [invalid_saved_query]

    explain_tester = DuckDbExplainTester(
        manifest_setup_source=InternalManifestSetupSource(
            manifest_name=nameof(invalid_saved_query_manifest),
            semantic_manifest=invalid_saved_query_manifest,
            schema_name="default_schema",
            table_snapshots=source_table_snapshot_repository.table_snapshots,
        ),
        result_file_directory=tmp_path,
        request_generator=SavedQueryRequestGenerator(),
    )
    results = explain_tester.run()
    assert len(results) == 1
    assert results[0].status is ExplainQueryStatus.SQL_EXCEPTION
    assert_explain_tester_results_equal(request=request, mf_test_configuration=mf_test_configuration, results=results)


@pytest.mark.slow
@pytest.mark.duckdb_only
def test_invalid_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    ddl_sql_client: SqlClientWithDDLMethods,
    sql_client: SqlClient,
    simple_semantic_manifest: PydanticSemanticManifest,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
    tmp_path: Path,
) -> None:
    """Test that an invalid metric is detected."""
    invalid_saved_query_manifest = copy.deepcopy(simple_semantic_manifest)

    invalid_saved_query = PydanticSavedQuery(
        name="invalid_metric_saved_query",
        query_params=PydanticSavedQueryQueryParams(
            metrics=["invalid_metric"],
            group_by=["metric_time"],
        ),
    )
    invalid_saved_query_manifest.saved_queries = [invalid_saved_query]

    explain_tester = DuckDbExplainTester(
        manifest_setup_source=InternalManifestSetupSource(
            manifest_name=nameof(invalid_saved_query_manifest),
            semantic_manifest=invalid_saved_query_manifest,
            schema_name="default_schema",
            table_snapshots=source_table_snapshot_repository.table_snapshots,
        ),
        result_file_directory=tmp_path,
        request_generator=SavedQueryRequestGenerator(),
    )
    results = explain_tester.run()
    assert len(results) == 1
    assert results[0].status is ExplainQueryStatus.MF_EXCEPTION
    assert_explain_tester_results_equal(request=request, mf_test_configuration=mf_test_configuration, results=results)
