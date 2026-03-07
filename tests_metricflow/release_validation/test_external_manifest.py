from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.toolkit.time_helpers import PrettyDuration

from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods
from tests_metricflow.release_validation.diff_files import diff_matching_files
from tests_metricflow.release_validation.explain_results_snapshot import assert_explain_tester_results_equal
from tests_metricflow.release_validation.explain_runner import ExplainQueryStatus, ExplainTaskResult
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
        manifest_setup_source=ExternalManifestSetupSource(manifest_directory, normalize_sql=True),
        result_file_directory=result_file_directory,
        request_generator=SavedQueryRequestGenerator(),
        explain_in_sql_engine=True,
    )

    results = explain_tester.run()
    assert all(result.status is ExplainQueryStatus.PASS for result in results)
    assert_explain_tester_results_equal(request=request, mf_test_configuration=mf_test_configuration, results=results)


@pytest.mark.skip
def test_manifests_in_local_directory(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    ddl_sql_client: SqlClientWithDDLMethods,
    sql_client: SqlClient,
    simple_semantic_manifest: PydanticSemanticManifest,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
) -> None:
    """Test generated SQL for all saved queries in a JSON-serialized manifest."""
    working_directory = Path().joinpath("git_ignored")
    manifest_directory = working_directory.joinpath("external_manifests/us_foods")
    results_directory = working_directory.joinpath("tester_results_after")
    explain_tester = DuckDbExplainTester(
        manifest_setup_source=ExternalManifestSetupSource(manifest_directory, normalize_sql=False),
        result_file_directory=results_directory,
        request_generator=SavedQueryRequestGenerator(
            # saved_query_names=["sq_steering_dashboard_export_weekly_market_1"]
            saved_query_names=["sq_act_trigger_dashboard_export_ytd_all_company_metrics_wo_sales"]
        ),
        explain_in_sql_engine=False,
    )

    results = explain_tester.run()
    total_count = len(results)
    successful_count = 0
    unsuccessful_results: list[ExplainTaskResult] = []
    for result in results:
        if result.status is ExplainQueryStatus.PASS:
            successful_count += 1
        else:
            unsuccessful_results.append(result)

    unsuccessful_count = len(unsuccessful_results)
    total_duration = PrettyDuration.sum(result.execution_duration for result in results)
    slowest_result = max(results, key=lambda _result: _result.execution_duration.seconds)

    result_dict = {
        "total_count": total_count,
        "total_duration": str(total_duration),
        "slowest_result": mf_pformat(slowest_result),
        "successful_count": successful_count,
        "unsuccessful_count": unsuccessful_count,
        "unsuccessful_results": mf_pformat(unsuccessful_results),
    }
    logger.info(LazyFormat("Finished test", **result_dict))
    result_summary_file_path = results_directory.joinpath("results.json")
    with open(result_summary_file_path, "w") as fp:
        json.dump(result_dict, fp, indent=4)


@pytest.mark.skip
def test_diff_explain_sql() -> None:  # noqa: D103
    working_directory = Path().joinpath("git_ignored")
    left_result_path = working_directory.joinpath("tester_results_before")
    right_result_path = working_directory.joinpath("tester_results_after")

    sql_diffs = diff_matching_files(left_result_path, right_result_path, "**/*_explain_sql.txt")
    logger.info(LazyFormat("Computed explain SQL diffs", sql_diff_count=len(sql_diffs)))
