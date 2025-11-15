from __future__ import annotations

import logging
import multiprocessing
from collections.abc import Set
from concurrent.futures import Future, ProcessPoolExecutor
from pathlib import Path

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.sql_clients.ddl_sql_client import SqlClientWithDDLMethods
from tests_metricflow.release_validation.explain_runner import (
    ExplainRunner,
    ExplainRunnerInput,
    ManifestHandle,
    ManifestStore, ExplainStatus,
)
from tests_metricflow.release_validation.query_generator import ExhaustiveQueryGenerator, SavedQueryGenerator

logger = logging.getLogger(__name__)


def check_explain_all_queries_in_manifest(manifest_path: Path, executor: ProcessPoolExecutor) -> None:
    # Skip empty manifests.
    if manifest_path.stat().st_size == 0:
        return

    manifest_handle = ManifestHandle.create(manifest_path)
    semantic_manifest = ManifestStore.get_manifest(manifest_handle)

    # query_generator = ExhaustiveQueryGenerator(semantic_manifest)
    query_generator = SavedQueryGenerator(semantic_manifest)

    successful_explain_count = 0
    queries = query_generator.generate_queries()
    query_count = len(queries)
    logger.info(LazyFormat("Generated possible queries", query_count=query_count))

    output_directory = Path("git_ignored/explain_runner").joinpath(manifest_handle.manifest_name)
    output_directory.mkdir(parents=True, exist_ok=True)

    future_to_runner_input: dict[Future, ExplainRunnerInput] ={}
    for i, query in enumerate(queries):
        base_name = f"query_{i:04}"
        pass_file_path = output_directory.joinpath(f"{base_name}_success.txt")
        if pass_file_path.exists():
            logger.info(f"Skipping due to existing PASS file: {manifest_handle.manifest_name}.{base_name}")
            continue

        log_file_path = output_directory.joinpath(f"{base_name}.log")
        fail_file_path = output_directory.joinpath(f"{base_name}_exception.txt")

        logger.info(LazyFormat(lambda: f"[{i + 1}/{query_count}] Submitting query"))

        runner_input = ExplainRunnerInput(
                    manifest_handle=manifest_handle,
                    mf_request=query,
                    log_file_path=log_file_path,
                    pass_file_path=pass_file_path,
                    fail_file_path=fail_file_path,
                )
        future = executor.submit(
            ExplainRunner.explain_query,
            runner_input,
        )
        future_to_runner_input[future] = runner_input
        successful_explain_count += 1
        # break

    actual_query_count = len(future_to_runner_input)
    for i, (future, runner_input) in enumerate(future_to_runner_input.items()):
        logger.info(f"Waiting for query #{i}")
        status: ExplainStatus = future.result()
        if status is ExplainStatus.PASS:
            logger.info(LazyFormat(lambda: f"[{i + 1}/{actual_query_count}] Query PASSED"))
        elif status is ExplainStatus.FAIL:
            logger.error(LazyFormat(lambda: f"[{i + 1}/{actual_query_count}] Query FAILED", log_file_path=runner_input.log_file_path))
        elif status is ExplainStatus.EXCEPTION_IGNORED:
            logger.warning(LazyFormat(lambda: f"[{i + 1}/{actual_query_count}] Query EXCEPTION_IGNORED", log_file_path=runner_input.log_file_path))
        else:
            assert_values_exhausted(status)

    # logger.info(LazyFormat("Finished explaining all queries", saved_query_count=len(semantic_manifest.saved_queries)))


def test_explain_all_queries(
    mf_test_configuration: MetricFlowTestConfiguration,
    ddl_sql_client: SqlClientWithDDLMethods,
    sql_client: SqlClient,
    simple_semantic_manifest: PydanticSemanticManifest,
    create_source_tables: None,
) -> None:
    # manifest_path = Path("git_ignored/tmp/dbt_manifest/manifest_json/semantic_manifest_4_pretty.json")
    # semantic_manifest = mf_load_manifest_from_json_file(manifest_path)
    # semantic_manifest = PydanticSemanticManifestTransformer.transform(semantic_manifest)
    #
    # query_generator = ExhaustiveQueryGenerator(semantic_manifest)
    # query_generator.count_possible_group_by_items()

    manifest_path = Path("git_ignored/tmp/dbt_manifest/manifest_json/semantic_manifest_128229.json")
    with ProcessPoolExecutor(mp_context=multiprocessing.get_context("spawn"), max_workers=5) as executor:
        check_explain_all_queries_in_manifest(manifest_path, executor)


def test_explain_all_saved_queries() -> None:
    # Check all manifests
    manifest_directory = Path("git_ignored/tmp/dbt_manifest/manifest_json_slack")
    manifest_paths = []
    for manifest_path in manifest_directory.rglob("*.json"):
        manifest_paths.append(manifest_path)

    skipped_manifest_paths: Set[str] = set()

    with ProcessPoolExecutor(mp_context=multiprocessing.get_context("spawn"), max_workers=8) as executor:

        for manifest_path in manifest_paths:
            if manifest_path in skipped_manifest_paths:
                logger.warning(LazyFormat("Skipping manifest path", manifest_path=manifest_path))
                continue

            logger.info(LazyFormat("Checking manifest path", manifest_path=manifest_path))
            check_explain_all_queries_in_manifest(manifest_path, executor)


# def test_executor() -> None:
#     futures: list[Future] = []
#     with ProcessPoolExecutor(mp_context=multiprocessing.get_context("spawn"), max_workers=5) as executor:
#         for i in range(10):
#             futures.append(executor.submit(square, i))
#
#     for future in futures:
#         logger.info(LazyFormat("Got result", result=future.result()))
