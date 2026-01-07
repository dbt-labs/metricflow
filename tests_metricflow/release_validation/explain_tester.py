from __future__ import annotations

import concurrent
import logging
import multiprocessing
import string
from collections import Counter
from collections.abc import Mapping, Sequence
from concurrent.futures import Future, ProcessPoolExecutor
from pathlib import Path

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet
from metricflow_semantics.toolkit.id_helpers import mf_sha1_iterables
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.engine.metricflow_engine import MetricFlowRequestId
from tests_metricflow.release_validation.explain_runner import (
    DuckDbExplainTaskRunner,
    ExplainQueryStatus,
    ExplainTaskResult,
    MetricFlowExplainTask,
)
from tests_metricflow.release_validation.manifest_setup.manifest_setup import ManifestSetup, ManifestSetupSource
from tests_metricflow.release_validation.request_generation.request_generator import MetricFlowRequestGenerator

logger = logging.getLogger(__name__)

DEFAULT_MAX_WORKER_COUNT = 8


class DuckDbExplainTester:
    """Using DuckDB, test the MF engine by generating SQL queries and running an EXPLAIN on those queries.

    Since calling `MetricFlowEngine.explain()` can be CPU intensive, this uses multiprocessing to distribute the tasks.
    To aid browsing of results, each query creates multiple files that groups the parts of the result. The parts of the
    result include the log messages, generated SQL, and exception messages.

    The separate files also aid comparison of results that are generated using different versions of MF.
    """

    def __init__(  # noqa: D107
        self,
        manifest_setup_source: ManifestSetupSource,
        result_file_directory: Path,
        request_generator: MetricFlowRequestGenerator,
        max_worker_count: int = DEFAULT_MAX_WORKER_COUNT,
    ) -> None:
        self._manifest_setup_source = manifest_setup_source
        self._result_file_directory = result_file_directory
        self._request_generator = request_generator
        self._max_worker_count = max_worker_count

    def run(self) -> Sequence[ExplainTaskResult]:  # noqa: D102
        executor = ProcessPoolExecutor(
            mp_context=multiprocessing.get_context("spawn"), max_workers=self._max_worker_count
        )
        try:
            return self._generate_and_submit_all(executor)
        finally:
            logger.info("Waiting for the executor to shut down")
            executor.shutdown(wait=True, cancel_futures=True)
            logger.info("Executor has shut down")

    def _generate_and_submit_all(self, executor: ProcessPoolExecutor) -> Sequence[ExplainTaskResult]:
        future_to_task: dict[Future, MetricFlowExplainTask] = {}
        for manifest_setup in self._manifest_setup_source.get_manifest_setups():
            logger.info(LazyFormat("Submitting tasks", manifest_name=manifest_setup.manifest_name))
            future_to_task.update(self._generate_and_submit_requests_for_one_manifest(executor, manifest_setup))

        submitted_count = len(future_to_task)
        logger.info(LazyFormat(lambda: f"Waiting for {submitted_count} tasks to finish"))

        status_counter: Counter[ExplainQueryStatus] = Counter()
        results: list[ExplainTaskResult] = []
        for future in concurrent.futures.as_completed(future_to_task):
            result: ExplainTaskResult = future.result()
            results.append(result)
            task_status = result.status
            status_counter[task_status] += 1

            log_level: int
            if task_status is ExplainQueryStatus.PASS or task_status is task_status.MF_UNSUPPORTED:
                log_level = logging.INFO
            elif task_status is ExplainQueryStatus.MF_EXCEPTION or ExplainQueryStatus.SQL_EXCEPTION:
                log_level = logging.ERROR
            else:
                assert_values_exhausted(task_status)

            logger.log(
                level=log_level,
                msg=string.Template("[$finished_count / $submitted_count] Query $task_status").substitute(
                    finished_count=sum(status_counter.values()),
                    submitted_count=submitted_count,
                    task_status=task_status.name,
                ),
            )

        manifest_names = sorted(FrozenOrderedSet(result.manifest_name for result in results))
        task_count = len(results)
        mf_exception_paths = [
            result.result_file_path_set.mf_exception_file_path
            for result in results
            if result.status is ExplainQueryStatus.MF_EXCEPTION
        ]
        mf_unsupported_paths = [
            result.result_file_path_set.mf_exception_file_path
            for result in results
            if result.status is ExplainQueryStatus.MF_UNSUPPORTED
        ]
        sql_exception_paths = [
            result.result_file_path_set.sql_exception_file_path
            for result in results
            if result.status is ExplainQueryStatus.SQL_EXCEPTION
        ]

        logger.info(
            LazyFormat(
                "Finished",
                manifest_names=manifest_names,
                task_count=task_count,
                result_file_directory=self._result_file_directory,
                mf_unsupported_paths=mf_unsupported_paths,
                mf_exception_paths=mf_exception_paths,
                sql_exception_paths=sql_exception_paths,
            )
        )
        return results

    @staticmethod
    def serialize_manifest_to_json_file(
        manifest_name: str, semantic_manifest: PydanticSemanticManifest, manifest_directory: Path
    ) -> Path:
        """Serializes the given manifest to a JSON file.

        The file is written to `<manifest_directory>.<manifest_name>.json`.
        """
        manifest_directory.mkdir(exist_ok=True, parents=True)
        manifest_path = manifest_directory.joinpath(manifest_name + ".json")
        manifest_path.write_text(semantic_manifest.json(indent=2))
        return manifest_path

    def _generate_and_submit_requests_for_one_manifest(
        self, executor: ProcessPoolExecutor, manifest_setup: ManifestSetup
    ) -> Mapping[Future, MetricFlowExplainTask]:
        future_to_task: dict[Future, MetricFlowExplainTask] = {}
        empty_request_id = MetricFlowRequestId("")

        for request in self._request_generator.generate_requests(manifest_setup.semantic_manifest):
            request_name = "query_" + mf_sha1_iterables([str(request.with_request_id(empty_request_id))])[:8]
            task = MetricFlowExplainTask(
                manifest_setup=manifest_setup,
                request_name=request_name,
                mf_request=request,
                result_file_prefix=self._result_file_directory.joinpath(manifest_setup.manifest_name, request_name),
            )
            future = executor.submit(DuckDbExplainTaskRunner.run_task, task)
            future_to_task[future] = task

        return future_to_task
