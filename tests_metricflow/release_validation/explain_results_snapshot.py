from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.string_helpers import mf_indent

from tests_metricflow.release_validation.explain_runner import ExplainQueryStatus, ExplainTaskResult
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal

logger = logging.getLogger(__name__)


def assert_explain_tester_results_equal(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, results: Iterable[ExplainTaskResult]
) -> None:
    """Snapshot the results of `DuckDbExplainTester`.

    The snapshot is of the form:

    query_id:
      status: ...
      explain_sql: ...
      mf_exception: ...
      ...
    """
    sorted_results = sorted(results, key=lambda result: result.request_name)
    snapshot_lines = []
    for result in sorted_results:
        file_path_set = result.result_file_path_set
        snapshot_lines.append(f"{result.request_name}:")
        request_lines = [f"status: {result.status.name}"]
        result_status = result.status
        if result_status is ExplainQueryStatus.PASS:
            request_lines.append(_render_section("explain_sql", file_path_set.explain_sql_file_path))
        elif result_status is ExplainQueryStatus.MF_UNSUPPORTED:
            request_lines.append(_render_section("mf_unsupported", file_path_set.mf_unsupported_file_path))
        elif result_status is ExplainQueryStatus.SQL_EXCEPTION:
            request_lines.append(_render_section("explain_sql", file_path_set.explain_sql_file_path))
            request_lines.append(_render_section("sql_exception", file_path_set.sql_exception_file_path))
        elif result_status is ExplainQueryStatus.MF_EXCEPTION:
            request_lines.append(_render_section("mf_exception", file_path_set.mf_exception_file_path))
        elif result_status is ExplainQueryStatus.MF_EXCEPTION:
            pass
        else:
            assert_values_exhausted(result_status)

        snapshot_lines.append(mf_indent("\n".join(request_lines)) + "\n")

    assert_str_snapshot_equal(
        request=request, mf_test_configuration=mf_test_configuration, snapshot_str="\n".join(snapshot_lines)
    )


def _render_section(section_name: str, file_path: Path) -> str:
    with open(file_path) as f:
        return "\n".join(
            [
                f"{section_name}:",
                mf_indent(f.read()),
            ]
        )
