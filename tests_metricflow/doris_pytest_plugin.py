"""Doris-specific pytest plugin that handles unsupported features gracefully.

This plugin is loaded only when running Doris tests (via -p flag in Makefile).

It handles two categories of test incompatibilities:

1. UnsupportedEngineFeatureError: Raised when MetricFlow tries to render SQL for
   features Doris doesn't support (e.g., MILLISECOND granularity). These are
   converted to pytest skips.

2. Known incompatible check_queries: Some YAML integration test check_queries
   hardcode SQL syntax that Doris doesn't support (e.g., CAST(... AS TIMESTAMP)
   instead of using the {{ cast_expr_to_ts() }} template). These specific tests
   are skipped by name.
"""

from __future__ import annotations

import pytest

from metricflow_semantics.errors.error_classes import UnsupportedEngineFeatureError

# Integration tests whose YAML check_queries contain hardcoded SQL syntax
# incompatible with Doris (e.g., CAST(... AS TIMESTAMP) instead of DATETIME).
# These cannot be fixed without modifying the shared YAML files.
_DORIS_INCOMPATIBLE_TESTS = {
    "test_case[name=itest_granularity.yaml/test_conversion_metric_with_custom_granularity_filter]",
    "test_case[name=itest_granularity.yaml/test_conversion_metric_with_custom_granularity_filter_not_in_group_by]",
}


def pytest_collection_modifyitems(items):  # noqa: D103
    for item in items:
        if item.name in _DORIS_INCOMPATIBLE_TESTS:
            item.add_marker(pytest.mark.skip(reason="YAML check_query uses CAST(... AS TIMESTAMP) unsupported by Doris"))


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):  # noqa: D103
    outcome = yield
    report = outcome.get_result()
    if call.when == "call" and report.failed:
        if call.excinfo is not None and call.excinfo.errisinstance(UnsupportedEngineFeatureError):
            report.outcome = "skipped"
            report.wasxfail = f"Unsupported engine feature for Doris: {call.excinfo.value}"
