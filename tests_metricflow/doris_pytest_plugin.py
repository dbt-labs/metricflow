"""Doris-specific pytest plugin that handles unsupported features gracefully.

This plugin is loaded only when running Doris tests (via -p flag in Makefile).

It converts UnsupportedEngineFeatureError (raised when MetricFlow tries to render
SQL for features Doris doesn't support, e.g. MILLISECOND granularity) into pytest
xfail results instead of hard failures.
"""

from __future__ import annotations

import pytest

from metricflow_semantics.errors.error_classes import UnsupportedEngineFeatureError


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):  # noqa: D103
    outcome = yield
    report = outcome.get_result()
    if call.when == "call" and report.failed:
        if call.excinfo is not None and call.excinfo.errisinstance(UnsupportedEngineFeatureError):
            report.outcome = "skipped"
            report.wasxfail = f"Unsupported engine feature for Doris: {call.excinfo.value}"
