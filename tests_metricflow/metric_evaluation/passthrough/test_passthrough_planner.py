from __future__ import annotations

import logging
from collections.abc import Mapping

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.metric_evaluation.passthrough.passthrough_me_planner import PassThroughMetricEvaluationPlanner
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.metric_evaluation.me_test_helpers import (
    METRIC_EVALUATION_TEST_CASES,
    MetricEvaluationTestCase,
    check_metric_evaluation_plan_test_case,
)

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "me_test_case",
    METRIC_EVALUATION_TEST_CASES,
    ids=[case.case_id for case in METRIC_EVALUATION_TEST_CASES],
)
@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_cases_with_passthrough_planner(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    me_test_case: MetricEvaluationTestCase,
) -> None:
    """Test enumerated cases using the `PassThroughMetricEvaluationPlanner`."""
    engine_test_fixture = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST]
    check_metric_evaluation_plan_test_case(
        request=request,
        mf_test_configuration=mf_test_configuration,
        engine_test_fixture=mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST],
        me_planner=PassThroughMetricEvaluationPlanner(
            manifest_object_lookup=engine_test_fixture.manifest_object_lookup,
            column_association_resolver=engine_test_fixture.column_association_resolver,
        ),
        me_test_case=me_test_case,
    )
