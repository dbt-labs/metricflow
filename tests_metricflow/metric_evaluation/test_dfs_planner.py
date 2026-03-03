from __future__ import annotations

import logging
from collections.abc import Mapping

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.metric_evaluation.dfs_me_planner import DepthFirstSearchMetricEvaluationPlanner
from metricflow.plan_conversion.node_processor import PredicatePushdownState
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.metric_evaluation.me_test_helpers import (
    METRIC_EVALUATION_TEST_CASES,
    MetricEvaluationTestCase,
    _create_filter_spec_factory,
    assert_me_plan_snapshot_equal,
)

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "me_test_case",
    METRIC_EVALUATION_TEST_CASES,
    ids=[case.case_id for case in METRIC_EVALUATION_TEST_CASES],
)
def test_cases_with_dfs_planner(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    me_test_case: MetricEvaluationTestCase,
) -> None:
    """Test enumerated cases using the `DepthFirstSearchMetricEvaluationPlanner`."""
    engine_test_fixture = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST]
    me_planner = DepthFirstSearchMetricEvaluationPlanner(
        manifest_object_lookup=engine_test_fixture.manifest_object_lookup,
        column_association_resolver=engine_test_fixture.column_association_resolver,
    )
    query_parser = engine_test_fixture.query_parser
    query_spec = query_parser.parse_and_validate_query(
        metric_names=me_test_case.request.metric_names,
        group_by_names=me_test_case.request.group_by_names,
    ).query_spec

    predicate_pushdown_state = PredicatePushdownState.create(
        time_range_constraint=query_spec.time_range_constraint,
        where_filter_specs=(),
    )
    me_plan = me_planner.build_plan(
        metric_specs=query_spec.metric_specs,
        group_by_item_specs=query_spec.linkable_specs.as_tuple,
        predicate_pushdown_state=predicate_pushdown_state,
        filter_spec_factory=_create_filter_spec_factory(
            query_spec=query_spec,
            manifest_object_lookup=engine_test_fixture.manifest_object_lookup,
            column_association_resolver=engine_test_fixture.column_association_resolver,
        ),
    )
    assert_me_plan_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        me_test_case=me_test_case,
        me_plan=me_plan,
    )
