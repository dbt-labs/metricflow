from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.metric_time_dimension import MTD_SPEC_DAY
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat

from metricflow.metric_evaluation.me_plan_table_formatter import MetricEvaluationPlanTableFormatter
from metricflow.metric_evaluation.plan.me_edges import MetricQueryDependencyEdge
from metricflow.metric_evaluation.plan.me_nodes import SimpleMetricsQueryNode, TopLevelQueryNode
from metricflow.metric_evaluation.plan.me_plan import MutableMetricEvaluationPlan
from metricflow.metric_evaluation.plan.query_element import MetricQueryPropertySet
from metricflow.plan_conversion.node_processor import PredicatePushdownState
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal

logger = logging.getLogger(__name__)


def test_me_plan_validation(request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration) -> None:
    """Test creating and validating an example metric evaluation plan."""
    me_plan = MutableMetricEvaluationPlan.create()
    bookings_spec = MetricSpec.create("bookings")
    listings_spec = MetricSpec.create("listings")
    query_properties = MetricQueryPropertySet.create([MTD_SPEC_DAY], PredicatePushdownState.create())
    bookings_query_node = SimpleMetricsQueryNode.create(
        SemanticModelId.get_instance("bookings_source"), metric_specs=[bookings_spec], query_properties=query_properties
    )
    listings_query_node = SimpleMetricsQueryNode.create(
        SemanticModelId.get_instance("listings_source"), metric_specs=[listings_spec], query_properties=query_properties
    )

    top_level_query_node = TopLevelQueryNode.create(
        passthrough_metric_specs=[bookings_spec, listings_spec], query_properties=query_properties
    )

    me_plan.add_edges(
        [
            MetricQueryDependencyEdge.create(
                target_node=top_level_query_node,
                target_node_output_spec=bookings_spec,
                source_node=bookings_query_node,
                source_node_output_spec=bookings_spec,
            ),
            MetricQueryDependencyEdge.create(
                target_node=top_level_query_node,
                target_node_output_spec=listings_spec,
                source_node=listings_query_node,
                source_node_output_spec=listings_spec,
            ),
        ]
    )
    me_plan.validate()

    format_result = MetricEvaluationPlanTableFormatter().format_plan(me_plan)
    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_str="\n\n".join([format_result.overview_table, format_result.node_output_table, mf_pformat(me_plan)]),
    )
