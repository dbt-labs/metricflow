from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.mf_logging.pretty_print import mf_pformat_dict
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    assert_str_snapshot_equal,
)

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan_analyzer import DataflowPlanAnalyzer

logger = logging.getLogger(__name__)


def test_shared_metric_query(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_association_resolver: ColumnAssociationResolver,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
) -> None:
    """Check common branches in a query that uses derived metrics defined from metrics that are also in the query."""
    parse_result = query_parser.parse_and_validate_query(
        metric_names=("bookings", "bookings_per_booker"),
        group_by_names=("metric_time",),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(parse_result.query_spec)

    obj_dict = {
        "dataflow_plan": dataflow_plan.structure_text(),
    }

    common_branch_leaf_nodes = DataflowPlanAnalyzer.find_common_branches(dataflow_plan)
    for i, common_branch_leaf_node in enumerate(common_branch_leaf_nodes):
        obj_dict[f"common_branch_{i}"] = common_branch_leaf_node.structure_text()

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="result",
        snapshot_str=mf_pformat_dict(
            description="Common branches.",
            obj_dict=obj_dict,
            preserve_raw_strings=True,
        ),
    )
