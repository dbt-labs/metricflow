from __future__ import annotations

from _pytest.fixtures import FixtureRequest

from metricflow.dag.id_generation import OPTIMIZED_DATAFLOW_PLAN_PREFIX, IdGeneratorRegistry
from metricflow.dataflow.dataflow_plan import (
    BaseOutput,
    DataflowPlan,
    FilterElementsNode,
    WriteToResultDataframeNode,
)
from metricflow.dataflow.dataflow_plan_to_text import dataflow_plan_as_text
from metricflow.dataflow.optimizer.source_scan.cm_branch_combiner import (
    ComputeMetricsBranchCombiner,
    ComputeMetricsBranchCombinerResult,
)
from metricflow.specs.specs import InstanceSpecSet, MeasureSpec
from metricflow.test.dataflow_plan_to_svg import display_graph_if_requested
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.snapshot_utils import assert_plan_snapshot_text_equal


def make_dataflow_plan(node: BaseOutput) -> DataflowPlan:  # noqa: D
    return DataflowPlan(
        plan_id=IdGeneratorRegistry.for_class(ComputeMetricsBranchCombiner).create_id(OPTIMIZED_DATAFLOW_PLAN_PREFIX),
        sink_output_nodes=[WriteToResultDataframeNode(node)],
    )


def test_read_sql_source_combination(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    consistent_id_object_repository: ConsistentIdObjectRepository,
) -> None:
    """Tests combining a single node."""
    source0 = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    source1 = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    combiner = ComputeMetricsBranchCombiner(source0)

    result: ComputeMetricsBranchCombinerResult = source1.accept(combiner)
    assert result.combined_branch

    dataflow_plan = make_dataflow_plan(result.combined_branch)
    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )


def test_filter_combination(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    consistent_id_object_repository: ConsistentIdObjectRepository,
) -> None:
    """Tests combining a single node."""
    source0 = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    filter0 = FilterElementsNode(
        parent_node=source0, include_specs=InstanceSpecSet(measure_specs=(MeasureSpec(element_name="bookings"),))
    )
    source1 = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    filter1 = FilterElementsNode(
        parent_node=source1,
        include_specs=InstanceSpecSet(
            measure_specs=(MeasureSpec(element_name="booking_value"),),
        ),
    )
    combiner = ComputeMetricsBranchCombiner(filter0)

    result: ComputeMetricsBranchCombinerResult = filter1.accept(combiner)
    assert result.combined_branch

    dataflow_plan = make_dataflow_plan(result.combined_branch)
    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )
