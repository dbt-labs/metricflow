from __future__ import annotations

from typing import Mapping

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_plan_snapshot_text_equal

from metricflow.dataflow.dataflow_plan import (
    DataflowPlan,
    DataflowPlanNode,
)
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.dataflow.nodes.write_to_data_table import WriteToResultDataTableNode
from metricflow.dataflow.optimizer.source_scan.cm_branch_combiner import (
    ComputeMetricsBranchCombiner,
    ComputeMetricsBranchCombinerResult,
)
from tests_metricflow.dataflow_plan_to_svg import display_graph_if_requested
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup


def make_dataflow_plan(node: DataflowPlanNode) -> DataflowPlan:  # noqa: D103
    return DataflowPlan(
        sink_nodes=[WriteToResultDataTableNode.create(node)],
        plan_id=DagId.from_id_prefix(StaticIdPrefix.OPTIMIZED_DATAFLOW_PLAN_PREFIX),
    )


@pytest.mark.sql_engine_snapshot
def test_read_sql_source_combination(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> None:
    """Tests combining a single node."""
    source0 = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping["bookings_source"]
    source1 = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping["bookings_source"]
    combiner = ComputeMetricsBranchCombiner(source0)

    result: ComputeMetricsBranchCombinerResult = source1.accept(combiner)
    assert result.combined_branch

    dataflow_plan = make_dataflow_plan(result.combined_branch)
    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )


@pytest.mark.sql_engine_snapshot
def test_filter_combination(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> None:
    """Tests combining a single node."""
    source0 = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping["bookings_source"]
    filter0 = FilterElementsNode.create(
        parent_node=source0,
        include_specs=InstanceSpecSet(simple_metric_input_specs=(SimpleMetricInputSpec(element_name="bookings"),)),
    )
    source1 = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping["bookings_source"]
    filter1 = FilterElementsNode.create(
        parent_node=source1,
        include_specs=InstanceSpecSet(
            simple_metric_input_specs=(SimpleMetricInputSpec(element_name="booking_value"),),
        ),
    )
    combiner = ComputeMetricsBranchCombiner(filter0)

    result: ComputeMetricsBranchCombinerResult = filter1.accept(combiner)
    assert result.combined_branch

    dataflow_plan = make_dataflow_plan(result.combined_branch)
    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )
