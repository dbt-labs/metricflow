from __future__ import annotations

import logging
from typing import Mapping

from _pytest.fixtures import FixtureRequest
from _pytest.logging import LogCaptureFixture
from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_plan_snapshot_text_equal

from metricflow.dataflow.builder.aggregation_helper import NullFillValueMapping
from metricflow.dataflow.dataflow_plan import (
    DataflowPlan,
    DataflowPlanNode,
)
from metricflow.dataflow.nodes.aggregate_simple_metric_inputs import AggregateSimpleMetricInputsNode
from metricflow.dataflow.nodes.filter_elements import SelectorNode
from metricflow.dataflow.nodes.write_to_data_table import WriteToResultDataTableNode
from metricflow.dataflow.optimizer.source_scan.cm_branch_combiner import (
    ComputeMetricsBranchCombiner,
    ComputeMetricsBranchCombinerResult,
)
from tests_metricflow.dataflow_plan_to_svg import display_graph_if_requested
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup

logger = logging.getLogger(__name__)


def make_dataflow_plan(node: DataflowPlanNode) -> DataflowPlan:  # noqa: D103
    return DataflowPlan(
        sink_nodes=[WriteToResultDataTableNode.create(node)],
        plan_id=DagId.from_id_prefix(StaticIdPrefix.OPTIMIZED_DATAFLOW_PLAN_PREFIX),
    )


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


def test_filter_combination(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> None:
    """Tests combining a single node."""
    source0 = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping["bookings_source"]
    selector0 = SelectorNode.create(
        parent_node=source0,
        include_specs=InstanceSpecSet(simple_metric_input_specs=(SimpleMetricInputSpec(element_name="bookings"),)),
    )
    source1 = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping["bookings_source"]
    selector1 = SelectorNode.create(
        parent_node=source1,
        include_specs=InstanceSpecSet(
            simple_metric_input_specs=(SimpleMetricInputSpec(element_name="booking_value"),),
        ),
    )
    combiner = ComputeMetricsBranchCombiner(selector0)

    result: ComputeMetricsBranchCombinerResult = selector1.accept(combiner)
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


def test_same_null_fill_value_mapping(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> None:
    """Test combination of branches with the same null-fill-value mapping."""
    source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "bookings_source"
    ]

    left_branch = AggregateSimpleMetricInputsNode.create(
        parent_node=source_node, null_fill_value_mapping=NullFillValueMapping.create({"bookings": None})
    )
    right_branch = AggregateSimpleMetricInputsNode.create(
        parent_node=source_node, null_fill_value_mapping=NullFillValueMapping.create({"bookings": None})
    )
    combiner = ComputeMetricsBranchCombiner(left_branch)
    result = right_branch.accept(combiner)
    assert result.combined_branch is not None
    assert result.combined_branch.functionally_identical(left_branch)


def test_different_fill_value_mapping(
    caplog: LogCaptureFixture,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> None:
    """Test combining branches with the different null-fill-value mappings.

    This should not happen in practice - see case handling in `ComputeMetricsBranchCombiner`.
    """
    source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "bookings_source"
    ]
    left_branch = AggregateSimpleMetricInputsNode.create(
        parent_node=source_node, null_fill_value_mapping=NullFillValueMapping.create({"bookings": None})
    )
    right_branch = AggregateSimpleMetricInputsNode.create(
        parent_node=source_node, null_fill_value_mapping=NullFillValueMapping.create({"bookings": 0})
    )
    combiner = ComputeMetricsBranchCombiner(left_branch)
    result = right_branch.accept(combiner)
    assert result.combined_branch is None


def test_compatible_fill_value_mapping(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> None:
    """Test combining branches with the compatible null-fill-value mappings.

    Branches that specify a null-fill value for different simple-metric inputs can be combined as they use different
    columns.
    """
    source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "bookings_source"
    ]

    left_branch = AggregateSimpleMetricInputsNode.create(
        parent_node=source_node, null_fill_value_mapping=NullFillValueMapping.create({"bookings": None})
    )
    right_branch = AggregateSimpleMetricInputsNode.create(
        parent_node=source_node, null_fill_value_mapping=NullFillValueMapping.create({"booking_value": 0})
    )
    combiner = ComputeMetricsBranchCombiner(left_branch)
    result = right_branch.accept(combiner)
    assert result.combined_branch is not None
    assert result.combined_branch.functionally_identical(
        AggregateSimpleMetricInputsNode.create(
            parent_node=source_node,
            null_fill_value_mapping=NullFillValueMapping.create(
                {
                    "bookings": None,
                    "booking_value": 0,
                }
            ),
        )
    )
