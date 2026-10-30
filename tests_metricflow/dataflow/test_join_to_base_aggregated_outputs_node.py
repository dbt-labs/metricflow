from __future__ import annotations

from typing import Mapping

import pytest
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.toolkit.cache.result_cache import ResultCache

from metricflow.dataflow.builder.aggregation_helper import NullFillValueMapping
from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.nodes.aggregate_simple_metric_inputs import AggregateSimpleMetricInputsNode
from metricflow.dataflow.nodes.combine_aggregated_outputs import CombineAggregatedOutputsNode
from metricflow.dataflow.nodes.filter_elements import SelectorNode
from metricflow.dataflow.nodes.join_to_base_aggregated_outputs import JoinToBaseAggregatedOutputsNode
from metricflow.dataflow.optimizer.source_scan.cm_branch_combiner import ComputeMetricsBranchCombiner
from metricflow.dataflow.optimizer.source_scan.source_scan_optimizer import SourceScanOptimizer
from metricflow_semantic_interfaces.references import EntityReference
from tests_metricflow.dataflow.optimizer.source_scan.test_cm_branch_combiner import make_dataflow_plan
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup

_COUNTRY = DimensionSpec(element_name="country_latest", entity_links=(EntityReference("listing"),))
_IS_LUX = DimensionSpec(element_name="is_lux_latest", entity_links=(EntityReference("listing"),))


@pytest.fixture
def listings_source_node(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> DataflowPlanNode:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping["listings_latest"]


def _aggregated_node(
    source_node: DataflowPlanNode, metric_name: str, dimension_spec: DimensionSpec
) -> AggregateSimpleMetricInputsNode:
    return AggregateSimpleMetricInputsNode.create(
        parent_node=SelectorNode.create(
            parent_node=source_node,
            include_specs=InstanceSpecSet(
                simple_metric_input_specs=(SimpleMetricInputSpec(element_name=metric_name),),
                dimension_specs=(dimension_spec,),
            ),
        ),
        null_fill_value_mapping=NullFillValueMapping.create(),
    )


def test_with_new_parents_keeps_base_first(listings_source_node: DataflowPlanNode) -> None:  # noqa: D103
    node = JoinToBaseAggregatedOutputsNode.create(
        base_node=_aggregated_node(listings_source_node, "largest_listing", _COUNTRY),
        declared_input_nodes=[_aggregated_node(listings_source_node, "listings", _COUNTRY)],
    )
    new_base = _aggregated_node(listings_source_node, "smallest_listing", _IS_LUX)
    new_declared = _aggregated_node(listings_source_node, "listings", _IS_LUX)

    new_node = node.with_new_parents((new_base, new_declared))

    assert isinstance(new_node, JoinToBaseAggregatedOutputsNode)
    assert new_node.base_node == new_base
    assert tuple(new_node.declared_input_nodes) == (new_declared,)


def test_functionally_identical_distinguishes_base_from_declared_inputs(  # noqa: D103
    listings_source_node: DataflowPlanNode,
) -> None:
    base = _aggregated_node(listings_source_node, "largest_listing", _COUNTRY)
    declared_0 = _aggregated_node(listings_source_node, "listings", _COUNTRY)
    declared_1 = _aggregated_node(listings_source_node, "smallest_listing", _COUNTRY)

    one_declared = JoinToBaseAggregatedOutputsNode.create(base_node=base, declared_input_nodes=[declared_0])
    two_declared = JoinToBaseAggregatedOutputsNode.create(base_node=base, declared_input_nodes=[declared_0, declared_1])
    combined = CombineAggregatedOutputsNode.create([base, declared_0])

    assert one_declared.functionally_identical(
        JoinToBaseAggregatedOutputsNode.create(base_node=base, declared_input_nodes=[declared_1])
    )
    assert not one_declared.functionally_identical(two_declared)
    assert not two_declared.functionally_identical(one_declared)
    assert not one_declared.functionally_identical(combined)
    assert not combined.functionally_identical(one_declared)


def test_branches_with_the_node_are_not_combined(listings_source_node: DataflowPlanNode) -> None:  # noqa: D103
    def make_node() -> JoinToBaseAggregatedOutputsNode:
        return JoinToBaseAggregatedOutputsNode.create(
            base_node=_aggregated_node(listings_source_node, "largest_listing", _COUNTRY),
            declared_input_nodes=[_aggregated_node(listings_source_node, "listings", _COUNTRY)],
        )

    left = make_node()
    result = make_node().accept(ComputeMetricsBranchCombiner(left, branch_combiner_cache=ResultCache()))

    assert result.combined_branch is None


def test_source_scan_optimizer_keeps_the_node_and_its_parents(  # noqa: D103
    listings_source_node: DataflowPlanNode,
) -> None:
    node = JoinToBaseAggregatedOutputsNode.create(
        base_node=_aggregated_node(listings_source_node, "largest_listing", _COUNTRY),
        declared_input_nodes=[
            _aggregated_node(listings_source_node, "listings", _COUNTRY),
            _aggregated_node(listings_source_node, "smallest_listing", _COUNTRY),
        ],
    )

    optimized_plan = SourceScanOptimizer().optimize(make_dataflow_plan(node))

    optimized_node = optimized_plan.sink_node.parent_nodes[0]
    assert isinstance(optimized_node, JoinToBaseAggregatedOutputsNode)
    assert optimized_node.parent_nodes == node.parent_nodes


def test_with_new_parents_requires_a_declared_input(listings_source_node: DataflowPlanNode) -> None:  # noqa: D103
    base = _aggregated_node(listings_source_node, "largest_listing", _COUNTRY)
    declared = _aggregated_node(listings_source_node, "listings", _COUNTRY)
    node = JoinToBaseAggregatedOutputsNode.create(base_node=base, declared_input_nodes=[declared])

    with pytest.raises(AssertionError):
        node.with_new_parents((base,))
