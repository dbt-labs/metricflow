# from __future__ import annotations
#
# import logging
#
# import pytest
# from _pytest.fixtures import FixtureRequest
# from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
# from metricflow_semantics.experimental.semantic_graph.builder.time_subraph import TimeSubgraphGenerator
# from metricflow_semantics.experimental.semantic_graph.semantic_manifest import ManifestObjectLookup
# from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
#
# from tests_metricflow_semantics.experimental.graph_helpers import assert_graph_snapshot_equal
#
# logger = logging.getLogger(__name__)
#
#
# @pytest.fixture(scope="session")
# def builder() -> SemanticGraphBuilder:
#     return SemanticGraphBuilder()
#
#
# def test_add_time_rule(  # noqa: D103
#     request: FixtureRequest,
#     mf_test_configuration: MetricFlowTestConfiguration,
#     builder: SemanticGraphBuilder,
#     sg_00_minimal_manifest_lookup: ManifestObjectLookup,
# ) -> None:
#     rules = (TimeSubgraphGenerator,)
#     semantic_graph = builder.build(sg_00_minimal_manifest_lookup, rules)
#     assert_graph_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, graph=semantic_graph)
