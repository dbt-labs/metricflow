from __future__ import annotations

import logging
from typing import Iterable, Type

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.experimental.dsi.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.mf_graph.path_finding.path_finder import MetricflowGraphPathFinder
from metricflow_semantics.experimental.mf_graph.path_finding.path_finder_cache import PathFinderCache
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
    SubgraphGeneratorArgumentSet,
)
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import SemanticGraphEdge, SemanticGraphNode
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.svg_snapshot import write_svg_snapshot_for_review

from tests_metricflow_semantics.experimental.graph_helpers import assert_graph_snapshot_equal
from tests_metricflow_semantics.experimental.mf_graph.formatting.svg_formatter import SvgFormatter

logger = logging.getLogger(__name__)


def check_subgraph_generation(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    manifest_object_lookup: ManifestObjectLookup,
    subgraph_generators: Iterable[Type[SemanticSubgraphGenerator]],
) -> None:
    path_finder_cache = PathFinderCache[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath]()
    path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath](
        path_finder_cache=path_finder_cache,
    )
    graph_builder = SemanticGraphBuilder(
        SubgraphGeneratorArgumentSet(
            manifest_object_lookup=manifest_object_lookup,
            path_finder=path_finder,
        )
    )
    semantic_graph = graph_builder.build(
        subgraph_generators=subgraph_generators,
    )
    assert_graph_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, graph=semantic_graph)
    write_svg_snapshot_for_review(
        request=request,
        snapshot_configuration=mf_test_configuration,
        svg_file_contents=semantic_graph.format(SvgFormatter()),
    )
