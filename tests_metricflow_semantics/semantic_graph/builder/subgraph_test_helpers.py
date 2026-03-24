from __future__ import annotations

import logging
from typing import Iterable, Optional, Type

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.semantic_graph.builder.partial_graph_builder import PartialSemanticGraphBuilder
from metricflow_semantics.semantic_graph.builder.subgraph_generator import (
    SemanticSubgraphGenerator,
)
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.semantic_graph.sg_interfaces import (
    MutableSemanticGraph,
)
from metricflow_semantics.test_helpers.snapshot_helpers import SnapshotConfiguration
from metricflow_semantics.test_helpers.svg_snapshot import write_svg_snapshot_for_review
from metricflow_semantics.toolkit.mf_graph.formatting.svg_formatter import SvgFormatter

from tests_metricflow_semantics.helpers.graph_helpers import assert_graph_snapshot_equal

logger = logging.getLogger(__name__)


def check_graph_build(
    request: FixtureRequest,
    mf_test_configuration: SnapshotConfiguration,
    semantic_manifest: SemanticManifest,
    subgraph_generators: Iterable[Type[SemanticSubgraphGenerator]],
    generate_svg: bool = False,
    expectation_description: Optional[str] = None,
) -> None:
    """Helper method to snapshot the result of subgraph generators and generate SVGs."""
    manifest_object_lookup = ManifestObjectLookup(semantic_manifest)
    semantic_graph = MutableSemanticGraph.create()
    builder = PartialSemanticGraphBuilder(manifest_object_lookup)
    builder.build(subgraph_generators)
    for generator in subgraph_generators:
        generator_instance = generator(manifest_object_lookup=manifest_object_lookup)
        semantic_graph.add_edges(generator_instance.generate_edges())

    assert_graph_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        graph=semantic_graph,
        expectation_description=expectation_description,
    )
    if generate_svg:
        write_svg_snapshot_for_review(
            request=request,
            snapshot_configuration=mf_test_configuration,
            svg_file_contents=semantic_graph.format(SvgFormatter()),
        )
