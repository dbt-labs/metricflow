from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.references import MeasureReference
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeComputationPath,
)
from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.experimental.semantic_graph.builder.group_by_attribute_subgraph import (
    GroupByAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import MetricNode
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import (
    MetricflowGraphPathFinder,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder_cache import PathFinderCache
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    assert_linkable_element_set_snapshot_equal,
)
from metricflow_semantics.test_helpers.svg_snapshot import write_svg_snapshot_for_review

from tests_metricflow_semantics.experimental.mf_graph.formatting.svg_formatter import SvgFormatter
from tests_metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator.conftest import (
    check_subgraph_generation,
)
from tests_metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator.test_sg_02 import (
    _create_sg_resolver,
)

logger = logging.getLogger(__name__)


def test_semantic_graph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_04_common_primary_entity_manifest: PydanticSemanticManifest,
) -> None:
    check_subgraph_generation(
        request=request,
        mf_test_configuration=mf_test_configuration,
        manifest_object_lookup=ManifestObjectLookup(sg_04_common_primary_entity_manifest),
        subgraph_generators=SemanticGraphBuilder._ALL_SUBGRAPH_GENERATORS,
    )


def test_group_by_attribute_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_04_common_primary_entity_manifest: PydanticSemanticManifest,
) -> None:
    manifest_object_lookup = ManifestObjectLookup(sg_04_common_primary_entity_manifest)
    path_finder_cache = PathFinderCache[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath]()
    path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath](
        path_finder_cache
    )
    builder = SemanticGraphBuilder(
        manifest_object_lookup=manifest_object_lookup,
        path_finder=path_finder,
    )
    graph = builder.build()
    metric_node = MetricNode(attribute_name="sm_0_measure_0_metric")

    subgraph_generator = GroupByAttributeSubgraphGenerator(
        semantic_graph=graph,
        path_finder=MetricflowGraphPathFinder(path_finder_cache=path_finder_cache),
    )

    result = subgraph_generator.generate_subgraph(FrozenOrderedSet((metric_node,)))
    subgraph = result.subgraph
    write_svg_snapshot_for_review(
        request=request, snapshot_configuration=mf_test_configuration, svg_file_contents=subgraph.format(SvgFormatter())
    )


def test_specs(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_04_common_primary_entity_manifest: PydanticSemanticManifest,
) -> None:
    element_filter = LinkableElementFilter()
    semantic_manifest = sg_04_common_primary_entity_manifest
    sg_linkable_spec_resolver = _create_sg_resolver(semantic_manifest)

    measure_reference = MeasureReference(element_name="sm_0_measure_0")
    sg_linkable_element_set = sg_linkable_spec_resolver.get_linkable_element_set_for_measure(
        measure_reference, element_filter
    )
    assert_linkable_element_set_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        linkable_element_set=sg_linkable_element_set,
        set_id="sg_result",
    )
