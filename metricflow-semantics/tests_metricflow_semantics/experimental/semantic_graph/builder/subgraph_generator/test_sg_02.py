from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import MeasureReference
from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.key_query_resolver import KeyQueryResolver
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.sg_linkable_spec_resolver import (
    SemanticGraphLinkableSpecResolver,
)
from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.experimental.semantic_graph.builder.group_by_attribute_subgraph import (
    GroupByAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.semantic_graph.nodes.attribute_node import MeasureNode
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import (
    DsiEntityLabel,
    GroupByAttributeLabel,
    KeyAttributeLabel,
    LocalModelLabel,
    MeasureLabel,
)
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import (
    MetricflowGraphPathFinder,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder_cache import PathFinderCache
from metricflow_semantics.experimental.semantic_graph.path_finding.weight_function import EdgeCountWeightFunction
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_spec_index_builder import LinkableSpecIndexBuilder
from metricflow_semantics.model.semantics.linkable_spec_resolver import LegacyLinkableSpecResolver
from metricflow_semantics.model.semantics.manifest_object_lookup import SemanticManifestObjectLookup
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    assert_linkable_element_set_snapshot_equal,
    assert_object_snapshot_equal,
    assert_str_snapshot_equal,
)
from metricflow_semantics.test_helpers.svg_snapshot import write_svg_snapshot_for_review

from tests_metricflow_semantics.experimental.mf_graph.formatting.svg_formatter import SvgFormatter
from tests_metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator.conftest import (
    check_subgraph_generation,
)
from tests_metricflow_semantics.experimental.semantic_graph.linkable_element_set_helpers import (
    assert_linkable_element_sets_equal,
)
from tests_metricflow_semantics.experimental.semantic_graph.test_helpers import LinkableSpecResolverTester

logger = logging.getLogger(__name__)


def test_all(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_semantic_manifest: PydanticSemanticManifest,
    sg_02_single_join_manifest: PydanticSemanticManifest,
    sg_04_common_unique_entity_manifest: PydanticSemanticManifest,
) -> None:
    check_subgraph_generation(
        request=request,
        mf_test_configuration=mf_test_configuration,
        manifest_object_lookup=ManifestObjectLookup(sg_02_single_join_manifest),
        subgraph_generators=SemanticGraphBuilder._ALL_SUBGRAPH_GENERATORS,
    )


def test_labels(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_lookup: ManifestObjectLookup,
) -> None:
    path_finder_cache = PathFinderCache[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath]()
    path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath](
        path_finder_cache
    )
    builder = SemanticGraphBuilder(
        manifest_object_lookup=sg_02_single_join_lookup,
        path_finder=path_finder,
    )
    graph = builder.build()
    labels = (DsiEntityLabel(), MeasureLabel(measure_name=None), GroupByAttributeLabel())
    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj={label: sorted(graph.nodes_with_label(label)) for label in labels},
    )


def test_descendants(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_lookup: ManifestObjectLookup,
) -> None:
    path_finder_cache = PathFinderCache[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath]()
    path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath](
        path_finder_cache
    )
    builder = SemanticGraphBuilder(
        manifest_object_lookup=sg_02_single_join_lookup,
        path_finder=path_finder,
    )
    graph = builder.build()
    mutable_path = AttributeRecipeWriterPath.create()
    source_node = MeasureNode.get_instance(measure_name="sm_0_measure_0", model_id=SemanticModelId(model_name="sm_0"))
    # source_node = MeasureNode.get_instance("sm_0_measure_0")

    candidate_target_nodes = graph.nodes_with_label(GroupByAttributeLabel())

    logger.info("Start path finding")

    result = path_finder.find_descendant_nodes(
        graph=graph,
        mutable_path=mutable_path,
        source_node=source_node,
        candidate_target_nodes=candidate_target_nodes,
        max_path_weight=1,
        weight_function=EdgeCountWeightFunction(),
    )

    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=LazyFormat(
            "Computed descendants",
            descendants=sorted(result.descendant_nodes),
        ).evaluated_value,
    )


def test_group_by_attribute_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_lookup: ManifestObjectLookup,
) -> None:
    path_finder_cache = PathFinderCache[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath]()

    path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath](
        path_finder_cache
    )
    builder = SemanticGraphBuilder(
        manifest_object_lookup=sg_02_single_join_lookup,
        path_finder=path_finder,
    )
    graph = builder.build()
    # source_node = MetricNode(attribute_name="sm_0_measure_0_metric")
    measure_name = "sm_0_measure_0"
    matching_nodes = graph.nodes_with_label(MeasureLabel.get_instance(measure_name))
    if len(matching_nodes) != 1:
        raise RuntimeError(LazyFormat("Found more than one matching measure node"))
    measure_node = mf_first_item(matching_nodes)

    subgraph_generator = GroupByAttributeSubgraphGenerator(
        semantic_graph=graph,
        path_finder=MetricflowGraphPathFinder(path_finder_cache=path_finder_cache),
    )

    result = subgraph_generator.generate_subgraph(measure_node)
    subgraph = result.subgraph
    write_svg_snapshot_for_review(
        request=request, snapshot_configuration=mf_test_configuration, svg_file_contents=subgraph.format(SvgFormatter())
    )
    logger.debug(
        LazyFormat(
            "Got attribute computation",
            min_time_grain=result.min_time_grain,
            model_ids=result.model_ids,
        )
    )


def test_specs(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_manifest: PydanticSemanticManifest,
    sg_02_single_join_lookup: ManifestObjectLookup,
) -> None:
    element_filter = LinkableElementFilter()
    semantic_manifest = sg_02_single_join_manifest
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


def test_linkable_spec_resolvers(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_manifest: PydanticSemanticManifest,
    sg_04_common_unique_entity_manifest: PydanticSemanticManifest,
    simple_semantic_manifest: PydanticSemanticManifest,
) -> None:
    element_filter = LinkableElementFilter()

    semantic_manifest = sg_02_single_join_manifest
    # semantic_manifest = simple_semantic_manifest
    # semantic_manifest = sg_04_common_primary_entity_manifest

    manifest_lookup = SemanticManifestLookup(semantic_manifest)

    # measure_references = manifest_lookup.semantic_model_lookup.measure_references
    # measure_references = (MeasureReference(element_name="bookings"),)
    measure_references = (MeasureReference(element_name="sm_0_measure_0"),)

    legacy_linkable_spec_resolver = LinkableSpecResolverTester.create_legacy_resolver(semantic_manifest)
    sg_linkable_spec_resolver = LinkableSpecResolverTester.create_sg_resolver(semantic_manifest)

    for measure_reference in measure_references:
        logger.debug(
            LazyFormat(
                "Comparing results from the legacy implementation and semantic-graph implementation",
                measure_reference=measure_reference,
            )
        )
        logger.debug("Generating using legacy implementation")
        legacy_linkable_element_set = legacy_linkable_spec_resolver.get_linkable_element_set_for_measure(
            measure_reference, element_filter
        )
        logger.debug("Generating using semantic graph implementation")
        sg_linkable_element_set = sg_linkable_spec_resolver.get_linkable_element_set_for_measure(
            measure_reference, element_filter
        )

        assert_linkable_element_sets_equal(
            left_set=legacy_linkable_element_set,
            right_set=sg_linkable_element_set,
        )
        # assert_linkable_element_set_snapshot_equal(
        #     request=request,
        #     snapshot_configuration=mf_test_configuration,
        #     linkable_element_set=legacy_linkable_element_set,
        #     set_id="legacy_result",
        # )
        #
        # assert_linkable_element_set_snapshot_equal(
        #     request=request,
        #     snapshot_configuration=mf_test_configuration,
        #     linkable_element_set=sg_linkable_element_set,
        #     set_id="sg_result",
        # )
        #
        # legacy_rows = convert_linkable_element_set_to_rows(legacy_linkable_element_set)
        # sg_rows = convert_linkable_element_set_to_rows(sg_linkable_element_set)
        #
        # for row_index, (legacy_row, sg_row) in enumerate(itertools.zip_longest(legacy_rows, sg_rows)):
        #     assert legacy_row is not None, LazyFormat(
        #         "Missing row from `legacy_rows`",
        #         measure_reference=measure_reference,
        #         row_index=row_index,
        #         legacy_row=legacy_row,
        #         sg_row=sg_row,
        #     )
        #     assert sg_row is not None, LazyFormat(
        #         "Missing row from `sg_rows`",
        #         measure_reference=measure_reference,
        #         row_index=row_index,
        #         legacy_row=legacy_row,
        #         sg_row=sg_row,
        #     )
        #     assert legacy_row == sg_row, LazyFormat(
        #         "Row mismatch", measure_reference=measure_reference, legacy_row=legacy_row, sg_row=sg_row
        #     )


def _create_legacy_resolver(semantic_manifest: SemanticManifest) -> LegacyLinkableSpecResolver:
    semantic_manifest_lookup = SemanticManifestLookup(semantic_manifest)
    linkable_spec_index_builder = LinkableSpecIndexBuilder(
        semantic_manifest=semantic_manifest,
        semantic_model_lookup=semantic_manifest_lookup.semantic_model_lookup,
        manifest_object_lookup=SemanticManifestObjectLookup(semantic_manifest),
        max_entity_links=MAX_JOIN_HOPS,
    )
    linkable_spec_index = linkable_spec_index_builder.build_index()
    legacy_linkable_spec_resolver = LegacyLinkableSpecResolver(
        semantic_manifest=semantic_manifest,
        semantic_model_lookup=semantic_manifest_lookup.semantic_model_lookup,
        manifest_object_lookup=SemanticManifestObjectLookup(semantic_manifest),
        linkable_spec_index=linkable_spec_index,
    )

    return legacy_linkable_spec_resolver


def _create_sg_resolver(semantic_manifest: SemanticManifest) -> SemanticGraphLinkableSpecResolver:
    manifest_object_lookup = ManifestObjectLookup(semantic_manifest)
    path_finder_cache = PathFinderCache[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath]()
    path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath](
        path_finder_cache
    )
    builder = SemanticGraphBuilder(
        manifest_object_lookup=manifest_object_lookup,
        path_finder=path_finder,
    )
    semantic_graph = builder.build()

    return SemanticGraphLinkableSpecResolver(
        manifest_object_lookup=manifest_object_lookup, semantic_graph=semantic_graph, path_finder=path_finder
    )


def test_resolver_outputs(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_manifest: PydanticSemanticManifest,
    sg_03_multi_entity_join_manifest: PydanticSemanticManifest,
    sg_04_common_unique_entity_manifest: PydanticSemanticManifest,
    sg_05_derived_metric_manifest: PydanticSemanticManifest,
    sg_06_ambiguous_join_manifest: PydanticSemanticManifest,
    simple_semantic_manifest: PydanticSemanticManifest,
) -> None:
    # LinkableSpecResolverTester.compare_resolver_outputs_for_measures(simple_semantic_manifest, measure_name="bookings")
    LinkableSpecResolverTester.compare_resolver_outputs_for_measures(sg_02_single_join_manifest)


def test_key_query_resolver(sg_02_single_join_manifest: PydanticSemanticManifest) -> None:
    semantic_manifest = sg_02_single_join_manifest
    manifest_object_lookup = ManifestObjectLookup(semantic_manifest)
    path_finder_cache = PathFinderCache[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath]()
    path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath](
        path_finder_cache
    )
    builder = SemanticGraphBuilder(
        manifest_object_lookup=manifest_object_lookup,
        path_finder=path_finder,
    )
    semantic_graph = builder.build()

    key_query_resolver = KeyQueryResolver()

    result = key_query_resolver.find_paths(
        graph=semantic_graph,
        source_nodes=semantic_graph.nodes_with_label(LocalModelLabel.get_instance()),
        target_nodes=semantic_graph.nodes_with_label(KeyAttributeLabel.get_instance()),
    )

    logger.debug(LazyFormat("Got result", result=result))
