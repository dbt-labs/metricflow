from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import MeasureReference
from metricflow_semantics.experimental.dsi.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.mf_graph.path_finding.path_finder import (
    MetricflowGraphPathFinder,
)
from metricflow_semantics.experimental.mf_graph.path_finding.path_finder_cache import PathFinderCache
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.key_query_resolver import (
    EntityKeyQueryResolver,
)
from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator import SubgraphGeneratorArgumentSet
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import SemanticGraphEdge, SemanticGraphNode
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
)

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
    sg_00_minimal_manifest: PydanticSemanticManifest,
    sg_02_single_join_manifest: PydanticSemanticManifest,
    sg_04_common_unique_entity_manifest: PydanticSemanticManifest,
) -> None:
    check_subgraph_generation(
        request=request,
        mf_test_configuration=mf_test_configuration,
        manifest_object_lookup=ManifestObjectLookup(sg_02_single_join_manifest),
        subgraph_generators=SemanticGraphBuilder._ALL_SUBGRAPH_GENERATORS,
    )


def test_specs(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest: PydanticSemanticManifest,
    sg_02_single_join_manifest: PydanticSemanticManifest,
    sg_02_single_join_lookup: ManifestObjectLookup,
) -> None:
    element_filter = LinkableElementFilter()
    semantic_manifest = sg_02_single_join_manifest
    sg_linkable_spec_resolver = LinkableSpecResolverTester.create_sg_resolver(semantic_manifest)

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


def test_legacy_resolver(simple_semantic_manifest: PydanticSemanticManifest) -> None:
    LinkableSpecResolverTester.log_legacy_resolver_output_for_metric(
        semantic_manifest=simple_semantic_manifest,
        metric_name="bookings_per_listing",
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
    cyclic_join_manifest: PydanticSemanticManifest,
    scd_semantic_manifest: PydanticSemanticManifest,
) -> None:
    # LinkableSpecResolverTester.compare_resolver_outputs_for_measures(sg_02_single_join_manifest)

    # logger.debug(LazyFormat("Manifest context", metrics=simple_semantic_manifest.metrics))
    # LinkableSpecResolverTester.compare_resolver_outputs_for_measures(simple_semantic_manifest, measure_name="bookings")
    tester = LinkableSpecResolverTester(simple_semantic_manifest)
    # tester.compare_resolver_outputs_for_all_metric_pairs()
    # tester.compare_resolver_outputs_for_all_metrics()
    tester.compare_resolver_outputs_for_a_measure(MeasureReference("bookings"))

    # tester.compare_resolver_outputs_for_single_metric(
    #     MetricReference("booking_fees_last_week_per_booker_this_week"),
    # )

    # metric_reference = MetricReference("bookings")
    # for element_property in LinkableElementProperty:
    #     element_filter = LinkableElementFilter(with_any_of=frozenset((element_property,)))
    #     tester.compare_resolver_outputs_for_single_metric(metric_reference, element_filter=element_filter)

    # for element_property in LinkableElementProperty:
    #     element_filter = LinkableElementFilter(without_any_of=frozenset((element_property,)))
    #     tester.compare_resolver_outputs_for_single_metric(metric_reference, element_filter=element_filter)

    # tester.compare_resolver_outputs((MetricReference("every_2_days_bookers_2_days_ago"),))
    # tester.compare_resolver_outputs((MetricReference("active_listings"),))
    # tester.compare_resolver_outputs((MetricReference("every_2_days_bookers_2_days_ago"), MetricReference("active_listings")))
    # tester.compare_resolver_outputs(
    #     (MetricReference("every_two_days_bookers"), MetricReference("active_listings")))

    # exclude_diff_prefixes = {
    #     f"+│ {item}"
    #     for item in (
    #         "user__listing__user__booking_value_per_view",
    #         "user__listing__user__bookings_per_listing",
    #         "user__listing__user__bookings_per_lux_listing_derived",
    #         "user__listing__user__views_times_booking_value",
    #         "user__listing__user__bookings_per_view",
    #         "listing__booking_value_per_view",
    #         "listing__bookings_per_listing",
    #         "listing__bookings_per_lux_listing_derived",
    #         "listing__bookings_per_view",
    #         "listing__views_times_booking_value",
    #     )
    # }

    # LinkableSpecResolverTester.compare_resolver_outputs_for_metrics(
    #     simple_semantic_manifest,
    #     # metric_name="every_2_days_bookers_2_days_ago",
    #     # metric_name="revenue_all_time",
    #     # metric_name="every_2_days_bookers_2_days_ago",
    #     # exclude_diff_prefixes=exclude_diff_prefixes,
    # )

    # LinkableSpecResolverTester.compare_resolver_outputs_for_measures(
    #     simple_semantic_manifest,
    #     # exclude_diff_prefixes=exclude_diff_prefixes,
    # )

    # LinkableSpecResolverTester.compare_resolver_outputs_for_measures(
    #     simple_semantic_manifest, measure_name="visits"
    # )

    # LinkableSpecResolverTester.compare_resolver_outputs_for_measures(sg_02_single_join_manifest)


def test_key_query_resolver(sg_02_single_join_manifest: PydanticSemanticManifest) -> None:
    semantic_manifest = sg_02_single_join_manifest
    manifest_object_lookup = ManifestObjectLookup(semantic_manifest)
    path_finder_cache = PathFinderCache[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath]()
    path_finder = MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeRecipeWriterPath](
        path_finder_cache
    )
    builder = SemanticGraphBuilder(
        SubgraphGeneratorArgumentSet(
            manifest_object_lookup=manifest_object_lookup,
            path_finder=path_finder,
        )
    )
    semantic_graph = builder.build()

    key_query_resolver = EntityKeyQueryResolver()

    result = key_query_resolver.find_key_query_groups(
        semantic_graph=semantic_graph,
        # source_nodes=FrozenOrderedSet(
        #     (mf_first_item(semantic_graph.nodes_with_label(LocalModelLabel.get_instance())),)
        # ),
        # target_nodes=FrozenOrderedSet(
        #     (mf_first_item(semantic_graph.nodes_with_label(KeyAttributeLabel.get_instance())),),
        # ),
    )

    logger.debug(LazyFormat("Got result", result=result))
