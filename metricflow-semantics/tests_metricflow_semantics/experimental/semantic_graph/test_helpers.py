from __future__ import annotations

import difflib
import logging

from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.sg_linkable_spec_resolver import (
    SemanticGraphLinkableSpecResolver,
)
from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import MetricflowGraphPathFinder
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder_cache import PathFinderCache
from metricflow_semantics.helpers.table_helpers import IsolatedTabulateRunner
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseLinkableElementSet
from metricflow_semantics.model.semantics.linkable_spec_index_builder import LinkableSpecIndexBuilder
from metricflow_semantics.model.semantics.linkable_spec_resolver import LegacyLinkableSpecResolver
from metricflow_semantics.model.semantics.manifest_object_lookup import SemanticManifestObjectLookup
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS

from tests_metricflow_semantics.experimental.semantic_graph.linkable_element_set_helpers import (
    convert_linkable_element_set_to_rows,
)
from tests_metricflow_semantics.experimental.semantic_graph.table_helpers import RowColumnWidthEqualizer

logger = logging.getLogger(__name__)


class LinkableSpecResolverTester:
    @staticmethod
    def create_legacy_resolver(semantic_manifest: SemanticManifest) -> LegacyLinkableSpecResolver:
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

    @staticmethod
    def create_sg_resolver(semantic_manifest: SemanticManifest) -> SemanticGraphLinkableSpecResolver:
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

    @staticmethod
    def compare_resolver_outputs_for_measures(
        semantic_manifest: SemanticManifest,
    ) -> None:
        element_filter = LinkableElementFilter()

        # semantic_manifest = sg_02_single_join_manifest
        # semantic_manifest = simple_semantic_manifest
        # semantic_manifest = sg_04_common_primary_entity_manifest

        manifest_lookup = SemanticManifestLookup(semantic_manifest)

        measure_references = manifest_lookup.semantic_model_lookup.measure_references
        # measure_references = (MeasureReference(element_name="bookings"),)
        # measure_references = (MeasureReference(element_name="sm_0_measure_0"),)

        legacy_resolver = LinkableSpecResolverTester.create_legacy_resolver(semantic_manifest)
        sg_resolver = LinkableSpecResolverTester.create_sg_resolver(semantic_manifest)

        for measure_reference in measure_references:
            logger.debug(
                LazyFormat(
                    "Comparing results from the legacy implementation and semantic-graph implementation",
                    measure_reference=measure_reference,
                )
            )
            logger.debug("Generating using legacy implementation")
            legacy_linkable_element_set = legacy_resolver.get_linkable_element_set_for_measure(
                measure_reference, element_filter
            )
            logger.debug("Generating using semantic graph implementation")
            sg_linkable_element_set = sg_resolver.get_linkable_element_set_for_measure(
                measure_reference, element_filter
            )

            LinkableSpecResolverTester.assert_linkable_element_sets_equal(
                left_set=legacy_linkable_element_set,
                right_set=sg_linkable_element_set,
            )

    @staticmethod
    def assert_linkable_element_sets_equal(  # noqa: D103
        left_set: BaseLinkableElementSet,
        right_set: BaseLinkableElementSet,
    ) -> None:
        headers = ("Dunder Name", "Metric-Subquery Entity-Links", "Type", "Properties", "Derived-From Semantic Models")

        left_rows = convert_linkable_element_set_to_rows(left_set)
        right_rows = convert_linkable_element_set_to_rows(right_set)

        equalizer = RowColumnWidthEqualizer()

        equalizer.add_headers(headers)
        equalizer.add_rows(left_rows)
        equalizer.add_rows(right_rows)

        new_left_rows = equalizer.reformat_rows(left_rows)
        new_right_rows = equalizer.reformat_rows(right_rows)

        left_table = IsolatedTabulateRunner.tabulate(
            tabular_data=new_left_rows, headers="keys", tablefmt="simple_outline"
        )
        right_table = IsolatedTabulateRunner.tabulate(
            tabular_data=new_right_rows, headers="keys", tablefmt="simple_outline"
        )

        if left_table != right_table:
            diff_lines = difflib.unified_diff(
                a=left_table.splitlines(keepends=True),
                b=right_table.splitlines(keepends=True),
                fromfile="Left Result",
                tofile="Right Result",
            )
            diff = "".join(diff_lines)
            assert False, LazyFormat(
                "Mismatch between left and right sets", left_table=left_table, right_table=right_table, diff=diff
            ).evaluated_value

        logger.debug(LazyFormat("Left and right sets match", matching_table=left_table))
