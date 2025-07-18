from __future__ import annotations

import difflib
import logging
import time
from typing import ContextManager, Iterable, Optional, Sequence, Set, Type

from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple, ExceptionTracebackAnyType
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.dsi.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.mf_graph.path_finding.path_finder import MetricflowGraphPathFinder
from metricflow_semantics.experimental.mf_graph.path_finding.path_finder_cache import PathFinderCache
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeRecipeWriterPath,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.sg_linkable_spec_resolver import (
    SemanticGraphLinkableSpecResolver,
)
from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator import SubgraphGeneratorArgumentSet
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import SemanticGraphEdge, SemanticGraphNode
from metricflow_semantics.helpers.table_helpers import IsolatedTabulateRunner
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseLinkableElementSet
from metricflow_semantics.model.semantics.linkable_spec_index_builder import LinkableSpecIndexBuilder
from metricflow_semantics.model.semantics.linkable_spec_resolver import LegacyLinkableSpecResolver
from metricflow_semantics.model.semantics.manifest_object_lookup import SemanticManifestObjectLookup
from metricflow_semantics.model.semantics.semantic_model_join_evaluator import MAX_JOIN_HOPS
from metricflow_semantics.specs.spec_set import group_spec_by_type

from tests_metricflow_semantics.experimental.semantic_graph.table_helpers import RowColumnWidthEqualizer

logger = logging.getLogger(__name__)


class LinkableSpecResolverTester:
    def __init__(self, semantic_manifest: SemanticManifest) -> None:
        self._manifest_lookup = SemanticManifestLookup(semantic_manifest)
        self._measure_references = self._manifest_lookup.semantic_model_lookup.measure_references
        self._metric_references = self._manifest_lookup.metric_lookup.metric_references

        self._legacy_resolver = LinkableSpecResolverTester.create_legacy_resolver(semantic_manifest)
        self._sg_resolver = LinkableSpecResolverTester.create_sg_resolver(semantic_manifest)

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
            SubgraphGeneratorArgumentSet(
                manifest_object_lookup=manifest_object_lookup,
                path_finder=path_finder,
            )
        )
        semantic_graph = builder.build()

        return SemanticGraphLinkableSpecResolver(
            manifest_object_lookup=manifest_object_lookup, semantic_graph=semantic_graph, path_finder=path_finder
        )

    def compare_resolver_outputs_for_all_metric_pairs(
        self, element_filter: LinkableElementFilter = LinkableElementFilter()
    ) -> None:
        test_cases: list[AnyLengthTuple[MetricReference]] = []
        metric_count = len(self._metric_references)

        for first_index in range(metric_count):
            for second_index in range(metric_count):
                test_cases.append((self._metric_references[first_index], self._metric_references[second_index]))

        resolution_times: list[ResolutionTime] = []
        for test_case in test_cases:
            logger.debug(
                LazyFormat(
                    "Comparing results from the legacy implementation and semantic-graph implementation",
                    test_case=test_case,
                )
            )
            resolution_times.append(
                self.compare_resolver_outputs(metric_references=test_case, element_filter=element_filter)
            )

        cumulative_time = ResolutionTime.sum(resolution_times)
        logger.debug(
            LazyFormat(
                "Cumulative resolution time",
                time_for_legacy_resolver=lambda: f"{cumulative_time.time_for_legacy_resolver:.2f}s",
                time_for_sg_resolver=lambda: f"{cumulative_time.time_for_sg_resolver:.2f}s",
            )
        )

    def compare_resolver_outputs_for_single_metric(
        self, metric_reference: MetricReference, element_filter: LinkableElementFilter = LinkableElementFilter()
    ) -> None:
        logger.debug(
            LazyFormat(
                "Comparing results from the legacy implementation and semantic-graph implementation",
            )
        )
        self.compare_resolver_outputs(metric_references=(metric_reference,), element_filter=element_filter)

    def compare_resolver_outputs_for_all_metrics(
        self, element_filter: LinkableElementFilter = LinkableElementFilter()
    ) -> None:
        resolution_times: list[ResolutionTime] = []
        for metric_reference in self._metric_references:
            resolution_times.append(
                self.compare_resolver_outputs(metric_references=(metric_reference,), element_filter=element_filter)
            )

        cumulative_time = ResolutionTime.sum(resolution_times)
        logger.debug(
            LazyFormat(
                "Cumulative resolution time",
                time_for_legacy_resolver=lambda: f"{cumulative_time.time_for_legacy_resolver:.2f}s",
                time_for_sg_resolver=lambda: f"{cumulative_time.time_for_sg_resolver:.2f}s",
            )
        )

    def compare_resolver_outputs(
        self,
        metric_references: Sequence[MetricReference],
        element_filter: LinkableElementFilter = LinkableElementFilter(),
    ) -> ResolutionTime:
        logger.debug("Generating using legacy implementation")

        with PerformanceTimer() as legacy_timer:
            legacy_linkable_element_set = self._legacy_resolver.get_linkable_elements_for_metrics(
                metric_references, element_filter
            )

        logger.debug("Generating using semantic graph implementation")

        with PerformanceTimer() as sg_timer:
            sg_linkable_element_set = self._sg_resolver.get_linkable_elements_for_metrics(
                metric_references, element_filter
            )

        logger.debug(LazyFormat("Checking results", metric_references=metric_references, element_filter=element_filter))

        LinkableSpecResolverTester.assert_linkable_element_sets_equal(
            left_set=legacy_linkable_element_set,
            right_set=sg_linkable_element_set,
        )

        logger.debug(LazyFormat("Matched sets", metric_references=metric_references))

        return ResolutionTime(
            time_for_sg_resolver=sg_timer.total_time,
            time_for_legacy_resolver=legacy_timer.total_time,
        )

    def compare_resolver_outputs_for_a_measure(
        self,
        measure_reference: MeasureReference,
        element_filter: LinkableElementFilter = LinkableElementFilter(),
    ) -> None:
        logger.debug("Generating using semantic graph implementation")
        sg_linkable_element_set = self._sg_resolver.get_linkable_element_set_for_measure(
            measure_reference, element_filter
        )

        try:
            logger.debug("Generating using legacy implementation")
            legacy_linkable_element_set = self._legacy_resolver.get_linkable_element_set_for_measure(
                measure_reference, element_filter
            )
        except KeyError:
            logger.debug(
                LazyFormat(
                    "Skipping comparison of the linkable-element set for a measure due to a bug in the legacy implementation. "
                    "It should be possible to make the request for the given measure, but it is not.",
                    measure_reference=measure_reference,
                ),
                exc_info=True,
            )
            return

        logger.debug(LazyFormat("Checking sets for measure", measure_reference=measure_reference))

        LinkableSpecResolverTester.assert_linkable_element_sets_equal(
            left_set=legacy_linkable_element_set,
            right_set=sg_linkable_element_set,
        )

        logger.debug(LazyFormat("Matched sets", measure_reference=measure_reference))

    @staticmethod
    def log_legacy_resolver_output_for_metric(
        semantic_manifest: SemanticManifest,
        metric_name: Optional[str] = None,
    ) -> None:
        legacy_resolver = LinkableSpecResolverTester.create_legacy_resolver(semantic_manifest)
        logger.debug(
            LazyFormat(
                "Linkable element set for metric",
                metric_name=metric_name,
                table=LinkableSpecResolverTester.convert_set_to_table(
                    legacy_resolver.get_linkable_elements_for_metrics(metric_references=(MetricReference("bookings"),))
                ),
            )
        )

    @staticmethod
    def convert_set_to_table(linkable_element_set: BaseLinkableElementSet) -> str:
        rows = convert_linkable_element_set_to_rows(linkable_element_set)
        return IsolatedTabulateRunner.tabulate(tabular_data=rows, headers="keys", tablefmt="simple_outline")

    @staticmethod
    def assert_linkable_element_sets_equal(  # noqa: D103
        left_set: BaseLinkableElementSet,
        right_set: BaseLinkableElementSet,
        exclude_diff_prefixes: Optional[Set[str]] = None,
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

        if left_table == right_table:
            logger.debug(LazyFormat("Left and right sets match"))
            return

        diff_lines = difflib.unified_diff(
            a=left_table.splitlines(keepends=True),
            b=right_table.splitlines(keepends=True),
            fromfile="Left Result",
            tofile="Right Result",
            n=0,
        )

        significant_diff_lines = []
        if exclude_diff_prefixes is not None:
            exclude_diff_prefixes = {
                "---",
                "+++",
                "@@",
            }.union(exclude_diff_prefixes)

            for diff_line in diff_lines:
                ignore_diff_line = False
                for exclude_prefix in exclude_diff_prefixes:
                    if diff_line.startswith(exclude_prefix):
                        ignore_diff_line = True
                        break

                if not ignore_diff_line:
                    significant_diff_lines.append(diff_line)

            if len(significant_diff_lines) == 0:
                return

        diff = "".join(diff_lines)
        assert False, LazyFormat(
            "Mismatch between left and right sets",
            left_table=left_table,
            right_table=right_table,
            diff=diff,
            significant_diff_lines="".join(significant_diff_lines),
            exclude_diff_prefixes="\n".join(exclude_diff_prefixes) if exclude_diff_prefixes is not None else None,
        ).evaluated_value


@fast_frozen_dataclass()
class ResolutionTime:
    time_for_legacy_resolver: float
    time_for_sg_resolver: float

    @staticmethod
    def sum(resolution_times: Iterable[ResolutionTime]) -> ResolutionTime:
        total_time_for_legacy_resolver = sum(
            resolution_time.time_for_legacy_resolver for resolution_time in resolution_times
        )
        total_time_for_sg_resolver = sum(resolution_time.time_for_sg_resolver for resolution_time in resolution_times)

        return ResolutionTime(
            time_for_legacy_resolver=total_time_for_legacy_resolver,
            time_for_sg_resolver=total_time_for_sg_resolver,
        )


class PerformanceTimer(ContextManager["PerformanceTimer"]):
    def __init__(self) -> None:
        self._start_time: Optional[float] = None
        self._total_time = 0.0

    @property
    def total_time(self) -> float:
        return self._total_time

    def __enter__(self) -> PerformanceTimer:
        self._start_time = time.perf_counter()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[ExceptionTracebackAnyType],
    ) -> None:
        if self._start_time is None:
            raise RuntimeError("Context manager shouldn't exit without first entering.")

        self._total_time += time.perf_counter() - self._start_time


def convert_linkable_element_set_to_rows(
    linkable_element_set: BaseLinkableElementSet,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    # ("Type", "Dunder Name", "Metric-Subquery Entity-Links", "Properties", "Derived-From Semantic Models")

    for annotated_spec in sorted(
        linkable_element_set.annotated_specs,
        key=lambda annotated_spec_in_lambda: annotated_spec_in_lambda.spec.qualified_name,
    ):
        # row: LinkableElementSetSnapshotRow = [annotated_spec.element_type.name, annotated_spec.spec.qualified_name]
        row_dict: dict[str, str] = {
            "Dunder Name": annotated_spec.spec.qualified_name.ljust(78),
        }
        spec_set = group_spec_by_type(annotated_spec.spec)

        if len(spec_set.group_by_metric_specs) == 0:
            # row.append(None)
            row_dict["Metric-Subquery Entity-Links"] = ""
        elif len(spec_set.group_by_metric_specs) == 1:
            group_by_metric_spec = spec_set.group_by_metric_specs[0]
            # row.append([entity_link.element_name for entity_link in group_by_metric_spec.metric_subquery_entity_links])
            row_dict["Metric-Subquery Entity-Links"] = ",".join(
                entity_link.element_name for entity_link in group_by_metric_spec.metric_subquery_entity_links
            )
        else:
            raise RuntimeError(LazyFormat("There should have been at most 1 group-by-metric spec", spec_set=spec_set))
        row_dict["Type"] = annotated_spec.element_type.name.ljust(14)
        # row.extend(
        #     (
        #         sorted(linkable_element_property.name for linkable_element_property in annotated_spec.properties),
        #         sorted(
        #             model_reference.semantic_model_name
        #             for model_reference in annotated_spec.derived_from_semantic_models
        #         ),
        #     )
        # )

        row_dict["Properties"] = ",".join(
            sorted(linkable_element_property.name for linkable_element_property in annotated_spec.properties)
        )
        row_dict["Derived-From Semantic Models"] = ",".join(
            sorted(
                model_reference.semantic_model_name for model_reference in annotated_spec.derived_from_semantic_models
            )
        )

        # row_dict["Derived-From Semantic Models"] = ""

        rows.append(row_dict)

    return rows
