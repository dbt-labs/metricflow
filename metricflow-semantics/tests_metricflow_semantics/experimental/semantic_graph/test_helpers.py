from __future__ import annotations

import cProfile
import difflib
import logging
import pstats
from typing import Iterable, Iterator, Optional, Sequence, Set

from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.recipe_writer_path import (
    AttributeRecipeWriterPath,
    RecipeWriterPathfinder,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.recipe_writer_weight import (
    AttributeRecipeWriterWeightFunction,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.sg_linkable_spec_resolver import (
    SemanticGraphLinkableSpecResolver,
)
from metricflow_semantics.experimental.semantic_graph.nodes.node_labels import GroupByAttributeLabel
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import (
    SemanticGraph,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.test_helpers.performance_helpers import BenchmarkFunction, PerformanceBenchmark
from metricflow_semantics.helpers.performance_helpers import ExecutionTimer
from metricflow_semantics.helpers.table_helpers import IsolatedTabulateRunner
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseLinkableElementSet
from metricflow_semantics.model.semantics.linkable_spec_resolver import LegacyLinkableSpecResolver
from metricflow_semantics.specs.spec_set import group_spec_by_type
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal
from run_pstats import CPROFILE_OUTPUT_FILE_PATH
from typing_extensions import override

from tests_metricflow_semantics.experimental.semantic_graph.sg_fixtures import SemanticGraphTestFixture
from tests_metricflow_semantics.experimental.semantic_graph.table_helpers2 import EqualColumnWidthTableFormatter

logger = logging.getLogger(__name__)


class SemanticGraphTester2:
    @override
    def __init__(self, fixture: SemanticGraphTestFixture) -> None:
        self._fixture = fixture
        self._measure_references = tuple(
            measure.reference
            for lookup in fixture.manifest_object_lookup.measure_containing_model_lookups
            for measures in lookup.aggregation_configuration_to_measures.values()
            for measure in measures
        )
        self._metric_references = tuple(
            MetricReference(metric.name) for metric in fixture.manifest_object_lookup.get_metrics()
        )

    @property
    def _legacy_resolver(self) -> LegacyLinkableSpecResolver:
        return self._fixture.legacy_resolver

    @property
    def _sg_resolver(self) -> SemanticGraphLinkableSpecResolver:
        return self._fixture.sg_resolver

    def profile_sg_init(self) -> None:
        output_filename = str(CPROFILE_OUTPUT_FILE_PATH)
        logger.info(LazyFormat("Running performance profiling", output_filename=output_filename))
        cProfile.runctx(
            statement="self._fixture.create_sg_resolver()",
            filename=str(CPROFILE_OUTPUT_FILE_PATH),
            locals=locals(),
            globals=globals(),
        )

    def profile_sg_resolution(self, measure_reference: MeasureReference) -> None:
        output_filename = str(CPROFILE_OUTPUT_FILE_PATH)
        logger.info(LazyFormat("Running performance profiling", output_filename=output_filename))
        assert self._fixture.sg_resolver
        cProfile.runctx(
            statement="self._fixture.sg_resolver.get_linkable_element_set_for_measure(measure_reference)",
            filename=str(CPROFILE_OUTPUT_FILE_PATH),
            locals=locals(),
            globals=globals(),
        )

    def compare_resolver_outputs_for_all_metric_pairs(
        self,
        element_filter: LinkableElementFilter = LinkableElementFilter(),
        log_result_table: bool = False,
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
                self.compare_resolver_outputs_for_metrics(
                    metric_references=test_case, element_filter=element_filter, log_result_table=log_result_table
                )
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
        self,
        metric_reference: MetricReference,
        element_filter: LinkableElementFilter = LinkableElementFilter(),
        log_result_table: bool = False,
    ) -> None:
        logger.debug(
            LazyFormat(
                "Comparing results from the legacy implementation and semantic-graph implementation",
            )
        )
        self.compare_resolver_outputs_for_metrics(
            metric_references=(metric_reference,), element_filter=element_filter, log_result_table=log_result_table
        )

    def compare_resolver_outputs_for_all_metrics_individually(
        self,
        element_filter: LinkableElementFilter = LinkableElementFilter(),
        log_result_table: bool = False,
    ) -> None:
        resolution_times: list[ResolutionTime] = []
        for metric_reference in self._metric_references:
            resolution_times.append(
                self.compare_resolver_outputs_for_metrics(
                    metric_references=(metric_reference,),
                    element_filter=element_filter,
                    log_result_table=log_result_table,
                )
            )

        cumulative_time = ResolutionTime.sum(resolution_times)
        logger.debug(
            LazyFormat(
                "Cumulative resolution time for all metrics",
                time_for_legacy_resolver=lambda: f"{cumulative_time.time_for_legacy_resolver:.2f}s",
                time_for_sg_resolver=lambda: f"{cumulative_time.time_for_sg_resolver:.2f}s",
            )
        )

    def compare_resolver_outputs_for_all_measures(
        self,
        element_filter: LinkableElementFilter = LinkableElementFilter(),
        log_result_table: bool = False,
    ) -> None:
        resolution_times: list[ResolutionTime] = []
        for measure_reference in self._measure_references:
            resolution_time = self.compare_resolver_outputs_for_a_measure(
                measure_reference=measure_reference,
                element_filter=element_filter,
                log_result_table=log_result_table,
            )
            if resolution_time is None:
                logger.debug(
                    LazyFormat(
                        "Skipping due to a `None` result.",
                        measure_reference=measure_reference,
                    )
                )
                continue
            resolution_times.append(resolution_time)

        cumulative_time = ResolutionTime.sum(resolution_times)
        logger.debug(
            LazyFormat(
                "Cumulative resolution time for all measures",
                time_for_legacy_resolver=lambda: f"{cumulative_time.time_for_legacy_resolver:.2f}s",
                time_for_sg_resolver=lambda: f"{cumulative_time.time_for_sg_resolver:.2f}s",
            )
        )

    def compare_resolver_outputs_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        element_filter: LinkableElementFilter,
        log_result_table: bool,
    ) -> ResolutionTime:
        logger.debug("Generating using legacy implementation")

        with ExecutionTimer() as legacy_timer:
            legacy_linkable_element_set = self._legacy_resolver.get_linkable_elements_for_metrics(
                metric_references, element_filter
            )

        logger.debug("Generating using semantic graph implementation")

        with ExecutionTimer() as sg_timer:
            sg_linkable_element_set = self._sg_resolver.get_linkable_elements_for_metrics(
                metric_references, element_filter
            )

        logger.debug(LazyFormat("Checking results", metric_references=metric_references, element_filter=element_filter))

        self.assert_linkable_element_sets_equal(
            left_set=legacy_linkable_element_set, right_set=sg_linkable_element_set, log_result_table=log_result_table
        )
        logger.debug(LazyFormat("Matched sets", metric_references=metric_references))

        return ResolutionTime(
            time_for_sg_resolver=sg_timer.execution_time_float,
            time_for_legacy_resolver=legacy_timer.execution_time_float,
        )

    def compare_resolver_outputs_for_distinct_values(
        self,
        element_filter: LinkableElementFilter,
        log_result_table: bool,
    ) -> ResolutionTime:
        logger.debug("Generating using legacy implementation")

        with ExecutionTimer() as legacy_timer:
            legacy_linkable_element_set = self._legacy_resolver.get_linkable_elements_for_distinct_values_query(
                element_filter
            )

        logger.debug("Generating using semantic graph implementation")

        with ExecutionTimer() as sg_timer:
            sg_linkable_element_set = self._sg_resolver.get_linkable_elements_for_distinct_values_query(element_filter)

        logger.debug(LazyFormat("Checking results", element_filter=element_filter))

        self.assert_linkable_element_sets_equal(
            left_set=legacy_linkable_element_set, right_set=sg_linkable_element_set, log_result_table=log_result_table
        )
        logger.debug(
            LazyFormat(
                "Matched sets",
            )
        )

        return ResolutionTime(
            time_for_sg_resolver=sg_timer.execution_time_float,
            time_for_legacy_resolver=legacy_timer.execution_time_float,
        )

    def compare_resolver_outputs_for_a_measure(
        self,
        measure_reference: MeasureReference,
        element_filter: LinkableElementFilter,
        log_result_table: bool,
    ) -> Optional[ResolutionTime]:
        logger.debug("Generating using semantic graph implementation")
        with ExecutionTimer() as sg_timer:
            sg_linkable_element_set = self._sg_resolver.get_linkable_element_set_for_measure(
                measure_reference, element_filter
            )

        try:
            logger.debug("Generating using legacy implementation")
            with ExecutionTimer() as legacy_timer:
                legacy_linkable_element_set = self._legacy_resolver.get_linkable_element_set_for_measure(
                    measure_reference, element_filter
                )
        except KeyError:
            logger.warning(
                LazyFormat(
                    "Skipping comparison of the linkable-element set for a measure due to a bug in the legacy implementation. "
                    "It should be possible to make the request for the given measure, but it is not.",
                    measure_reference=measure_reference,
                ),
                exc_info=True,
            )
            return None

        logger.debug(LazyFormat("Checking sets for measure", measure_reference=measure_reference))

        self.assert_linkable_element_sets_equal(
            left_set=legacy_linkable_element_set,
            right_set=sg_linkable_element_set,
            log_result_table=log_result_table,
        )

        logger.debug(LazyFormat("Matched sets", measure_reference=measure_reference))

        return ResolutionTime(
            time_for_sg_resolver=sg_timer.execution_time_float,
            time_for_legacy_resolver=legacy_timer.execution_time_float,
        )

    def log_legacy_resolver_output_for_metric(
        self,
        metric_name: Optional[str] = None,
    ) -> None:
        legacy_resolver = self._legacy_resolver
        logger.debug(
            LazyFormat(
                "Linkable element set for metric",
                metric_name=metric_name,
                table=self.convert_set_to_table(
                    legacy_resolver.get_linkable_elements_for_metrics(metric_references=(MetricReference("bookings"),))
                ),
            )
        )

    def log_sg_resolver_output_for_measure(
        self,
        measure_reference: MeasureReference,
    ) -> None:
        sg_resolver = self._sg_resolver
        logger.debug(
            LazyFormat(
                "Logging element set for a measure",
                measure_reference=measure_reference,
                table=self.convert_set_to_table(
                    sg_resolver.get_linkable_element_set_for_measure(
                        measure_reference, element_filter=LinkableElementFilter()
                    )
                ),
            )
        )

    def time_resolver_output_for_measure(self, measure_reference: MeasureReference) -> None:
        self.time_resolver_output_for_measures((measure_reference,))

    def time_resolver_output_for_measures(self, measure_references: Iterable[MeasureReference]) -> None:
        resolver = self._sg_resolver

        for measure_reference in measure_references:
            with ExecutionTimer() as timer:
                element_set = resolver.get_linkable_element_set_for_measure(measure_reference)
                spec_count = len(element_set.annotated_specs)
            logger.info(
                LazyFormat(
                    "Finished resolution",
                    runtime=f"{timer.execution_time_float:.2f}s",
                    spec_count=spec_count,
                )
            )

    def time_resolver_output_for_metrics(self, metric_references: Sequence[MetricReference]) -> None:
        resolver = self._sg_resolver

        with ExecutionTimer("Resolve many metrics") as timer:
            for metric_reference in metric_references:
                element_set = resolver.get_linkable_elements_for_metrics([metric_reference])
                spec_count = len(element_set.annotated_specs)
                logger.info(
                    LazyFormat(
                        "Finished resolution",
                        metric_name=metric_reference.element_name,
                        runtime=f"{timer.execution_time_float:.2f}s",
                        spec_count=spec_count,
                    )
                )

    def time_resolver_output_for_all_metrics(self) -> None:
        resolver = self._sg_resolver
        element_filter = LinkableElementFilter()

        with ExecutionTimer() as resolve_all_metrics_timer:
            for metric_reference in self._metric_references:
                with ExecutionTimer() as resolve_metric_timer:
                    element_set = resolver.get_linkable_elements_for_metrics(
                        [metric_reference], element_filter=element_filter
                    )
                    spec_count = len(element_set.annotated_specs)
                logger.info(
                    LazyFormat(
                        "Finished metric resolution",
                        runtime=f"{resolve_metric_timer.execution_time_float:.2f}s",
                        spec_count=spec_count,
                    )
                )
        logger.info(
            LazyFormat(
                "Finished resolution for all metrics",
                runtime=f"{resolve_all_metrics_timer.execution_time_float:.2f}s",
                metric_count=len(self._metric_references),
            )
        )

    def time_resolver_output_for_all_measures(self, limit: int = -1) -> None:
        element_filter = LinkableElementFilter()
        with ExecutionTimer() as resolve_all_measures_timer:
            resolver = self._fixture.create_sg_resolver()
            for measure_reference in self._measure_references[:limit]:
                with ExecutionTimer() as resolve_metric_timer:
                    element_set = resolver.get_linkable_element_set_for_measure(
                        measure_reference, element_filter=element_filter
                    )
                    spec_count = len(element_set.annotated_specs)
                logger.info(
                    LazyFormat(
                        "Finished resolution for one measure",
                        runtime=f"{resolve_metric_timer.execution_time_float:.2f}s",
                        measure=measure_reference.element_name,
                        spec_count=spec_count,
                    )
                )
        logger.info(
            LazyFormat(
                "Finished resolution for all measures",
                runtime=f"{resolve_all_measures_timer.execution_time_float:.2f}s",
                measure_count=len(self._measure_references),
            )
        )

    @property
    def _semantic_graph(self) -> SemanticGraph:
        return self._fixture.semantic_graph

    @property
    def _path_finder(self) -> RecipeWriterPathfinder:
        return self._fixture.pathfinder

    def log_paths_from_node(
        self, source_node: SemanticGraphNode, element_filter: Optional[LinkableElementFilter] = None
    ) -> None:
        target_nodes = self._semantic_graph.nodes_with_labels(GroupByAttributeLabel.get_instance())
        logger.info(LazyFormat("Starting traversal", source_node=source_node, element_filter=element_filter))

        for found_path in self._path_finder.find_paths_dfs(
            graph=self._semantic_graph,
            initial_path=AttributeRecipeWriterPath.create(source_node),
            target_nodes=target_nodes,
            weight_function=AttributeRecipeWriterWeightFunction(element_filter=element_filter),
            max_path_weight=2,
            node_allow_set=None,
            node_deny_set=None,
        ):
            # attribute_recipe = mutable_path.recipe_writer.latest_recipe
            logger.info(
                LazyFormat(
                    "Found path",
                    found_path=found_path,
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
        log_result_table: bool = False,
    ) -> None:
        headers = ("Dunder Name", "Metric-Subquery Entity-Links", "Type", "Properties", "Derived-From Semantic Models")

        left_rows = convert_linkable_element_set_to_rows(left_set)
        right_rows = convert_linkable_element_set_to_rows(right_set)

        equalizer = EqualColumnWidthTableFormatter()

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
            additional_format_kwargs = {"left_table": left_table} if log_result_table else {}
            logger.debug(LazyFormat("Left and right sets match", **additional_format_kwargs))
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

    def assert_initialization_performance_factor(self, min_performance_factor: float) -> None:
        fixture = self._fixture

        class _LeftFunction(BenchmarkFunction):
            def run(self) -> None:
                fixture.create_legacy_resolver()

        class _RightFunction(BenchmarkFunction):
            def run(self) -> None:
                fixture.create_sg_resolver()

        PerformanceBenchmark.assert_function_performance(
            left_function_class=_LeftFunction,
            right_function_class=_RightFunction,
            min_performance_factor=min_performance_factor,
        )

    def find_paths(self, source_node: SemanticGraphNode) -> Iterator[AttributeRecipeWriterPath]:
        path_finder = self._path_finder
        target_nodes = self._semantic_graph.nodes_with_labels(GroupByAttributeLabel.get_instance())
        element_filter = LinkableElementFilter()

        for found_path in path_finder.find_paths_dfs(
            graph=self._semantic_graph,
            initial_path=AttributeRecipeWriterPath.create(source_node),
            target_nodes=target_nodes,
            weight_function=AttributeRecipeWriterWeightFunction(element_filter),
            max_path_weight=2,
            node_allow_set=None,
            node_deny_set=None,
        ):
            yield found_path

    def time_path_finding(self, source_node: SemanticGraphNode) -> float:
        timer = ExecutionTimer("Path Finding")
        assert self._semantic_graph
        with timer:
            for _ in self.find_paths(source_node):
                pass
        return timer.execution_time_float

    def profile_path_finding(self, source_node: SemanticGraphNode) -> None:
        output_filename = str(CPROFILE_OUTPUT_FILE_PATH)
        assert self._semantic_graph

        # Populate cache
        with ExecutionTimer("Populate Cache"):
            self.time_path_finding(source_node)

        logger.info(LazyFormat("Running performance profiling", output_filename=output_filename))
        cProfile.runctx(
            statement="self.time_path_finding(source_node)",
            filename=output_filename,
            locals=locals(),
            globals=globals(),
        )

        p = pstats.Stats(output_filename)
        p.strip_dirs()
        p.sort_stats("cumtime")
        p.print_stats(50)

    def assert_found_paths(self, source_node: SemanticGraphNode) -> None:
        fixture = self._fixture
        path_finder = self._path_finder
        target_nodes = self._semantic_graph.nodes_with_labels(GroupByAttributeLabel.get_instance())
        element_filter = LinkableElementFilter()

        found_paths: list[AttributeRecipeWriterPath] = []
        for found_path in path_finder.find_paths_dfs(
            graph=self._semantic_graph,
            initial_path=AttributeRecipeWriterPath.create(source_node),
            target_nodes=target_nodes,
            weight_function=AttributeRecipeWriterWeightFunction(element_filter),
            max_path_weight=2,
            node_allow_set=None,
            node_deny_set=None,
        ):
            # logger.debug(LazyFormat("Found path.", path=path))
            found_paths.append(found_path.copy())

        assert len(found_paths) > 0

        found_paths.sort()
        assert_str_snapshot_equal(
            request=fixture.request,
            snapshot_configuration=fixture.snapshot_configuration,
            snapshot_str="\n".join(
                [found_path.arrow_format() + "\n" + mf_pformat(found_path.latest_recipe) for found_path in found_paths]
            ),
        )


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
            sorted(linkable_element_property.name for linkable_element_property in annotated_spec.property_set)
        )
        row_dict["Derived-From Semantic Models"] = ",".join(
            sorted(
                model_reference.semantic_model_name for model_reference in annotated_spec.derived_from_semantic_models
            )
        )

        # row_dict["Derived-From Semantic Models"] = ""

        rows.append(row_dict)

    return rows
