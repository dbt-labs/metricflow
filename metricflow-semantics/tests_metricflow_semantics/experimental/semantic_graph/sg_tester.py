from __future__ import annotations

import logging
from typing import Iterable, Optional, Sequence

from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.recipe_writer_path import (
    RecipeWriterPathfinder,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.sg_linkable_spec_resolver import (
    SemanticGraphLinkableSpecResolver,
)
from metricflow_semantics.experimental.semantic_graph.sg_interfaces import (
    SemanticGraph,
)
from metricflow_semantics.helpers.performance_helpers import ExecutionTimer
from metricflow_semantics.helpers.time_helpers import PrettyTimeDelta
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseLinkableElementSet
from metricflow_semantics.model.semantics.linkable_spec_resolver import LegacyLinkableSpecResolver
from metricflow_semantics.specs.spec_set import group_spec_by_type
from metricflow_semantics.test_helpers.table_helpers import PaddedTextTableBuilder

from tests_metricflow_semantics.experimental.semantic_graph.sg_fixtures import SemanticGraphTestFixture

logger = logging.getLogger(__name__)


class SemanticGraphTester:
    """A temporary class to help run tests during the migration to the semantic graph."""

    def __init__(self, fixture: SemanticGraphTestFixture) -> None:  # noqa: D107
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

    def compare_resolver_outputs_for_one_measure(
        self,
        measure_reference: MeasureReference,
        element_filter: Optional[LinkableElementFilter],
        log_result_table: bool = False,
    ) -> Optional[_ResolutionTimePair]:
        """Compare the result for the legacy resolver and the semantic-graph resolver for one measure."""
        logger.debug("Generating using semantic graph implementation")
        with ExecutionTimer() as sg_timer:
            sg_linkable_element_set = self._sg_resolver.get_linkable_element_set_for_measure(
                measure_reference, element_filter
            )

        try:
            logger.debug("Generating using legacy implementation")
            with ExecutionTimer() as legacy_timer:
                legacy_linkable_element_set = self._legacy_resolver.get_linkable_element_set_for_measure(
                    measure_reference, element_filter or LinkableElementFilter()
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

        logger.debug(
            LazyFormat("Checking sets for measure", measure_reference=measure_reference, element_filter=element_filter)
        )

        try:
            self.assert_linkable_element_sets_equal(
                left_set=legacy_linkable_element_set,
                right_set=sg_linkable_element_set,
                log_result_table=log_result_table,
            )
        except AssertionError as e:
            raise AssertionError(
                LazyFormat("Result mismatch", measure_reference=measure_reference, element_filter=element_filter)
            ) from e
        logger.debug(LazyFormat("Matched sets", measure_reference=measure_reference))

        return _ResolutionTimePair(
            time_for_sg_resolver=sg_timer.execution_time,
            time_for_legacy_resolver=legacy_timer.execution_time,
        )

    def compare_resolver_outputs_for_all_measures(  # noqa: D102
        self,
        element_filter: Optional[LinkableElementFilter],
        log_result_table: bool = False,
    ) -> None:
        for measure_reference in self._measure_references:
            self.compare_resolver_outputs_for_one_measure(measure_reference, element_filter, log_result_table)

    def compare_resolver_outputs_for_metrics(  # noqa: D102
        self,
        metric_references: Sequence[MetricReference],
        element_filter: LinkableElementFilter,
        log_result_table: bool = False,
    ) -> _ResolutionTimePair:
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

        return _ResolutionTimePair(
            time_for_sg_resolver=sg_timer.execution_time,
            time_for_legacy_resolver=legacy_timer.execution_time,
        )

    @property
    def _semantic_graph(self) -> SemanticGraph:
        return self._fixture.semantic_graph

    @property
    def _path_finder(self) -> RecipeWriterPathfinder:
        return self._fixture.pathfinder

    @staticmethod
    def assert_linkable_element_sets_equal(  # noqa: D102
        left_set: BaseLinkableElementSet, right_set: BaseLinkableElementSet, log_result_table: bool
    ) -> None:
        left_rows = SemanticGraphTester._convert_linkable_element_set_to_rows(left_set)
        right_rows = SemanticGraphTester._convert_linkable_element_set_to_rows(right_set)

        comparison_helper = PaddedTextTableBuilder()
        comparison_helper.add_left_rows(left_rows)
        comparison_helper.add_right_rows(right_rows)
        comparison_helper.assert_tables_equal(log_result_table)

    @staticmethod
    def _convert_linkable_element_set_to_rows(
        linkable_element_set: BaseLinkableElementSet,
    ) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []

        for annotated_spec in sorted(
            linkable_element_set.annotated_specs,
            key=lambda annotated_spec_in_lambda: annotated_spec_in_lambda.spec.qualified_name,
        ):
            row_dict: dict[str, str] = {
                "Dunder Name": annotated_spec.spec.qualified_name,
            }
            spec_set = group_spec_by_type(annotated_spec.spec)

            if len(spec_set.group_by_metric_specs) == 0:
                row_dict["Metric-Subquery Entity-Links"] = ""
            elif len(spec_set.group_by_metric_specs) == 1:
                group_by_metric_spec = spec_set.group_by_metric_specs[0]
                row_dict["Metric-Subquery Entity-Links"] = ",".join(
                    entity_link.element_name for entity_link in group_by_metric_spec.metric_subquery_entity_links
                )
            else:
                raise RuntimeError(
                    LazyFormat("There should have been at most 1 group-by-metric spec", spec_set=spec_set)
                )
            row_dict["Type"] = annotated_spec.element_type.name
            row_dict["Properties"] = ",".join(
                sorted(linkable_element_property.name for linkable_element_property in annotated_spec.property_set)
            )
            row_dict["Derived-From Semantic Models"] = ",".join(
                sorted(
                    model_reference.semantic_model_name
                    for model_reference in annotated_spec.derived_from_semantic_models
                )
            )
            rows.append(row_dict)

        return rows


@fast_frozen_dataclass()
class _ResolutionTimePair:
    """Bundles resolution times between the legacy resolver and the SG resolver.

    This will be removed after migration.
    """

    time_for_legacy_resolver: PrettyTimeDelta
    time_for_sg_resolver: PrettyTimeDelta

    @staticmethod
    def sum(resolution_times: Iterable[_ResolutionTimePair]) -> _ResolutionTimePair:  # noqa: D102
        return _ResolutionTimePair(
            time_for_legacy_resolver=PrettyTimeDelta.sum(
                resolution_time.time_for_legacy_resolver for resolution_time in resolution_times
            ),
            time_for_sg_resolver=PrettyTimeDelta.sum(
                resolution_time.time_for_sg_resolver for resolution_time in resolution_times
            ),
        )
