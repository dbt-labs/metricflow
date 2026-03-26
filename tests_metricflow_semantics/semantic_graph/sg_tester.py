from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Callable, Iterable, Optional, Sequence

from dbt_semantic_interfaces.references import MetricReference
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseGroupByItemSet
from metricflow_semantics.semantic_graph.attribute_resolution.recipe_writer_path import (
    RecipeWriterPathfinder,
)
from metricflow_semantics.semantic_graph.attribute_resolution.sg_linkable_spec_resolver import (
    SemanticGraphGroupByItemSetResolver,
)
from metricflow_semantics.semantic_graph.sg_interfaces import (
    SemanticGraph,
)
from metricflow_semantics.specs.spec_set import group_spec_by_type
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal
from metricflow_semantics.test_helpers.table_helpers import PaddedTextTableBuilder
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.string_helpers import mf_indent
from metricflow_semantics.toolkit.time_helpers import PrettyDuration

from tests_metricflow_semantics.semantic_graph.sg_fixtures import SemanticGraphTestFixture

logger = logging.getLogger(__name__)


class SemanticGraphTester:
    """A temporary class to help run tests during the migration to the semantic graph."""

    def __init__(self, fixture: SemanticGraphTestFixture) -> None:  # noqa: D107
        self._fixture = fixture
        self._metric_references = tuple(
            MetricReference(metric.name) for metric in fixture.manifest_object_lookup.get_metrics()
        )

    @property
    def sg_resolver(self) -> SemanticGraphGroupByItemSetResolver:  # noqa: D102
        return self._fixture.sg_resolver

    @property
    def _semantic_graph(self) -> SemanticGraph:
        return self._fixture.semantic_graph

    @property
    def _path_finder(self) -> RecipeWriterPathfinder:
        return self._fixture.pathfinder

    @staticmethod
    def assert_linkable_element_sets_equal(  # noqa: D102
        left_set: BaseGroupByItemSet, right_set: BaseGroupByItemSet, log_result_table: bool
    ) -> None:
        left_rows = SemanticGraphTester._convert_linkable_element_set_to_rows(left_set)
        right_rows = SemanticGraphTester._convert_linkable_element_set_to_rows(right_set)

        comparison_helper = PaddedTextTableBuilder()
        comparison_helper.add_left_rows(left_rows)
        comparison_helper.add_right_rows(right_rows)
        comparison_helper.assert_tables_equal(log_result_table)

    def assert_attribute_set_snapshot_equal_for_simple_metrics(  # noqa: D102
        self,
        simple_metric_names: Sequence[str],
        expectation_description: Optional[str] = None,
    ) -> None:
        sg_resolver = self.sg_resolver
        description_to_set = {
            str(simple_metric_name): sg_resolver.get_common_set(
                metric_references=(MetricReference(simple_metric_name),),
                set_filter=None,
            )
            for simple_metric_name in simple_metric_names
        }
        self.assert_attribute_set_snapshot_equal(description_to_set, expectation_description)

    def assert_attribute_set_snapshot_equal(  # noqa: D102
        self,
        description_to_set: Mapping[str, BaseGroupByItemSet],
        expectation_description: Optional[str] = None,
    ) -> None:
        lines = []
        for description, group_by_item_set in description_to_set.items():
            lines.append(f"{description}:")
            table_builder = PaddedTextTableBuilder()
            table_builder.add_left_rows(SemanticGraphTester._convert_linkable_element_set_to_rows(group_by_item_set))
            lines.append(mf_indent(table_builder.format_left_table()))
            lines.append("")

        assert_str_snapshot_equal(
            request=self._fixture.request,
            snapshot_configuration=self._fixture.snapshot_configuration,
            snapshot_str="\n".join(lines),
            expectation_description=expectation_description,
        )

    def check_set_filtering(
        self,
        complete_set: BaseGroupByItemSet,
        filtered_set_callable: Callable[[GroupByItemSetFilter], BaseGroupByItemSet],
    ) -> None:
        """Given the set containing all items, check that the given callable returns correctly filtered results.

        This calls `BaseGroupByItemSet.filter()` so the callable should have differences in logic for filtered
        set generation.
        """
        for element_property in GroupByItemProperty:
            any_properties_allowlist_filter = GroupByItemSetFilter.create(any_properties_allowlist=(element_property,))
            filtered_set = filtered_set_callable(any_properties_allowlist_filter)
            # The resolver uses the filter to limit graph traversal, so this is not the same logic.
            expected_items = set(complete_set.filter(any_properties_allowlist_filter).annotated_specs)
            actual_items = set(filtered_set.annotated_specs)

            assert expected_items == actual_items

            any_properties_denylist_filter = GroupByItemSetFilter.create(any_properties_denylist=(element_property,))
            filtered_set = filtered_set_callable(any_properties_denylist_filter)
            expected_items = set(complete_set.filter(any_properties_denylist_filter).annotated_specs)
            actual_items = set(filtered_set.annotated_specs)

            assert expected_items == actual_items

    @staticmethod
    def _convert_linkable_element_set_to_rows(
        linkable_element_set: BaseGroupByItemSet,
    ) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []

        for annotated_spec in sorted(
            linkable_element_set.annotated_specs,
            key=lambda annotated_spec_in_lambda: annotated_spec_in_lambda.spec.dunder_name,
        ):
            row_dict: dict[str, str] = {
                "Dunder Name": annotated_spec.spec.dunder_name,
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

    duration_for_legacy_resolver: PrettyDuration
    duration_for_sg_resolver: PrettyDuration

    @staticmethod
    def sum(resolution_times: Iterable[_ResolutionTimePair]) -> _ResolutionTimePair:  # noqa: D102
        return _ResolutionTimePair(
            duration_for_legacy_resolver=PrettyDuration.sum(
                resolution_time.duration_for_legacy_resolver for resolution_time in resolution_times
            ),
            duration_for_sg_resolver=PrettyDuration.sum(
                resolution_time.duration_for_sg_resolver for resolution_time in resolution_times
            ),
        )
