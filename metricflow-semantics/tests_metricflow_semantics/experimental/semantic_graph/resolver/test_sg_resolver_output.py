from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.semantic_graph.sg_fixtures import SemanticGraphTestFixture
from tests_metricflow_semantics.experimental.semantic_graph.sg_tester import SemanticGraphTester

logger = logging.getLogger(__name__)


@pytest.fixture
def sg_tester(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_semantic_manifest: SemanticManifest,
) -> SemanticGraphTester:
    fixture = SemanticGraphTestFixture(
        request=request, snapshot_configuration=mf_test_configuration, semantic_manifest=simple_semantic_manifest
    )
    return SemanticGraphTester(fixture)


def test_set_for_measures(sg_tester: SemanticGraphTester) -> None:
    """Check the set for a few measures."""
    cases = ("bookings", "account_balance")
    description_to_set = {
        str(measure_name): sg_tester.sg_resolver.get_linkable_element_set_for_measure(MeasureReference(measure_name))
        for measure_name in cases
    }
    sg_tester.assert_attribute_set_snapshot_equal(description_to_set)


def test_set_filtering_for_measure(sg_tester: SemanticGraphTester) -> None:
    """Check filtering of the set for a measure."""
    measure_reference = MeasureReference("bookings")
    complete_set = sg_tester.sg_resolver.get_linkable_element_set_for_measure(measure_reference)
    for element_property in LinkableElementProperty:
        with_any_of_filter = LinkableElementFilter(with_any_of=frozenset((element_property,)))
        filtered_set = sg_tester.sg_resolver.get_linkable_element_set_for_measure(measure_reference, with_any_of_filter)
        # The resolver uses the filter to limit graph traversal, so this is not the same logic.
        expected_items = set(complete_set.filter(with_any_of_filter).annotated_specs)
        actual_items = set(filtered_set.annotated_specs)

        assert expected_items == actual_items

        without_any_of_filter = LinkableElementFilter(without_any_of=frozenset((element_property,)))
        filtered_set = sg_tester.sg_resolver.get_linkable_element_set_for_measure(
            measure_reference, without_any_of_filter
        )
        expected_items = set(complete_set.filter(without_any_of_filter).annotated_specs)
        actual_items = set(filtered_set.annotated_specs)

        assert expected_items == actual_items


def test_set_for_metrics(sg_tester: SemanticGraphTester) -> None:
    """Check the set for a few multi-metric cases."""
    cases = ((), ("bookings",), ("bookings_per_view",))
    description_to_set = {
        str(metric_names): sg_tester.sg_resolver.get_linkable_elements_for_metrics(
            [MetricReference(metric_name) for metric_name in metric_names]
        )
        for metric_names in cases
    }
    sg_tester.assert_attribute_set_snapshot_equal(description_to_set)


def test_set_for_distinct_values_query(sg_tester: SemanticGraphTester) -> None:
    """Check the attribute set for a distinct-values query / no-metric query."""
    sg_tester.assert_attribute_set_snapshot_equal(
        {
            "Distinct-Values Query": sg_tester.sg_resolver.get_linkable_elements_for_distinct_values_query(
                LinkableElementFilter()
            )
        }
    )


def test_set_filtering_for_distinct_values_query(sg_tester: SemanticGraphTester) -> None:
    """Check filtering of the set for a distinct values query."""
    complete_set = sg_tester.sg_resolver.get_linkable_elements_for_distinct_values_query(LinkableElementFilter())
    for element_property in LinkableElementProperty:
        with_any_of_filter = LinkableElementFilter(with_any_of=frozenset((element_property,)))
        filtered_set = sg_tester.sg_resolver.get_linkable_elements_for_distinct_values_query(with_any_of_filter)
        # The resolver uses the filter to limit graph traversal, so this is not the same logic.
        expected_items = set(complete_set.filter(with_any_of_filter).annotated_specs)
        actual_items = set(filtered_set.annotated_specs)

        assert expected_items == actual_items

        without_any_of_filter = LinkableElementFilter(without_any_of=frozenset((element_property,)))
        filtered_set = sg_tester.sg_resolver.get_linkable_elements_for_distinct_values_query(without_any_of_filter)
        expected_items = set(complete_set.filter(without_any_of_filter).annotated_specs)
        actual_items = set(filtered_set.annotated_specs)

        assert expected_items == actual_items
