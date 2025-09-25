from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseGroupByItemSet
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
    sg_tester.assert_attribute_set_snapshot_equal_for_a_measure(cases)


def test_set_filtering_for_measure(sg_tester: SemanticGraphTester) -> None:
    """Check filtering of the set for a measure."""
    measure_reference = MeasureReference("bookings")
    sg_resolver = sg_tester.sg_resolver
    complete_set = sg_resolver.get_common_set(measure_references=(measure_reference,))
    sg_tester.check_set_filtering(
        complete_set=complete_set,
        filtered_set_callable=lambda set_filter: sg_resolver.get_common_set(
            measure_references=(measure_reference,), set_filter=set_filter
        ),
    )


def test_set_for_metrics(sg_tester: SemanticGraphTester) -> None:
    """Check the set for a few different types of inputs for metrics."""
    sg_resolver = sg_tester.sg_resolver
    description_to_set: dict[str, BaseGroupByItemSet] = {}

    # Include cases: no metrics, simple metric, derived metric, multiple metrics, cumulative metric.
    for metric_names in (
        (),
        ("bookings",),
        ("bookings_per_view",),
        ("bookings", "views"),
        ("trailing_2_months_revenue",),
    ):
        metric_references = [MetricReference(metric_name) for metric_name in metric_names]
        complete_set = sg_resolver.get_linkable_elements_for_metrics(metric_references)
        description_to_set[str(metric_names)] = complete_set
        sg_tester.check_set_filtering(
            complete_set=complete_set,
            filtered_set_callable=lambda set_filter: sg_resolver.get_linkable_elements_for_metrics(
                metric_references, set_filter
            ),
        )

    sg_tester.assert_attribute_set_snapshot_equal(description_to_set)


def test_set_for_distinct_values_query(sg_tester: SemanticGraphTester) -> None:
    """Check the attribute set for a distinct-values query / no-metric query."""
    sg_tester.assert_attribute_set_snapshot_equal(
        {
            "Distinct-Values Query": sg_tester.sg_resolver.get_linkable_elements_for_distinct_values_query(
                GroupByItemSetFilter()
            )
        }
    )


def test_set_filtering_for_distinct_values_query(sg_tester: SemanticGraphTester) -> None:
    """Check filtering of the set for a distinct values query."""
    complete_set = sg_tester.sg_resolver.get_linkable_elements_for_distinct_values_query(GroupByItemSetFilter())
    sg_tester.check_set_filtering(
        complete_set=complete_set,
        filtered_set_callable=lambda set_filter: sg_tester.sg_resolver.get_linkable_elements_for_distinct_values_query(
            set_filter
        ),
    )
