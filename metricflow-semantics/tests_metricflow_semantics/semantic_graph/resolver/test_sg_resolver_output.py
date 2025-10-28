from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import MetricReference
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantics.element_filter import GroupByItemSetFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseGroupByItemSet
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.semantic_graph.sg_fixtures import SemanticGraphTestFixture
from tests_metricflow_semantics.semantic_graph.sg_tester import SemanticGraphTester

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


def test_complete_set_for_simple_metrics(sg_tester: SemanticGraphTester) -> None:
    """Check the complete set for a few simple metrics."""
    cases = ("bookings", "account_balance")
    sg_tester.assert_attribute_set_snapshot_equal_for_simple_metrics(cases)


def test_set_filtering_for_simple_metric(sg_tester: SemanticGraphTester) -> None:
    """Check filtering of the set for a simple-metric input."""
    metric_reference = MetricReference("bookings")
    metric_references = (metric_reference,)
    sg_resolver = sg_tester.sg_resolver
    complete_set = sg_resolver.get_common_set(metric_references=metric_references)
    sg_tester.check_set_filtering(
        complete_set=complete_set,
        filtered_set_callable=lambda set_filter: sg_resolver.get_common_set(
            metric_references=metric_references, set_filter=set_filter
        ),
    )


def test_set_for_metrics(sg_tester: SemanticGraphTester) -> None:
    """Check the set for a few different types of inputs for metrics."""
    sg_resolver = sg_tester.sg_resolver
    description_to_set: dict[str, BaseGroupByItemSet] = {}

    for case_description, metric_names in (
        ("No metrics", ()),
        ("Simple metric", ("bookings",)),
        ("Derived metric", ("bookings_per_view",)),
        ("Multiple metrics", ("bookings", "views")),
        ("Cumulative metric", ("trailing_2_months_revenue",)),
        ("Derived metric from cumulative metric", ("trailing_2_months_revenue_sub_10",)),
    ):
        # Group-by metrics should not be called for metrics, so skip them for smaller snapshots.
        metric_references = tuple(MetricReference(metric_name) for metric_name in metric_names)
        set_filter = GroupByItemSetFilter.create(any_properties_denylist=(GroupByItemProperty.METRIC,))
        complete_set = sg_resolver.get_common_set(metric_references=metric_references, set_filter=set_filter)
        description_to_set[f"{case_description} {str(list(metric_names))}"] = complete_set
        sg_tester.check_set_filtering(
            complete_set=complete_set,
            filtered_set_callable=lambda _filter: sg_resolver.get_common_set(
                metric_references=metric_references,
                set_filter=_filter.copy(
                    any_properties_denylist=_filter.any_properties_denylist.union((GroupByItemProperty.METRIC,))
                ),
            ),
        )

    sg_tester.assert_attribute_set_snapshot_equal(description_to_set)


def test_set_for_distinct_values_query(sg_tester: SemanticGraphTester) -> None:
    """Check the attribute set for a distinct-values query / no-metric query."""
    sg_tester.assert_attribute_set_snapshot_equal(
        {
            "Distinct-Values Query": sg_tester.sg_resolver.get_set_for_distinct_values_query(
                GroupByItemSetFilter.create()
            )
        }
    )


def test_set_filtering_for_distinct_values_query(sg_tester: SemanticGraphTester) -> None:
    """Check filtering of the set for a distinct values query."""
    complete_set = sg_tester.sg_resolver.get_set_for_distinct_values_query(GroupByItemSetFilter.create())
    sg_tester.check_set_filtering(
        complete_set=complete_set,
        filtered_set_callable=lambda set_filter: sg_tester.sg_resolver.get_set_for_distinct_values_query(set_filter),
    )
