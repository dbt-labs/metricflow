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


@pytest.fixture(scope="module")
def sg_tester(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_semantic_manifest: SemanticManifest,
) -> SemanticGraphTester:
    fixture = SemanticGraphTestFixture(
        request=request, snapshot_configuration=mf_test_configuration, semantic_manifest=simple_semantic_manifest
    )
    return SemanticGraphTester(fixture)


def test_measure_correctness(sg_tester: SemanticGraphTester) -> None:  # noqa: D103
    sg_tester.compare_resolver_outputs_for_one_measure(
        MeasureReference("bookings"), element_filter=LinkableElementFilter()
    )


def test_all_measure_correctness(sg_tester: SemanticGraphTester) -> None:  # noqa: D103
    sg_tester.compare_resolver_outputs_for_all_measures(element_filter=LinkableElementFilter())


def test_filter_without_any_of(sg_tester: SemanticGraphTester) -> None:  # noqa: D103
    for element_property in LinkableElementProperty:
        sg_tester.compare_resolver_outputs_for_one_measure(
            MeasureReference("bookings"),
            element_filter=LinkableElementFilter().copy(without_any_of=frozenset((element_property,))),
        )


def test_filter_with_any_of(sg_tester: SemanticGraphTester) -> None:  # noqa: D103
    for element_property in LinkableElementProperty:
        sg_tester.compare_resolver_outputs_for_one_measure(
            MeasureReference("bookings"),
            element_filter=LinkableElementFilter(with_any_of=frozenset((element_property,))),
        )


def test_metric_correctness(sg_tester: SemanticGraphTester) -> None:  # noqa: D103
    sg_tester.compare_resolver_outputs_for_metrics(
        (MetricReference("bookings"),), element_filter=LinkableElementFilter()
    )


def test_multi_metric_correctness(sg_tester: SemanticGraphTester) -> None:  # noqa: D103
    sg_tester.compare_resolver_outputs_for_metrics(
        (MetricReference("bookings"), MetricReference("listings")),
        element_filter=LinkableElementFilter(),
    )


def test_derived_metric_correctness(sg_tester: SemanticGraphTester) -> None:  # noqa: D103
    sg_tester.compare_resolver_outputs_for_metrics(
        (MetricReference("bookings_per_view"),), element_filter=LinkableElementFilter()
    )
