from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import MeasureReference
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.semantic_graph.sg_fixtures import SemanticGraphTestFixture
from tests_metricflow_semantics.experimental.semantic_graph.test_helpers import (
    SemanticGraphTester2,
)

logger = logging.getLogger(__name__)


def test_output_correctness(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_semantic_manifest: SemanticManifest,
) -> None:
    tester = SemanticGraphTester2(
        SemanticGraphTestFixture(
            request=request,
            snapshot_configuration=mf_test_configuration,
            semantic_manifest=simple_semantic_manifest,
        )
    )

    tester.compare_resolver_outputs_for_all_measures()
    tester.compare_resolver_outputs_for_all_metrics_individually()


def test_correctness_for_distinct_values(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_semantic_manifest: SemanticManifest,
) -> None:
    tester = SemanticGraphTester2(
        SemanticGraphTestFixture(
            request=request, snapshot_configuration=mf_test_configuration, semantic_manifest=simple_semantic_manifest
        )
    )

    tester.compare_resolver_outputs_for_distinct_values(element_filter=LinkableElementFilter(), log_result_table=False)


def test_measure_correctness(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_semantic_manifest: SemanticManifest,
) -> None:
    tester = SemanticGraphTester2(
        SemanticGraphTestFixture(
            request=request, snapshot_configuration=mf_test_configuration, semantic_manifest=simple_semantic_manifest
        )
    )

    tester.compare_resolver_outputs_for_a_measure(
        MeasureReference("bookings"), element_filter=LinkableElementFilter(), log_result_table=True
    )


# def test_measure_correctness(
#     request: FixtureRequest,
#     mf_test_configuration: MetricFlowTestConfiguration,
#     sg_05_derived_metric_manifest: SemanticManifest,
# ) -> None:
#     tester = SemanticGraphTester(
#         SemanticGraphTestFixture(
#             request=request,
#             snapshot_configuration=mf_test_configuration,
#             semantic_manifest=sg_05_derived_metric_manifest,
#         )
#     )
#
#     tester.compare_resolver_outputs_for_a_measure(
#         MeasureReference("booking_count"), element_filter=LinkableElementFilter(), log_result_table=True
#     )


def test_log_resolver_output(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_semantic_manifest: SemanticManifest,
) -> None:
    tester = SemanticGraphTester2(
        SemanticGraphTestFixture(
            request=request, snapshot_configuration=mf_test_configuration, semantic_manifest=simple_semantic_manifest
        )
    )

    tester.log_sg_resolver_output_for_measure(MeasureReference("bookings"))
