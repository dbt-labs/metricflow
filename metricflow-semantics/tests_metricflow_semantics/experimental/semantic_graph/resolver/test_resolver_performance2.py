from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from metricflow_semantics.helpers.performance_helpers import ExecutionTimer
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.semantic_graph.sg_fixtures import SemanticGraphTestFixture
from tests_metricflow_semantics.experimental.semantic_graph.test_helpers import (
    SemanticGraphTester2,
)

logger = logging.getLogger(__name__)


# def test_init_simple_manifest(simple_semantic_manifest: SemanticManifest) -> None:
#
#     tester = SemanticGraphTester.create_from_manifest(simple_semantic_manifest)
#     tester.assert_initialization_performance_factor(95)
#
#
def test_init_medium_complexity_manifest(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    manifest_with_50_models_25_metrics: SemanticManifest,
) -> None:
    tester = SemanticGraphTester2(
        SemanticGraphTestFixture(
            request=request,
            snapshot_configuration=mf_test_configuration,
            semantic_manifest=manifest_with_50_models_25_metrics,
        )
    )

    tester.assert_initialization_performance_factor(0)


#
#
# def test_init_high_complexity_manifest(manifest_with_200_models_100_metrics: SemanticManifest) -> None:
#
#     tester = SemanticGraphTester.create_from_manifest(manifest_with_200_models_100_metrics)
#     tester.assert_initialization_performance_factor(0)


def test_time_init(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_semantic_manifest: SemanticManifest,
) -> None:
    fixture = SemanticGraphTestFixture(
        request=request,
        snapshot_configuration=mf_test_configuration,
        semantic_manifest=simple_semantic_manifest,
    )

    # tester = SemanticGraphTester(fixture)

    # with ExecutionTimer("Initialize SG Resolver"):
    #     fixture.create_sg_resolver()

    with ExecutionTimer("Initialize Legacy Resolver"):
        fixture.create_legacy_resolver()

    # logger.info(LazyFormat("Finished SG resolver init", time=f"{timer.total_time:.2f})"))


def test_profile_init(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    manifest_with_200_models_100_metrics: SemanticManifest,
) -> None:
    tester = SemanticGraphTester2(
        SemanticGraphTestFixture(
            request=request,
            snapshot_configuration=mf_test_configuration,
            semantic_manifest=manifest_with_200_models_100_metrics,
        )
    )
    tester.profile_sg_init()


def test_time_resolution_for_measures(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    manifest_with_200_models_100_metrics: SemanticManifest,
) -> None:
    tester = SemanticGraphTester2(
        SemanticGraphTestFixture(
            request=request,
            snapshot_configuration=mf_test_configuration,
            semantic_manifest=manifest_with_200_models_100_metrics,
        )
    )

    tester.time_resolver_output_for_measures(
        [MeasureReference("measure_000"), MeasureReference("measure_100")]
        # [MeasureReference("measure_000")]
    )


def test_time_resolution_for_metrics(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    manifest_with_200_models_100_metrics: SemanticManifest,
) -> None:
    fixture = SemanticGraphTestFixture(
        request=request,
        snapshot_configuration=mf_test_configuration,
        semantic_manifest=manifest_with_200_models_100_metrics,
    )
    tester = SemanticGraphTester2(fixture)

    metric_references = [MetricReference(metric.name) for metric in fixture.manifest_object_lookup.get_metrics()]
    tester.time_resolver_output_for_metrics(metric_references[-10:])


def test_time_complete_resolution(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    manifest_with_200_models_100_metrics: SemanticManifest,
) -> None:
    tester = SemanticGraphTester2(
        SemanticGraphTestFixture(
            request=request,
            snapshot_configuration=mf_test_configuration,
            semantic_manifest=manifest_with_200_models_100_metrics,
        )
    )

    measure_count = 20
    with ExecutionTimer(f"Resolve group-by items for {measure_count} measures"):
        tester.time_resolver_output_for_all_measures(limit=measure_count)


def test_profile_resolution(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    manifest_with_200_models_100_metrics: SemanticManifest,
) -> None:
    tester = SemanticGraphTester2(
        SemanticGraphTestFixture(
            request=request,
            snapshot_configuration=mf_test_configuration,
            semantic_manifest=manifest_with_200_models_100_metrics,
        )
    )
    tester.profile_sg_resolution(MeasureReference("measure_000"))
