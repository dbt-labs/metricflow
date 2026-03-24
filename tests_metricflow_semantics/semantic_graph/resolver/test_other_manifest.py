from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.semantic_graph.sg_fixtures import SemanticGraphTestFixture
from tests_metricflow_semantics.semantic_graph.sg_tester import SemanticGraphTester

logger = logging.getLogger(__name__)


@pytest.fixture
def sg_tester_cyclic_manifest(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    cyclic_join_manifest: SemanticManifest,
) -> SemanticGraphTester:
    fixture = SemanticGraphTestFixture(
        request=request, snapshot_configuration=mf_test_configuration, semantic_manifest=cyclic_join_manifest
    )
    return SemanticGraphTester(fixture)


def test_simple_metric_with_cyclic_join_path(sg_tester_cyclic_manifest: SemanticGraphTester) -> None:
    """Check that a cyclic join path doesn't cause infinite loop / recursion."""
    cases = ("listings",)
    sg_tester_cyclic_manifest.assert_attribute_set_snapshot_equal_for_simple_metrics(cases)
