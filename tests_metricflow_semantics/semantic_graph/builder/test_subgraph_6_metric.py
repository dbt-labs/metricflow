from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.semantic_graph.builder.metric_subgraph import ComplexMetricSubgraphGenerator
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.string_helpers import mf_dedent

from tests_metricflow_semantics.semantic_graph.builder.subgraph_test_helpers import (
    check_graph_build,
)

logger = logging.getLogger(__name__)


def test_derived_metric_manifest(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_05_derived_metric_manifest: SemanticManifest,
) -> None:
    """Test generation of the metric subgraph using the derived-metric manifest."""
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_05_derived_metric_manifest,
        subgraph_generators=[ComplexMetricSubgraphGenerator],
        expectation_description=mf_dedent(
            """
            The graph should show a complex-metric node that has edges to simple-metric nodes.
            """
        ),
    )
