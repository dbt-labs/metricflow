from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.experimental.semantic_graph.builder.measure_subgraph import MeasureSubgraphGenerator
from metricflow_semantics.helpers.string_helpers import mf_dedent
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.semantic_graph.builder.subgraph_test_helpers import (
    check_graph_build,
)

logger = logging.getLogger(__name__)


def test_minimal_manifest(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest: SemanticManifest,
) -> None:
    """Test generation of the measure subgraph using the minimal manifest."""
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_00_minimal_manifest,
        subgraph_generators=[MeasureSubgraphGenerator],
        expectation_description=mf_dedent(
            """
            The graph should show the measure node with edges to the model nodes and metric time.
            """
        ),
    )
