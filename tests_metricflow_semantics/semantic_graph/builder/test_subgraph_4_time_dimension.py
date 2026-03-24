from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.semantic_graph.builder.entity_join_subgraph import EntityJoinSubgraphGenerator
from metricflow_semantics.semantic_graph.builder.simple_metric_subgraph import (
    SimpleMetricSubgraphGenerator,
)
from metricflow_semantics.semantic_graph.builder.time_dimension_subgraph import (
    TimeDimensionSubgraphGenerator,
)
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.string_helpers import mf_dedent

from tests_metricflow_semantics.semantic_graph.builder.subgraph_test_helpers import (
    check_graph_build,
)

logger = logging.getLogger(__name__)


def test_minimal_manifest(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest: SemanticManifest,
) -> None:
    """Test generation of the time-dimension subgraph using the minimal manifest."""
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_00_minimal_manifest,
        subgraph_generators=[
            SimpleMetricSubgraphGenerator,
            EntityJoinSubgraphGenerator,
            TimeDimensionSubgraphGenerator,
        ],
        expectation_description=mf_dedent(
            """
            * The graph should show an edge from the joined-model node to the time-dimension node.
            * There should be edges from all time-dimension nodes to the time-entity node.
            """
        ),
    )
