from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.semantic_graph.builder.entity_join_subgraph import EntityJoinSubgraphGenerator
from metricflow_semantics.semantic_graph.builder.entity_key_subgraph import EntityKeySubgraphGenerator
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.string_helpers import mf_dedent

from tests_metricflow_semantics.semantic_graph.builder.subgraph_test_helpers import (
    check_graph_build,
)

logger = logging.getLogger(__name__)


def test_minimal_manifest(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest: SemanticManifest,
) -> None:
    """Test generation of the entity-key subgraph using the minimal manifest.

    The `EntityJoinSubgraphGenerator` is included in the graphs to show how the keys relate to the configured entities.
    """
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_00_minimal_manifest,
        subgraph_generators=[
            EntityJoinSubgraphGenerator,
            EntityKeySubgraphGenerator,
        ],
        expectation_description=mf_dedent(
            """
            The graph should show an edge to the primary entity.
            """
        ),
    )
