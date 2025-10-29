from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.semantic_graph.builder.entity_join_subgraph import EntityJoinSubgraphGenerator
from metricflow_semantics.semantic_graph.builder.simple_metric_subgraph import (
    SimpleMetricSubgraphGenerator,
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
    """Test generation of the subgraph that describes joins / entity-link using the minimal manifest."""
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_00_minimal_manifest,
        subgraph_generators=[SimpleMetricSubgraphGenerator, EntityJoinSubgraphGenerator],
        expectation_description=mf_dedent(
            """
            The minimal manifest only has a single primary entity and no joins are possible.
            """
        ),
    )


def test_single_join_manifest(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_manifest: SemanticManifest,
) -> None:
    """Test generation of the subgraph that describes joins / entity-link using the single-join manifest."""
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_02_single_join_manifest,
        subgraph_generators=[SimpleMetricSubgraphGenerator, EntityJoinSubgraphGenerator],
        expectation_description=mf_dedent(
            """
            The single-join manifest has one model with the `listing` foreign entity and another model with the
            corresponding primary entity.
            """
        ),
    )
