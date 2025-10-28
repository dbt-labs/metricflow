from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.semantic_graph.builder.categorical_dimension_subgraph import (
    CategoricalDimensionSubgraphGenerator,
)
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.string_helpers import mf_dedent

from tests_metricflow_semantics.semantic_graph.builder.subgraph_test_helpers import (
    check_graph_build,
)

logger = logging.getLogger(__name__)


def test_single_join_manifest(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_manifest: SemanticManifest,
) -> None:
    """Test generation of the categorical-dimension subgraph using the single-join manifest."""
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_02_single_join_manifest,
        subgraph_generators=[
            CategoricalDimensionSubgraphGenerator,
        ],
        expectation_description=mf_dedent(
            """
            The graph should show an edge to the categorical dimension.
            """
        ),
    )
