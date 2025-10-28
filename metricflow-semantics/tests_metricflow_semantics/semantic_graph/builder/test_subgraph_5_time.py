from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.semantic_graph.builder.time_dimension_subgraph import (
    TimeDimensionSubgraphGenerator,
)
from metricflow_semantics.semantic_graph.builder.time_entity_subgraph import TimeEntitySubgraphGenerator
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
    """Test generation of the time-entity subgraph using the minimal manifest."""
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_00_minimal_manifest,
        subgraph_generators=[
            TimeDimensionSubgraphGenerator,
            TimeEntitySubgraphGenerator,
        ],
        expectation_description=mf_dedent(
            """
            The semantic manifest has a minimum grain of `quarter`, so there should only be attribute nodes applicable
            for that grain.
            """
        ),
    )
