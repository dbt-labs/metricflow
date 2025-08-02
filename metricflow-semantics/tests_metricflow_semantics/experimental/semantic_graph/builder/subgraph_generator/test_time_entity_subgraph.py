from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.experimental.semantic_graph.builder.time_entity_subgraph import TimeEntitySubgraphGenerator
from metricflow_semantics.helpers.string_helpers import mf_dedent
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.semantic_graph.builder.subgraph_test_helpers import (
    check_graph_build,
)

logger = logging.getLogger(__name__)


def test_sg_00_minimal_manifest(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest: SemanticManifest,
) -> None:
    """Check the result of the time-entity subgraph generator."""
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_00_minimal_manifest,
        subgraph_generators=(TimeEntitySubgraphGenerator,),
        expectation_description=mf_dedent(
            "Since the manifest only defines a time spine for `year` and a `custom_year`, those should be the only "
            "attribute nodes present."
        ),
    )
