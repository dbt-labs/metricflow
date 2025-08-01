from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.semantic_graph.builder.subgraph_test_helpers import (
    check_graph_build,
)

logger = logging.getLogger(__name__)


def test_all(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    simple_semantic_manifest: PydanticSemanticManifest,
    sg_00_minimal_manifest: PydanticSemanticManifest,
    sg_02_single_join_manifest: PydanticSemanticManifest,
    sg_03_multi_hop_join_manifest: PydanticSemanticManifest,
    sg_04_common_unique_entity_manifest: PydanticSemanticManifest,
) -> None:
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_03_multi_hop_join_manifest,
        subgraph_generators=SemanticGraphBuilder._ALL_SUBGRAPH_GENERATORS,
        generate_svg=True,
    )
