from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.experimental.semantic_graph.builder.entity_join_subgraph import EntityJoinSubgraphGenerator
from metricflow_semantics.experimental.semantic_graph.builder.group_by_metric_subgraph import GroupByMetricSubgraph
from metricflow_semantics.experimental.semantic_graph.builder.measure_attribute_subgraph import (
    MeasureAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator.conftest import (
    check_subgraph_generation,
)

logger = logging.getLogger(__name__)


def test_metric_attribute_subgraph_generation(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest_lookup: ManifestObjectLookup,
) -> None:
    check_subgraph_generation(
        request=request,
        mf_test_configuration=mf_test_configuration,
        manifest_object_lookup=sg_00_minimal_manifest_lookup,
        subgraph_generators=(
            MeasureAttributeSubgraphGenerator,
            EntityJoinSubgraphGenerator,
            GroupByMetricSubgraph,
        ),
    )
