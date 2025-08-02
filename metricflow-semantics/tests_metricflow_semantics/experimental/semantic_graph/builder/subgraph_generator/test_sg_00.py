from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from metricflow_semantics.experimental.semantic_graph.builder.categorical_dimension_subgraph import (
    CategoricalDimensionSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.entity_join_subgraph import EntityJoinSubgraphGenerator
from metricflow_semantics.experimental.semantic_graph.builder.entity_key_subgraph import (
    EntityKeySubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.measure_subgraph import (
    MeasureSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.time_dimension_subgraph import (
    TimeDimensionSubgraphGenerator,
)
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.semantic_graph.builder.subgraph_test_helpers import (
    check_graph_build,
)

logger = logging.getLogger(__name__)


def test_measure_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest: SemanticManifest,
) -> None:
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_00_minimal_manifest,
        subgraph_generators=(MeasureSubgraphGenerator,),
    )


def test_time_dimension_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest: SemanticManifest,
) -> None:
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_00_minimal_manifest,
        subgraph_generators=(
            MeasureSubgraphGenerator,
            TimeDimensionSubgraphGenerator,
        ),
    )


def test_categorical_dimension_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest: SemanticManifest,
) -> None:
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_00_minimal_manifest,
        subgraph_generators=(CategoricalDimensionSubgraphGenerator,),
    )


def test_entity_attribute_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest: SemanticManifest,
) -> None:
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_00_minimal_manifest,
        subgraph_generators=(EntityKeySubgraphGenerator,),
    )


def test_entity_join_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest: SemanticManifest,
) -> None:
    check_graph_build(
        request=request,
        mf_test_configuration=mf_test_configuration,
        semantic_manifest=sg_00_minimal_manifest,
        subgraph_generators=(EntityJoinSubgraphGenerator,),
    )
