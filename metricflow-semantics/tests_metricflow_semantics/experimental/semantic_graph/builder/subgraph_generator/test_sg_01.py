from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.experimental.semantic_graph.builder.categorical_dimension_attribute_subgraph import (
    CategoricalDimensionAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.entity_attribute_subgraph import (
    EntityAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.entity_join_subgraph import EntityJoinSubgraphGenerator
from metricflow_semantics.experimental.semantic_graph.builder.graph_builder import SemanticGraphBuilder
from metricflow_semantics.experimental.semantic_graph.builder.graph_change_rule import SubgraphGeneratorArgumentSet
from metricflow_semantics.experimental.semantic_graph.builder.measure_attribute_subgraph import (
    MeasureAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.time_dimension_subgraph import (
    TimeDimensionSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.time_subraph import TimeSubgraphGenerator
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.graph_helpers import assert_graph_snapshot_equal

logger = logging.getLogger(__name__)


def test_time_dimension_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_01_primary_entity_defined_lookup: ManifestObjectLookup,
) -> None:
    subgraph_generator = TimeDimensionSubgraphGenerator(
        SubgraphGeneratorArgumentSet(manifest_object_lookup=sg_01_primary_entity_defined_lookup)
    )
    subgraph = subgraph_generator.generate_subgraph()
    assert_graph_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, graph=subgraph)


def test_measure_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_01_primary_entity_defined_lookup: ManifestObjectLookup,
) -> None:
    subgraph_generator = MeasureAttributeSubgraphGenerator(
        SubgraphGeneratorArgumentSet(manifest_object_lookup=sg_01_primary_entity_defined_lookup)
    )
    subgraph = subgraph_generator.generate_subgraph()
    assert_graph_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, graph=subgraph)


def test_categorical_dimension_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_01_primary_entity_defined_lookup: ManifestObjectLookup,
) -> None:
    subgraph_generator = CategoricalDimensionAttributeSubgraphGenerator(
        SubgraphGeneratorArgumentSet(manifest_object_lookup=sg_01_primary_entity_defined_lookup)
    )
    subgraph = subgraph_generator.generate_subgraph()
    assert_graph_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, graph=subgraph)


def test_entity_attribute_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_01_primary_entity_defined_lookup: ManifestObjectLookup,
) -> None:
    subgraph_generator = EntityAttributeSubgraphGenerator(
        SubgraphGeneratorArgumentSet(manifest_object_lookup=sg_01_primary_entity_defined_lookup)
    )
    subgraph = subgraph_generator.generate_subgraph()
    assert_graph_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, graph=subgraph)


def test_entity_join_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_01_primary_entity_defined_lookup: ManifestObjectLookup,
) -> None:
    subgraph_generator = EntityJoinSubgraphGenerator(
        SubgraphGeneratorArgumentSet(manifest_object_lookup=sg_01_primary_entity_defined_lookup)
    )
    subgraph = subgraph_generator.generate_subgraph()
    assert_graph_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, graph=subgraph)


def test_time_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_01_primary_entity_defined_lookup: ManifestObjectLookup,
) -> None:
    subgraph_generator = TimeSubgraphGenerator(
        SubgraphGeneratorArgumentSet(manifest_object_lookup=sg_01_primary_entity_defined_lookup)
    )
    subgraph = subgraph_generator.generate_subgraph()
    assert_graph_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, graph=subgraph)


def test_all(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_01_primary_entity_defined_lookup: ManifestObjectLookup,
) -> None:
    builder = SemanticGraphBuilder()
    graph = builder.build(
        manifest_object_lookup=sg_01_primary_entity_defined_lookup,
        subgraph_generators=SemanticGraphBuilder.ALL_SUBGRAPH_GENERATORS,
    )
    assert_graph_snapshot_equal(request=request, snapshot_configuration=mf_test_configuration, graph=graph)
