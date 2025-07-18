from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.experimental.dsi.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.builder.categorical_dimension_attribute_subgraph import (
    CategoricalDimensionAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.entity_attribute_subgraph import (
    EntityAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.entity_join_subgraph import EntityJoinSubgraphGenerator
from metricflow_semantics.experimental.semantic_graph.builder.measure_attribute_subgraph import (
    MeasureAttributeSubgraphGenerator,
)
from metricflow_semantics.experimental.semantic_graph.builder.time_dimension_subgraph import (
    TimeDimensionSubgraphGenerator,
)
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.semantic_graph.builder.subgraph_generator.conftest import (
    check_subgraph_generation,
)

logger = logging.getLogger(__name__)


def test_measure_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest_lookup: ManifestObjectLookup,
) -> None:
    check_subgraph_generation(
        request=request,
        mf_test_configuration=mf_test_configuration,
        manifest_object_lookup=sg_00_minimal_manifest_lookup,
        subgraph_generators=(MeasureAttributeSubgraphGenerator,),
    )


def test_time_dimension_subgraph(  # noqa: D103
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
            TimeDimensionSubgraphGenerator,
        ),
    )


def test_categorical_dimension_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest_lookup: ManifestObjectLookup,
) -> None:
    check_subgraph_generation(
        request=request,
        mf_test_configuration=mf_test_configuration,
        manifest_object_lookup=sg_00_minimal_manifest_lookup,
        subgraph_generators=(CategoricalDimensionAttributeSubgraphGenerator,),
    )


def test_entity_attribute_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest_lookup: ManifestObjectLookup,
) -> None:
    check_subgraph_generation(
        request=request,
        mf_test_configuration=mf_test_configuration,
        manifest_object_lookup=sg_00_minimal_manifest_lookup,
        subgraph_generators=(EntityAttributeSubgraphGenerator,),
    )


def test_entity_join_subgraph(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest_lookup: ManifestObjectLookup,
) -> None:
    check_subgraph_generation(
        request=request,
        mf_test_configuration=mf_test_configuration,
        manifest_object_lookup=sg_00_minimal_manifest_lookup,
        subgraph_generators=(EntityJoinSubgraphGenerator,),
    )
