from __future__ import annotations

import logging
from typing import Mapping

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.references import EntityReference
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_plan_snapshot_text_equal

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from tests_metricflow.dataflow_plan_to_svg import display_graph_if_requested
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.fixtures.sql_client_fixtures import sql_client  # noqa: F401, F403

logger = logging.getLogger(__name__)


@pytest.fixture
def cyclic_join_manifest_dataflow_plan_builder(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> DataflowPlanBuilder:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.CYCLIC_JOIN_MANIFEST].dataflow_plan_builder


def test_cyclic_join(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    cyclic_join_manifest_dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests that sources with the same joinable keys don't cause cycle issues."""
    dataflow_plan = cyclic_join_manifest_dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="listings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="capacity_latest",
                    entity_links=(EntityReference(element_name="cyclic_entity"),),
                ),
            ),
        )
    )

    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dag_graph=dataflow_plan,
    )
