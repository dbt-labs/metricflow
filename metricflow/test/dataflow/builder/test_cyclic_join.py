from __future__ import annotations

import logging
from typing import Mapping

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.references import EntityReference

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.specs.specs import (
    DimensionSpec,
    MetricFlowQuerySpec,
    MetricSpec,
)
from metricflow.test.dataflow_plan_to_svg import display_graph_if_requested
from metricflow.test.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.fixtures.sql_client_fixtures import sql_client  # noqa: F401, F403
from metricflow.test.snapshot_utils import assert_plan_snapshot_text_equal

logger = logging.getLogger(__name__)


@pytest.fixture
def cyclic_join_manifest_dataflow_plan_builder(  # noqa: D
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> DataflowPlanBuilder:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.CYCLIC_JOIN_MANIFEST].dataflow_plan_builder


def test_cyclic_join(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
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
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.text_structure(),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )
