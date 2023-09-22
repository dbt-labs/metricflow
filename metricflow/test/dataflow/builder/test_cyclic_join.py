from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.references import EntityReference

from metricflow.dataflow.builder.costing import DefaultCostFunction
from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan_to_text import dataflow_plan_as_text
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.specs.specs import (
    DimensionSpec,
    MetricFlowQuerySpec,
    MetricSpec,
)
from metricflow.test.dataflow_plan_to_svg import display_graph_if_requested
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.fixtures.sql_client_fixtures import sql_client  # noqa: F401, F403
from metricflow.test.snapshot_utils import assert_plan_snapshot_text_equal

logger = logging.getLogger(__name__)


@pytest.fixture
def cyclic_join_manifest_dataflow_plan_builder(  # noqa: D
    cyclic_join_semantic_manifest_lookup: SemanticManifestLookup,
    consistent_id_object_repository: ConsistentIdObjectRepository,
) -> DataflowPlanBuilder:
    for source_node in consistent_id_object_repository.cyclic_join_source_nodes:
        logger.error(f"Source node is: {source_node}")

    return DataflowPlanBuilder(
        source_nodes=consistent_id_object_repository.cyclic_join_source_nodes,
        semantic_manifest_lookup=cyclic_join_semantic_manifest_lookup,
        cost_function=DefaultCostFunction(),
    )


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
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
    )

    display_graph_if_requested(
        request=request,
        mf_test_session_state=mf_test_session_state,
        dag_graph=dataflow_plan,
    )
