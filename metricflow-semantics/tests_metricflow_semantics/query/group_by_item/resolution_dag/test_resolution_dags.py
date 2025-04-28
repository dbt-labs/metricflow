from __future__ import annotations

import logging
from typing import Dict

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_plan_snapshot_text_equal

from tests_metricflow_semantics.query.group_by_item.ambiguous_resolution_query_id import AmbiguousResolutionQueryId

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("dag_case_id", [case_id.value for case_id in AmbiguousResolutionQueryId])
def test_snapshot(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
    resolution_dags: Dict[AmbiguousResolutionQueryId, GroupByItemResolutionDag],
    dag_case_id: str,
) -> None:
    """Checks that the resolution DAGs have been built correctly via checks against a snapshot."""
    resolution_dag = resolution_dags[AmbiguousResolutionQueryId(dag_case_id)]
    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=resolution_dag,
        plan_snapshot_text=resolution_dag.structure_text(),
    )
