from __future__ import annotations

import logging
from typing import Dict

import pytest
from _pytest.fixtures import FixtureRequest

from metricflow.dag.dag_to_text import dag_as_text
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.query.group_by_item.ambiguous_resolution_query_id import AmbiguousResolutionQueryId
from metricflow.test.snapshot_utils import assert_plan_snapshot_text_equal

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("dag_case_id", [case_id.value for case_id in AmbiguousResolutionQueryId])
def test_snapshot(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
    resolution_dags: Dict[AmbiguousResolutionQueryId, GroupByItemResolutionDag],
    dag_case_id: str,
) -> None:
    """Checks that the resolution DAGs have been built correctly via checks against a snapshot."""
    resolution_dag = resolution_dags[AmbiguousResolutionQueryId(dag_case_id)]
    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=resolution_dag,
        plan_snapshot_text=dag_as_text(resolution_dag),
    )
