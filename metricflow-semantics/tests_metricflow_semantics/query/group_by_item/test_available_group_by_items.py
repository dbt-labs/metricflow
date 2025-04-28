from __future__ import annotations

import logging
from typing import Dict

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.group_by_item.group_by_item_resolver import GroupByItemResolver
from metricflow_semantics.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_linkable_spec_set_snapshot_equal

from tests_metricflow_semantics.query.group_by_item.conftest import AmbiguousResolutionQueryId

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("dag_case_id", [case_id.value for case_id in AmbiguousResolutionQueryId])
def test_available_group_by_items(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
    resolution_dags: Dict[AmbiguousResolutionQueryId, GroupByItemResolutionDag],
    dag_case_id: str,
) -> None:
    resolution_dag = resolution_dags[AmbiguousResolutionQueryId(dag_case_id)]
    group_by_item_resolver = GroupByItemResolver(
        manifest_lookup=ambiguous_resolution_manifest_lookup,
        resolution_dag=resolution_dag,
    )

    result = group_by_item_resolver.resolve_available_items()
    assert_linkable_spec_set_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        set_id="set0",
        spec_set=LinkableSpecSet.create_from_specs(result.specs),
    )
