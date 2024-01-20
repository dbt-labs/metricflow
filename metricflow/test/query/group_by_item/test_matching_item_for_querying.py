from __future__ import annotations

import logging
from typing import Dict

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME

from metricflow.mf_logging.pretty_print import mf_pformat
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow.query.group_by_item.group_by_item_resolver import GroupByItemResolver
from metricflow.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.query.group_by_item.conftest import AmbiguousResolutionQueryId
from metricflow.test.snapshot_utils import assert_object_snapshot_equal
from metricflow.test.time.metric_time_dimension import MTD_SPEC_DAY, MTD_SPEC_MONTH, MTD_SPEC_YEAR

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("dag_case_id", [case_id.value for case_id in AmbiguousResolutionQueryId])
def test_ambiguous_metric_time_in_query(  # noqa: D
    mf_test_session_state: MetricFlowTestSessionState,
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
    resolution_dags: Dict[AmbiguousResolutionQueryId, GroupByItemResolutionDag],
    dag_case_id: str,
) -> None:
    case_id = AmbiguousResolutionQueryId(dag_case_id)
    resolution_dag = resolution_dags[case_id]
    group_by_item_resolver = GroupByItemResolver(
        manifest_lookup=ambiguous_resolution_manifest_lookup,
        resolution_dag=resolution_dag,
    )

    spec_pattern = ObjectBuilderNamingScheme().spec_pattern(f"TimeDimension('{METRIC_TIME_ELEMENT_NAME}')")

    result = group_by_item_resolver.resolve_matching_item_for_querying(
        spec_pattern=spec_pattern,
        suggestion_generator=None,
    )

    if case_id is AmbiguousResolutionQueryId.NO_METRICS:
        assert result.spec == MTD_SPEC_DAY
    elif case_id is AmbiguousResolutionQueryId.SIMPLE_METRIC:
        assert result.spec == MTD_SPEC_MONTH
    elif case_id is AmbiguousResolutionQueryId.METRICS_WITH_SAME_TIME_GRAINS:
        assert result.spec == MTD_SPEC_MONTH
    elif case_id is AmbiguousResolutionQueryId.METRICS_WITH_DIFFERENT_TIME_GRAINS:
        assert result.spec == MTD_SPEC_YEAR
    elif case_id is AmbiguousResolutionQueryId.DERIVED_METRIC_WITH_SAME_PARENT_TIME_GRAINS:
        assert result.spec == MTD_SPEC_MONTH
    elif case_id is AmbiguousResolutionQueryId.DERIVED_METRIC_WITH_DIFFERENT_PARENT_TIME_GRAINS:
        assert result.spec == MTD_SPEC_YEAR
    elif case_id is AmbiguousResolutionQueryId.CUMULATIVE_METRIC:
        assert result.spec == MTD_SPEC_MONTH
    else:
        assert_values_exhausted(case_id)


def test_unavailable_group_by_item_in_derived_metric_parent(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    naming_scheme: QueryItemNamingScheme,
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
    resolution_dags: Dict[AmbiguousResolutionQueryId, GroupByItemResolutionDag],
) -> None:
    """Tests a group-by-item that's only available in one of the parent metrics of a derived metric."""
    resolution_dag = resolution_dags[AmbiguousResolutionQueryId.DERIVED_METRIC_WITH_DIFFERENT_PARENT_TIME_GRAINS]
    group_by_item_resolver = GroupByItemResolver(
        manifest_lookup=ambiguous_resolution_manifest_lookup,
        resolution_dag=resolution_dag,
    )
    spec_pattern = naming_scheme.spec_pattern("Dimension('monthly_measure_entity__creation_time')")

    result = group_by_item_resolver.resolve_matching_item_for_querying(
        spec_pattern=spec_pattern,
        suggestion_generator=None,
    )

    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id="result",
        obj=mf_pformat(result),
    )


def test_invalid_group_by_item(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    naming_scheme: QueryItemNamingScheme,
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
    resolution_dags: Dict[AmbiguousResolutionQueryId, GroupByItemResolutionDag],
) -> None:
    resolution_dag = resolution_dags[AmbiguousResolutionQueryId.METRICS_WITH_DIFFERENT_TIME_GRAINS]
    group_by_item_resolver = GroupByItemResolver(
        manifest_lookup=ambiguous_resolution_manifest_lookup,
        resolution_dag=resolution_dag,
    )
    input_str = "Dimension('monthly_measure_entity__invalid_dimension')"

    result = group_by_item_resolver.resolve_matching_item_for_querying(
        spec_pattern=naming_scheme.spec_pattern(input_str),
        suggestion_generator=None,
    )

    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id="result",
        obj=mf_pformat(result),
    )
