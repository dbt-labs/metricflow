from __future__ import annotations

from typing import Dict, Optional, Sequence

import pytest
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.references import MetricReference

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow.query.group_by_item.resolution_dag.dag import GroupByItemResolutionDag
from metricflow.query.group_by_item.resolution_dag.dag_builder import GroupByItemResolutionDagBuilder
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.query.group_by_item.ambiguous_resolution_query_id import AmbiguousResolutionQueryId


def _build_resolution_dag(
    manifest_lookup: SemanticManifestLookup,
    queried_metrics: Sequence[MetricReference],
    where_filter_intersection: Optional[WhereFilterIntersection] = None,
) -> GroupByItemResolutionDag:
    resolution_dag_builder = GroupByItemResolutionDagBuilder(
        manifest_lookup=manifest_lookup,
    )

    return resolution_dag_builder.build(
        metric_references=queried_metrics,
        where_filter_intersection=where_filter_intersection,
    )


@pytest.fixture(scope="session")
def resolution_dags(
    mf_test_session_state: MetricFlowTestSessionState,
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
) -> Dict[AmbiguousResolutionQueryId, GroupByItemResolutionDag]:
    """Return a dict that maps the ID to the resolution DAG for use in test cases."""
    result = {}
    resolution_dag_builder = GroupByItemResolutionDagBuilder(
        manifest_lookup=ambiguous_resolution_manifest_lookup,
    )

    where_filter_intersection = PydanticWhereFilterIntersection(
        where_filters=[
            PydanticWhereFilter(
                where_sql_template="{{ TimeDimension('" + METRIC_TIME_ELEMENT_NAME + "') }} > '2020-01-01'"
            ),
        ]
    )

    result[AmbiguousResolutionQueryId.NO_METRICS] = resolution_dag_builder.build(
        metric_references=(),
        where_filter_intersection=None,
    )

    result[AmbiguousResolutionQueryId.SIMPLE_METRIC] = resolution_dag_builder.build(
        metric_references=(MetricReference("monthly_metric_0"),),
        where_filter_intersection=where_filter_intersection,
    )

    result[AmbiguousResolutionQueryId.METRICS_WITH_SAME_TIME_GRAINS] = resolution_dag_builder.build(
        metric_references=(
            MetricReference("monthly_metric_0"),
            MetricReference("monthly_metric_1"),
        ),
        where_filter_intersection=where_filter_intersection,
    )

    result[AmbiguousResolutionQueryId.METRICS_WITH_DIFFERENT_TIME_GRAINS] = resolution_dag_builder.build(
        metric_references=(
            MetricReference("monthly_metric_0"),
            MetricReference("yearly_metric_0"),
        ),
        where_filter_intersection=where_filter_intersection,
    )

    result[AmbiguousResolutionQueryId.DERIVED_METRIC_WITH_SAME_PARENT_TIME_GRAINS] = resolution_dag_builder.build(
        metric_references=(MetricReference("derived_metric_with_same_parent_time_grains"),),
        where_filter_intersection=where_filter_intersection,
    )

    result[AmbiguousResolutionQueryId.DERIVED_METRIC_WITH_DIFFERENT_PARENT_TIME_GRAINS] = resolution_dag_builder.build(
        metric_references=(MetricReference("derived_metric_with_different_parent_time_grains"),),
        where_filter_intersection=where_filter_intersection,
    )

    result[AmbiguousResolutionQueryId.CUMULATIVE_METRIC] = resolution_dag_builder.build(
        metric_references=(MetricReference("accumulate_last_2_months_metric"),),
        where_filter_intersection=where_filter_intersection,
    )

    return result


@pytest.fixture(scope="session")
def naming_scheme() -> QueryItemNamingScheme:  # noqa: D
    return ObjectBuilderNamingScheme()
