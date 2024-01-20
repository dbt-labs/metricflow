from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import MetricReference

from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.model.modify.modify_manifest import modify_manifest
from metricflow.test.model.modify.modify_metric_filter import ModifyMetricFilterTransform
from metricflow.test.snapshot_utils import assert_str_snapshot_equal

logger = logging.getLogger(__name__)


def test_suggestions_for_group_by_item(  # noqa: D
    request: FixtureRequest, mf_test_session_state: MetricFlowTestSessionState, query_parser: MetricFlowQueryParser
) -> None:
    with pytest.raises(InvalidQueryException) as e:
        query_parser.parse_and_validate_query(metric_names=("bookings",), group_by_names=("booking__instant",))

    assert_str_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        snapshot_id="result_0",
        snapshot_str=str(e.value),
    )


def test_suggestions_for_metric(  # noqa: D
    request: FixtureRequest, mf_test_session_state: MetricFlowTestSessionState, query_parser: MetricFlowQueryParser
) -> None:
    with pytest.raises(InvalidQueryException) as e:
        query_parser.parse_and_validate_query(metric_names=("booking",), group_by_names=(METRIC_TIME_ELEMENT_NAME,))

    assert_str_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        snapshot_id="result_0",
        snapshot_str=str(e.value),
    )


def test_suggestions_for_multiple_metrics(  # noqa: D
    request: FixtureRequest, mf_test_session_state: MetricFlowTestSessionState, query_parser: MetricFlowQueryParser
) -> None:
    with pytest.raises(InvalidQueryException) as e:
        query_parser.parse_and_validate_query(metric_names=("bookings", "listings"), group_by_names=("booking__ds",))

    assert_str_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        snapshot_id="result_0",
        snapshot_str=str(e.value),
    )


def test_suggestions_for_defined_where_filter(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    simple_semantic_manifest: PydanticSemanticManifest,
) -> None:
    modified_manifest = modify_manifest(
        semantic_manifest=simple_semantic_manifest,
        transform_rule=ModifyMetricFilterTransform(
            metric_reference=MetricReference(element_name="listings"),
            where_filter_intersection=PydanticWhereFilterIntersection(
                where_filters=[
                    PydanticWhereFilter(where_sql_template=("{{ TimeDimension('listing__paid_at') }} > '2020-01-01'")),
                ]
            ),
        ),
    )

    semantic_manifest_lookup = SemanticManifestLookup(modified_manifest)

    query_parser = MetricFlowQueryParser(
        semantic_manifest_lookup=semantic_manifest_lookup,
    )
    with pytest.raises(InvalidQueryException) as e:
        query_parser.parse_and_validate_query(metric_names=("listings",), group_by_names=(METRIC_TIME_ELEMENT_NAME,))

    assert_str_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        snapshot_id="result_0",
        snapshot_str=str(e.value),
    )


def test_suggestions_for_defined_filters_in_multi_metric_query(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    simple_semantic_manifest: PydanticSemanticManifest,
) -> None:
    """Tests that the suggestions for invalid items in filters are specific to the metric."""
    where_sql_template = "{{ TimeDimension('booking__paid_at') }} > '2020-01-01'"
    modified_manifest = modify_manifest(
        semantic_manifest=simple_semantic_manifest,
        transform_rule=ModifyMetricFilterTransform(
            metric_reference=MetricReference(element_name="listings"),
            where_filter_intersection=PydanticWhereFilterIntersection(
                where_filters=[
                    PydanticWhereFilter(where_sql_template=where_sql_template),
                ]
            ),
        ),
    )
    modified_manifest = modify_manifest(
        semantic_manifest=modified_manifest,
        transform_rule=ModifyMetricFilterTransform(
            metric_reference=MetricReference(element_name="bookings"),
            where_filter_intersection=PydanticWhereFilterIntersection(
                where_filters=[
                    PydanticWhereFilter(where_sql_template=where_sql_template),
                ]
            ),
        ),
    )

    semantic_manifest_lookup = SemanticManifestLookup(modified_manifest)
    DunderColumnAssociationResolver(modified_manifest)

    query_parser = MetricFlowQueryParser(
        semantic_manifest_lookup=semantic_manifest_lookup,
    )
    with pytest.raises(InvalidQueryException) as e:
        query_parser.parse_and_validate_query(
            metric_names=(
                "bookings",
                "listings",
            ),
            group_by_names=(METRIC_TIME_ELEMENT_NAME,),
        )

    assert_str_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        snapshot_id="result_0",
        snapshot_str=str(e.value),
    )
