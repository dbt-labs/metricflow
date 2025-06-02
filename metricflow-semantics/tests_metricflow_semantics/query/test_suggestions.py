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
from metricflow_semantics.errors.error_classes import InvalidQueryException
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal

from tests_metricflow_semantics.model.modify.modify_manifest import modify_manifest
from tests_metricflow_semantics.model.modify.modify_metric_filter import ModifyMetricFilterTransform

logger = logging.getLogger(__name__)


@pytest.fixture
def query_parser(simple_semantic_manifest_lookup: SemanticManifestLookup) -> MetricFlowQueryParser:  # noqa: D103
    return MetricFlowQueryParser(simple_semantic_manifest_lookup)


def test_suggestions_for_group_by_item(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, query_parser: MetricFlowQueryParser
) -> None:
    with pytest.raises(InvalidQueryException) as e:
        query_parser.parse_and_validate_query(
            metric_names=("bookings",), group_by_names=("booking__instant",)
        ).query_spec

    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_id="result_0",
        snapshot_str=str(e.value),
    )


def test_suggestions_for_metric(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, query_parser: MetricFlowQueryParser
) -> None:
    with pytest.raises(InvalidQueryException) as e:
        query_parser.parse_and_validate_query(
            metric_names=("booking",), group_by_names=(METRIC_TIME_ELEMENT_NAME,)
        ).query_spec

    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_id="result_0",
        snapshot_str=str(e.value),
    )


def test_suggestions_for_multiple_metrics(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, query_parser: MetricFlowQueryParser
) -> None:
    with pytest.raises(InvalidQueryException) as e:
        query_parser.parse_and_validate_query(
            metric_names=("bookings", "listings"), group_by_names=("booking__ds",)
        ).query_spec

    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_id="result_0",
        snapshot_str=str(e.value),
    )


def test_suggestions_for_defined_where_filter(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
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
        query_parser.parse_and_validate_query(
            metric_names=("listings",), group_by_names=(METRIC_TIME_ELEMENT_NAME,)
        ).query_spec

    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_id="result_0",
        snapshot_str=str(e.value),
    )


def test_suggestions_for_defined_filters_in_multi_metric_query(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
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
        ).query_spec

    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_id="result_0",
        snapshot_str=str(e.value),
    )
