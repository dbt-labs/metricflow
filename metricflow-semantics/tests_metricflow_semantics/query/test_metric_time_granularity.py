from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal


@pytest.fixture
def query_parser(  # noqa: D103
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> MetricFlowQueryParser:
    return MetricFlowQueryParser(semantic_manifest_lookup=simple_semantic_manifest_lookup)


@pytest.fixture
def ambiguous_resolution_query_parser(  # noqa: D103
    ambiguous_resolution_manifest_lookup: SemanticManifestLookup,
) -> MetricFlowQueryParser:
    return MetricFlowQueryParser(semantic_manifest_lookup=ambiguous_resolution_manifest_lookup)


def test_simple_metric_with_explicit_time_granularity(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=["largest_listing"], group_by_names=["metric_time"]
    ).query_spec

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj_id="result_0",
        obj=query_spec,
    )


def test_simple_metric_without_explicit_time_granularity(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    ambiguous_resolution_query_parser: MetricFlowQueryParser,
) -> None:
    """Tests that a metric without default granularity uses the min granualrity for its agg_time_dim."""
    query_spec = ambiguous_resolution_query_parser.parse_and_validate_query(
        metric_names=["monthly_metric_0"],
        group_by_names=["metric_time"],
    ).query_spec

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj_id="result_0",
        obj=query_spec,
    )


def test_derived_metric_with_explicit_time_granularity(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    ambiguous_resolution_query_parser: MetricFlowQueryParser,
) -> None:
    """Tests that a derived metric with default granularity ignores the default granularities set on its input metrics."""
    query_spec = ambiguous_resolution_query_parser.parse_and_validate_query(
        metric_names=["derived_metric_with_time_granularity"],
        group_by_names=["metric_time"],
    ).query_spec

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj_id="result_0",
        obj=query_spec,
    )


def test_derived_metric_without_explicit_time_granularity(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    ambiguous_resolution_query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a derived metric without explicit default granularity.

    Should ignore the default granularities set on its input metrics.
    """
    query_spec = ambiguous_resolution_query_parser.parse_and_validate_query(
        metric_names=["derived_metric_without_time_granularity"],
        group_by_names=["metric_time"],
    ).query_spec

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj_id="result_0",
        obj=query_spec,
    )


def test_non_metric_time_ignores_default_granularity(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
) -> None:
    query_spec = query_parser.parse_and_validate_query(
        metric_names=["largest_listing"], group_by_names=["listing__ds"]
    ).query_spec

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj_id="result_0",
        obj=query_spec,
    )


def test_simple_metric_with_defined_metric_time_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    ambiguous_resolution_query_parser: MetricFlowQueryParser,
) -> None:
    """Tests that a simple metric's metric_time filter defined in its YAML uses metric's default granularity."""
    query_spec = ambiguous_resolution_query_parser.parse_and_validate_query(
        metric_names=["simple_metric_with_default_time_granularity_and_metric_time_filter"]
    ).query_spec

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj_id="result_0",
        obj=query_spec,
    )


def test_derived_metric_with_defined_metric_time_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    ambiguous_resolution_query_parser: MetricFlowQueryParser,
) -> None:
    """Tests that a derived metric's metric_time filter defined in its YAML uses outer metric's default granularity."""
    query_spec = ambiguous_resolution_query_parser.parse_and_validate_query(
        metric_names=["derived_metric_with_time_granularity_and_outer_metric_time_filter"]
    ).query_spec

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj_id="result_0",
        obj=query_spec,
    )


def test_derived_metric_with_defined_metric_time_filter_on_input_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    ambiguous_resolution_query_parser: MetricFlowQueryParser,
) -> None:
    """Tests a derived metric with a metric_time filter on its input metric.

    Should use the outer metric's default granularity.
    Should always use the default granularity for the object where the filter is defined.
    """
    query_spec = ambiguous_resolution_query_parser.parse_and_validate_query(
        metric_names=["derived_metric_with_time_granularity_and_inner_metric_time_filter"]
    ).query_spec

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj_id="result_0",
        obj=query_spec,
    )
