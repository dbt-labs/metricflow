from __future__ import annotations

from typing import Mapping

import pytest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.instances import InstanceSet
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAggregateFunctionExpression,
    SqlFunction,
    SqlPercentileExpression,
)

from metricflow.plan_conversion.instance_set_transforms.aggregated_simple_metric_input import (
    CreateAggregatedSimpleMetricInputsTransform,
)
from metricflow.plan_conversion.instance_set_transforms.instance_converters import (
    FilterElements,
)
from metricflow.plan_conversion.select_column_gen import SelectColumnSet
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup

__SOURCE_TABLE_ALIAS = "a"


def __get_filtered_simple_metric_input_instance_set(
    semantic_model_name: str,
    simple_metric_input_name: str,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> InstanceSet:
    """Gets an InstanceSet consisting of only the simple-metric input instance matching the given name and semantic model."""
    dataset = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].data_set_mapping[
        semantic_model_name
    ]
    instance_set = dataset.instance_set
    include_specs = tuple(
        instance.spec
        for instance in instance_set.simple_metric_input_instances
        if instance.spec.element_name == simple_metric_input_name
    )
    return FilterElements(include_specs=InstanceSpecSet(simple_metric_input_specs=include_specs)).transform(
        instance_set
    )


def test_sum_aggregation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_manifest_object_lookup: ManifestObjectLookup,
) -> None:
    """Checks for function expression handling for booking_value, a SUM type metric in the simple model."""
    simple_metric_input_name = "booking_value"
    instance_set = __get_filtered_simple_metric_input_instance_set(
        "bookings_source", simple_metric_input_name, mf_engine_test_fixture_mapping
    )

    select_column_set: SelectColumnSet = (
        CreateAggregatedSimpleMetricInputsTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            manifest_object_lookup=simple_manifest_object_lookup,
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.simple_metric_input_columns) == 1
    measure_column = select_column_set.simple_metric_input_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlAggregateFunctionExpression)
    assert expr.sql_function == SqlFunction.SUM


@pytest.fixture
def simple_manifest_object_lookup(  # noqa: D103
    simple_semantic_manifest: PydanticSemanticManifest,
) -> ManifestObjectLookup:
    return ManifestObjectLookup(simple_semantic_manifest)


def test_sum_boolean_aggregation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_manifest_object_lookup: ManifestObjectLookup,
) -> None:
    """Checks for function expression handling for instant_bookings, a SUM_BOOLEAN type metric in the simple model."""
    simple_metric_input_name = "instant_bookings"
    instance_set = __get_filtered_simple_metric_input_instance_set(
        "bookings_source", simple_metric_input_name, mf_engine_test_fixture_mapping
    )

    select_column_set: SelectColumnSet = (
        CreateAggregatedSimpleMetricInputsTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            manifest_object_lookup=simple_manifest_object_lookup,
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.simple_metric_input_columns) == 1
    measure_column = select_column_set.simple_metric_input_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlAggregateFunctionExpression)
    # The SUM_BOOLEAN aggregation type is transformed to SUM at model parsing time
    assert expr.sql_function == SqlFunction.SUM


def test_avg_aggregation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_manifest_object_lookup: ManifestObjectLookup,
) -> None:
    """Checks for function expression handling for average_booking_value, an AVG type metric in the simple model."""
    simple_metric_input_name = "average_booking_value"
    instance_set = __get_filtered_simple_metric_input_instance_set(
        "bookings_source", simple_metric_input_name, mf_engine_test_fixture_mapping
    )

    select_column_set: SelectColumnSet = (
        CreateAggregatedSimpleMetricInputsTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            manifest_object_lookup=simple_manifest_object_lookup,
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.simple_metric_input_columns) == 1
    measure_column = select_column_set.simple_metric_input_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlAggregateFunctionExpression)
    assert expr.sql_function == SqlFunction.AVERAGE


def test_count_distinct_aggregation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_manifest_object_lookup: ManifestObjectLookup,
) -> None:
    """Checks for function expression handling for bookers, a COUNT_DISTINCT type metric in the simple model."""
    simple_metric_input_name = "bookers"
    instance_set = __get_filtered_simple_metric_input_instance_set(
        "bookings_source", simple_metric_input_name, mf_engine_test_fixture_mapping
    )

    select_column_set: SelectColumnSet = (
        CreateAggregatedSimpleMetricInputsTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            manifest_object_lookup=simple_manifest_object_lookup,
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.simple_metric_input_columns) == 1
    measure_column = select_column_set.simple_metric_input_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlAggregateFunctionExpression)
    assert expr.sql_function == SqlFunction.COUNT_DISTINCT


def test_max_aggregation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_manifest_object_lookup: ManifestObjectLookup,
) -> None:
    """Checks for function expression handling for largest_listing, a MAX type metric in the simple model."""
    simple_metric_input_name = "largest_listing"
    instance_set = __get_filtered_simple_metric_input_instance_set(
        "listings_latest", simple_metric_input_name, mf_engine_test_fixture_mapping
    )

    select_column_set: SelectColumnSet = (
        CreateAggregatedSimpleMetricInputsTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            manifest_object_lookup=simple_manifest_object_lookup,
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.simple_metric_input_columns) == 1
    measure_column = select_column_set.simple_metric_input_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlAggregateFunctionExpression)
    assert expr.sql_function == SqlFunction.MAX


def test_min_aggregation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_manifest_object_lookup: ManifestObjectLookup,
) -> None:
    """Checks for function expression handling for smallest_listing, a MIN type metric in the simple model."""
    simple_metric_input_name = "smallest_listing"
    instance_set = __get_filtered_simple_metric_input_instance_set(
        "listings_latest", simple_metric_input_name, mf_engine_test_fixture_mapping
    )

    select_column_set: SelectColumnSet = (
        CreateAggregatedSimpleMetricInputsTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            manifest_object_lookup=simple_manifest_object_lookup,
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.simple_metric_input_columns) == 1
    measure_column = select_column_set.simple_metric_input_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlAggregateFunctionExpression)
    assert expr.sql_function == SqlFunction.MIN


def test_percentile_aggregation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_manifest_object_lookup: ManifestObjectLookup,
) -> None:
    """Checks for function expression handling for booking_value, a percentile type metric in the simple model."""
    simple_metric_input_name = "booking_value_p99"
    instance_set = __get_filtered_simple_metric_input_instance_set(
        "bookings_source", simple_metric_input_name, mf_engine_test_fixture_mapping
    )

    select_column_set: SelectColumnSet = (
        CreateAggregatedSimpleMetricInputsTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            manifest_object_lookup=simple_manifest_object_lookup,
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.simple_metric_input_columns) == 1
    measure_column = select_column_set.simple_metric_input_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlPercentileExpression)
    assert expr.percentile_args.percentile == 0.99
