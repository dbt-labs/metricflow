from __future__ import annotations

from typing import Mapping

from metricflow_semantics.instances import InstanceSet
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.specs.measure_spec import MeasureSpec, MetricInputMeasureSpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAggregateFunctionExpression,
    SqlFunction,
    SqlPercentileExpression,
)

from metricflow.plan_conversion.instance_set_transforms.aggregated_measure import (
    CreateAggregatedMeasuresTransform,
)
from metricflow.plan_conversion.instance_set_transforms.instance_converters import (
    FilterElements,
)
from metricflow.plan_conversion.select_column_gen import SelectColumnSet
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup

__SOURCE_TABLE_ALIAS = "a"


def __get_filtered_measure_instance_set(
    semantic_model_name: str,
    measure_name: str,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> InstanceSet:
    """Gets an InstanceSet consisting of only the measure instance matching the given name and semantic model."""
    dataset = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].data_set_mapping[
        semantic_model_name
    ]
    instance_set = dataset.instance_set
    include_specs = tuple(
        instance.spec for instance in instance_set.measure_instances if instance.spec.element_name == measure_name
    )
    return FilterElements(include_specs=InstanceSpecSet(measure_specs=include_specs)).transform(instance_set)


def test_sum_aggregation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Checks for function expression handling for booking_value, a SUM type metric in the simple model."""
    measure_name = "booking_value"
    instance_set = __get_filtered_measure_instance_set("bookings_source", measure_name, mf_engine_test_fixture_mapping)

    select_column_set: SelectColumnSet = (
        CreateAggregatedMeasuresTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            simple_semantic_manifest_lookup.semantic_model_lookup,
            (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="booking_value")),),
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlAggregateFunctionExpression)
    assert expr.sql_function == SqlFunction.SUM


def test_sum_boolean_aggregation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Checks for function expression handling for instant_bookings, a SUM_BOOLEAN type metric in the simple model."""
    measure_name = "instant_bookings"
    instance_set = __get_filtered_measure_instance_set("bookings_source", measure_name, mf_engine_test_fixture_mapping)

    select_column_set: SelectColumnSet = (
        CreateAggregatedMeasuresTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            simple_semantic_manifest_lookup.semantic_model_lookup,
            (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="instant_bookings")),),
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlAggregateFunctionExpression)
    # The SUM_BOOLEAN aggregation type is transformed to SUM at model parsing time
    assert expr.sql_function == SqlFunction.SUM


def test_avg_aggregation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Checks for function expression handling for average_booking_value, an AVG type metric in the simple model."""
    measure_name = "average_booking_value"
    instance_set = __get_filtered_measure_instance_set("bookings_source", measure_name, mf_engine_test_fixture_mapping)

    select_column_set: SelectColumnSet = (
        CreateAggregatedMeasuresTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            simple_semantic_manifest_lookup.semantic_model_lookup,
            (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="average_booking_value")),),
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlAggregateFunctionExpression)
    assert expr.sql_function == SqlFunction.AVERAGE


def test_count_distinct_aggregation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Checks for function expression handling for bookers, a COUNT_DISTINCT type metric in the simple model."""
    measure_name = "bookers"
    instance_set = __get_filtered_measure_instance_set("bookings_source", measure_name, mf_engine_test_fixture_mapping)

    select_column_set: SelectColumnSet = (
        CreateAggregatedMeasuresTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            simple_semantic_manifest_lookup.semantic_model_lookup,
            (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="bookers")),),
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlAggregateFunctionExpression)
    assert expr.sql_function == SqlFunction.COUNT_DISTINCT


def test_max_aggregation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Checks for function expression handling for largest_listing, a MAX type metric in the simple model."""
    measure_name = "largest_listing"
    instance_set = __get_filtered_measure_instance_set("listings_latest", measure_name, mf_engine_test_fixture_mapping)

    select_column_set: SelectColumnSet = (
        CreateAggregatedMeasuresTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            simple_semantic_manifest_lookup.semantic_model_lookup,
            (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="largest_listing")),),
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlAggregateFunctionExpression)
    assert expr.sql_function == SqlFunction.MAX


def test_min_aggregation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Checks for function expression handling for smallest_listing, a MIN type metric in the simple model."""
    measure_name = "smallest_listing"
    instance_set = __get_filtered_measure_instance_set("listings_latest", measure_name, mf_engine_test_fixture_mapping)

    select_column_set: SelectColumnSet = (
        CreateAggregatedMeasuresTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            simple_semantic_manifest_lookup.semantic_model_lookup,
            (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="smallest_listing")),),
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlAggregateFunctionExpression)
    assert expr.sql_function == SqlFunction.MIN


def test_aliased_sum(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Checks for function expression handling for booking_value, a SUM type metric in the simple model, with an alias."""
    measure_name = "booking_value"
    instance_set = __get_filtered_measure_instance_set("bookings_source", measure_name, mf_engine_test_fixture_mapping)

    select_column_set: SelectColumnSet = (
        CreateAggregatedMeasuresTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            simple_semantic_manifest_lookup.semantic_model_lookup,
            (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="booking_value"), alias="bvalue"),),
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlAggregateFunctionExpression)
    assert expr.sql_function == SqlFunction.SUM
    assert measure_column.column_alias == "bvalue"


def test_percentile_aggregation(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Checks for function expression handling for booking_value, a percentile type metric in the simple model."""
    measure_name = "booking_value_p99"
    instance_set = __get_filtered_measure_instance_set("bookings_source", measure_name, mf_engine_test_fixture_mapping)

    select_column_set: SelectColumnSet = (
        CreateAggregatedMeasuresTransform(
            __SOURCE_TABLE_ALIAS,
            DunderColumnAssociationResolver(),
            simple_semantic_manifest_lookup.semantic_model_lookup,
            (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="booking_value_p99")),),
        )
        .transform(instance_set=instance_set)
        .select_column_set
    )

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlPercentileExpression)
    assert expr.percentile_args.percentile == 0.99
