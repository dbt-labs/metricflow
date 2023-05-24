from __future__ import annotations

import datetime

import pytest
from dbt_semantic_interfaces.references import MetricReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.source_node import SourceNodeBuilder
from metricflow.dataset.convert_semantic_model import SemanticModelToDataSetConverter
from metricflow.dataset.dataset import DataSet
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.test.time.metric_time_dimension import MTD_REFERENCE, MTD_SPEC_DAY, MTD_SPEC_MONTH, MTD_SPEC_YEAR
from metricflow.time.time_granularity_solver import (
    PartialTimeDimensionSpec,
    RequestTimeGranularityException,
    TimeGranularitySolver,
)


@pytest.fixture(scope="session")
def time_granularity_solver(  # noqa: D
    extended_date_semantic_manifest_lookup: SemanticManifestLookup,
    time_spine_source: TimeSpineSource,
) -> TimeGranularitySolver:
    column_association_resolver = DunderColumnAssociationResolver(extended_date_semantic_manifest_lookup)
    node_output_resolver = DataflowPlanNodeOutputDataSetResolver[SemanticModelDataSet](
        column_association_resolver=column_association_resolver,
        semantic_manifest_lookup=extended_date_semantic_manifest_lookup,
        time_spine_source=time_spine_source,
    )
    to_data_set_converter = SemanticModelToDataSetConverter(column_association_resolver)
    source_data_sets = [
        to_data_set_converter.create_sql_source_data_set(x)
        for x in extended_date_semantic_manifest_lookup.semantic_manifest.semantic_models
    ]

    source_node_builder = SourceNodeBuilder(extended_date_semantic_manifest_lookup)
    source_nodes = source_node_builder.create_from_data_sets(source_data_sets)
    return TimeGranularitySolver(
        semantic_manifest_lookup=extended_date_semantic_manifest_lookup,
        source_nodes=source_nodes,
        node_output_resolver=node_output_resolver,
    )


def test_validate_day_granuarity_for_day_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    time_granularity_solver.validate_time_granularity(
        metric_references=[MetricReference(element_name="bookings")],
        time_dimension_specs=[DataSet.metric_time_dimension_spec(TimeGranularity.DAY)],
    )


def test_validate_month_granuarity_for_day_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    time_granularity_solver.validate_time_granularity(
        metric_references=[MetricReference(element_name="bookings")],
        time_dimension_specs=[DataSet.metric_time_dimension_spec(TimeGranularity.MONTH)],
    )


def test_validate_month_granuarity_for_month_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    time_granularity_solver.validate_time_granularity(
        metric_references=[MetricReference(element_name="bookings_monthly")],
        time_dimension_specs=[DataSet.metric_time_dimension_spec(TimeGranularity.MONTH)],
    )


def test_validate_month_granuarity_for_day_and_month_metrics(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    time_granularity_solver.validate_time_granularity(
        metric_references=[MetricReference(element_name="bookings"), MetricReference(element_name="bookings_monthly")],
        time_dimension_specs=[DataSet.metric_time_dimension_spec(TimeGranularity.MONTH)],
    )


def test_validate_year_granularity_for_day_and_month_metrics(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    time_granularity_solver.validate_time_granularity(
        metric_references=[MetricReference(element_name="bookings"), MetricReference(element_name="bookings_monthly")],
        time_dimension_specs=[DataSet.metric_time_dimension_spec(TimeGranularity.YEAR)],
    )


def test_validate_day_granuarity_for_month_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    with pytest.raises(RequestTimeGranularityException):
        time_granularity_solver.validate_time_granularity(
            metric_references=[MetricReference(element_name="bookings_monthly")],
            time_dimension_specs=[DataSet.metric_time_dimension_spec(TimeGranularity.DAY)],
        )


def test_validate_day_granularity_for_day_and_month_metric(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    with pytest.raises(RequestTimeGranularityException):
        time_granularity_solver.validate_time_granularity(
            metric_references=[
                MetricReference(element_name="bookings"),
                MetricReference(element_name="bookings_monthly"),
            ],
            time_dimension_specs=[DataSet.metric_time_dimension_spec(TimeGranularity.DAY)],
        )


PARTIAL_PTD_SPEC = PartialTimeDimensionSpec(element_name=DataSet.metric_time_dimension_name(), entity_links=())


def test_granularity_solution_for_day_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    assert time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
        metric_references=[MetricReference(element_name="bookings")],
        partial_time_dimension_specs=[PARTIAL_PTD_SPEC],
        metric_time_dimension_reference=MTD_REFERENCE,
    ) == {
        PARTIAL_PTD_SPEC: MTD_SPEC_DAY,
    }


def test_granularity_solution_for_month_metric(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    assert time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
        metric_references=[MetricReference(element_name="bookings_monthly")],
        partial_time_dimension_specs=[PARTIAL_PTD_SPEC],
        metric_time_dimension_reference=MTD_REFERENCE,
    ) == {
        PARTIAL_PTD_SPEC: MTD_SPEC_MONTH,
    }


def test_granularity_solution_for_day_and_month_metrics(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    assert time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
        metric_references=[MetricReference(element_name="bookings"), MetricReference(element_name="bookings_monthly")],
        partial_time_dimension_specs=[PARTIAL_PTD_SPEC],
        metric_time_dimension_reference=MTD_REFERENCE,
    ) == {PARTIAL_PTD_SPEC: MTD_SPEC_MONTH}


def test_granularity_error_for_cumulative_metric(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    with pytest.raises(RequestTimeGranularityException):
        time_granularity_solver.validate_time_granularity(
            metric_references=[
                MetricReference(element_name="weekly_bookers"),
                MetricReference(element_name="bookings_monthly"),
            ],
            time_dimension_specs=[MTD_SPEC_MONTH],
        )


def test_time_granularity_parameter(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    assert time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
        metric_references=[MetricReference(element_name="bookings"), MetricReference(element_name="bookings_monthly")],
        partial_time_dimension_specs=[PARTIAL_PTD_SPEC],
        metric_time_dimension_reference=MTD_REFERENCE,
        time_granularity=TimeGranularity.YEAR,
    ) == {PARTIAL_PTD_SPEC: MTD_SPEC_YEAR}


def test_invalid_time_granularity_parameter(  # noqa: D
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    with pytest.raises(RequestTimeGranularityException, match="Can't use time granularity.*"):
        time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
            metric_references=[
                MetricReference(element_name="bookings"),
                MetricReference(element_name="bookings_monthly"),
            ],
            partial_time_dimension_specs=[PARTIAL_PTD_SPEC],
            metric_time_dimension_reference=MTD_REFERENCE,
            time_granularity=TimeGranularity.DAY,
        )


def test_adjusted_time_constraint(time_granularity_solver: TimeGranularitySolver) -> None:  # noqa: D
    assert time_granularity_solver.adjust_time_range_to_granularity(
        time_range_constraint=TimeRangeConstraint(
            start_time=datetime.datetime(2020, 1, 1),
            end_time=datetime.datetime(2020, 1, 15),
        ),
        time_granularity=TimeGranularity.DAY,
    ) == TimeRangeConstraint(
        start_time=datetime.datetime(2020, 1, 1),
        end_time=datetime.datetime(2020, 1, 15),
    )

    assert time_granularity_solver.adjust_time_range_to_granularity(
        time_range_constraint=TimeRangeConstraint(
            start_time=datetime.datetime(2020, 1, 1),
            end_time=datetime.datetime(2020, 1, 15),
        ),
        time_granularity=TimeGranularity.MONTH,
    ) == TimeRangeConstraint(
        start_time=datetime.datetime(2020, 1, 1),
        end_time=datetime.datetime(2020, 1, 31),
    )

    assert time_granularity_solver.adjust_time_range_to_granularity(
        time_range_constraint=TimeRangeConstraint(
            start_time=datetime.datetime(2020, 1, 15),
            end_time=datetime.datetime(2020, 2, 15),
        ),
        time_granularity=TimeGranularity.MONTH,
    ) == TimeRangeConstraint(
        start_time=datetime.datetime(2020, 1, 1),
        end_time=datetime.datetime(2020, 2, 29),
    )


def test_adjusted_time_constraint_for_week_granularity(
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    """Tests the time range adjustment behavior for weekly granularities."""
    assert time_granularity_solver.adjust_time_range_to_granularity(
        time_range_constraint=TimeRangeConstraint(
            start_time=datetime.datetime(2020, 1, 8),
            end_time=datetime.datetime(2020, 1, 15),
        ),
        time_granularity=TimeGranularity.WEEK,
    ) == TimeRangeConstraint(
        start_time=datetime.datetime(2020, 1, 6),
        end_time=datetime.datetime(2020, 1, 19),
    )


def test_adjusted_time_constraint_at_boundaries(
    time_granularity_solver: TimeGranularitySolver,
) -> None:
    """Tests the time range adjustment behavior at the min / max boundaries."""
    assert (
        time_granularity_solver.adjust_time_range_to_granularity(
            time_range_constraint=TimeRangeConstraint(
                start_time=TimeRangeConstraint.all_time().start_time + datetime.timedelta(days=1),
                end_time=TimeRangeConstraint.all_time().end_time,
            ),
            time_granularity=TimeGranularity.WEEK,
        )
        == TimeRangeConstraint.all_time()
    )

    assert (
        time_granularity_solver.adjust_time_range_to_granularity(
            time_range_constraint=TimeRangeConstraint.all_time(),
            time_granularity=TimeGranularity.WEEK,
        )
        == TimeRangeConstraint.all_time()
    )
