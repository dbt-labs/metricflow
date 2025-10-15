from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.specs.query_param_implementations import OrderByParameter, TimeDimensionParameter
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.integration.conftest import IntegrationTestHelpers
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal


@pytest.mark.sql_engine_snapshot
def test_offset_to_grain_with_single_granularity(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_at_start_of_month"],
            group_by_names=["metric_time__day"],
            order_by_names=["metric_time__day"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
def test_offset_to_grain_with_multiple_granularities(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_at_start_of_month"],
            group_by_names=["metric_time__day", "metric_time__month", "metric_time__year"],
            order_by_names=["metric_time__day", "metric_time__month", "metric_time__year"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
def test_custom_offset_window_with_base_grain(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Gives a side by side comparison of bookings and bookings_offset_one_alien_day."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings", "bookings_offset_one_alien_day"],
            group_by_names=["metric_time__day", "metric_time__alien_day"],
            order_by_names=["metric_time__day", "metric_time__alien_day"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
def test_custom_offset_window_with_grains_and_date_part(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_offset_one_alien_day"],
            group_by=(
                TimeDimensionParameter(name="booking__ds", grain=TimeGranularity.MONTH.name),
                TimeDimensionParameter(name="metric_time", date_part=DatePart.YEAR),
                TimeDimensionParameter(name="metric_time", grain="alien_day"),
            ),
            order_by=(
                OrderByParameter(TimeDimensionParameter(name="booking__ds", grain=TimeGranularity.MONTH.name)),
                OrderByParameter(TimeDimensionParameter(name="metric_time", date_part=DatePart.YEAR)),
                OrderByParameter(TimeDimensionParameter(name="metric_time", grain="alien_day")),
            ),
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
def test_custom_offset_window_with_matching_custom_grain(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Gives a side by side comparison of bookings and bookings_offset_one_alien_day."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings", "bookings_offset_one_alien_day"],
            group_by_names=["booking__ds__alien_day", "metric_time__alien_day"],
            order_by_names=["booking__ds__alien_day", "metric_time__alien_day"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


# Offset window tests
@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_window_with_grain_smaller_than_offset(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset window metric queried with grain smaller than offset grain."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings", "bookings_1_month_ago", "bookings_mom"],
            group_by_names=["metric_time__day"],
            order_by_names=["metric_time__day"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_window_with_grain_smaller_than_offset_non_default(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset window metric queried with grain that is not the default and is smaller than offset grain."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings", "bookings_1_year_ago", "bookings_yoy"],
            group_by_names=["metric_time__month"],
            order_by_names=["metric_time__month"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_window_with_grain_matching_offset(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset window metric queried with grain matching offset grain."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings", "bookings_1_month_ago", "bookings_mom"],
            group_by_names=["metric_time__month"],
            order_by_names=["metric_time__month"],
            where_constraints=["{{ TimeDimension('metric_time', 'day') }} < '2021-01-01'"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_window_with_grain_larger_than_offset(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset window metric queried with grain larger than offset grain.

    Not a very useful query, but still demonstrates that we can handle these params.
    """
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings", "bookings_1_month_ago", "bookings_mom"],
            group_by_names=["metric_time__year"],
            order_by_names=["metric_time__year"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_window_with_custom_grain(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset window metric queried with custom grain.

    Not a very useful query, but still demonstrates that we can handle this query.
    """
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings", "bookings_1_month_ago", "bookings_mom"],
            group_by_names=["metric_time__alien_day"],
            order_by_names=["metric_time__alien_day"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_window_with_multiple_grains(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset window metric queried with multiple grains."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings", "bookings_1_month_ago", "bookings_mom"],
            group_by_names=["metric_time__day", "metric_time__month", "metric_time__year"],
            order_by_names=["metric_time__day", "metric_time__month", "metric_time__year"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_window_with_date_part_only(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset window metric queried with date part only (no grain specified).

    Not a very useful query, but still demonstrates that we can handle this query.
    Note: date part is allowed for offset_window but not for offset_to_grain.
    """
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings", "bookings_1_month_ago", "bookings_mom"],
            group_by=(TimeDimensionParameter(name="metric_time", date_part=DatePart.MONTH),),
            order_by=(OrderByParameter(TimeDimensionParameter(name="metric_time", date_part=DatePart.MONTH)),),
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


# Offset to grain tests
@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_to_grain_with_grain_smaller_than_offset(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset to grain metric queried with grain smaller than offset grain."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_all_time", "bookings_all_time_at_start_of_month", "bookings_since_start_of_month"],
            group_by_names=["metric_time__day"],
            order_by_names=["metric_time__day"],
            where_constraints=["{{ TimeDimension('metric_time', 'day') }} < '2021-01-01'"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_to_grain_with_grain_smaller_than_offset_non_default(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset to grain metric queried with grain that is not the default and is smaller than offset grain."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_all_time", "bookings_all_time_at_start_of_year", "bookings_since_start_of_year"],
            group_by_names=["metric_time__month"],
            order_by_names=["metric_time__month"],
            where_constraints=["{{ TimeDimension('metric_time', 'year') }} < '2021-01-01'"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_to_grain_with_grain_matching_offset(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset to grain metric queried with grain matching offset grain.

    Not likely a useful query, but still demonstrates that we do this correctly.
    """
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_all_time", "bookings_all_time_at_start_of_month", "bookings_since_start_of_month"],
            group_by_names=["metric_time__month"],
            order_by_names=["metric_time__month"],
            where_constraints=["{{ TimeDimension('metric_time', 'day') }} < '2021-01-01'"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_to_grain_with_grain_larger_than_offset(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset to grain metric queried with grain larger than offset grain.

    Not likely a useful query, but still demonstrates that we do this correctly.
    """
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_all_time", "bookings_all_time_at_start_of_month", "bookings_since_start_of_month"],
            group_by_names=["metric_time__year"],
            order_by_names=["metric_time__year"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_to_grain_with_custom_grain(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset to grain metric queried with custom grain.

    Not a very useful query, but still demonstrates that we can handle this query.
    """
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_all_time", "bookings_all_time_at_start_of_month", "bookings_since_start_of_month"],
            group_by_names=["metric_time__alien_day"],
            order_by_names=["metric_time__alien_day"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_to_grain_with_multiple_grains(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset to grain metric queried with multiple grains."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_all_time", "bookings_all_time_at_start_of_month", "bookings_since_start_of_month"],
            group_by_names=["metric_time__day", "metric_time__month", "metric_time__year"],
            order_by_names=["metric_time__day", "metric_time__month", "metric_time__year"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_offset_window_with_cumulative_input_metric(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset window metric that uses a cumulative metric with window as input."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["trailing_7_days_bookings", "trailing_7_days_bookings_offset_1_week"],
            group_by_names=["metric_time__day"],
            order_by_names=["metric_time__day"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )


# TODO: bug to fix if the constraint below is applied. All agg time dims need to be selected from time spine in
# JoinOverTimeRangeNod & JoinToTimeSpineNode for constraint to work.
@pytest.mark.sql_engine_snapshot
@pytest.mark.duckdb_only
def test_non_default_grain_where_constraint_with_cumulative_offset_to_grain(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test offset to grain metric queried with non-default grain and where constraint.

    Tests that where constraints work properly with JoinOverTimeRangeNode and JoinToTimeSpineNode.
    """
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_all_time", "bookings_all_time_at_start_of_month", "bookings_since_start_of_month"],
            group_by_names=["metric_time__month"],
            order_by_names=["metric_time__month"],
            where_constraints=["{{ TimeDimension('metric_time', 'year') }} < '2021-01-01'"],
        )
    )
    assert query_result.result_df is not None, "Unexpected empty result."

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="query_output",
        snapshot_str=query_result.result_df.text_format(),
        sql_engine=sql_client.sql_engine_type,
    )
