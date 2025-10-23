from __future__ import annotations

import datetime

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from metricflow_semantics.specs.query_param_implementations import OrderByParameter, TimeDimensionParameter
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient, SqlEngine
from tests_metricflow.integration.conftest import IntegrationTestHelpers
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal


@pytest.mark.sql_engine_snapshot
def test_simple_fill_nulls_with_0_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_fill_nulls_with_0"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            time_constraint_start=datetime.datetime(2019, 11, 27),
            time_constraint_end=datetime.datetime(2020, 1, 5),
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
def test_simple_fill_nulls_with_0_month(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_fill_nulls_with_0"],
            group_by_names=["metric_time__month"],
            order_by_names=["metric_time__month"],
            time_constraint_start=datetime.datetime(2019, 1, 1),
            time_constraint_end=datetime.datetime(2020, 12, 1),
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
def test_simple_join_to_time_spine(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_join_to_time_spine"],
            group_by_names=["metric_time"],
            time_constraint_start=datetime.datetime(2019, 11, 27),
            time_constraint_end=datetime.datetime(2020, 1, 5),
            order_by_names=["metric_time"],
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
def test_fill_nulls_with_0_multi_metric_query(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_fill_nulls_with_0", "views"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            time_constraint_start=datetime.datetime(2019, 11, 27),
            time_constraint_end=datetime.datetime(2020, 1, 5),
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
def test_fill_nulls_with_0_multi_metric_query_with_categorical_dimension(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_fill_nulls_with_0_without_time_spine", "views"],
            group_by_names=["metric_time", "listing__is_lux_latest"],
            order_by_names=["metric_time", "listing__is_lux_latest"],
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
def test_join_to_time_spine_with_filter_not_in_group_by(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    if sql_client.sql_engine_type is SqlEngine.TRINO:
        pytest.skip(
            "Trino does not support the syntax used in this where filter, but it can't be made engine-agnostic."
        )

    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_join_to_time_spine_with_tiered_filters"],
            group_by_names=["metric_time__day"],
            order_by_names=["metric_time__day"],
            where_constraints=["{{ TimeDimension('metric_time', 'month') }} = '2020-01-01'"],
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
def test_join_to_time_spine_with_filter_smaller_than_group_by(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    if sql_client.sql_engine_type is SqlEngine.TRINO:
        pytest.skip(
            "Trino does not support the syntax used in this where filter, but it can't be made engine-agnostic."
        )

    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["archived_users_join_to_time_spine"],
            group_by_names=["metric_time__day"],
            order_by_names=["metric_time__day"],
            where_constraints=[
                "{{ TimeDimension('metric_time', 'hour') }} >= '2020-01-01 00:09:00'",
                "{{ TimeDimension('metric_time', 'day') }} = '2020-01-01'",
            ],
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
def test_join_to_time_spine_with_filter_not_in_group_by_using_agg_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    if sql_client.sql_engine_type is SqlEngine.TRINO:
        pytest.skip(
            "Trino does not support the syntax used in this where filter, but it can't be made engine-agnostic."
        )

    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_join_to_time_spine_with_tiered_filters"],
            group_by_names=["booking__ds__day"],
            order_by_names=["booking__ds__day"],
            where_constraints=["{{ TimeDimension('booking__ds', 'month') }} = '2020-01-01'"],
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
def test_join_to_time_spine_with_filter_not_in_group_by_using_agg_time_and_metric_time(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    if sql_client.sql_engine_type is SqlEngine.TRINO:
        pytest.skip(
            "Trino does not support the syntax used in this where filter, but it can't be made engine-agnostic."
        )

    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_join_to_time_spine_with_tiered_filters"],
            group_by_names=["metric_time__day"],
            order_by_names=["metric_time__day"],
            where_constraints=["{{ TimeDimension('booking__ds', 'month') }} = '2020-01-01'"],
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
def test_join_to_time_spine_with_custom_grain_in_group_by(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["bookings_join_to_time_spine"],
            group_by_names=["booking__ds__alien_day"],
            order_by_names=["booking__ds__alien_day"],
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
def test_join_to_timespine_metric_with_custom_granularity_filter(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    if sql_client.sql_engine_type is SqlEngine.TRINO:
        pytest.skip(
            "Trino does not support the syntax used in this where filter, but it can't be made engine-agnostic."
        )

    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=("bookings_join_to_time_spine",),
            group_by_names=("metric_time__alien_day",),
            order_by_names=("metric_time__alien_day",),
            where_constraints=["{{ TimeDimension('metric_time', 'alien_day') }} = '2020-01-02'"],
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
def test_join_to_timespine_metric_with_custom_granularity_filter_not_in_group_by(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    if sql_client.sql_engine_type is SqlEngine.TRINO:
        pytest.skip(
            "Trino does not support the syntax used in this where filter, but it can't be made engine-agnostic."
        )

    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=("bookings_join_to_time_spine",),
            group_by_names=("metric_time__day",),
            order_by_names=("metric_time__day",),
            where_constraints=[
                "{{ TimeDimension('metric_time', 'alien_day') }} = '2020-01-02'",
                "{{ TimeDimension('metric_time', 'year') }} = '2019-01-01'",
            ],
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


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_join_to_timespine_metric_with_date_part(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    it_helpers: IntegrationTestHelpers,
) -> None:
    """Test join_to_timespine metric with date part."""
    query_result = it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=("bookings_join_to_time_spine",),
            group_by=(TimeDimensionParameter(name="metric_time", date_part=DatePart.MONTH),),
            order_by=(OrderByParameter(order_by=TimeDimensionParameter(name="metric_time", date_part=DatePart.MONTH)),),
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
