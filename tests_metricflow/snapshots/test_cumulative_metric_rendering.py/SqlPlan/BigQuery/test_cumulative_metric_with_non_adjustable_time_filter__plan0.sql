test_name: test_cumulative_metric_with_non_adjustable_time_filter
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a cumulative metric query with a time filter that cannot be automatically adjusted.

      Not all query inputs with time constraint filters allow us to adjust the time constraint to include the full
      span of input data for a cumulative metric. When we do not have an adjustable time filter we must include all
      input data in order to ensure the cumulative metric is correct.
sql_engine: BigQuery
---
-- Write to DataTable
SELECT
  subq_10.metric_time__day
  , subq_10.every_two_days_bookers
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_9.metric_time__day
    , subq_9.bookers AS every_two_days_bookers
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_8.metric_time__day
      , subq_8.__bookers AS bookers
    FROM (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_7.metric_time__day
        , COUNT(DISTINCT subq_7.__bookers) AS __bookers
      FROM (
        -- Pass Only Elements: ['__bookers', 'metric_time__day']
        SELECT
          subq_6.metric_time__day
          , subq_6.__bookers
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_5.bookers AS __bookers
            , subq_5.metric_time__day
          FROM (
            -- Pass Only Elements: ['__bookers', 'metric_time__day']
            SELECT
              subq_4.metric_time__day
              , subq_4.__bookers AS bookers
            FROM (
              -- Join Self Over Time Range
              SELECT
                subq_2.metric_time__day AS metric_time__day
                , subq_1.ds__day AS ds__day
                , subq_1.ds__week AS ds__week
                , subq_1.ds__month AS ds__month
                , subq_1.ds__quarter AS ds__quarter
                , subq_1.ds__year AS ds__year
                , subq_1.ds__extract_year AS ds__extract_year
                , subq_1.ds__extract_quarter AS ds__extract_quarter
                , subq_1.ds__extract_month AS ds__extract_month
                , subq_1.ds__extract_day AS ds__extract_day
                , subq_1.ds__extract_dow AS ds__extract_dow
                , subq_1.ds__extract_doy AS ds__extract_doy
                , subq_1.ds_partitioned__day AS ds_partitioned__day
                , subq_1.ds_partitioned__week AS ds_partitioned__week
                , subq_1.ds_partitioned__month AS ds_partitioned__month
                , subq_1.ds_partitioned__quarter AS ds_partitioned__quarter
                , subq_1.ds_partitioned__year AS ds_partitioned__year
                , subq_1.ds_partitioned__extract_year AS ds_partitioned__extract_year
                , subq_1.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                , subq_1.ds_partitioned__extract_month AS ds_partitioned__extract_month
                , subq_1.ds_partitioned__extract_day AS ds_partitioned__extract_day
                , subq_1.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                , subq_1.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                , subq_1.paid_at__day AS paid_at__day
                , subq_1.paid_at__week AS paid_at__week
                , subq_1.paid_at__month AS paid_at__month
                , subq_1.paid_at__quarter AS paid_at__quarter
                , subq_1.paid_at__year AS paid_at__year
                , subq_1.paid_at__extract_year AS paid_at__extract_year
                , subq_1.paid_at__extract_quarter AS paid_at__extract_quarter
                , subq_1.paid_at__extract_month AS paid_at__extract_month
                , subq_1.paid_at__extract_day AS paid_at__extract_day
                , subq_1.paid_at__extract_dow AS paid_at__extract_dow
                , subq_1.paid_at__extract_doy AS paid_at__extract_doy
                , subq_1.booking__ds__day AS booking__ds__day
                , subq_1.booking__ds__week AS booking__ds__week
                , subq_1.booking__ds__month AS booking__ds__month
                , subq_1.booking__ds__quarter AS booking__ds__quarter
                , subq_1.booking__ds__year AS booking__ds__year
                , subq_1.booking__ds__extract_year AS booking__ds__extract_year
                , subq_1.booking__ds__extract_quarter AS booking__ds__extract_quarter
                , subq_1.booking__ds__extract_month AS booking__ds__extract_month
                , subq_1.booking__ds__extract_day AS booking__ds__extract_day
                , subq_1.booking__ds__extract_dow AS booking__ds__extract_dow
                , subq_1.booking__ds__extract_doy AS booking__ds__extract_doy
                , subq_1.booking__ds_partitioned__day AS booking__ds_partitioned__day
                , subq_1.booking__ds_partitioned__week AS booking__ds_partitioned__week
                , subq_1.booking__ds_partitioned__month AS booking__ds_partitioned__month
                , subq_1.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                , subq_1.booking__ds_partitioned__year AS booking__ds_partitioned__year
                , subq_1.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                , subq_1.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                , subq_1.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                , subq_1.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                , subq_1.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                , subq_1.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                , subq_1.booking__paid_at__day AS booking__paid_at__day
                , subq_1.booking__paid_at__week AS booking__paid_at__week
                , subq_1.booking__paid_at__month AS booking__paid_at__month
                , subq_1.booking__paid_at__quarter AS booking__paid_at__quarter
                , subq_1.booking__paid_at__year AS booking__paid_at__year
                , subq_1.booking__paid_at__extract_year AS booking__paid_at__extract_year
                , subq_1.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                , subq_1.booking__paid_at__extract_month AS booking__paid_at__extract_month
                , subq_1.booking__paid_at__extract_day AS booking__paid_at__extract_day
                , subq_1.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                , subq_1.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
                , subq_1.metric_time__week AS metric_time__week
                , subq_1.metric_time__month AS metric_time__month
                , subq_1.metric_time__quarter AS metric_time__quarter
                , subq_1.metric_time__year AS metric_time__year
                , subq_1.metric_time__extract_year AS metric_time__extract_year
                , subq_1.metric_time__extract_quarter AS metric_time__extract_quarter
                , subq_1.metric_time__extract_month AS metric_time__extract_month
                , subq_1.metric_time__extract_day AS metric_time__extract_day
                , subq_1.metric_time__extract_dow AS metric_time__extract_dow
                , subq_1.metric_time__extract_doy AS metric_time__extract_doy
                , subq_1.listing AS listing
                , subq_1.guest AS guest
                , subq_1.host AS host
                , subq_1.booking__listing AS booking__listing
                , subq_1.booking__guest AS booking__guest
                , subq_1.booking__host AS booking__host
                , subq_1.is_instant AS is_instant
                , subq_1.booking__is_instant AS booking__is_instant
                , subq_1.__bookings AS __bookings
                , subq_1.__average_booking_value AS __average_booking_value
                , subq_1.__instant_bookings AS __instant_bookings
                , subq_1.__booking_value AS __booking_value
                , subq_1.__max_booking_value AS __max_booking_value
                , subq_1.__min_booking_value AS __min_booking_value
                , subq_1.__instant_booking_value AS __instant_booking_value
                , subq_1.__average_instant_booking_value AS __average_instant_booking_value
                , subq_1.__booking_value_for_non_null_listing_id AS __booking_value_for_non_null_listing_id
                , subq_1.__bookers AS __bookers
                , subq_1.__referred_bookings AS __referred_bookings
                , subq_1.__median_booking_value AS __median_booking_value
                , subq_1.__booking_value_p99 AS __booking_value_p99
                , subq_1.__discrete_booking_value_p99 AS __discrete_booking_value_p99
                , subq_1.__approximate_continuous_booking_value_p99 AS __approximate_continuous_booking_value_p99
                , subq_1.__approximate_discrete_booking_value_p99 AS __approximate_discrete_booking_value_p99
                , subq_1.__bookings_join_to_time_spine AS __bookings_join_to_time_spine
                , subq_1.__bookings_fill_nulls_with_0_without_time_spine AS __bookings_fill_nulls_with_0_without_time_spine
                , subq_1.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
                , subq_1.__instant_bookings_with_measure_filter AS __instant_bookings_with_measure_filter
                , subq_1.__bookings_join_to_time_spine_with_tiered_filters AS __bookings_join_to_time_spine_with_tiered_filters
                , subq_1.__bookers_fill_nulls_with_0_join_to_timespine AS __bookers_fill_nulls_with_0_join_to_timespine
              FROM (
                -- Read From Time Spine 'mf_time_spine'
                SELECT
                  subq_3.ds AS metric_time__day
                FROM ***************************.mf_time_spine subq_3
              ) subq_2
              INNER JOIN (
                -- Metric Time Dimension 'ds'
                SELECT
                  subq_0.ds__day
                  , subq_0.ds__week
                  , subq_0.ds__month
                  , subq_0.ds__quarter
                  , subq_0.ds__year
                  , subq_0.ds__extract_year
                  , subq_0.ds__extract_quarter
                  , subq_0.ds__extract_month
                  , subq_0.ds__extract_day
                  , subq_0.ds__extract_dow
                  , subq_0.ds__extract_doy
                  , subq_0.ds_partitioned__day
                  , subq_0.ds_partitioned__week
                  , subq_0.ds_partitioned__month
                  , subq_0.ds_partitioned__quarter
                  , subq_0.ds_partitioned__year
                  , subq_0.ds_partitioned__extract_year
                  , subq_0.ds_partitioned__extract_quarter
                  , subq_0.ds_partitioned__extract_month
                  , subq_0.ds_partitioned__extract_day
                  , subq_0.ds_partitioned__extract_dow
                  , subq_0.ds_partitioned__extract_doy
                  , subq_0.paid_at__day
                  , subq_0.paid_at__week
                  , subq_0.paid_at__month
                  , subq_0.paid_at__quarter
                  , subq_0.paid_at__year
                  , subq_0.paid_at__extract_year
                  , subq_0.paid_at__extract_quarter
                  , subq_0.paid_at__extract_month
                  , subq_0.paid_at__extract_day
                  , subq_0.paid_at__extract_dow
                  , subq_0.paid_at__extract_doy
                  , subq_0.booking__ds__day
                  , subq_0.booking__ds__week
                  , subq_0.booking__ds__month
                  , subq_0.booking__ds__quarter
                  , subq_0.booking__ds__year
                  , subq_0.booking__ds__extract_year
                  , subq_0.booking__ds__extract_quarter
                  , subq_0.booking__ds__extract_month
                  , subq_0.booking__ds__extract_day
                  , subq_0.booking__ds__extract_dow
                  , subq_0.booking__ds__extract_doy
                  , subq_0.booking__ds_partitioned__day
                  , subq_0.booking__ds_partitioned__week
                  , subq_0.booking__ds_partitioned__month
                  , subq_0.booking__ds_partitioned__quarter
                  , subq_0.booking__ds_partitioned__year
                  , subq_0.booking__ds_partitioned__extract_year
                  , subq_0.booking__ds_partitioned__extract_quarter
                  , subq_0.booking__ds_partitioned__extract_month
                  , subq_0.booking__ds_partitioned__extract_day
                  , subq_0.booking__ds_partitioned__extract_dow
                  , subq_0.booking__ds_partitioned__extract_doy
                  , subq_0.booking__paid_at__day
                  , subq_0.booking__paid_at__week
                  , subq_0.booking__paid_at__month
                  , subq_0.booking__paid_at__quarter
                  , subq_0.booking__paid_at__year
                  , subq_0.booking__paid_at__extract_year
                  , subq_0.booking__paid_at__extract_quarter
                  , subq_0.booking__paid_at__extract_month
                  , subq_0.booking__paid_at__extract_day
                  , subq_0.booking__paid_at__extract_dow
                  , subq_0.booking__paid_at__extract_doy
                  , subq_0.ds__day AS metric_time__day
                  , subq_0.ds__week AS metric_time__week
                  , subq_0.ds__month AS metric_time__month
                  , subq_0.ds__quarter AS metric_time__quarter
                  , subq_0.ds__year AS metric_time__year
                  , subq_0.ds__extract_year AS metric_time__extract_year
                  , subq_0.ds__extract_quarter AS metric_time__extract_quarter
                  , subq_0.ds__extract_month AS metric_time__extract_month
                  , subq_0.ds__extract_day AS metric_time__extract_day
                  , subq_0.ds__extract_dow AS metric_time__extract_dow
                  , subq_0.ds__extract_doy AS metric_time__extract_doy
                  , subq_0.listing
                  , subq_0.guest
                  , subq_0.host
                  , subq_0.booking__listing
                  , subq_0.booking__guest
                  , subq_0.booking__host
                  , subq_0.is_instant
                  , subq_0.booking__is_instant
                  , subq_0.__bookings
                  , subq_0.__average_booking_value
                  , subq_0.__instant_bookings
                  , subq_0.__booking_value
                  , subq_0.__max_booking_value
                  , subq_0.__min_booking_value
                  , subq_0.__instant_booking_value
                  , subq_0.__average_instant_booking_value
                  , subq_0.__booking_value_for_non_null_listing_id
                  , subq_0.__bookers
                  , subq_0.__referred_bookings
                  , subq_0.__median_booking_value
                  , subq_0.__booking_value_p99
                  , subq_0.__discrete_booking_value_p99
                  , subq_0.__approximate_continuous_booking_value_p99
                  , subq_0.__approximate_discrete_booking_value_p99
                  , subq_0.__bookings_join_to_time_spine
                  , subq_0.__bookings_fill_nulls_with_0_without_time_spine
                  , subq_0.__bookings_fill_nulls_with_0
                  , subq_0.__instant_bookings_with_measure_filter
                  , subq_0.__bookings_join_to_time_spine_with_tiered_filters
                  , subq_0.__bookers_fill_nulls_with_0_join_to_timespine
                FROM (
                  -- Read Elements From Semantic Model 'bookings_source'
                  SELECT
                    1 AS __bookings
                    , bookings_source_src_28000.booking_value AS __average_booking_value
                    , CASE WHEN is_instant THEN 1 ELSE 0 END AS __instant_bookings
                    , bookings_source_src_28000.booking_value AS __booking_value
                    , bookings_source_src_28000.booking_value AS __max_booking_value
                    , bookings_source_src_28000.booking_value AS __min_booking_value
                    , bookings_source_src_28000.booking_value AS __instant_booking_value
                    , bookings_source_src_28000.booking_value AS __average_instant_booking_value
                    , bookings_source_src_28000.booking_value AS __booking_value_for_non_null_listing_id
                    , bookings_source_src_28000.guest_id AS __bookers
                    , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS __referred_bookings
                    , bookings_source_src_28000.booking_value AS __median_booking_value
                    , bookings_source_src_28000.booking_value AS __booking_value_p99
                    , bookings_source_src_28000.booking_value AS __discrete_booking_value_p99
                    , bookings_source_src_28000.booking_value AS __approximate_continuous_booking_value_p99
                    , bookings_source_src_28000.booking_value AS __approximate_discrete_booking_value_p99
                    , 1 AS __bookings_join_to_time_spine
                    , 1 AS __bookings_fill_nulls_with_0_without_time_spine
                    , 1 AS __bookings_fill_nulls_with_0
                    , 1 AS __instant_bookings_with_measure_filter
                    , 1 AS __bookings_join_to_time_spine_with_tiered_filters
                    , bookings_source_src_28000.guest_id AS __bookers_fill_nulls_with_0_join_to_timespine
                    , bookings_source_src_28000.booking_value AS __booking_payments
                    , bookings_source_src_28000.is_instant
                    , DATETIME_TRUNC(bookings_source_src_28000.ds, day) AS ds__day
                    , DATETIME_TRUNC(bookings_source_src_28000.ds, isoweek) AS ds__week
                    , DATETIME_TRUNC(bookings_source_src_28000.ds, month) AS ds__month
                    , DATETIME_TRUNC(bookings_source_src_28000.ds, quarter) AS ds__quarter
                    , DATETIME_TRUNC(bookings_source_src_28000.ds, year) AS ds__year
                    , EXTRACT(year FROM bookings_source_src_28000.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM bookings_source_src_28000.ds) AS ds__extract_month
                    , EXTRACT(day FROM bookings_source_src_28000.ds) AS ds__extract_day
                    , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds) - 1) AS ds__extract_dow
                    , EXTRACT(dayofyear FROM bookings_source_src_28000.ds) AS ds__extract_doy
                    , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, day) AS ds_partitioned__day
                    , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, isoweek) AS ds_partitioned__week
                    , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, month) AS ds_partitioned__month
                    , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, quarter) AS ds_partitioned__quarter
                    , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, year) AS ds_partitioned__year
                    , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                    , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                    , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                    , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                    , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) - 1) AS ds_partitioned__extract_dow
                    , EXTRACT(dayofyear FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                    , DATETIME_TRUNC(bookings_source_src_28000.paid_at, day) AS paid_at__day
                    , DATETIME_TRUNC(bookings_source_src_28000.paid_at, isoweek) AS paid_at__week
                    , DATETIME_TRUNC(bookings_source_src_28000.paid_at, month) AS paid_at__month
                    , DATETIME_TRUNC(bookings_source_src_28000.paid_at, quarter) AS paid_at__quarter
                    , DATETIME_TRUNC(bookings_source_src_28000.paid_at, year) AS paid_at__year
                    , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS paid_at__extract_year
                    , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
                    , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS paid_at__extract_month
                    , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS paid_at__extract_day
                    , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) - 1) AS paid_at__extract_dow
                    , EXTRACT(dayofyear FROM bookings_source_src_28000.paid_at) AS paid_at__extract_doy
                    , bookings_source_src_28000.is_instant AS booking__is_instant
                    , DATETIME_TRUNC(bookings_source_src_28000.ds, day) AS booking__ds__day
                    , DATETIME_TRUNC(bookings_source_src_28000.ds, isoweek) AS booking__ds__week
                    , DATETIME_TRUNC(bookings_source_src_28000.ds, month) AS booking__ds__month
                    , DATETIME_TRUNC(bookings_source_src_28000.ds, quarter) AS booking__ds__quarter
                    , DATETIME_TRUNC(bookings_source_src_28000.ds, year) AS booking__ds__year
                    , EXTRACT(year FROM bookings_source_src_28000.ds) AS booking__ds__extract_year
                    , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS booking__ds__extract_quarter
                    , EXTRACT(month FROM bookings_source_src_28000.ds) AS booking__ds__extract_month
                    , EXTRACT(day FROM bookings_source_src_28000.ds) AS booking__ds__extract_day
                    , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds) - 1) AS booking__ds__extract_dow
                    , EXTRACT(dayofyear FROM bookings_source_src_28000.ds) AS booking__ds__extract_doy
                    , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, day) AS booking__ds_partitioned__day
                    , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, isoweek) AS booking__ds_partitioned__week
                    , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, month) AS booking__ds_partitioned__month
                    , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, quarter) AS booking__ds_partitioned__quarter
                    , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, year) AS booking__ds_partitioned__year
                    , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
                    , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                    , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
                    , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
                    , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) - 1) AS booking__ds_partitioned__extract_dow
                    , EXTRACT(dayofyear FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
                    , DATETIME_TRUNC(bookings_source_src_28000.paid_at, day) AS booking__paid_at__day
                    , DATETIME_TRUNC(bookings_source_src_28000.paid_at, isoweek) AS booking__paid_at__week
                    , DATETIME_TRUNC(bookings_source_src_28000.paid_at, month) AS booking__paid_at__month
                    , DATETIME_TRUNC(bookings_source_src_28000.paid_at, quarter) AS booking__paid_at__quarter
                    , DATETIME_TRUNC(bookings_source_src_28000.paid_at, year) AS booking__paid_at__year
                    , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_year
                    , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_quarter
                    , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_month
                    , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_day
                    , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) - 1) AS booking__paid_at__extract_dow
                    , EXTRACT(dayofyear FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
                    , bookings_source_src_28000.listing_id AS listing
                    , bookings_source_src_28000.guest_id AS guest
                    , bookings_source_src_28000.host_id AS host
                    , bookings_source_src_28000.listing_id AS booking__listing
                    , bookings_source_src_28000.guest_id AS booking__guest
                    , bookings_source_src_28000.host_id AS booking__host
                  FROM ***************************.fct_bookings bookings_source_src_28000
                ) subq_0
              ) subq_1
              ON
                (
                  subq_1.metric_time__day <= subq_2.metric_time__day
                ) AND (
                  subq_1.metric_time__day > DATE_SUB(CAST(subq_2.metric_time__day AS DATETIME), INTERVAL 2 day)
                )
            ) subq_4
          ) subq_5
          WHERE metric_time__day = '2020-01-03' or metric_time__day = '2020-01-07'
        ) subq_6
      ) subq_7
      GROUP BY
        metric_time__day
    ) subq_8
  ) subq_9
) subq_10
