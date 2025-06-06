test_name: test_offset_to_grain_metric_filter_and_query_have_different_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset to grain metric is queried with one granularity and filtered by a different one.
sql_engine: BigQuery
---
-- Write to DataTable
SELECT
  subq_10.metric_time__month
  , subq_10.bookings_at_start_of_month
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_9.metric_time__month
    , bookings_start_of_month AS bookings_at_start_of_month
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_8.metric_time__month
      , subq_8.bookings AS bookings_start_of_month
    FROM (
      -- Aggregate Measures
      SELECT
        subq_7.metric_time__month
        , SUM(subq_7.bookings) AS bookings
      FROM (
        -- Pass Only Elements: ['bookings', 'metric_time__month']
        SELECT
          subq_6.metric_time__month
          , subq_6.bookings
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_5.ds__day
            , subq_5.ds__week
            , subq_5.ds__month
            , subq_5.ds__quarter
            , subq_5.ds__year
            , subq_5.ds__extract_year
            , subq_5.ds__extract_quarter
            , subq_5.ds__extract_month
            , subq_5.ds__extract_day
            , subq_5.ds__extract_dow
            , subq_5.ds__extract_doy
            , subq_5.ds_partitioned__day
            , subq_5.ds_partitioned__week
            , subq_5.ds_partitioned__month
            , subq_5.ds_partitioned__quarter
            , subq_5.ds_partitioned__year
            , subq_5.ds_partitioned__extract_year
            , subq_5.ds_partitioned__extract_quarter
            , subq_5.ds_partitioned__extract_month
            , subq_5.ds_partitioned__extract_day
            , subq_5.ds_partitioned__extract_dow
            , subq_5.ds_partitioned__extract_doy
            , subq_5.paid_at__day
            , subq_5.paid_at__week
            , subq_5.paid_at__month
            , subq_5.paid_at__quarter
            , subq_5.paid_at__year
            , subq_5.paid_at__extract_year
            , subq_5.paid_at__extract_quarter
            , subq_5.paid_at__extract_month
            , subq_5.paid_at__extract_day
            , subq_5.paid_at__extract_dow
            , subq_5.paid_at__extract_doy
            , subq_5.booking__ds__day
            , subq_5.booking__ds__week
            , subq_5.booking__ds__month
            , subq_5.booking__ds__quarter
            , subq_5.booking__ds__year
            , subq_5.booking__ds__extract_year
            , subq_5.booking__ds__extract_quarter
            , subq_5.booking__ds__extract_month
            , subq_5.booking__ds__extract_day
            , subq_5.booking__ds__extract_dow
            , subq_5.booking__ds__extract_doy
            , subq_5.booking__ds_partitioned__day
            , subq_5.booking__ds_partitioned__week
            , subq_5.booking__ds_partitioned__month
            , subq_5.booking__ds_partitioned__quarter
            , subq_5.booking__ds_partitioned__year
            , subq_5.booking__ds_partitioned__extract_year
            , subq_5.booking__ds_partitioned__extract_quarter
            , subq_5.booking__ds_partitioned__extract_month
            , subq_5.booking__ds_partitioned__extract_day
            , subq_5.booking__ds_partitioned__extract_dow
            , subq_5.booking__ds_partitioned__extract_doy
            , subq_5.booking__paid_at__day
            , subq_5.booking__paid_at__week
            , subq_5.booking__paid_at__month
            , subq_5.booking__paid_at__quarter
            , subq_5.booking__paid_at__year
            , subq_5.booking__paid_at__extract_year
            , subq_5.booking__paid_at__extract_quarter
            , subq_5.booking__paid_at__extract_month
            , subq_5.booking__paid_at__extract_day
            , subq_5.booking__paid_at__extract_dow
            , subq_5.booking__paid_at__extract_doy
            , subq_5.metric_time__week
            , subq_5.metric_time__quarter
            , subq_5.metric_time__year
            , subq_5.metric_time__extract_year
            , subq_5.metric_time__extract_quarter
            , subq_5.metric_time__extract_month
            , subq_5.metric_time__extract_day
            , subq_5.metric_time__extract_dow
            , subq_5.metric_time__extract_doy
            , subq_5.metric_time__day
            , subq_5.metric_time__month
            , subq_5.listing
            , subq_5.guest
            , subq_5.host
            , subq_5.booking__listing
            , subq_5.booking__guest
            , subq_5.booking__host
            , subq_5.is_instant
            , subq_5.booking__is_instant
            , subq_5.bookings
            , subq_5.instant_bookings
            , subq_5.booking_value
            , subq_5.max_booking_value
            , subq_5.min_booking_value
            , subq_5.bookers
            , subq_5.average_booking_value
            , subq_5.referred_bookings
            , subq_5.median_booking_value
            , subq_5.booking_value_p99
            , subq_5.discrete_booking_value_p99
            , subq_5.approximate_continuous_booking_value_p99
            , subq_5.approximate_discrete_booking_value_p99
          FROM (
            -- Join to Time Spine Dataset
            SELECT
              subq_4.metric_time__day AS metric_time__day
              , subq_4.metric_time__month AS metric_time__month
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
              , subq_1.bookings AS bookings
              , subq_1.instant_bookings AS instant_bookings
              , subq_1.booking_value AS booking_value
              , subq_1.max_booking_value AS max_booking_value
              , subq_1.min_booking_value AS min_booking_value
              , subq_1.bookers AS bookers
              , subq_1.average_booking_value AS average_booking_value
              , subq_1.referred_bookings AS referred_bookings
              , subq_1.median_booking_value AS median_booking_value
              , subq_1.booking_value_p99 AS booking_value_p99
              , subq_1.discrete_booking_value_p99 AS discrete_booking_value_p99
              , subq_1.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
              , subq_1.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
            FROM (
              -- Pass Only Elements: ['metric_time__day', 'metric_time__month']
              SELECT
                subq_3.metric_time__day
                , subq_3.metric_time__month
              FROM (
                -- Change Column Aliases
                SELECT
                  subq_2.ds__day AS metric_time__day
                  , subq_2.ds__week
                  , subq_2.ds__month AS metric_time__month
                  , subq_2.ds__quarter
                  , subq_2.ds__year
                  , subq_2.ds__extract_year
                  , subq_2.ds__extract_quarter
                  , subq_2.ds__extract_month
                  , subq_2.ds__extract_day
                  , subq_2.ds__extract_dow
                  , subq_2.ds__extract_doy
                  , subq_2.ds__alien_day
                FROM (
                  -- Read From Time Spine 'mf_time_spine'
                  SELECT
                    time_spine_src_28006.ds AS ds__day
                    , DATETIME_TRUNC(time_spine_src_28006.ds, isoweek) AS ds__week
                    , DATETIME_TRUNC(time_spine_src_28006.ds, month) AS ds__month
                    , DATETIME_TRUNC(time_spine_src_28006.ds, quarter) AS ds__quarter
                    , DATETIME_TRUNC(time_spine_src_28006.ds, year) AS ds__year
                    , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
                    , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
                    , IF(EXTRACT(dayofweek FROM time_spine_src_28006.ds) = 1, 7, EXTRACT(dayofweek FROM time_spine_src_28006.ds) - 1) AS ds__extract_dow
                    , EXTRACT(dayofyear FROM time_spine_src_28006.ds) AS ds__extract_doy
                    , time_spine_src_28006.alien_day AS ds__alien_day
                  FROM ***************************.mf_time_spine time_spine_src_28006
                ) subq_2
              ) subq_3
            ) subq_4
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
                , subq_0.bookings
                , subq_0.instant_bookings
                , subq_0.booking_value
                , subq_0.max_booking_value
                , subq_0.min_booking_value
                , subq_0.bookers
                , subq_0.average_booking_value
                , subq_0.referred_bookings
                , subq_0.median_booking_value
                , subq_0.booking_value_p99
                , subq_0.discrete_booking_value_p99
                , subq_0.approximate_continuous_booking_value_p99
                , subq_0.approximate_discrete_booking_value_p99
              FROM (
                -- Read Elements From Semantic Model 'bookings_source'
                SELECT
                  1 AS bookings
                  , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
                  , bookings_source_src_28000.booking_value
                  , bookings_source_src_28000.booking_value AS max_booking_value
                  , bookings_source_src_28000.booking_value AS min_booking_value
                  , bookings_source_src_28000.guest_id AS bookers
                  , bookings_source_src_28000.booking_value AS average_booking_value
                  , bookings_source_src_28000.booking_value AS booking_payments
                  , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
                  , bookings_source_src_28000.booking_value AS median_booking_value
                  , bookings_source_src_28000.booking_value AS booking_value_p99
                  , bookings_source_src_28000.booking_value AS discrete_booking_value_p99
                  , bookings_source_src_28000.booking_value AS approximate_continuous_booking_value_p99
                  , bookings_source_src_28000.booking_value AS approximate_discrete_booking_value_p99
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
              DATETIME_TRUNC(subq_4.metric_time__day, month) = subq_1.metric_time__day
            WHERE subq_4.metric_time__month = subq_4.metric_time__day
          ) subq_5
          WHERE metric_time__day = '2020-01-01'
        ) subq_6
      ) subq_7
      GROUP BY
        metric_time__month
    ) subq_8
  ) subq_9
) subq_10
