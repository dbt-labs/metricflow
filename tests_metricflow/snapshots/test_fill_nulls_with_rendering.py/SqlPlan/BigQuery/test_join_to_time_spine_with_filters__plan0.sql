test_name: test_join_to_time_spine_with_filters
test_filename: test_fill_nulls_with_rendering.py
sql_engine: BigQuery
---
-- Write to DataTable
SELECT
  subq_14.metric_time__day
  , subq_14.bookings_fill_nulls_with_0
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_13.metric_time__day
    , COALESCE(subq_13.bookings, 0) AS bookings_fill_nulls_with_0
  FROM (
    -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
    SELECT
      subq_12.metric_time__day
      , subq_12.bookings
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_11.metric_time__day AS metric_time__day
        , subq_6.bookings AS bookings
      FROM (
        -- Pass Only Elements: ['metric_time__day']
        SELECT
          subq_10.metric_time__day
        FROM (
          -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
          SELECT
            subq_9.metric_time__day
            , subq_9.metric_time__week
            , subq_9.ds__month
            , subq_9.ds__quarter
            , subq_9.ds__year
            , subq_9.ds__extract_year
            , subq_9.ds__extract_quarter
            , subq_9.ds__extract_month
            , subq_9.ds__extract_day
            , subq_9.ds__extract_dow
            , subq_9.ds__extract_doy
            , subq_9.ds__alien_day
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_8.metric_time__day
              , subq_8.metric_time__week
              , subq_8.ds__month
              , subq_8.ds__quarter
              , subq_8.ds__year
              , subq_8.ds__extract_year
              , subq_8.ds__extract_quarter
              , subq_8.ds__extract_month
              , subq_8.ds__extract_day
              , subq_8.ds__extract_dow
              , subq_8.ds__extract_doy
              , subq_8.ds__alien_day
            FROM (
              -- Change Column Aliases
              SELECT
                subq_7.ds__day AS metric_time__day
                , subq_7.ds__week AS metric_time__week
                , subq_7.ds__month
                , subq_7.ds__quarter
                , subq_7.ds__year
                , subq_7.ds__extract_year
                , subq_7.ds__extract_quarter
                , subq_7.ds__extract_month
                , subq_7.ds__extract_day
                , subq_7.ds__extract_dow
                , subq_7.ds__extract_doy
                , subq_7.ds__alien_day
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
              ) subq_7
            ) subq_8
            WHERE metric_time__week > '2020-01-01'
          ) subq_9
          WHERE subq_9.metric_time__day BETWEEN '2020-01-03' AND '2020-01-05'
        ) subq_10
      ) subq_11
      LEFT OUTER JOIN (
        -- Aggregate Measures
        SELECT
          subq_5.metric_time__day
          , SUM(subq_5.bookings) AS bookings
        FROM (
          -- Pass Only Elements: ['bookings', 'metric_time__day']
          SELECT
            subq_4.metric_time__day
            , subq_4.bookings
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_3.ds__day
              , subq_3.ds__week
              , subq_3.ds__month
              , subq_3.ds__quarter
              , subq_3.ds__year
              , subq_3.ds__extract_year
              , subq_3.ds__extract_quarter
              , subq_3.ds__extract_month
              , subq_3.ds__extract_day
              , subq_3.ds__extract_dow
              , subq_3.ds__extract_doy
              , subq_3.ds_partitioned__day
              , subq_3.ds_partitioned__week
              , subq_3.ds_partitioned__month
              , subq_3.ds_partitioned__quarter
              , subq_3.ds_partitioned__year
              , subq_3.ds_partitioned__extract_year
              , subq_3.ds_partitioned__extract_quarter
              , subq_3.ds_partitioned__extract_month
              , subq_3.ds_partitioned__extract_day
              , subq_3.ds_partitioned__extract_dow
              , subq_3.ds_partitioned__extract_doy
              , subq_3.paid_at__day
              , subq_3.paid_at__week
              , subq_3.paid_at__month
              , subq_3.paid_at__quarter
              , subq_3.paid_at__year
              , subq_3.paid_at__extract_year
              , subq_3.paid_at__extract_quarter
              , subq_3.paid_at__extract_month
              , subq_3.paid_at__extract_day
              , subq_3.paid_at__extract_dow
              , subq_3.paid_at__extract_doy
              , subq_3.booking__ds__day
              , subq_3.booking__ds__week
              , subq_3.booking__ds__month
              , subq_3.booking__ds__quarter
              , subq_3.booking__ds__year
              , subq_3.booking__ds__extract_year
              , subq_3.booking__ds__extract_quarter
              , subq_3.booking__ds__extract_month
              , subq_3.booking__ds__extract_day
              , subq_3.booking__ds__extract_dow
              , subq_3.booking__ds__extract_doy
              , subq_3.booking__ds_partitioned__day
              , subq_3.booking__ds_partitioned__week
              , subq_3.booking__ds_partitioned__month
              , subq_3.booking__ds_partitioned__quarter
              , subq_3.booking__ds_partitioned__year
              , subq_3.booking__ds_partitioned__extract_year
              , subq_3.booking__ds_partitioned__extract_quarter
              , subq_3.booking__ds_partitioned__extract_month
              , subq_3.booking__ds_partitioned__extract_day
              , subq_3.booking__ds_partitioned__extract_dow
              , subq_3.booking__ds_partitioned__extract_doy
              , subq_3.booking__paid_at__day
              , subq_3.booking__paid_at__week
              , subq_3.booking__paid_at__month
              , subq_3.booking__paid_at__quarter
              , subq_3.booking__paid_at__year
              , subq_3.booking__paid_at__extract_year
              , subq_3.booking__paid_at__extract_quarter
              , subq_3.booking__paid_at__extract_month
              , subq_3.booking__paid_at__extract_day
              , subq_3.booking__paid_at__extract_dow
              , subq_3.booking__paid_at__extract_doy
              , subq_3.metric_time__day
              , subq_3.metric_time__week
              , subq_3.metric_time__month
              , subq_3.metric_time__quarter
              , subq_3.metric_time__year
              , subq_3.metric_time__extract_year
              , subq_3.metric_time__extract_quarter
              , subq_3.metric_time__extract_month
              , subq_3.metric_time__extract_day
              , subq_3.metric_time__extract_dow
              , subq_3.metric_time__extract_doy
              , subq_3.listing
              , subq_3.guest
              , subq_3.host
              , subq_3.booking__listing
              , subq_3.booking__guest
              , subq_3.booking__host
              , subq_3.is_instant
              , subq_3.booking__is_instant
              , subq_3.bookings
              , subq_3.instant_bookings
              , subq_3.booking_value
              , subq_3.max_booking_value
              , subq_3.min_booking_value
              , subq_3.bookers
              , subq_3.average_booking_value
              , subq_3.referred_bookings
              , subq_3.median_booking_value
              , subq_3.booking_value_p99
              , subq_3.discrete_booking_value_p99
              , subq_3.approximate_continuous_booking_value_p99
              , subq_3.approximate_discrete_booking_value_p99
            FROM (
              -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
              SELECT
                subq_2.ds__day
                , subq_2.ds__week
                , subq_2.ds__month
                , subq_2.ds__quarter
                , subq_2.ds__year
                , subq_2.ds__extract_year
                , subq_2.ds__extract_quarter
                , subq_2.ds__extract_month
                , subq_2.ds__extract_day
                , subq_2.ds__extract_dow
                , subq_2.ds__extract_doy
                , subq_2.ds_partitioned__day
                , subq_2.ds_partitioned__week
                , subq_2.ds_partitioned__month
                , subq_2.ds_partitioned__quarter
                , subq_2.ds_partitioned__year
                , subq_2.ds_partitioned__extract_year
                , subq_2.ds_partitioned__extract_quarter
                , subq_2.ds_partitioned__extract_month
                , subq_2.ds_partitioned__extract_day
                , subq_2.ds_partitioned__extract_dow
                , subq_2.ds_partitioned__extract_doy
                , subq_2.paid_at__day
                , subq_2.paid_at__week
                , subq_2.paid_at__month
                , subq_2.paid_at__quarter
                , subq_2.paid_at__year
                , subq_2.paid_at__extract_year
                , subq_2.paid_at__extract_quarter
                , subq_2.paid_at__extract_month
                , subq_2.paid_at__extract_day
                , subq_2.paid_at__extract_dow
                , subq_2.paid_at__extract_doy
                , subq_2.booking__ds__day
                , subq_2.booking__ds__week
                , subq_2.booking__ds__month
                , subq_2.booking__ds__quarter
                , subq_2.booking__ds__year
                , subq_2.booking__ds__extract_year
                , subq_2.booking__ds__extract_quarter
                , subq_2.booking__ds__extract_month
                , subq_2.booking__ds__extract_day
                , subq_2.booking__ds__extract_dow
                , subq_2.booking__ds__extract_doy
                , subq_2.booking__ds_partitioned__day
                , subq_2.booking__ds_partitioned__week
                , subq_2.booking__ds_partitioned__month
                , subq_2.booking__ds_partitioned__quarter
                , subq_2.booking__ds_partitioned__year
                , subq_2.booking__ds_partitioned__extract_year
                , subq_2.booking__ds_partitioned__extract_quarter
                , subq_2.booking__ds_partitioned__extract_month
                , subq_2.booking__ds_partitioned__extract_day
                , subq_2.booking__ds_partitioned__extract_dow
                , subq_2.booking__ds_partitioned__extract_doy
                , subq_2.booking__paid_at__day
                , subq_2.booking__paid_at__week
                , subq_2.booking__paid_at__month
                , subq_2.booking__paid_at__quarter
                , subq_2.booking__paid_at__year
                , subq_2.booking__paid_at__extract_year
                , subq_2.booking__paid_at__extract_quarter
                , subq_2.booking__paid_at__extract_month
                , subq_2.booking__paid_at__extract_day
                , subq_2.booking__paid_at__extract_dow
                , subq_2.booking__paid_at__extract_doy
                , subq_2.metric_time__day
                , subq_2.metric_time__week
                , subq_2.metric_time__month
                , subq_2.metric_time__quarter
                , subq_2.metric_time__year
                , subq_2.metric_time__extract_year
                , subq_2.metric_time__extract_quarter
                , subq_2.metric_time__extract_month
                , subq_2.metric_time__extract_day
                , subq_2.metric_time__extract_dow
                , subq_2.metric_time__extract_doy
                , subq_2.listing
                , subq_2.guest
                , subq_2.host
                , subq_2.booking__listing
                , subq_2.booking__guest
                , subq_2.booking__host
                , subq_2.is_instant
                , subq_2.booking__is_instant
                , subq_2.bookings
                , subq_2.instant_bookings
                , subq_2.booking_value
                , subq_2.max_booking_value
                , subq_2.min_booking_value
                , subq_2.bookers
                , subq_2.average_booking_value
                , subq_2.referred_bookings
                , subq_2.median_booking_value
                , subq_2.booking_value_p99
                , subq_2.discrete_booking_value_p99
                , subq_2.approximate_continuous_booking_value_p99
                , subq_2.approximate_discrete_booking_value_p99
              FROM (
                -- Metric Time Dimension 'ds'
                SELECT
                  subq_1.ds__day
                  , subq_1.ds__week
                  , subq_1.ds__month
                  , subq_1.ds__quarter
                  , subq_1.ds__year
                  , subq_1.ds__extract_year
                  , subq_1.ds__extract_quarter
                  , subq_1.ds__extract_month
                  , subq_1.ds__extract_day
                  , subq_1.ds__extract_dow
                  , subq_1.ds__extract_doy
                  , subq_1.ds_partitioned__day
                  , subq_1.ds_partitioned__week
                  , subq_1.ds_partitioned__month
                  , subq_1.ds_partitioned__quarter
                  , subq_1.ds_partitioned__year
                  , subq_1.ds_partitioned__extract_year
                  , subq_1.ds_partitioned__extract_quarter
                  , subq_1.ds_partitioned__extract_month
                  , subq_1.ds_partitioned__extract_day
                  , subq_1.ds_partitioned__extract_dow
                  , subq_1.ds_partitioned__extract_doy
                  , subq_1.paid_at__day
                  , subq_1.paid_at__week
                  , subq_1.paid_at__month
                  , subq_1.paid_at__quarter
                  , subq_1.paid_at__year
                  , subq_1.paid_at__extract_year
                  , subq_1.paid_at__extract_quarter
                  , subq_1.paid_at__extract_month
                  , subq_1.paid_at__extract_day
                  , subq_1.paid_at__extract_dow
                  , subq_1.paid_at__extract_doy
                  , subq_1.booking__ds__day
                  , subq_1.booking__ds__week
                  , subq_1.booking__ds__month
                  , subq_1.booking__ds__quarter
                  , subq_1.booking__ds__year
                  , subq_1.booking__ds__extract_year
                  , subq_1.booking__ds__extract_quarter
                  , subq_1.booking__ds__extract_month
                  , subq_1.booking__ds__extract_day
                  , subq_1.booking__ds__extract_dow
                  , subq_1.booking__ds__extract_doy
                  , subq_1.booking__ds_partitioned__day
                  , subq_1.booking__ds_partitioned__week
                  , subq_1.booking__ds_partitioned__month
                  , subq_1.booking__ds_partitioned__quarter
                  , subq_1.booking__ds_partitioned__year
                  , subq_1.booking__ds_partitioned__extract_year
                  , subq_1.booking__ds_partitioned__extract_quarter
                  , subq_1.booking__ds_partitioned__extract_month
                  , subq_1.booking__ds_partitioned__extract_day
                  , subq_1.booking__ds_partitioned__extract_dow
                  , subq_1.booking__ds_partitioned__extract_doy
                  , subq_1.booking__paid_at__day
                  , subq_1.booking__paid_at__week
                  , subq_1.booking__paid_at__month
                  , subq_1.booking__paid_at__quarter
                  , subq_1.booking__paid_at__year
                  , subq_1.booking__paid_at__extract_year
                  , subq_1.booking__paid_at__extract_quarter
                  , subq_1.booking__paid_at__extract_month
                  , subq_1.booking__paid_at__extract_day
                  , subq_1.booking__paid_at__extract_dow
                  , subq_1.booking__paid_at__extract_doy
                  , subq_1.ds__day AS metric_time__day
                  , subq_1.ds__week AS metric_time__week
                  , subq_1.ds__month AS metric_time__month
                  , subq_1.ds__quarter AS metric_time__quarter
                  , subq_1.ds__year AS metric_time__year
                  , subq_1.ds__extract_year AS metric_time__extract_year
                  , subq_1.ds__extract_quarter AS metric_time__extract_quarter
                  , subq_1.ds__extract_month AS metric_time__extract_month
                  , subq_1.ds__extract_day AS metric_time__extract_day
                  , subq_1.ds__extract_dow AS metric_time__extract_dow
                  , subq_1.ds__extract_doy AS metric_time__extract_doy
                  , subq_1.listing
                  , subq_1.guest
                  , subq_1.host
                  , subq_1.booking__listing
                  , subq_1.booking__guest
                  , subq_1.booking__host
                  , subq_1.is_instant
                  , subq_1.booking__is_instant
                  , subq_1.bookings
                  , subq_1.instant_bookings
                  , subq_1.booking_value
                  , subq_1.max_booking_value
                  , subq_1.min_booking_value
                  , subq_1.bookers
                  , subq_1.average_booking_value
                  , subq_1.referred_bookings
                  , subq_1.median_booking_value
                  , subq_1.booking_value_p99
                  , subq_1.discrete_booking_value_p99
                  , subq_1.approximate_continuous_booking_value_p99
                  , subq_1.approximate_discrete_booking_value_p99
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
                ) subq_1
              ) subq_2
              WHERE subq_2.metric_time__day BETWEEN '2020-01-03' AND '2020-01-05'
            ) subq_3
            WHERE metric_time__week > '2020-01-01'
          ) subq_4
        ) subq_5
        GROUP BY
          metric_time__day
      ) subq_6
      ON
        subq_11.metric_time__day = subq_6.metric_time__day
    ) subq_12
    WHERE subq_12.metric_time__day BETWEEN '2020-01-03' AND '2020-01-05'
  ) subq_13
) subq_14
