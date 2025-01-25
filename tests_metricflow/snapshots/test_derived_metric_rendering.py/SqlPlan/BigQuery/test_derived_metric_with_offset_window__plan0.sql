test_name: test_derived_metric_with_offset_window
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_12.metric_time__day
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_3.metric_time__day, nr_subq_11.metric_time__day) AS metric_time__day
    , MAX(nr_subq_3.bookings) AS bookings
    , MAX(nr_subq_11.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      nr_subq_2.metric_time__day
      , nr_subq_2.bookings
    FROM (
      -- Aggregate Measures
      SELECT
        nr_subq_1.metric_time__day
        , SUM(nr_subq_1.bookings) AS bookings
      FROM (
        -- Pass Only Elements: ['bookings', 'metric_time__day']
        SELECT
          nr_subq_0.metric_time__day
          , nr_subq_0.bookings
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            nr_subq_28002.ds__day
            , nr_subq_28002.ds__week
            , nr_subq_28002.ds__month
            , nr_subq_28002.ds__quarter
            , nr_subq_28002.ds__year
            , nr_subq_28002.ds__extract_year
            , nr_subq_28002.ds__extract_quarter
            , nr_subq_28002.ds__extract_month
            , nr_subq_28002.ds__extract_day
            , nr_subq_28002.ds__extract_dow
            , nr_subq_28002.ds__extract_doy
            , nr_subq_28002.ds_partitioned__day
            , nr_subq_28002.ds_partitioned__week
            , nr_subq_28002.ds_partitioned__month
            , nr_subq_28002.ds_partitioned__quarter
            , nr_subq_28002.ds_partitioned__year
            , nr_subq_28002.ds_partitioned__extract_year
            , nr_subq_28002.ds_partitioned__extract_quarter
            , nr_subq_28002.ds_partitioned__extract_month
            , nr_subq_28002.ds_partitioned__extract_day
            , nr_subq_28002.ds_partitioned__extract_dow
            , nr_subq_28002.ds_partitioned__extract_doy
            , nr_subq_28002.paid_at__day
            , nr_subq_28002.paid_at__week
            , nr_subq_28002.paid_at__month
            , nr_subq_28002.paid_at__quarter
            , nr_subq_28002.paid_at__year
            , nr_subq_28002.paid_at__extract_year
            , nr_subq_28002.paid_at__extract_quarter
            , nr_subq_28002.paid_at__extract_month
            , nr_subq_28002.paid_at__extract_day
            , nr_subq_28002.paid_at__extract_dow
            , nr_subq_28002.paid_at__extract_doy
            , nr_subq_28002.booking__ds__day
            , nr_subq_28002.booking__ds__week
            , nr_subq_28002.booking__ds__month
            , nr_subq_28002.booking__ds__quarter
            , nr_subq_28002.booking__ds__year
            , nr_subq_28002.booking__ds__extract_year
            , nr_subq_28002.booking__ds__extract_quarter
            , nr_subq_28002.booking__ds__extract_month
            , nr_subq_28002.booking__ds__extract_day
            , nr_subq_28002.booking__ds__extract_dow
            , nr_subq_28002.booking__ds__extract_doy
            , nr_subq_28002.booking__ds_partitioned__day
            , nr_subq_28002.booking__ds_partitioned__week
            , nr_subq_28002.booking__ds_partitioned__month
            , nr_subq_28002.booking__ds_partitioned__quarter
            , nr_subq_28002.booking__ds_partitioned__year
            , nr_subq_28002.booking__ds_partitioned__extract_year
            , nr_subq_28002.booking__ds_partitioned__extract_quarter
            , nr_subq_28002.booking__ds_partitioned__extract_month
            , nr_subq_28002.booking__ds_partitioned__extract_day
            , nr_subq_28002.booking__ds_partitioned__extract_dow
            , nr_subq_28002.booking__ds_partitioned__extract_doy
            , nr_subq_28002.booking__paid_at__day
            , nr_subq_28002.booking__paid_at__week
            , nr_subq_28002.booking__paid_at__month
            , nr_subq_28002.booking__paid_at__quarter
            , nr_subq_28002.booking__paid_at__year
            , nr_subq_28002.booking__paid_at__extract_year
            , nr_subq_28002.booking__paid_at__extract_quarter
            , nr_subq_28002.booking__paid_at__extract_month
            , nr_subq_28002.booking__paid_at__extract_day
            , nr_subq_28002.booking__paid_at__extract_dow
            , nr_subq_28002.booking__paid_at__extract_doy
            , nr_subq_28002.ds__day AS metric_time__day
            , nr_subq_28002.ds__week AS metric_time__week
            , nr_subq_28002.ds__month AS metric_time__month
            , nr_subq_28002.ds__quarter AS metric_time__quarter
            , nr_subq_28002.ds__year AS metric_time__year
            , nr_subq_28002.ds__extract_year AS metric_time__extract_year
            , nr_subq_28002.ds__extract_quarter AS metric_time__extract_quarter
            , nr_subq_28002.ds__extract_month AS metric_time__extract_month
            , nr_subq_28002.ds__extract_day AS metric_time__extract_day
            , nr_subq_28002.ds__extract_dow AS metric_time__extract_dow
            , nr_subq_28002.ds__extract_doy AS metric_time__extract_doy
            , nr_subq_28002.listing
            , nr_subq_28002.guest
            , nr_subq_28002.host
            , nr_subq_28002.booking__listing
            , nr_subq_28002.booking__guest
            , nr_subq_28002.booking__host
            , nr_subq_28002.is_instant
            , nr_subq_28002.booking__is_instant
            , nr_subq_28002.bookings
            , nr_subq_28002.instant_bookings
            , nr_subq_28002.booking_value
            , nr_subq_28002.max_booking_value
            , nr_subq_28002.min_booking_value
            , nr_subq_28002.bookers
            , nr_subq_28002.average_booking_value
            , nr_subq_28002.referred_bookings
            , nr_subq_28002.median_booking_value
            , nr_subq_28002.booking_value_p99
            , nr_subq_28002.discrete_booking_value_p99
            , nr_subq_28002.approximate_continuous_booking_value_p99
            , nr_subq_28002.approximate_discrete_booking_value_p99
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
          ) nr_subq_28002
        ) nr_subq_0
      ) nr_subq_1
      GROUP BY
        metric_time__day
    ) nr_subq_2
  ) nr_subq_3
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      nr_subq_10.metric_time__day
      , nr_subq_10.bookings AS bookings_2_weeks_ago
    FROM (
      -- Aggregate Measures
      SELECT
        nr_subq_9.metric_time__day
        , SUM(nr_subq_9.bookings) AS bookings
      FROM (
        -- Pass Only Elements: ['bookings', 'metric_time__day']
        SELECT
          nr_subq_8.metric_time__day
          , nr_subq_8.bookings
        FROM (
          -- Join to Time Spine Dataset
          SELECT
            nr_subq_7.metric_time__day AS metric_time__day
            , nr_subq_4.ds__day AS ds__day
            , nr_subq_4.ds__week AS ds__week
            , nr_subq_4.ds__month AS ds__month
            , nr_subq_4.ds__quarter AS ds__quarter
            , nr_subq_4.ds__year AS ds__year
            , nr_subq_4.ds__extract_year AS ds__extract_year
            , nr_subq_4.ds__extract_quarter AS ds__extract_quarter
            , nr_subq_4.ds__extract_month AS ds__extract_month
            , nr_subq_4.ds__extract_day AS ds__extract_day
            , nr_subq_4.ds__extract_dow AS ds__extract_dow
            , nr_subq_4.ds__extract_doy AS ds__extract_doy
            , nr_subq_4.ds_partitioned__day AS ds_partitioned__day
            , nr_subq_4.ds_partitioned__week AS ds_partitioned__week
            , nr_subq_4.ds_partitioned__month AS ds_partitioned__month
            , nr_subq_4.ds_partitioned__quarter AS ds_partitioned__quarter
            , nr_subq_4.ds_partitioned__year AS ds_partitioned__year
            , nr_subq_4.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , nr_subq_4.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , nr_subq_4.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , nr_subq_4.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , nr_subq_4.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , nr_subq_4.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , nr_subq_4.paid_at__day AS paid_at__day
            , nr_subq_4.paid_at__week AS paid_at__week
            , nr_subq_4.paid_at__month AS paid_at__month
            , nr_subq_4.paid_at__quarter AS paid_at__quarter
            , nr_subq_4.paid_at__year AS paid_at__year
            , nr_subq_4.paid_at__extract_year AS paid_at__extract_year
            , nr_subq_4.paid_at__extract_quarter AS paid_at__extract_quarter
            , nr_subq_4.paid_at__extract_month AS paid_at__extract_month
            , nr_subq_4.paid_at__extract_day AS paid_at__extract_day
            , nr_subq_4.paid_at__extract_dow AS paid_at__extract_dow
            , nr_subq_4.paid_at__extract_doy AS paid_at__extract_doy
            , nr_subq_4.booking__ds__day AS booking__ds__day
            , nr_subq_4.booking__ds__week AS booking__ds__week
            , nr_subq_4.booking__ds__month AS booking__ds__month
            , nr_subq_4.booking__ds__quarter AS booking__ds__quarter
            , nr_subq_4.booking__ds__year AS booking__ds__year
            , nr_subq_4.booking__ds__extract_year AS booking__ds__extract_year
            , nr_subq_4.booking__ds__extract_quarter AS booking__ds__extract_quarter
            , nr_subq_4.booking__ds__extract_month AS booking__ds__extract_month
            , nr_subq_4.booking__ds__extract_day AS booking__ds__extract_day
            , nr_subq_4.booking__ds__extract_dow AS booking__ds__extract_dow
            , nr_subq_4.booking__ds__extract_doy AS booking__ds__extract_doy
            , nr_subq_4.booking__ds_partitioned__day AS booking__ds_partitioned__day
            , nr_subq_4.booking__ds_partitioned__week AS booking__ds_partitioned__week
            , nr_subq_4.booking__ds_partitioned__month AS booking__ds_partitioned__month
            , nr_subq_4.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
            , nr_subq_4.booking__ds_partitioned__year AS booking__ds_partitioned__year
            , nr_subq_4.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
            , nr_subq_4.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
            , nr_subq_4.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
            , nr_subq_4.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
            , nr_subq_4.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
            , nr_subq_4.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
            , nr_subq_4.booking__paid_at__day AS booking__paid_at__day
            , nr_subq_4.booking__paid_at__week AS booking__paid_at__week
            , nr_subq_4.booking__paid_at__month AS booking__paid_at__month
            , nr_subq_4.booking__paid_at__quarter AS booking__paid_at__quarter
            , nr_subq_4.booking__paid_at__year AS booking__paid_at__year
            , nr_subq_4.booking__paid_at__extract_year AS booking__paid_at__extract_year
            , nr_subq_4.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
            , nr_subq_4.booking__paid_at__extract_month AS booking__paid_at__extract_month
            , nr_subq_4.booking__paid_at__extract_day AS booking__paid_at__extract_day
            , nr_subq_4.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
            , nr_subq_4.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
            , nr_subq_4.metric_time__week AS metric_time__week
            , nr_subq_4.metric_time__month AS metric_time__month
            , nr_subq_4.metric_time__quarter AS metric_time__quarter
            , nr_subq_4.metric_time__year AS metric_time__year
            , nr_subq_4.metric_time__extract_year AS metric_time__extract_year
            , nr_subq_4.metric_time__extract_quarter AS metric_time__extract_quarter
            , nr_subq_4.metric_time__extract_month AS metric_time__extract_month
            , nr_subq_4.metric_time__extract_day AS metric_time__extract_day
            , nr_subq_4.metric_time__extract_dow AS metric_time__extract_dow
            , nr_subq_4.metric_time__extract_doy AS metric_time__extract_doy
            , nr_subq_4.listing AS listing
            , nr_subq_4.guest AS guest
            , nr_subq_4.host AS host
            , nr_subq_4.booking__listing AS booking__listing
            , nr_subq_4.booking__guest AS booking__guest
            , nr_subq_4.booking__host AS booking__host
            , nr_subq_4.is_instant AS is_instant
            , nr_subq_4.booking__is_instant AS booking__is_instant
            , nr_subq_4.bookings AS bookings
            , nr_subq_4.instant_bookings AS instant_bookings
            , nr_subq_4.booking_value AS booking_value
            , nr_subq_4.max_booking_value AS max_booking_value
            , nr_subq_4.min_booking_value AS min_booking_value
            , nr_subq_4.bookers AS bookers
            , nr_subq_4.average_booking_value AS average_booking_value
            , nr_subq_4.referred_bookings AS referred_bookings
            , nr_subq_4.median_booking_value AS median_booking_value
            , nr_subq_4.booking_value_p99 AS booking_value_p99
            , nr_subq_4.discrete_booking_value_p99 AS discrete_booking_value_p99
            , nr_subq_4.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
            , nr_subq_4.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
          FROM (
            -- Pass Only Elements: ['metric_time__day',]
            SELECT
              nr_subq_6.metric_time__day
            FROM (
              -- Change Column Aliases
              SELECT
                nr_subq_5.ds__day AS metric_time__day
                , nr_subq_5.ds__week
                , nr_subq_5.ds__month
                , nr_subq_5.ds__quarter
                , nr_subq_5.ds__year
                , nr_subq_5.ds__extract_year
                , nr_subq_5.ds__extract_quarter
                , nr_subq_5.ds__extract_month
                , nr_subq_5.ds__extract_day
                , nr_subq_5.ds__extract_dow
                , nr_subq_5.ds__extract_doy
                , nr_subq_5.ds__martian_day
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
                  , time_spine_src_28006.martian_day AS ds__martian_day
                FROM ***************************.mf_time_spine time_spine_src_28006
              ) nr_subq_5
            ) nr_subq_6
          ) nr_subq_7
          INNER JOIN (
            -- Metric Time Dimension 'ds'
            SELECT
              nr_subq_28002.ds__day
              , nr_subq_28002.ds__week
              , nr_subq_28002.ds__month
              , nr_subq_28002.ds__quarter
              , nr_subq_28002.ds__year
              , nr_subq_28002.ds__extract_year
              , nr_subq_28002.ds__extract_quarter
              , nr_subq_28002.ds__extract_month
              , nr_subq_28002.ds__extract_day
              , nr_subq_28002.ds__extract_dow
              , nr_subq_28002.ds__extract_doy
              , nr_subq_28002.ds_partitioned__day
              , nr_subq_28002.ds_partitioned__week
              , nr_subq_28002.ds_partitioned__month
              , nr_subq_28002.ds_partitioned__quarter
              , nr_subq_28002.ds_partitioned__year
              , nr_subq_28002.ds_partitioned__extract_year
              , nr_subq_28002.ds_partitioned__extract_quarter
              , nr_subq_28002.ds_partitioned__extract_month
              , nr_subq_28002.ds_partitioned__extract_day
              , nr_subq_28002.ds_partitioned__extract_dow
              , nr_subq_28002.ds_partitioned__extract_doy
              , nr_subq_28002.paid_at__day
              , nr_subq_28002.paid_at__week
              , nr_subq_28002.paid_at__month
              , nr_subq_28002.paid_at__quarter
              , nr_subq_28002.paid_at__year
              , nr_subq_28002.paid_at__extract_year
              , nr_subq_28002.paid_at__extract_quarter
              , nr_subq_28002.paid_at__extract_month
              , nr_subq_28002.paid_at__extract_day
              , nr_subq_28002.paid_at__extract_dow
              , nr_subq_28002.paid_at__extract_doy
              , nr_subq_28002.booking__ds__day
              , nr_subq_28002.booking__ds__week
              , nr_subq_28002.booking__ds__month
              , nr_subq_28002.booking__ds__quarter
              , nr_subq_28002.booking__ds__year
              , nr_subq_28002.booking__ds__extract_year
              , nr_subq_28002.booking__ds__extract_quarter
              , nr_subq_28002.booking__ds__extract_month
              , nr_subq_28002.booking__ds__extract_day
              , nr_subq_28002.booking__ds__extract_dow
              , nr_subq_28002.booking__ds__extract_doy
              , nr_subq_28002.booking__ds_partitioned__day
              , nr_subq_28002.booking__ds_partitioned__week
              , nr_subq_28002.booking__ds_partitioned__month
              , nr_subq_28002.booking__ds_partitioned__quarter
              , nr_subq_28002.booking__ds_partitioned__year
              , nr_subq_28002.booking__ds_partitioned__extract_year
              , nr_subq_28002.booking__ds_partitioned__extract_quarter
              , nr_subq_28002.booking__ds_partitioned__extract_month
              , nr_subq_28002.booking__ds_partitioned__extract_day
              , nr_subq_28002.booking__ds_partitioned__extract_dow
              , nr_subq_28002.booking__ds_partitioned__extract_doy
              , nr_subq_28002.booking__paid_at__day
              , nr_subq_28002.booking__paid_at__week
              , nr_subq_28002.booking__paid_at__month
              , nr_subq_28002.booking__paid_at__quarter
              , nr_subq_28002.booking__paid_at__year
              , nr_subq_28002.booking__paid_at__extract_year
              , nr_subq_28002.booking__paid_at__extract_quarter
              , nr_subq_28002.booking__paid_at__extract_month
              , nr_subq_28002.booking__paid_at__extract_day
              , nr_subq_28002.booking__paid_at__extract_dow
              , nr_subq_28002.booking__paid_at__extract_doy
              , nr_subq_28002.ds__day AS metric_time__day
              , nr_subq_28002.ds__week AS metric_time__week
              , nr_subq_28002.ds__month AS metric_time__month
              , nr_subq_28002.ds__quarter AS metric_time__quarter
              , nr_subq_28002.ds__year AS metric_time__year
              , nr_subq_28002.ds__extract_year AS metric_time__extract_year
              , nr_subq_28002.ds__extract_quarter AS metric_time__extract_quarter
              , nr_subq_28002.ds__extract_month AS metric_time__extract_month
              , nr_subq_28002.ds__extract_day AS metric_time__extract_day
              , nr_subq_28002.ds__extract_dow AS metric_time__extract_dow
              , nr_subq_28002.ds__extract_doy AS metric_time__extract_doy
              , nr_subq_28002.listing
              , nr_subq_28002.guest
              , nr_subq_28002.host
              , nr_subq_28002.booking__listing
              , nr_subq_28002.booking__guest
              , nr_subq_28002.booking__host
              , nr_subq_28002.is_instant
              , nr_subq_28002.booking__is_instant
              , nr_subq_28002.bookings
              , nr_subq_28002.instant_bookings
              , nr_subq_28002.booking_value
              , nr_subq_28002.max_booking_value
              , nr_subq_28002.min_booking_value
              , nr_subq_28002.bookers
              , nr_subq_28002.average_booking_value
              , nr_subq_28002.referred_bookings
              , nr_subq_28002.median_booking_value
              , nr_subq_28002.booking_value_p99
              , nr_subq_28002.discrete_booking_value_p99
              , nr_subq_28002.approximate_continuous_booking_value_p99
              , nr_subq_28002.approximate_discrete_booking_value_p99
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
            ) nr_subq_28002
          ) nr_subq_4
          ON
            DATE_SUB(CAST(nr_subq_7.metric_time__day AS DATETIME), INTERVAL 14 day) = nr_subq_4.metric_time__day
        ) nr_subq_8
      ) nr_subq_9
      GROUP BY
        metric_time__day
    ) nr_subq_10
  ) nr_subq_11
  ON
    nr_subq_3.metric_time__day = nr_subq_11.metric_time__day
  GROUP BY
    metric_time__day
) nr_subq_12
