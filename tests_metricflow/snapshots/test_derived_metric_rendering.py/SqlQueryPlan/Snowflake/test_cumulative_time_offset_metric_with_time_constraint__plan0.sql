test_name: test_cumulative_time_offset_metric_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
SELECT
  subq_11.metric_time__day
  , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_10.metric_time__day
    , subq_10.bookers AS every_2_days_bookers_2_days_ago
  FROM (
    -- Aggregate Measures
    SELECT
      subq_9.metric_time__day
      , COUNT(DISTINCT subq_9.bookers) AS bookers
    FROM (
      -- Pass Only Elements: ['bookers', 'metric_time__day']
      SELECT
        subq_8.metric_time__day
        , subq_8.bookers
      FROM (
        -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
        SELECT
          subq_7.metric_time__day
          , subq_7.ds__day
          , subq_7.ds__week
          , subq_7.ds__month
          , subq_7.ds__quarter
          , subq_7.ds__year
          , subq_7.ds__extract_year
          , subq_7.ds__extract_quarter
          , subq_7.ds__extract_month
          , subq_7.ds__extract_day
          , subq_7.ds__extract_dow
          , subq_7.ds__extract_doy
          , subq_7.ds_partitioned__day
          , subq_7.ds_partitioned__week
          , subq_7.ds_partitioned__month
          , subq_7.ds_partitioned__quarter
          , subq_7.ds_partitioned__year
          , subq_7.ds_partitioned__extract_year
          , subq_7.ds_partitioned__extract_quarter
          , subq_7.ds_partitioned__extract_month
          , subq_7.ds_partitioned__extract_day
          , subq_7.ds_partitioned__extract_dow
          , subq_7.ds_partitioned__extract_doy
          , subq_7.paid_at__day
          , subq_7.paid_at__week
          , subq_7.paid_at__month
          , subq_7.paid_at__quarter
          , subq_7.paid_at__year
          , subq_7.paid_at__extract_year
          , subq_7.paid_at__extract_quarter
          , subq_7.paid_at__extract_month
          , subq_7.paid_at__extract_day
          , subq_7.paid_at__extract_dow
          , subq_7.paid_at__extract_doy
          , subq_7.booking__ds__day
          , subq_7.booking__ds__week
          , subq_7.booking__ds__month
          , subq_7.booking__ds__quarter
          , subq_7.booking__ds__year
          , subq_7.booking__ds__extract_year
          , subq_7.booking__ds__extract_quarter
          , subq_7.booking__ds__extract_month
          , subq_7.booking__ds__extract_day
          , subq_7.booking__ds__extract_dow
          , subq_7.booking__ds__extract_doy
          , subq_7.booking__ds_partitioned__day
          , subq_7.booking__ds_partitioned__week
          , subq_7.booking__ds_partitioned__month
          , subq_7.booking__ds_partitioned__quarter
          , subq_7.booking__ds_partitioned__year
          , subq_7.booking__ds_partitioned__extract_year
          , subq_7.booking__ds_partitioned__extract_quarter
          , subq_7.booking__ds_partitioned__extract_month
          , subq_7.booking__ds_partitioned__extract_day
          , subq_7.booking__ds_partitioned__extract_dow
          , subq_7.booking__ds_partitioned__extract_doy
          , subq_7.booking__paid_at__day
          , subq_7.booking__paid_at__week
          , subq_7.booking__paid_at__month
          , subq_7.booking__paid_at__quarter
          , subq_7.booking__paid_at__year
          , subq_7.booking__paid_at__extract_year
          , subq_7.booking__paid_at__extract_quarter
          , subq_7.booking__paid_at__extract_month
          , subq_7.booking__paid_at__extract_day
          , subq_7.booking__paid_at__extract_dow
          , subq_7.booking__paid_at__extract_doy
          , subq_7.metric_time__week
          , subq_7.metric_time__month
          , subq_7.metric_time__quarter
          , subq_7.metric_time__year
          , subq_7.metric_time__extract_year
          , subq_7.metric_time__extract_quarter
          , subq_7.metric_time__extract_month
          , subq_7.metric_time__extract_day
          , subq_7.metric_time__extract_dow
          , subq_7.metric_time__extract_doy
          , subq_7.listing
          , subq_7.guest
          , subq_7.host
          , subq_7.booking__listing
          , subq_7.booking__guest
          , subq_7.booking__host
          , subq_7.is_instant
          , subq_7.booking__is_instant
          , subq_7.bookings
          , subq_7.instant_bookings
          , subq_7.booking_value
          , subq_7.max_booking_value
          , subq_7.min_booking_value
          , subq_7.bookers
          , subq_7.average_booking_value
          , subq_7.referred_bookings
          , subq_7.median_booking_value
          , subq_7.booking_value_p99
          , subq_7.discrete_booking_value_p99
          , subq_7.approximate_continuous_booking_value_p99
          , subq_7.approximate_discrete_booking_value_p99
        FROM (
          -- Join to Time Spine Dataset
          SELECT
            subq_4.ds__day AS ds__day
            , subq_4.ds__week AS ds__week
            , subq_4.ds__month AS ds__month
            , subq_4.ds__quarter AS ds__quarter
            , subq_4.ds__year AS ds__year
            , subq_4.ds__extract_year AS ds__extract_year
            , subq_4.ds__extract_quarter AS ds__extract_quarter
            , subq_4.ds__extract_month AS ds__extract_month
            , subq_4.ds__extract_day AS ds__extract_day
            , subq_4.ds__extract_dow AS ds__extract_dow
            , subq_4.ds__extract_doy AS ds__extract_doy
            , subq_4.ds_partitioned__day AS ds_partitioned__day
            , subq_4.ds_partitioned__week AS ds_partitioned__week
            , subq_4.ds_partitioned__month AS ds_partitioned__month
            , subq_4.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_4.ds_partitioned__year AS ds_partitioned__year
            , subq_4.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , subq_4.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , subq_4.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , subq_4.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , subq_4.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , subq_4.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , subq_4.paid_at__day AS paid_at__day
            , subq_4.paid_at__week AS paid_at__week
            , subq_4.paid_at__month AS paid_at__month
            , subq_4.paid_at__quarter AS paid_at__quarter
            , subq_4.paid_at__year AS paid_at__year
            , subq_4.paid_at__extract_year AS paid_at__extract_year
            , subq_4.paid_at__extract_quarter AS paid_at__extract_quarter
            , subq_4.paid_at__extract_month AS paid_at__extract_month
            , subq_4.paid_at__extract_day AS paid_at__extract_day
            , subq_4.paid_at__extract_dow AS paid_at__extract_dow
            , subq_4.paid_at__extract_doy AS paid_at__extract_doy
            , subq_4.booking__ds__day AS booking__ds__day
            , subq_4.booking__ds__week AS booking__ds__week
            , subq_4.booking__ds__month AS booking__ds__month
            , subq_4.booking__ds__quarter AS booking__ds__quarter
            , subq_4.booking__ds__year AS booking__ds__year
            , subq_4.booking__ds__extract_year AS booking__ds__extract_year
            , subq_4.booking__ds__extract_quarter AS booking__ds__extract_quarter
            , subq_4.booking__ds__extract_month AS booking__ds__extract_month
            , subq_4.booking__ds__extract_day AS booking__ds__extract_day
            , subq_4.booking__ds__extract_dow AS booking__ds__extract_dow
            , subq_4.booking__ds__extract_doy AS booking__ds__extract_doy
            , subq_4.booking__ds_partitioned__day AS booking__ds_partitioned__day
            , subq_4.booking__ds_partitioned__week AS booking__ds_partitioned__week
            , subq_4.booking__ds_partitioned__month AS booking__ds_partitioned__month
            , subq_4.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
            , subq_4.booking__ds_partitioned__year AS booking__ds_partitioned__year
            , subq_4.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
            , subq_4.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
            , subq_4.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
            , subq_4.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
            , subq_4.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
            , subq_4.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
            , subq_4.booking__paid_at__day AS booking__paid_at__day
            , subq_4.booking__paid_at__week AS booking__paid_at__week
            , subq_4.booking__paid_at__month AS booking__paid_at__month
            , subq_4.booking__paid_at__quarter AS booking__paid_at__quarter
            , subq_4.booking__paid_at__year AS booking__paid_at__year
            , subq_4.booking__paid_at__extract_year AS booking__paid_at__extract_year
            , subq_4.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
            , subq_4.booking__paid_at__extract_month AS booking__paid_at__extract_month
            , subq_4.booking__paid_at__extract_day AS booking__paid_at__extract_day
            , subq_4.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
            , subq_4.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
            , subq_4.metric_time__week AS metric_time__week
            , subq_4.metric_time__month AS metric_time__month
            , subq_4.metric_time__quarter AS metric_time__quarter
            , subq_4.metric_time__year AS metric_time__year
            , subq_4.metric_time__extract_year AS metric_time__extract_year
            , subq_4.metric_time__extract_quarter AS metric_time__extract_quarter
            , subq_4.metric_time__extract_month AS metric_time__extract_month
            , subq_4.metric_time__extract_day AS metric_time__extract_day
            , subq_4.metric_time__extract_dow AS metric_time__extract_dow
            , subq_4.metric_time__extract_doy AS metric_time__extract_doy
            , subq_5.metric_time__day AS metric_time__day
            , subq_4.listing AS listing
            , subq_4.guest AS guest
            , subq_4.host AS host
            , subq_4.booking__listing AS booking__listing
            , subq_4.booking__guest AS booking__guest
            , subq_4.booking__host AS booking__host
            , subq_4.is_instant AS is_instant
            , subq_4.booking__is_instant AS booking__is_instant
            , subq_4.bookings AS bookings
            , subq_4.instant_bookings AS instant_bookings
            , subq_4.booking_value AS booking_value
            , subq_4.max_booking_value AS max_booking_value
            , subq_4.min_booking_value AS min_booking_value
            , subq_4.bookers AS bookers
            , subq_4.average_booking_value AS average_booking_value
            , subq_4.referred_bookings AS referred_bookings
            , subq_4.median_booking_value AS median_booking_value
            , subq_4.booking_value_p99 AS booking_value_p99
            , subq_4.discrete_booking_value_p99 AS discrete_booking_value_p99
            , subq_4.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
            , subq_4.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
          FROM (
            -- Read From Time Spine 'mf_time_spine'
            SELECT
              subq_6.ds AS metric_time__day
            FROM ***************************.mf_time_spine subq_6
          ) subq_5
          INNER JOIN (
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
                  , DATE_TRUNC('day', bookings_source_src_28000.ds) AS ds__day
                  , DATE_TRUNC('week', bookings_source_src_28000.ds) AS ds__week
                  , DATE_TRUNC('month', bookings_source_src_28000.ds) AS ds__month
                  , DATE_TRUNC('quarter', bookings_source_src_28000.ds) AS ds__quarter
                  , DATE_TRUNC('year', bookings_source_src_28000.ds) AS ds__year
                  , EXTRACT(year FROM bookings_source_src_28000.ds) AS ds__extract_year
                  , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS ds__extract_quarter
                  , EXTRACT(month FROM bookings_source_src_28000.ds) AS ds__extract_month
                  , EXTRACT(day FROM bookings_source_src_28000.ds) AS ds__extract_day
                  , EXTRACT(dayofweekiso FROM bookings_source_src_28000.ds) AS ds__extract_dow
                  , EXTRACT(doy FROM bookings_source_src_28000.ds) AS ds__extract_doy
                  , DATE_TRUNC('day', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__day
                  , DATE_TRUNC('week', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__week
                  , DATE_TRUNC('month', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__month
                  , DATE_TRUNC('quarter', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                  , DATE_TRUNC('year', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__year
                  , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                  , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                  , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                  , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                  , EXTRACT(dayofweekiso FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
                  , EXTRACT(doy FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                  , DATE_TRUNC('day', bookings_source_src_28000.paid_at) AS paid_at__day
                  , DATE_TRUNC('week', bookings_source_src_28000.paid_at) AS paid_at__week
                  , DATE_TRUNC('month', bookings_source_src_28000.paid_at) AS paid_at__month
                  , DATE_TRUNC('quarter', bookings_source_src_28000.paid_at) AS paid_at__quarter
                  , DATE_TRUNC('year', bookings_source_src_28000.paid_at) AS paid_at__year
                  , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS paid_at__extract_year
                  , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
                  , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS paid_at__extract_month
                  , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS paid_at__extract_day
                  , EXTRACT(dayofweekiso FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
                  , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS paid_at__extract_doy
                  , bookings_source_src_28000.is_instant AS booking__is_instant
                  , DATE_TRUNC('day', bookings_source_src_28000.ds) AS booking__ds__day
                  , DATE_TRUNC('week', bookings_source_src_28000.ds) AS booking__ds__week
                  , DATE_TRUNC('month', bookings_source_src_28000.ds) AS booking__ds__month
                  , DATE_TRUNC('quarter', bookings_source_src_28000.ds) AS booking__ds__quarter
                  , DATE_TRUNC('year', bookings_source_src_28000.ds) AS booking__ds__year
                  , EXTRACT(year FROM bookings_source_src_28000.ds) AS booking__ds__extract_year
                  , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS booking__ds__extract_quarter
                  , EXTRACT(month FROM bookings_source_src_28000.ds) AS booking__ds__extract_month
                  , EXTRACT(day FROM bookings_source_src_28000.ds) AS booking__ds__extract_day
                  , EXTRACT(dayofweekiso FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
                  , EXTRACT(doy FROM bookings_source_src_28000.ds) AS booking__ds__extract_doy
                  , DATE_TRUNC('day', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__day
                  , DATE_TRUNC('week', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__week
                  , DATE_TRUNC('month', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__month
                  , DATE_TRUNC('quarter', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__quarter
                  , DATE_TRUNC('year', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__year
                  , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
                  , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                  , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
                  , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
                  , EXTRACT(dayofweekiso FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
                  , EXTRACT(doy FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
                  , DATE_TRUNC('day', bookings_source_src_28000.paid_at) AS booking__paid_at__day
                  , DATE_TRUNC('week', bookings_source_src_28000.paid_at) AS booking__paid_at__week
                  , DATE_TRUNC('month', bookings_source_src_28000.paid_at) AS booking__paid_at__month
                  , DATE_TRUNC('quarter', bookings_source_src_28000.paid_at) AS booking__paid_at__quarter
                  , DATE_TRUNC('year', bookings_source_src_28000.paid_at) AS booking__paid_at__year
                  , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_year
                  , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_quarter
                  , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_month
                  , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_day
                  , EXTRACT(dayofweekiso FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
                  , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
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
                subq_1.metric_time__day > DATEADD(day, -2, subq_2.metric_time__day)
              )
          ) subq_4
          ON
            DATEADD(day, -2, subq_5.metric_time__day) = subq_4.metric_time__day
        ) subq_7
        WHERE subq_7.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
      ) subq_8
    ) subq_9
    GROUP BY
      subq_9.metric_time__day
  ) subq_10
) subq_11
