test_name: test_fill_nulls_time_spine_metric_predicate_pushdown
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a metric with a time spine and fill_nulls_with enabled.

      TODO: support metric time filters
sql_engine: Trino
---
-- Compute Metrics via Expressions
SELECT
  subq_30.metric_time__day
  , subq_30.listing__country_latest
  , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_13.metric_time__day, subq_29.metric_time__day) AS metric_time__day
    , COALESCE(subq_13.listing__country_latest, subq_29.listing__country_latest) AS listing__country_latest
    , COALESCE(MAX(subq_13.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , COALESCE(MAX(subq_29.bookings_2_weeks_ago), 0) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_12.metric_time__day
      , subq_12.listing__country_latest
      , COALESCE(subq_12.bookings, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_11.metric_time__day AS metric_time__day
        , subq_8.listing__country_latest AS listing__country_latest
        , subq_8.bookings AS bookings
      FROM (
        -- Pass Only Elements: ['metric_time__day',]
        SELECT
          subq_10.metric_time__day
        FROM (
          -- Change Column Aliases
          SELECT
            subq_9.ds__day AS metric_time__day
            , subq_9.ds__week
            , subq_9.ds__month
            , subq_9.ds__quarter
            , subq_9.ds__year
            , subq_9.ds__extract_year
            , subq_9.ds__extract_quarter
            , subq_9.ds__extract_month
            , subq_9.ds__extract_day
            , subq_9.ds__extract_dow
            , subq_9.ds__extract_doy
            , subq_9.ds__martian_day
          FROM (
            -- Read From Time Spine 'mf_time_spine'
            SELECT
              time_spine_src_28006.ds AS ds__day
              , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
              , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
              , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
              , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
              , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
              , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
              , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
              , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
              , EXTRACT(DAY_OF_WEEK FROM time_spine_src_28006.ds) AS ds__extract_dow
              , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
              , time_spine_src_28006.martian_day AS ds__martian_day
            FROM ***************************.mf_time_spine time_spine_src_28006
          ) subq_9
        ) subq_10
      ) subq_11
      LEFT OUTER JOIN (
        -- Aggregate Measures
        SELECT
          subq_7.metric_time__day
          , subq_7.listing__country_latest
          , SUM(subq_7.bookings) AS bookings
        FROM (
          -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
          SELECT
            subq_6.metric_time__day
            , subq_6.listing__country_latest
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
              , subq_5.metric_time__day
              , subq_5.metric_time__week
              , subq_5.metric_time__month
              , subq_5.metric_time__quarter
              , subq_5.metric_time__year
              , subq_5.metric_time__extract_year
              , subq_5.metric_time__extract_quarter
              , subq_5.metric_time__extract_month
              , subq_5.metric_time__extract_day
              , subq_5.metric_time__extract_dow
              , subq_5.metric_time__extract_doy
              , subq_5.listing
              , subq_5.guest
              , subq_5.host
              , subq_5.booking__listing
              , subq_5.booking__guest
              , subq_5.booking__host
              , subq_5.is_instant
              , subq_5.booking__is_instant
              , subq_5.listing__country_latest
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
              -- Join Standard Outputs
              SELECT
                subq_4.country_latest AS listing__country_latest
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
                , subq_1.metric_time__day AS metric_time__day
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
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
              LEFT OUTER JOIN (
                -- Pass Only Elements: ['country_latest', 'listing']
                SELECT
                  subq_3.listing
                  , subq_3.country_latest
                FROM (
                  -- Metric Time Dimension 'ds'
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
                    , subq_2.created_at__day
                    , subq_2.created_at__week
                    , subq_2.created_at__month
                    , subq_2.created_at__quarter
                    , subq_2.created_at__year
                    , subq_2.created_at__extract_year
                    , subq_2.created_at__extract_quarter
                    , subq_2.created_at__extract_month
                    , subq_2.created_at__extract_day
                    , subq_2.created_at__extract_dow
                    , subq_2.created_at__extract_doy
                    , subq_2.listing__ds__day
                    , subq_2.listing__ds__week
                    , subq_2.listing__ds__month
                    , subq_2.listing__ds__quarter
                    , subq_2.listing__ds__year
                    , subq_2.listing__ds__extract_year
                    , subq_2.listing__ds__extract_quarter
                    , subq_2.listing__ds__extract_month
                    , subq_2.listing__ds__extract_day
                    , subq_2.listing__ds__extract_dow
                    , subq_2.listing__ds__extract_doy
                    , subq_2.listing__created_at__day
                    , subq_2.listing__created_at__week
                    , subq_2.listing__created_at__month
                    , subq_2.listing__created_at__quarter
                    , subq_2.listing__created_at__year
                    , subq_2.listing__created_at__extract_year
                    , subq_2.listing__created_at__extract_quarter
                    , subq_2.listing__created_at__extract_month
                    , subq_2.listing__created_at__extract_day
                    , subq_2.listing__created_at__extract_dow
                    , subq_2.listing__created_at__extract_doy
                    , subq_2.ds__day AS metric_time__day
                    , subq_2.ds__week AS metric_time__week
                    , subq_2.ds__month AS metric_time__month
                    , subq_2.ds__quarter AS metric_time__quarter
                    , subq_2.ds__year AS metric_time__year
                    , subq_2.ds__extract_year AS metric_time__extract_year
                    , subq_2.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_2.ds__extract_month AS metric_time__extract_month
                    , subq_2.ds__extract_day AS metric_time__extract_day
                    , subq_2.ds__extract_dow AS metric_time__extract_dow
                    , subq_2.ds__extract_doy AS metric_time__extract_doy
                    , subq_2.listing
                    , subq_2.user
                    , subq_2.listing__user
                    , subq_2.country_latest
                    , subq_2.is_lux_latest
                    , subq_2.capacity_latest
                    , subq_2.listing__country_latest
                    , subq_2.listing__is_lux_latest
                    , subq_2.listing__capacity_latest
                    , subq_2.listings
                    , subq_2.largest_listing
                    , subq_2.smallest_listing
                  FROM (
                    -- Read Elements From Semantic Model 'listings_latest'
                    SELECT
                      1 AS listings
                      , listings_latest_src_28000.capacity AS largest_listing
                      , listings_latest_src_28000.capacity AS smallest_listing
                      , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS ds__day
                      , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS ds__week
                      , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS ds__month
                      , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS ds__quarter
                      , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS ds__year
                      , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
                      , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS ds__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS ds__extract_doy
                      , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS created_at__day
                      , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS created_at__week
                      , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS created_at__month
                      , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS created_at__quarter
                      , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS created_at__year
                      , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
                      , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
                      , listings_latest_src_28000.country AS country_latest
                      , listings_latest_src_28000.is_lux AS is_lux_latest
                      , listings_latest_src_28000.capacity AS capacity_latest
                      , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__ds__day
                      , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__ds__week
                      , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__ds__month
                      , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__ds__quarter
                      , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__ds__year
                      , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
                      , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                      , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__created_at__day
                      , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__created_at__week
                      , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__created_at__month
                      , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__created_at__quarter
                      , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__created_at__year
                      , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_month
                      , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_day
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                      , listings_latest_src_28000.country AS listing__country_latest
                      , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                      , listings_latest_src_28000.capacity AS listing__capacity_latest
                      , listings_latest_src_28000.listing_id AS listing
                      , listings_latest_src_28000.user_id AS user
                      , listings_latest_src_28000.user_id AS listing__user
                    FROM ***************************.dim_listings_latest listings_latest_src_28000
                  ) subq_2
                ) subq_3
              ) subq_4
              ON
                subq_1.listing = subq_4.listing
            ) subq_5
            WHERE booking__is_instant
          ) subq_6
        ) subq_7
        GROUP BY
          subq_7.metric_time__day
          , subq_7.listing__country_latest
      ) subq_8
      ON
        subq_11.metric_time__day = subq_8.metric_time__day
    ) subq_12
  ) subq_13
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_28.metric_time__day
      , subq_28.listing__country_latest
      , COALESCE(subq_28.bookings, 0) AS bookings_2_weeks_ago
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_27.metric_time__day AS metric_time__day
        , subq_24.listing__country_latest AS listing__country_latest
        , subq_24.bookings AS bookings
      FROM (
        -- Pass Only Elements: ['metric_time__day',]
        SELECT
          subq_26.metric_time__day
        FROM (
          -- Change Column Aliases
          SELECT
            subq_25.ds__day AS metric_time__day
            , subq_25.ds__week
            , subq_25.ds__month
            , subq_25.ds__quarter
            , subq_25.ds__year
            , subq_25.ds__extract_year
            , subq_25.ds__extract_quarter
            , subq_25.ds__extract_month
            , subq_25.ds__extract_day
            , subq_25.ds__extract_dow
            , subq_25.ds__extract_doy
            , subq_25.ds__martian_day
          FROM (
            -- Read From Time Spine 'mf_time_spine'
            SELECT
              time_spine_src_28006.ds AS ds__day
              , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
              , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
              , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
              , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
              , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
              , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
              , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
              , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
              , EXTRACT(DAY_OF_WEEK FROM time_spine_src_28006.ds) AS ds__extract_dow
              , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
              , time_spine_src_28006.martian_day AS ds__martian_day
            FROM ***************************.mf_time_spine time_spine_src_28006
          ) subq_25
        ) subq_26
      ) subq_27
      LEFT OUTER JOIN (
        -- Aggregate Measures
        SELECT
          subq_23.metric_time__day
          , subq_23.listing__country_latest
          , SUM(subq_23.bookings) AS bookings
        FROM (
          -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
          SELECT
            subq_22.metric_time__day
            , subq_22.listing__country_latest
            , subq_22.bookings
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_21.ds__day
              , subq_21.ds__week
              , subq_21.ds__month
              , subq_21.ds__quarter
              , subq_21.ds__year
              , subq_21.ds__extract_year
              , subq_21.ds__extract_quarter
              , subq_21.ds__extract_month
              , subq_21.ds__extract_day
              , subq_21.ds__extract_dow
              , subq_21.ds__extract_doy
              , subq_21.ds_partitioned__day
              , subq_21.ds_partitioned__week
              , subq_21.ds_partitioned__month
              , subq_21.ds_partitioned__quarter
              , subq_21.ds_partitioned__year
              , subq_21.ds_partitioned__extract_year
              , subq_21.ds_partitioned__extract_quarter
              , subq_21.ds_partitioned__extract_month
              , subq_21.ds_partitioned__extract_day
              , subq_21.ds_partitioned__extract_dow
              , subq_21.ds_partitioned__extract_doy
              , subq_21.paid_at__day
              , subq_21.paid_at__week
              , subq_21.paid_at__month
              , subq_21.paid_at__quarter
              , subq_21.paid_at__year
              , subq_21.paid_at__extract_year
              , subq_21.paid_at__extract_quarter
              , subq_21.paid_at__extract_month
              , subq_21.paid_at__extract_day
              , subq_21.paid_at__extract_dow
              , subq_21.paid_at__extract_doy
              , subq_21.booking__ds__day
              , subq_21.booking__ds__week
              , subq_21.booking__ds__month
              , subq_21.booking__ds__quarter
              , subq_21.booking__ds__year
              , subq_21.booking__ds__extract_year
              , subq_21.booking__ds__extract_quarter
              , subq_21.booking__ds__extract_month
              , subq_21.booking__ds__extract_day
              , subq_21.booking__ds__extract_dow
              , subq_21.booking__ds__extract_doy
              , subq_21.booking__ds_partitioned__day
              , subq_21.booking__ds_partitioned__week
              , subq_21.booking__ds_partitioned__month
              , subq_21.booking__ds_partitioned__quarter
              , subq_21.booking__ds_partitioned__year
              , subq_21.booking__ds_partitioned__extract_year
              , subq_21.booking__ds_partitioned__extract_quarter
              , subq_21.booking__ds_partitioned__extract_month
              , subq_21.booking__ds_partitioned__extract_day
              , subq_21.booking__ds_partitioned__extract_dow
              , subq_21.booking__ds_partitioned__extract_doy
              , subq_21.booking__paid_at__day
              , subq_21.booking__paid_at__week
              , subq_21.booking__paid_at__month
              , subq_21.booking__paid_at__quarter
              , subq_21.booking__paid_at__year
              , subq_21.booking__paid_at__extract_year
              , subq_21.booking__paid_at__extract_quarter
              , subq_21.booking__paid_at__extract_month
              , subq_21.booking__paid_at__extract_day
              , subq_21.booking__paid_at__extract_dow
              , subq_21.booking__paid_at__extract_doy
              , subq_21.metric_time__week
              , subq_21.metric_time__month
              , subq_21.metric_time__quarter
              , subq_21.metric_time__year
              , subq_21.metric_time__extract_year
              , subq_21.metric_time__extract_quarter
              , subq_21.metric_time__extract_month
              , subq_21.metric_time__extract_day
              , subq_21.metric_time__extract_dow
              , subq_21.metric_time__extract_doy
              , subq_21.metric_time__day
              , subq_21.listing
              , subq_21.guest
              , subq_21.host
              , subq_21.booking__listing
              , subq_21.booking__guest
              , subq_21.booking__host
              , subq_21.is_instant
              , subq_21.booking__is_instant
              , subq_21.listing__country_latest
              , subq_21.bookings
              , subq_21.instant_bookings
              , subq_21.booking_value
              , subq_21.max_booking_value
              , subq_21.min_booking_value
              , subq_21.bookers
              , subq_21.average_booking_value
              , subq_21.referred_bookings
              , subq_21.median_booking_value
              , subq_21.booking_value_p99
              , subq_21.discrete_booking_value_p99
              , subq_21.approximate_continuous_booking_value_p99
              , subq_21.approximate_discrete_booking_value_p99
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_20.country_latest AS listing__country_latest
                , subq_18.ds__day AS ds__day
                , subq_18.ds__week AS ds__week
                , subq_18.ds__month AS ds__month
                , subq_18.ds__quarter AS ds__quarter
                , subq_18.ds__year AS ds__year
                , subq_18.ds__extract_year AS ds__extract_year
                , subq_18.ds__extract_quarter AS ds__extract_quarter
                , subq_18.ds__extract_month AS ds__extract_month
                , subq_18.ds__extract_day AS ds__extract_day
                , subq_18.ds__extract_dow AS ds__extract_dow
                , subq_18.ds__extract_doy AS ds__extract_doy
                , subq_18.ds_partitioned__day AS ds_partitioned__day
                , subq_18.ds_partitioned__week AS ds_partitioned__week
                , subq_18.ds_partitioned__month AS ds_partitioned__month
                , subq_18.ds_partitioned__quarter AS ds_partitioned__quarter
                , subq_18.ds_partitioned__year AS ds_partitioned__year
                , subq_18.ds_partitioned__extract_year AS ds_partitioned__extract_year
                , subq_18.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                , subq_18.ds_partitioned__extract_month AS ds_partitioned__extract_month
                , subq_18.ds_partitioned__extract_day AS ds_partitioned__extract_day
                , subq_18.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                , subq_18.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                , subq_18.paid_at__day AS paid_at__day
                , subq_18.paid_at__week AS paid_at__week
                , subq_18.paid_at__month AS paid_at__month
                , subq_18.paid_at__quarter AS paid_at__quarter
                , subq_18.paid_at__year AS paid_at__year
                , subq_18.paid_at__extract_year AS paid_at__extract_year
                , subq_18.paid_at__extract_quarter AS paid_at__extract_quarter
                , subq_18.paid_at__extract_month AS paid_at__extract_month
                , subq_18.paid_at__extract_day AS paid_at__extract_day
                , subq_18.paid_at__extract_dow AS paid_at__extract_dow
                , subq_18.paid_at__extract_doy AS paid_at__extract_doy
                , subq_18.booking__ds__day AS booking__ds__day
                , subq_18.booking__ds__week AS booking__ds__week
                , subq_18.booking__ds__month AS booking__ds__month
                , subq_18.booking__ds__quarter AS booking__ds__quarter
                , subq_18.booking__ds__year AS booking__ds__year
                , subq_18.booking__ds__extract_year AS booking__ds__extract_year
                , subq_18.booking__ds__extract_quarter AS booking__ds__extract_quarter
                , subq_18.booking__ds__extract_month AS booking__ds__extract_month
                , subq_18.booking__ds__extract_day AS booking__ds__extract_day
                , subq_18.booking__ds__extract_dow AS booking__ds__extract_dow
                , subq_18.booking__ds__extract_doy AS booking__ds__extract_doy
                , subq_18.booking__ds_partitioned__day AS booking__ds_partitioned__day
                , subq_18.booking__ds_partitioned__week AS booking__ds_partitioned__week
                , subq_18.booking__ds_partitioned__month AS booking__ds_partitioned__month
                , subq_18.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                , subq_18.booking__ds_partitioned__year AS booking__ds_partitioned__year
                , subq_18.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                , subq_18.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                , subq_18.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                , subq_18.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                , subq_18.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                , subq_18.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                , subq_18.booking__paid_at__day AS booking__paid_at__day
                , subq_18.booking__paid_at__week AS booking__paid_at__week
                , subq_18.booking__paid_at__month AS booking__paid_at__month
                , subq_18.booking__paid_at__quarter AS booking__paid_at__quarter
                , subq_18.booking__paid_at__year AS booking__paid_at__year
                , subq_18.booking__paid_at__extract_year AS booking__paid_at__extract_year
                , subq_18.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                , subq_18.booking__paid_at__extract_month AS booking__paid_at__extract_month
                , subq_18.booking__paid_at__extract_day AS booking__paid_at__extract_day
                , subq_18.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                , subq_18.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
                , subq_18.metric_time__week AS metric_time__week
                , subq_18.metric_time__month AS metric_time__month
                , subq_18.metric_time__quarter AS metric_time__quarter
                , subq_18.metric_time__year AS metric_time__year
                , subq_18.metric_time__extract_year AS metric_time__extract_year
                , subq_18.metric_time__extract_quarter AS metric_time__extract_quarter
                , subq_18.metric_time__extract_month AS metric_time__extract_month
                , subq_18.metric_time__extract_day AS metric_time__extract_day
                , subq_18.metric_time__extract_dow AS metric_time__extract_dow
                , subq_18.metric_time__extract_doy AS metric_time__extract_doy
                , subq_18.metric_time__day AS metric_time__day
                , subq_18.listing AS listing
                , subq_18.guest AS guest
                , subq_18.host AS host
                , subq_18.booking__listing AS booking__listing
                , subq_18.booking__guest AS booking__guest
                , subq_18.booking__host AS booking__host
                , subq_18.is_instant AS is_instant
                , subq_18.booking__is_instant AS booking__is_instant
                , subq_18.bookings AS bookings
                , subq_18.instant_bookings AS instant_bookings
                , subq_18.booking_value AS booking_value
                , subq_18.max_booking_value AS max_booking_value
                , subq_18.min_booking_value AS min_booking_value
                , subq_18.bookers AS bookers
                , subq_18.average_booking_value AS average_booking_value
                , subq_18.referred_bookings AS referred_bookings
                , subq_18.median_booking_value AS median_booking_value
                , subq_18.booking_value_p99 AS booking_value_p99
                , subq_18.discrete_booking_value_p99 AS discrete_booking_value_p99
                , subq_18.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
                , subq_18.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
              FROM (
                -- Join to Time Spine Dataset
                SELECT
                  subq_17.metric_time__day AS metric_time__day
                  , subq_14.ds__day AS ds__day
                  , subq_14.ds__week AS ds__week
                  , subq_14.ds__month AS ds__month
                  , subq_14.ds__quarter AS ds__quarter
                  , subq_14.ds__year AS ds__year
                  , subq_14.ds__extract_year AS ds__extract_year
                  , subq_14.ds__extract_quarter AS ds__extract_quarter
                  , subq_14.ds__extract_month AS ds__extract_month
                  , subq_14.ds__extract_day AS ds__extract_day
                  , subq_14.ds__extract_dow AS ds__extract_dow
                  , subq_14.ds__extract_doy AS ds__extract_doy
                  , subq_14.ds_partitioned__day AS ds_partitioned__day
                  , subq_14.ds_partitioned__week AS ds_partitioned__week
                  , subq_14.ds_partitioned__month AS ds_partitioned__month
                  , subq_14.ds_partitioned__quarter AS ds_partitioned__quarter
                  , subq_14.ds_partitioned__year AS ds_partitioned__year
                  , subq_14.ds_partitioned__extract_year AS ds_partitioned__extract_year
                  , subq_14.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                  , subq_14.ds_partitioned__extract_month AS ds_partitioned__extract_month
                  , subq_14.ds_partitioned__extract_day AS ds_partitioned__extract_day
                  , subq_14.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                  , subq_14.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                  , subq_14.paid_at__day AS paid_at__day
                  , subq_14.paid_at__week AS paid_at__week
                  , subq_14.paid_at__month AS paid_at__month
                  , subq_14.paid_at__quarter AS paid_at__quarter
                  , subq_14.paid_at__year AS paid_at__year
                  , subq_14.paid_at__extract_year AS paid_at__extract_year
                  , subq_14.paid_at__extract_quarter AS paid_at__extract_quarter
                  , subq_14.paid_at__extract_month AS paid_at__extract_month
                  , subq_14.paid_at__extract_day AS paid_at__extract_day
                  , subq_14.paid_at__extract_dow AS paid_at__extract_dow
                  , subq_14.paid_at__extract_doy AS paid_at__extract_doy
                  , subq_14.booking__ds__day AS booking__ds__day
                  , subq_14.booking__ds__week AS booking__ds__week
                  , subq_14.booking__ds__month AS booking__ds__month
                  , subq_14.booking__ds__quarter AS booking__ds__quarter
                  , subq_14.booking__ds__year AS booking__ds__year
                  , subq_14.booking__ds__extract_year AS booking__ds__extract_year
                  , subq_14.booking__ds__extract_quarter AS booking__ds__extract_quarter
                  , subq_14.booking__ds__extract_month AS booking__ds__extract_month
                  , subq_14.booking__ds__extract_day AS booking__ds__extract_day
                  , subq_14.booking__ds__extract_dow AS booking__ds__extract_dow
                  , subq_14.booking__ds__extract_doy AS booking__ds__extract_doy
                  , subq_14.booking__ds_partitioned__day AS booking__ds_partitioned__day
                  , subq_14.booking__ds_partitioned__week AS booking__ds_partitioned__week
                  , subq_14.booking__ds_partitioned__month AS booking__ds_partitioned__month
                  , subq_14.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                  , subq_14.booking__ds_partitioned__year AS booking__ds_partitioned__year
                  , subq_14.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                  , subq_14.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                  , subq_14.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                  , subq_14.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                  , subq_14.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                  , subq_14.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                  , subq_14.booking__paid_at__day AS booking__paid_at__day
                  , subq_14.booking__paid_at__week AS booking__paid_at__week
                  , subq_14.booking__paid_at__month AS booking__paid_at__month
                  , subq_14.booking__paid_at__quarter AS booking__paid_at__quarter
                  , subq_14.booking__paid_at__year AS booking__paid_at__year
                  , subq_14.booking__paid_at__extract_year AS booking__paid_at__extract_year
                  , subq_14.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                  , subq_14.booking__paid_at__extract_month AS booking__paid_at__extract_month
                  , subq_14.booking__paid_at__extract_day AS booking__paid_at__extract_day
                  , subq_14.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                  , subq_14.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
                  , subq_14.metric_time__week AS metric_time__week
                  , subq_14.metric_time__month AS metric_time__month
                  , subq_14.metric_time__quarter AS metric_time__quarter
                  , subq_14.metric_time__year AS metric_time__year
                  , subq_14.metric_time__extract_year AS metric_time__extract_year
                  , subq_14.metric_time__extract_quarter AS metric_time__extract_quarter
                  , subq_14.metric_time__extract_month AS metric_time__extract_month
                  , subq_14.metric_time__extract_day AS metric_time__extract_day
                  , subq_14.metric_time__extract_dow AS metric_time__extract_dow
                  , subq_14.metric_time__extract_doy AS metric_time__extract_doy
                  , subq_14.listing AS listing
                  , subq_14.guest AS guest
                  , subq_14.host AS host
                  , subq_14.booking__listing AS booking__listing
                  , subq_14.booking__guest AS booking__guest
                  , subq_14.booking__host AS booking__host
                  , subq_14.is_instant AS is_instant
                  , subq_14.booking__is_instant AS booking__is_instant
                  , subq_14.bookings AS bookings
                  , subq_14.instant_bookings AS instant_bookings
                  , subq_14.booking_value AS booking_value
                  , subq_14.max_booking_value AS max_booking_value
                  , subq_14.min_booking_value AS min_booking_value
                  , subq_14.bookers AS bookers
                  , subq_14.average_booking_value AS average_booking_value
                  , subq_14.referred_bookings AS referred_bookings
                  , subq_14.median_booking_value AS median_booking_value
                  , subq_14.booking_value_p99 AS booking_value_p99
                  , subq_14.discrete_booking_value_p99 AS discrete_booking_value_p99
                  , subq_14.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
                  , subq_14.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
                FROM (
                  -- Pass Only Elements: ['metric_time__day',]
                  SELECT
                    subq_16.metric_time__day
                  FROM (
                    -- Change Column Aliases
                    SELECT
                      subq_15.ds__day AS metric_time__day
                      , subq_15.ds__week
                      , subq_15.ds__month
                      , subq_15.ds__quarter
                      , subq_15.ds__year
                      , subq_15.ds__extract_year
                      , subq_15.ds__extract_quarter
                      , subq_15.ds__extract_month
                      , subq_15.ds__extract_day
                      , subq_15.ds__extract_dow
                      , subq_15.ds__extract_doy
                      , subq_15.ds__martian_day
                    FROM (
                      -- Read From Time Spine 'mf_time_spine'
                      SELECT
                        time_spine_src_28006.ds AS ds__day
                        , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
                        , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
                        , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
                        , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
                        , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
                        , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
                        , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
                        , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
                        , EXTRACT(DAY_OF_WEEK FROM time_spine_src_28006.ds) AS ds__extract_dow
                        , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
                        , time_spine_src_28006.martian_day AS ds__martian_day
                      FROM ***************************.mf_time_spine time_spine_src_28006
                    ) subq_15
                  ) subq_16
                ) subq_17
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
                      , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
                      , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
                      , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
                      , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
                      , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
                      , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
                      , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
                      , bookings_source_src_28000.listing_id AS listing
                      , bookings_source_src_28000.guest_id AS guest
                      , bookings_source_src_28000.host_id AS host
                      , bookings_source_src_28000.listing_id AS booking__listing
                      , bookings_source_src_28000.guest_id AS booking__guest
                      , bookings_source_src_28000.host_id AS booking__host
                    FROM ***************************.fct_bookings bookings_source_src_28000
                  ) subq_0
                ) subq_14
                ON
                  DATE_ADD('day', -14, subq_17.metric_time__day) = subq_14.metric_time__day
              ) subq_18
              LEFT OUTER JOIN (
                -- Pass Only Elements: ['country_latest', 'listing']
                SELECT
                  subq_19.listing
                  , subq_19.country_latest
                FROM (
                  -- Metric Time Dimension 'ds'
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
                    , subq_2.created_at__day
                    , subq_2.created_at__week
                    , subq_2.created_at__month
                    , subq_2.created_at__quarter
                    , subq_2.created_at__year
                    , subq_2.created_at__extract_year
                    , subq_2.created_at__extract_quarter
                    , subq_2.created_at__extract_month
                    , subq_2.created_at__extract_day
                    , subq_2.created_at__extract_dow
                    , subq_2.created_at__extract_doy
                    , subq_2.listing__ds__day
                    , subq_2.listing__ds__week
                    , subq_2.listing__ds__month
                    , subq_2.listing__ds__quarter
                    , subq_2.listing__ds__year
                    , subq_2.listing__ds__extract_year
                    , subq_2.listing__ds__extract_quarter
                    , subq_2.listing__ds__extract_month
                    , subq_2.listing__ds__extract_day
                    , subq_2.listing__ds__extract_dow
                    , subq_2.listing__ds__extract_doy
                    , subq_2.listing__created_at__day
                    , subq_2.listing__created_at__week
                    , subq_2.listing__created_at__month
                    , subq_2.listing__created_at__quarter
                    , subq_2.listing__created_at__year
                    , subq_2.listing__created_at__extract_year
                    , subq_2.listing__created_at__extract_quarter
                    , subq_2.listing__created_at__extract_month
                    , subq_2.listing__created_at__extract_day
                    , subq_2.listing__created_at__extract_dow
                    , subq_2.listing__created_at__extract_doy
                    , subq_2.ds__day AS metric_time__day
                    , subq_2.ds__week AS metric_time__week
                    , subq_2.ds__month AS metric_time__month
                    , subq_2.ds__quarter AS metric_time__quarter
                    , subq_2.ds__year AS metric_time__year
                    , subq_2.ds__extract_year AS metric_time__extract_year
                    , subq_2.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_2.ds__extract_month AS metric_time__extract_month
                    , subq_2.ds__extract_day AS metric_time__extract_day
                    , subq_2.ds__extract_dow AS metric_time__extract_dow
                    , subq_2.ds__extract_doy AS metric_time__extract_doy
                    , subq_2.listing
                    , subq_2.user
                    , subq_2.listing__user
                    , subq_2.country_latest
                    , subq_2.is_lux_latest
                    , subq_2.capacity_latest
                    , subq_2.listing__country_latest
                    , subq_2.listing__is_lux_latest
                    , subq_2.listing__capacity_latest
                    , subq_2.listings
                    , subq_2.largest_listing
                    , subq_2.smallest_listing
                  FROM (
                    -- Read Elements From Semantic Model 'listings_latest'
                    SELECT
                      1 AS listings
                      , listings_latest_src_28000.capacity AS largest_listing
                      , listings_latest_src_28000.capacity AS smallest_listing
                      , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS ds__day
                      , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS ds__week
                      , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS ds__month
                      , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS ds__quarter
                      , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS ds__year
                      , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
                      , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS ds__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS ds__extract_doy
                      , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS created_at__day
                      , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS created_at__week
                      , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS created_at__month
                      , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS created_at__quarter
                      , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS created_at__year
                      , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
                      , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
                      , listings_latest_src_28000.country AS country_latest
                      , listings_latest_src_28000.is_lux AS is_lux_latest
                      , listings_latest_src_28000.capacity AS capacity_latest
                      , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__ds__day
                      , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__ds__week
                      , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__ds__month
                      , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__ds__quarter
                      , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__ds__year
                      , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
                      , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                      , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__created_at__day
                      , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__created_at__week
                      , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__created_at__month
                      , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__created_at__quarter
                      , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__created_at__year
                      , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_month
                      , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_day
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                      , listings_latest_src_28000.country AS listing__country_latest
                      , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                      , listings_latest_src_28000.capacity AS listing__capacity_latest
                      , listings_latest_src_28000.listing_id AS listing
                      , listings_latest_src_28000.user_id AS user
                      , listings_latest_src_28000.user_id AS listing__user
                    FROM ***************************.dim_listings_latest listings_latest_src_28000
                  ) subq_2
                ) subq_19
              ) subq_20
              ON
                subq_18.listing = subq_20.listing
            ) subq_21
            WHERE booking__is_instant
          ) subq_22
        ) subq_23
        GROUP BY
          subq_23.metric_time__day
          , subq_23.listing__country_latest
      ) subq_24
      ON
        subq_27.metric_time__day = subq_24.metric_time__day
    ) subq_28
  ) subq_29
  ON
    (
      subq_13.listing__country_latest = subq_29.listing__country_latest
    ) AND (
      subq_13.metric_time__day = subq_29.metric_time__day
    )
  GROUP BY
    COALESCE(subq_13.metric_time__day, subq_29.metric_time__day)
    , COALESCE(subq_13.listing__country_latest, subq_29.listing__country_latest)
) subq_30