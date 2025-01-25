test_name: test_filter_by_metric_in_same_semantic_model_as_queried_metric
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a simple metric in the query-level where filter.
sql_engine: Trino
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_12.bookers
FROM (
  -- Aggregate Measures
  SELECT
    COUNT(DISTINCT nr_subq_11.bookers) AS bookers
  FROM (
    -- Pass Only Elements: ['bookers',]
    SELECT
      nr_subq_10.bookers
    FROM (
      -- Constrain Output with WHERE
      SELECT
        nr_subq_9.ds__day
        , nr_subq_9.ds__week
        , nr_subq_9.ds__month
        , nr_subq_9.ds__quarter
        , nr_subq_9.ds__year
        , nr_subq_9.ds__extract_year
        , nr_subq_9.ds__extract_quarter
        , nr_subq_9.ds__extract_month
        , nr_subq_9.ds__extract_day
        , nr_subq_9.ds__extract_dow
        , nr_subq_9.ds__extract_doy
        , nr_subq_9.ds_partitioned__day
        , nr_subq_9.ds_partitioned__week
        , nr_subq_9.ds_partitioned__month
        , nr_subq_9.ds_partitioned__quarter
        , nr_subq_9.ds_partitioned__year
        , nr_subq_9.ds_partitioned__extract_year
        , nr_subq_9.ds_partitioned__extract_quarter
        , nr_subq_9.ds_partitioned__extract_month
        , nr_subq_9.ds_partitioned__extract_day
        , nr_subq_9.ds_partitioned__extract_dow
        , nr_subq_9.ds_partitioned__extract_doy
        , nr_subq_9.paid_at__day
        , nr_subq_9.paid_at__week
        , nr_subq_9.paid_at__month
        , nr_subq_9.paid_at__quarter
        , nr_subq_9.paid_at__year
        , nr_subq_9.paid_at__extract_year
        , nr_subq_9.paid_at__extract_quarter
        , nr_subq_9.paid_at__extract_month
        , nr_subq_9.paid_at__extract_day
        , nr_subq_9.paid_at__extract_dow
        , nr_subq_9.paid_at__extract_doy
        , nr_subq_9.booking__ds__day
        , nr_subq_9.booking__ds__week
        , nr_subq_9.booking__ds__month
        , nr_subq_9.booking__ds__quarter
        , nr_subq_9.booking__ds__year
        , nr_subq_9.booking__ds__extract_year
        , nr_subq_9.booking__ds__extract_quarter
        , nr_subq_9.booking__ds__extract_month
        , nr_subq_9.booking__ds__extract_day
        , nr_subq_9.booking__ds__extract_dow
        , nr_subq_9.booking__ds__extract_doy
        , nr_subq_9.booking__ds_partitioned__day
        , nr_subq_9.booking__ds_partitioned__week
        , nr_subq_9.booking__ds_partitioned__month
        , nr_subq_9.booking__ds_partitioned__quarter
        , nr_subq_9.booking__ds_partitioned__year
        , nr_subq_9.booking__ds_partitioned__extract_year
        , nr_subq_9.booking__ds_partitioned__extract_quarter
        , nr_subq_9.booking__ds_partitioned__extract_month
        , nr_subq_9.booking__ds_partitioned__extract_day
        , nr_subq_9.booking__ds_partitioned__extract_dow
        , nr_subq_9.booking__ds_partitioned__extract_doy
        , nr_subq_9.booking__paid_at__day
        , nr_subq_9.booking__paid_at__week
        , nr_subq_9.booking__paid_at__month
        , nr_subq_9.booking__paid_at__quarter
        , nr_subq_9.booking__paid_at__year
        , nr_subq_9.booking__paid_at__extract_year
        , nr_subq_9.booking__paid_at__extract_quarter
        , nr_subq_9.booking__paid_at__extract_month
        , nr_subq_9.booking__paid_at__extract_day
        , nr_subq_9.booking__paid_at__extract_dow
        , nr_subq_9.booking__paid_at__extract_doy
        , nr_subq_9.metric_time__day
        , nr_subq_9.metric_time__week
        , nr_subq_9.metric_time__month
        , nr_subq_9.metric_time__quarter
        , nr_subq_9.metric_time__year
        , nr_subq_9.metric_time__extract_year
        , nr_subq_9.metric_time__extract_quarter
        , nr_subq_9.metric_time__extract_month
        , nr_subq_9.metric_time__extract_day
        , nr_subq_9.metric_time__extract_dow
        , nr_subq_9.metric_time__extract_doy
        , nr_subq_9.listing
        , nr_subq_9.guest
        , nr_subq_9.host
        , nr_subq_9.booking__listing
        , nr_subq_9.booking__guest
        , nr_subq_9.booking__host
        , nr_subq_9.is_instant
        , nr_subq_9.booking__is_instant
        , nr_subq_9.guest__booking_value
        , nr_subq_9.bookings
        , nr_subq_9.instant_bookings
        , nr_subq_9.booking_value
        , nr_subq_9.max_booking_value
        , nr_subq_9.min_booking_value
        , nr_subq_9.bookers
        , nr_subq_9.average_booking_value
        , nr_subq_9.referred_bookings
        , nr_subq_9.median_booking_value
        , nr_subq_9.booking_value_p99
        , nr_subq_9.discrete_booking_value_p99
        , nr_subq_9.approximate_continuous_booking_value_p99
        , nr_subq_9.approximate_discrete_booking_value_p99
      FROM (
        -- Join Standard Outputs
        SELECT
          nr_subq_8.guest__booking_value AS guest__booking_value
          , nr_subq_3.ds__day AS ds__day
          , nr_subq_3.ds__week AS ds__week
          , nr_subq_3.ds__month AS ds__month
          , nr_subq_3.ds__quarter AS ds__quarter
          , nr_subq_3.ds__year AS ds__year
          , nr_subq_3.ds__extract_year AS ds__extract_year
          , nr_subq_3.ds__extract_quarter AS ds__extract_quarter
          , nr_subq_3.ds__extract_month AS ds__extract_month
          , nr_subq_3.ds__extract_day AS ds__extract_day
          , nr_subq_3.ds__extract_dow AS ds__extract_dow
          , nr_subq_3.ds__extract_doy AS ds__extract_doy
          , nr_subq_3.ds_partitioned__day AS ds_partitioned__day
          , nr_subq_3.ds_partitioned__week AS ds_partitioned__week
          , nr_subq_3.ds_partitioned__month AS ds_partitioned__month
          , nr_subq_3.ds_partitioned__quarter AS ds_partitioned__quarter
          , nr_subq_3.ds_partitioned__year AS ds_partitioned__year
          , nr_subq_3.ds_partitioned__extract_year AS ds_partitioned__extract_year
          , nr_subq_3.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
          , nr_subq_3.ds_partitioned__extract_month AS ds_partitioned__extract_month
          , nr_subq_3.ds_partitioned__extract_day AS ds_partitioned__extract_day
          , nr_subq_3.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
          , nr_subq_3.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
          , nr_subq_3.paid_at__day AS paid_at__day
          , nr_subq_3.paid_at__week AS paid_at__week
          , nr_subq_3.paid_at__month AS paid_at__month
          , nr_subq_3.paid_at__quarter AS paid_at__quarter
          , nr_subq_3.paid_at__year AS paid_at__year
          , nr_subq_3.paid_at__extract_year AS paid_at__extract_year
          , nr_subq_3.paid_at__extract_quarter AS paid_at__extract_quarter
          , nr_subq_3.paid_at__extract_month AS paid_at__extract_month
          , nr_subq_3.paid_at__extract_day AS paid_at__extract_day
          , nr_subq_3.paid_at__extract_dow AS paid_at__extract_dow
          , nr_subq_3.paid_at__extract_doy AS paid_at__extract_doy
          , nr_subq_3.booking__ds__day AS booking__ds__day
          , nr_subq_3.booking__ds__week AS booking__ds__week
          , nr_subq_3.booking__ds__month AS booking__ds__month
          , nr_subq_3.booking__ds__quarter AS booking__ds__quarter
          , nr_subq_3.booking__ds__year AS booking__ds__year
          , nr_subq_3.booking__ds__extract_year AS booking__ds__extract_year
          , nr_subq_3.booking__ds__extract_quarter AS booking__ds__extract_quarter
          , nr_subq_3.booking__ds__extract_month AS booking__ds__extract_month
          , nr_subq_3.booking__ds__extract_day AS booking__ds__extract_day
          , nr_subq_3.booking__ds__extract_dow AS booking__ds__extract_dow
          , nr_subq_3.booking__ds__extract_doy AS booking__ds__extract_doy
          , nr_subq_3.booking__ds_partitioned__day AS booking__ds_partitioned__day
          , nr_subq_3.booking__ds_partitioned__week AS booking__ds_partitioned__week
          , nr_subq_3.booking__ds_partitioned__month AS booking__ds_partitioned__month
          , nr_subq_3.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
          , nr_subq_3.booking__ds_partitioned__year AS booking__ds_partitioned__year
          , nr_subq_3.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
          , nr_subq_3.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
          , nr_subq_3.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
          , nr_subq_3.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
          , nr_subq_3.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
          , nr_subq_3.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
          , nr_subq_3.booking__paid_at__day AS booking__paid_at__day
          , nr_subq_3.booking__paid_at__week AS booking__paid_at__week
          , nr_subq_3.booking__paid_at__month AS booking__paid_at__month
          , nr_subq_3.booking__paid_at__quarter AS booking__paid_at__quarter
          , nr_subq_3.booking__paid_at__year AS booking__paid_at__year
          , nr_subq_3.booking__paid_at__extract_year AS booking__paid_at__extract_year
          , nr_subq_3.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
          , nr_subq_3.booking__paid_at__extract_month AS booking__paid_at__extract_month
          , nr_subq_3.booking__paid_at__extract_day AS booking__paid_at__extract_day
          , nr_subq_3.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
          , nr_subq_3.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
          , nr_subq_3.metric_time__day AS metric_time__day
          , nr_subq_3.metric_time__week AS metric_time__week
          , nr_subq_3.metric_time__month AS metric_time__month
          , nr_subq_3.metric_time__quarter AS metric_time__quarter
          , nr_subq_3.metric_time__year AS metric_time__year
          , nr_subq_3.metric_time__extract_year AS metric_time__extract_year
          , nr_subq_3.metric_time__extract_quarter AS metric_time__extract_quarter
          , nr_subq_3.metric_time__extract_month AS metric_time__extract_month
          , nr_subq_3.metric_time__extract_day AS metric_time__extract_day
          , nr_subq_3.metric_time__extract_dow AS metric_time__extract_dow
          , nr_subq_3.metric_time__extract_doy AS metric_time__extract_doy
          , nr_subq_3.listing AS listing
          , nr_subq_3.guest AS guest
          , nr_subq_3.host AS host
          , nr_subq_3.booking__listing AS booking__listing
          , nr_subq_3.booking__guest AS booking__guest
          , nr_subq_3.booking__host AS booking__host
          , nr_subq_3.is_instant AS is_instant
          , nr_subq_3.booking__is_instant AS booking__is_instant
          , nr_subq_3.bookings AS bookings
          , nr_subq_3.instant_bookings AS instant_bookings
          , nr_subq_3.booking_value AS booking_value
          , nr_subq_3.max_booking_value AS max_booking_value
          , nr_subq_3.min_booking_value AS min_booking_value
          , nr_subq_3.bookers AS bookers
          , nr_subq_3.average_booking_value AS average_booking_value
          , nr_subq_3.referred_bookings AS referred_bookings
          , nr_subq_3.median_booking_value AS median_booking_value
          , nr_subq_3.booking_value_p99 AS booking_value_p99
          , nr_subq_3.discrete_booking_value_p99 AS discrete_booking_value_p99
          , nr_subq_3.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
          , nr_subq_3.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
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
          ) nr_subq_28002
        ) nr_subq_3
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['guest', 'guest__booking_value']
          SELECT
            nr_subq_7.guest
            , nr_subq_7.guest__booking_value
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              nr_subq_6.guest
              , nr_subq_6.booking_value AS guest__booking_value
            FROM (
              -- Aggregate Measures
              SELECT
                nr_subq_5.guest
                , SUM(nr_subq_5.booking_value) AS booking_value
              FROM (
                -- Pass Only Elements: ['booking_value', 'guest']
                SELECT
                  nr_subq_4.guest
                  , nr_subq_4.booking_value
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
                  ) nr_subq_28002
                ) nr_subq_4
              ) nr_subq_5
              GROUP BY
                nr_subq_5.guest
            ) nr_subq_6
          ) nr_subq_7
        ) nr_subq_8
        ON
          nr_subq_3.guest = nr_subq_8.guest
      ) nr_subq_9
      WHERE guest__booking_value > 1.00
    ) nr_subq_10
  ) nr_subq_11
) nr_subq_12
