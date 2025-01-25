test_name: test_single_categorical_dimension_pushdown
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we expect predicate pushdown for a single categorical dimension.
sql_engine: Databricks
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_6.listing__country_latest
  , nr_subq_6.bookings
FROM (
  -- Aggregate Measures
  SELECT
    nr_subq_5.listing__country_latest
    , SUM(nr_subq_5.bookings) AS bookings
  FROM (
    -- Pass Only Elements: ['bookings', 'listing__country_latest']
    SELECT
      nr_subq_4.listing__country_latest
      , nr_subq_4.bookings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        nr_subq_3.ds__day
        , nr_subq_3.ds__week
        , nr_subq_3.ds__month
        , nr_subq_3.ds__quarter
        , nr_subq_3.ds__year
        , nr_subq_3.ds__extract_year
        , nr_subq_3.ds__extract_quarter
        , nr_subq_3.ds__extract_month
        , nr_subq_3.ds__extract_day
        , nr_subq_3.ds__extract_dow
        , nr_subq_3.ds__extract_doy
        , nr_subq_3.ds_partitioned__day
        , nr_subq_3.ds_partitioned__week
        , nr_subq_3.ds_partitioned__month
        , nr_subq_3.ds_partitioned__quarter
        , nr_subq_3.ds_partitioned__year
        , nr_subq_3.ds_partitioned__extract_year
        , nr_subq_3.ds_partitioned__extract_quarter
        , nr_subq_3.ds_partitioned__extract_month
        , nr_subq_3.ds_partitioned__extract_day
        , nr_subq_3.ds_partitioned__extract_dow
        , nr_subq_3.ds_partitioned__extract_doy
        , nr_subq_3.paid_at__day
        , nr_subq_3.paid_at__week
        , nr_subq_3.paid_at__month
        , nr_subq_3.paid_at__quarter
        , nr_subq_3.paid_at__year
        , nr_subq_3.paid_at__extract_year
        , nr_subq_3.paid_at__extract_quarter
        , nr_subq_3.paid_at__extract_month
        , nr_subq_3.paid_at__extract_day
        , nr_subq_3.paid_at__extract_dow
        , nr_subq_3.paid_at__extract_doy
        , nr_subq_3.booking__ds__day
        , nr_subq_3.booking__ds__week
        , nr_subq_3.booking__ds__month
        , nr_subq_3.booking__ds__quarter
        , nr_subq_3.booking__ds__year
        , nr_subq_3.booking__ds__extract_year
        , nr_subq_3.booking__ds__extract_quarter
        , nr_subq_3.booking__ds__extract_month
        , nr_subq_3.booking__ds__extract_day
        , nr_subq_3.booking__ds__extract_dow
        , nr_subq_3.booking__ds__extract_doy
        , nr_subq_3.booking__ds_partitioned__day
        , nr_subq_3.booking__ds_partitioned__week
        , nr_subq_3.booking__ds_partitioned__month
        , nr_subq_3.booking__ds_partitioned__quarter
        , nr_subq_3.booking__ds_partitioned__year
        , nr_subq_3.booking__ds_partitioned__extract_year
        , nr_subq_3.booking__ds_partitioned__extract_quarter
        , nr_subq_3.booking__ds_partitioned__extract_month
        , nr_subq_3.booking__ds_partitioned__extract_day
        , nr_subq_3.booking__ds_partitioned__extract_dow
        , nr_subq_3.booking__ds_partitioned__extract_doy
        , nr_subq_3.booking__paid_at__day
        , nr_subq_3.booking__paid_at__week
        , nr_subq_3.booking__paid_at__month
        , nr_subq_3.booking__paid_at__quarter
        , nr_subq_3.booking__paid_at__year
        , nr_subq_3.booking__paid_at__extract_year
        , nr_subq_3.booking__paid_at__extract_quarter
        , nr_subq_3.booking__paid_at__extract_month
        , nr_subq_3.booking__paid_at__extract_day
        , nr_subq_3.booking__paid_at__extract_dow
        , nr_subq_3.booking__paid_at__extract_doy
        , nr_subq_3.metric_time__day
        , nr_subq_3.metric_time__week
        , nr_subq_3.metric_time__month
        , nr_subq_3.metric_time__quarter
        , nr_subq_3.metric_time__year
        , nr_subq_3.metric_time__extract_year
        , nr_subq_3.metric_time__extract_quarter
        , nr_subq_3.metric_time__extract_month
        , nr_subq_3.metric_time__extract_day
        , nr_subq_3.metric_time__extract_dow
        , nr_subq_3.metric_time__extract_doy
        , nr_subq_3.listing
        , nr_subq_3.guest
        , nr_subq_3.host
        , nr_subq_3.booking__listing
        , nr_subq_3.booking__guest
        , nr_subq_3.booking__host
        , nr_subq_3.is_instant
        , nr_subq_3.booking__is_instant
        , nr_subq_3.listing__country_latest
        , nr_subq_3.bookings
        , nr_subq_3.instant_bookings
        , nr_subq_3.booking_value
        , nr_subq_3.max_booking_value
        , nr_subq_3.min_booking_value
        , nr_subq_3.bookers
        , nr_subq_3.average_booking_value
        , nr_subq_3.referred_bookings
        , nr_subq_3.median_booking_value
        , nr_subq_3.booking_value_p99
        , nr_subq_3.discrete_booking_value_p99
        , nr_subq_3.approximate_continuous_booking_value_p99
        , nr_subq_3.approximate_discrete_booking_value_p99
      FROM (
        -- Join Standard Outputs
        SELECT
          nr_subq_2.country_latest AS listing__country_latest
          , nr_subq_0.ds__day AS ds__day
          , nr_subq_0.ds__week AS ds__week
          , nr_subq_0.ds__month AS ds__month
          , nr_subq_0.ds__quarter AS ds__quarter
          , nr_subq_0.ds__year AS ds__year
          , nr_subq_0.ds__extract_year AS ds__extract_year
          , nr_subq_0.ds__extract_quarter AS ds__extract_quarter
          , nr_subq_0.ds__extract_month AS ds__extract_month
          , nr_subq_0.ds__extract_day AS ds__extract_day
          , nr_subq_0.ds__extract_dow AS ds__extract_dow
          , nr_subq_0.ds__extract_doy AS ds__extract_doy
          , nr_subq_0.ds_partitioned__day AS ds_partitioned__day
          , nr_subq_0.ds_partitioned__week AS ds_partitioned__week
          , nr_subq_0.ds_partitioned__month AS ds_partitioned__month
          , nr_subq_0.ds_partitioned__quarter AS ds_partitioned__quarter
          , nr_subq_0.ds_partitioned__year AS ds_partitioned__year
          , nr_subq_0.ds_partitioned__extract_year AS ds_partitioned__extract_year
          , nr_subq_0.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
          , nr_subq_0.ds_partitioned__extract_month AS ds_partitioned__extract_month
          , nr_subq_0.ds_partitioned__extract_day AS ds_partitioned__extract_day
          , nr_subq_0.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
          , nr_subq_0.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
          , nr_subq_0.paid_at__day AS paid_at__day
          , nr_subq_0.paid_at__week AS paid_at__week
          , nr_subq_0.paid_at__month AS paid_at__month
          , nr_subq_0.paid_at__quarter AS paid_at__quarter
          , nr_subq_0.paid_at__year AS paid_at__year
          , nr_subq_0.paid_at__extract_year AS paid_at__extract_year
          , nr_subq_0.paid_at__extract_quarter AS paid_at__extract_quarter
          , nr_subq_0.paid_at__extract_month AS paid_at__extract_month
          , nr_subq_0.paid_at__extract_day AS paid_at__extract_day
          , nr_subq_0.paid_at__extract_dow AS paid_at__extract_dow
          , nr_subq_0.paid_at__extract_doy AS paid_at__extract_doy
          , nr_subq_0.booking__ds__day AS booking__ds__day
          , nr_subq_0.booking__ds__week AS booking__ds__week
          , nr_subq_0.booking__ds__month AS booking__ds__month
          , nr_subq_0.booking__ds__quarter AS booking__ds__quarter
          , nr_subq_0.booking__ds__year AS booking__ds__year
          , nr_subq_0.booking__ds__extract_year AS booking__ds__extract_year
          , nr_subq_0.booking__ds__extract_quarter AS booking__ds__extract_quarter
          , nr_subq_0.booking__ds__extract_month AS booking__ds__extract_month
          , nr_subq_0.booking__ds__extract_day AS booking__ds__extract_day
          , nr_subq_0.booking__ds__extract_dow AS booking__ds__extract_dow
          , nr_subq_0.booking__ds__extract_doy AS booking__ds__extract_doy
          , nr_subq_0.booking__ds_partitioned__day AS booking__ds_partitioned__day
          , nr_subq_0.booking__ds_partitioned__week AS booking__ds_partitioned__week
          , nr_subq_0.booking__ds_partitioned__month AS booking__ds_partitioned__month
          , nr_subq_0.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
          , nr_subq_0.booking__ds_partitioned__year AS booking__ds_partitioned__year
          , nr_subq_0.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
          , nr_subq_0.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
          , nr_subq_0.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
          , nr_subq_0.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
          , nr_subq_0.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
          , nr_subq_0.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
          , nr_subq_0.booking__paid_at__day AS booking__paid_at__day
          , nr_subq_0.booking__paid_at__week AS booking__paid_at__week
          , nr_subq_0.booking__paid_at__month AS booking__paid_at__month
          , nr_subq_0.booking__paid_at__quarter AS booking__paid_at__quarter
          , nr_subq_0.booking__paid_at__year AS booking__paid_at__year
          , nr_subq_0.booking__paid_at__extract_year AS booking__paid_at__extract_year
          , nr_subq_0.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
          , nr_subq_0.booking__paid_at__extract_month AS booking__paid_at__extract_month
          , nr_subq_0.booking__paid_at__extract_day AS booking__paid_at__extract_day
          , nr_subq_0.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
          , nr_subq_0.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
          , nr_subq_0.metric_time__day AS metric_time__day
          , nr_subq_0.metric_time__week AS metric_time__week
          , nr_subq_0.metric_time__month AS metric_time__month
          , nr_subq_0.metric_time__quarter AS metric_time__quarter
          , nr_subq_0.metric_time__year AS metric_time__year
          , nr_subq_0.metric_time__extract_year AS metric_time__extract_year
          , nr_subq_0.metric_time__extract_quarter AS metric_time__extract_quarter
          , nr_subq_0.metric_time__extract_month AS metric_time__extract_month
          , nr_subq_0.metric_time__extract_day AS metric_time__extract_day
          , nr_subq_0.metric_time__extract_dow AS metric_time__extract_dow
          , nr_subq_0.metric_time__extract_doy AS metric_time__extract_doy
          , nr_subq_0.listing AS listing
          , nr_subq_0.guest AS guest
          , nr_subq_0.host AS host
          , nr_subq_0.booking__listing AS booking__listing
          , nr_subq_0.booking__guest AS booking__guest
          , nr_subq_0.booking__host AS booking__host
          , nr_subq_0.is_instant AS is_instant
          , nr_subq_0.booking__is_instant AS booking__is_instant
          , nr_subq_0.bookings AS bookings
          , nr_subq_0.instant_bookings AS instant_bookings
          , nr_subq_0.booking_value AS booking_value
          , nr_subq_0.max_booking_value AS max_booking_value
          , nr_subq_0.min_booking_value AS min_booking_value
          , nr_subq_0.bookers AS bookers
          , nr_subq_0.average_booking_value AS average_booking_value
          , nr_subq_0.referred_bookings AS referred_bookings
          , nr_subq_0.median_booking_value AS median_booking_value
          , nr_subq_0.booking_value_p99 AS booking_value_p99
          , nr_subq_0.discrete_booking_value_p99 AS discrete_booking_value_p99
          , nr_subq_0.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
          , nr_subq_0.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
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
              , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
              , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
              , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
              , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
              , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
              , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
              , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
              , bookings_source_src_28000.listing_id AS listing
              , bookings_source_src_28000.guest_id AS guest
              , bookings_source_src_28000.host_id AS host
              , bookings_source_src_28000.listing_id AS booking__listing
              , bookings_source_src_28000.guest_id AS booking__guest
              , bookings_source_src_28000.host_id AS booking__host
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) nr_subq_28002
        ) nr_subq_0
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['country_latest', 'listing']
          SELECT
            nr_subq_1.listing
            , nr_subq_1.country_latest
          FROM (
            -- Metric Time Dimension 'ds'
            SELECT
              nr_subq_28007.ds__day
              , nr_subq_28007.ds__week
              , nr_subq_28007.ds__month
              , nr_subq_28007.ds__quarter
              , nr_subq_28007.ds__year
              , nr_subq_28007.ds__extract_year
              , nr_subq_28007.ds__extract_quarter
              , nr_subq_28007.ds__extract_month
              , nr_subq_28007.ds__extract_day
              , nr_subq_28007.ds__extract_dow
              , nr_subq_28007.ds__extract_doy
              , nr_subq_28007.created_at__day
              , nr_subq_28007.created_at__week
              , nr_subq_28007.created_at__month
              , nr_subq_28007.created_at__quarter
              , nr_subq_28007.created_at__year
              , nr_subq_28007.created_at__extract_year
              , nr_subq_28007.created_at__extract_quarter
              , nr_subq_28007.created_at__extract_month
              , nr_subq_28007.created_at__extract_day
              , nr_subq_28007.created_at__extract_dow
              , nr_subq_28007.created_at__extract_doy
              , nr_subq_28007.listing__ds__day
              , nr_subq_28007.listing__ds__week
              , nr_subq_28007.listing__ds__month
              , nr_subq_28007.listing__ds__quarter
              , nr_subq_28007.listing__ds__year
              , nr_subq_28007.listing__ds__extract_year
              , nr_subq_28007.listing__ds__extract_quarter
              , nr_subq_28007.listing__ds__extract_month
              , nr_subq_28007.listing__ds__extract_day
              , nr_subq_28007.listing__ds__extract_dow
              , nr_subq_28007.listing__ds__extract_doy
              , nr_subq_28007.listing__created_at__day
              , nr_subq_28007.listing__created_at__week
              , nr_subq_28007.listing__created_at__month
              , nr_subq_28007.listing__created_at__quarter
              , nr_subq_28007.listing__created_at__year
              , nr_subq_28007.listing__created_at__extract_year
              , nr_subq_28007.listing__created_at__extract_quarter
              , nr_subq_28007.listing__created_at__extract_month
              , nr_subq_28007.listing__created_at__extract_day
              , nr_subq_28007.listing__created_at__extract_dow
              , nr_subq_28007.listing__created_at__extract_doy
              , nr_subq_28007.ds__day AS metric_time__day
              , nr_subq_28007.ds__week AS metric_time__week
              , nr_subq_28007.ds__month AS metric_time__month
              , nr_subq_28007.ds__quarter AS metric_time__quarter
              , nr_subq_28007.ds__year AS metric_time__year
              , nr_subq_28007.ds__extract_year AS metric_time__extract_year
              , nr_subq_28007.ds__extract_quarter AS metric_time__extract_quarter
              , nr_subq_28007.ds__extract_month AS metric_time__extract_month
              , nr_subq_28007.ds__extract_day AS metric_time__extract_day
              , nr_subq_28007.ds__extract_dow AS metric_time__extract_dow
              , nr_subq_28007.ds__extract_doy AS metric_time__extract_doy
              , nr_subq_28007.listing
              , nr_subq_28007.user
              , nr_subq_28007.listing__user
              , nr_subq_28007.country_latest
              , nr_subq_28007.is_lux_latest
              , nr_subq_28007.capacity_latest
              , nr_subq_28007.listing__country_latest
              , nr_subq_28007.listing__is_lux_latest
              , nr_subq_28007.listing__capacity_latest
              , nr_subq_28007.listings
              , nr_subq_28007.largest_listing
              , nr_subq_28007.smallest_listing
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
                , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS ds__extract_dow
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
                , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
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
                , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
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
                , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
                , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                , listings_latest_src_28000.country AS listing__country_latest
                , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                , listings_latest_src_28000.capacity AS listing__capacity_latest
                , listings_latest_src_28000.listing_id AS listing
                , listings_latest_src_28000.user_id AS user
                , listings_latest_src_28000.user_id AS listing__user
              FROM ***************************.dim_listings_latest listings_latest_src_28000
            ) nr_subq_28007
          ) nr_subq_1
        ) nr_subq_2
        ON
          nr_subq_0.listing = nr_subq_2.listing
      ) nr_subq_3
      WHERE booking__is_instant
    ) nr_subq_4
  ) nr_subq_5
  GROUP BY
    nr_subq_5.listing__country_latest
) nr_subq_6
