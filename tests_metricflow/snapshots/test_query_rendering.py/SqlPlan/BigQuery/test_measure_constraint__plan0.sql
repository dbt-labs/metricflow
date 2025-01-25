test_name: test_measure_constraint
test_filename: test_query_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_20.metric_time__day
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_7.metric_time__day, nr_subq_15.metric_time__day, nr_subq_19.metric_time__day) AS metric_time__day
    , MAX(nr_subq_7.average_booking_value) AS average_booking_value
    , MAX(nr_subq_15.bookings) AS bookings
    , MAX(nr_subq_19.booking_value) AS booking_value
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      nr_subq_6.metric_time__day
      , nr_subq_6.average_booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        nr_subq_5.metric_time__day
        , AVG(nr_subq_5.average_booking_value) AS average_booking_value
      FROM (
        -- Pass Only Elements: ['average_booking_value', 'metric_time__day']
        SELECT
          nr_subq_4.metric_time__day
          , nr_subq_4.average_booking_value
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
            , nr_subq_3.listing__is_lux_latest
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
              nr_subq_2.is_lux_latest AS listing__is_lux_latest
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
            LEFT OUTER JOIN (
              -- Pass Only Elements: ['is_lux_latest', 'listing']
              SELECT
                nr_subq_1.listing
                , nr_subq_1.is_lux_latest
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
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS ds__day
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS ds__week
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS ds__month
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS ds__quarter
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS ds__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
                    , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS ds__extract_dow
                    , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS ds__extract_doy
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS created_at__day
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS created_at__week
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS created_at__month
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS created_at__quarter
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS created_at__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
                    , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS created_at__extract_dow
                    , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
                    , listings_latest_src_28000.country AS country_latest
                    , listings_latest_src_28000.is_lux AS is_lux_latest
                    , listings_latest_src_28000.capacity AS capacity_latest
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS listing__ds__day
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__ds__week
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS listing__ds__month
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__ds__quarter
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS listing__ds__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
                    , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS listing__ds__extract_dow
                    , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS listing__created_at__day
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__created_at__week
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS listing__created_at__month
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__created_at__quarter
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS listing__created_at__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_day
                    , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS listing__created_at__extract_dow
                    , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
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
          WHERE listing__is_lux_latest
        ) nr_subq_4
      ) nr_subq_5
      GROUP BY
        metric_time__day
    ) nr_subq_6
  ) nr_subq_7
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      nr_subq_14.metric_time__day
      , nr_subq_14.bookings
    FROM (
      -- Aggregate Measures
      SELECT
        nr_subq_13.metric_time__day
        , SUM(nr_subq_13.bookings) AS bookings
      FROM (
        -- Pass Only Elements: ['bookings', 'metric_time__day']
        SELECT
          nr_subq_12.metric_time__day
          , nr_subq_12.bookings
        FROM (
          -- Constrain Output with WHERE
          SELECT
            nr_subq_11.ds__day
            , nr_subq_11.ds__week
            , nr_subq_11.ds__month
            , nr_subq_11.ds__quarter
            , nr_subq_11.ds__year
            , nr_subq_11.ds__extract_year
            , nr_subq_11.ds__extract_quarter
            , nr_subq_11.ds__extract_month
            , nr_subq_11.ds__extract_day
            , nr_subq_11.ds__extract_dow
            , nr_subq_11.ds__extract_doy
            , nr_subq_11.ds_partitioned__day
            , nr_subq_11.ds_partitioned__week
            , nr_subq_11.ds_partitioned__month
            , nr_subq_11.ds_partitioned__quarter
            , nr_subq_11.ds_partitioned__year
            , nr_subq_11.ds_partitioned__extract_year
            , nr_subq_11.ds_partitioned__extract_quarter
            , nr_subq_11.ds_partitioned__extract_month
            , nr_subq_11.ds_partitioned__extract_day
            , nr_subq_11.ds_partitioned__extract_dow
            , nr_subq_11.ds_partitioned__extract_doy
            , nr_subq_11.paid_at__day
            , nr_subq_11.paid_at__week
            , nr_subq_11.paid_at__month
            , nr_subq_11.paid_at__quarter
            , nr_subq_11.paid_at__year
            , nr_subq_11.paid_at__extract_year
            , nr_subq_11.paid_at__extract_quarter
            , nr_subq_11.paid_at__extract_month
            , nr_subq_11.paid_at__extract_day
            , nr_subq_11.paid_at__extract_dow
            , nr_subq_11.paid_at__extract_doy
            , nr_subq_11.booking__ds__day
            , nr_subq_11.booking__ds__week
            , nr_subq_11.booking__ds__month
            , nr_subq_11.booking__ds__quarter
            , nr_subq_11.booking__ds__year
            , nr_subq_11.booking__ds__extract_year
            , nr_subq_11.booking__ds__extract_quarter
            , nr_subq_11.booking__ds__extract_month
            , nr_subq_11.booking__ds__extract_day
            , nr_subq_11.booking__ds__extract_dow
            , nr_subq_11.booking__ds__extract_doy
            , nr_subq_11.booking__ds_partitioned__day
            , nr_subq_11.booking__ds_partitioned__week
            , nr_subq_11.booking__ds_partitioned__month
            , nr_subq_11.booking__ds_partitioned__quarter
            , nr_subq_11.booking__ds_partitioned__year
            , nr_subq_11.booking__ds_partitioned__extract_year
            , nr_subq_11.booking__ds_partitioned__extract_quarter
            , nr_subq_11.booking__ds_partitioned__extract_month
            , nr_subq_11.booking__ds_partitioned__extract_day
            , nr_subq_11.booking__ds_partitioned__extract_dow
            , nr_subq_11.booking__ds_partitioned__extract_doy
            , nr_subq_11.booking__paid_at__day
            , nr_subq_11.booking__paid_at__week
            , nr_subq_11.booking__paid_at__month
            , nr_subq_11.booking__paid_at__quarter
            , nr_subq_11.booking__paid_at__year
            , nr_subq_11.booking__paid_at__extract_year
            , nr_subq_11.booking__paid_at__extract_quarter
            , nr_subq_11.booking__paid_at__extract_month
            , nr_subq_11.booking__paid_at__extract_day
            , nr_subq_11.booking__paid_at__extract_dow
            , nr_subq_11.booking__paid_at__extract_doy
            , nr_subq_11.metric_time__day
            , nr_subq_11.metric_time__week
            , nr_subq_11.metric_time__month
            , nr_subq_11.metric_time__quarter
            , nr_subq_11.metric_time__year
            , nr_subq_11.metric_time__extract_year
            , nr_subq_11.metric_time__extract_quarter
            , nr_subq_11.metric_time__extract_month
            , nr_subq_11.metric_time__extract_day
            , nr_subq_11.metric_time__extract_dow
            , nr_subq_11.metric_time__extract_doy
            , nr_subq_11.listing
            , nr_subq_11.guest
            , nr_subq_11.host
            , nr_subq_11.booking__listing
            , nr_subq_11.booking__guest
            , nr_subq_11.booking__host
            , nr_subq_11.is_instant
            , nr_subq_11.booking__is_instant
            , nr_subq_11.listing__is_lux_latest
            , nr_subq_11.bookings
            , nr_subq_11.instant_bookings
            , nr_subq_11.booking_value
            , nr_subq_11.max_booking_value
            , nr_subq_11.min_booking_value
            , nr_subq_11.bookers
            , nr_subq_11.average_booking_value
            , nr_subq_11.referred_bookings
            , nr_subq_11.median_booking_value
            , nr_subq_11.booking_value_p99
            , nr_subq_11.discrete_booking_value_p99
            , nr_subq_11.approximate_continuous_booking_value_p99
            , nr_subq_11.approximate_discrete_booking_value_p99
          FROM (
            -- Join Standard Outputs
            SELECT
              nr_subq_10.is_lux_latest AS listing__is_lux_latest
              , nr_subq_8.ds__day AS ds__day
              , nr_subq_8.ds__week AS ds__week
              , nr_subq_8.ds__month AS ds__month
              , nr_subq_8.ds__quarter AS ds__quarter
              , nr_subq_8.ds__year AS ds__year
              , nr_subq_8.ds__extract_year AS ds__extract_year
              , nr_subq_8.ds__extract_quarter AS ds__extract_quarter
              , nr_subq_8.ds__extract_month AS ds__extract_month
              , nr_subq_8.ds__extract_day AS ds__extract_day
              , nr_subq_8.ds__extract_dow AS ds__extract_dow
              , nr_subq_8.ds__extract_doy AS ds__extract_doy
              , nr_subq_8.ds_partitioned__day AS ds_partitioned__day
              , nr_subq_8.ds_partitioned__week AS ds_partitioned__week
              , nr_subq_8.ds_partitioned__month AS ds_partitioned__month
              , nr_subq_8.ds_partitioned__quarter AS ds_partitioned__quarter
              , nr_subq_8.ds_partitioned__year AS ds_partitioned__year
              , nr_subq_8.ds_partitioned__extract_year AS ds_partitioned__extract_year
              , nr_subq_8.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
              , nr_subq_8.ds_partitioned__extract_month AS ds_partitioned__extract_month
              , nr_subq_8.ds_partitioned__extract_day AS ds_partitioned__extract_day
              , nr_subq_8.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
              , nr_subq_8.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
              , nr_subq_8.paid_at__day AS paid_at__day
              , nr_subq_8.paid_at__week AS paid_at__week
              , nr_subq_8.paid_at__month AS paid_at__month
              , nr_subq_8.paid_at__quarter AS paid_at__quarter
              , nr_subq_8.paid_at__year AS paid_at__year
              , nr_subq_8.paid_at__extract_year AS paid_at__extract_year
              , nr_subq_8.paid_at__extract_quarter AS paid_at__extract_quarter
              , nr_subq_8.paid_at__extract_month AS paid_at__extract_month
              , nr_subq_8.paid_at__extract_day AS paid_at__extract_day
              , nr_subq_8.paid_at__extract_dow AS paid_at__extract_dow
              , nr_subq_8.paid_at__extract_doy AS paid_at__extract_doy
              , nr_subq_8.booking__ds__day AS booking__ds__day
              , nr_subq_8.booking__ds__week AS booking__ds__week
              , nr_subq_8.booking__ds__month AS booking__ds__month
              , nr_subq_8.booking__ds__quarter AS booking__ds__quarter
              , nr_subq_8.booking__ds__year AS booking__ds__year
              , nr_subq_8.booking__ds__extract_year AS booking__ds__extract_year
              , nr_subq_8.booking__ds__extract_quarter AS booking__ds__extract_quarter
              , nr_subq_8.booking__ds__extract_month AS booking__ds__extract_month
              , nr_subq_8.booking__ds__extract_day AS booking__ds__extract_day
              , nr_subq_8.booking__ds__extract_dow AS booking__ds__extract_dow
              , nr_subq_8.booking__ds__extract_doy AS booking__ds__extract_doy
              , nr_subq_8.booking__ds_partitioned__day AS booking__ds_partitioned__day
              , nr_subq_8.booking__ds_partitioned__week AS booking__ds_partitioned__week
              , nr_subq_8.booking__ds_partitioned__month AS booking__ds_partitioned__month
              , nr_subq_8.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
              , nr_subq_8.booking__ds_partitioned__year AS booking__ds_partitioned__year
              , nr_subq_8.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
              , nr_subq_8.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
              , nr_subq_8.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
              , nr_subq_8.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
              , nr_subq_8.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
              , nr_subq_8.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
              , nr_subq_8.booking__paid_at__day AS booking__paid_at__day
              , nr_subq_8.booking__paid_at__week AS booking__paid_at__week
              , nr_subq_8.booking__paid_at__month AS booking__paid_at__month
              , nr_subq_8.booking__paid_at__quarter AS booking__paid_at__quarter
              , nr_subq_8.booking__paid_at__year AS booking__paid_at__year
              , nr_subq_8.booking__paid_at__extract_year AS booking__paid_at__extract_year
              , nr_subq_8.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
              , nr_subq_8.booking__paid_at__extract_month AS booking__paid_at__extract_month
              , nr_subq_8.booking__paid_at__extract_day AS booking__paid_at__extract_day
              , nr_subq_8.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
              , nr_subq_8.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
              , nr_subq_8.metric_time__day AS metric_time__day
              , nr_subq_8.metric_time__week AS metric_time__week
              , nr_subq_8.metric_time__month AS metric_time__month
              , nr_subq_8.metric_time__quarter AS metric_time__quarter
              , nr_subq_8.metric_time__year AS metric_time__year
              , nr_subq_8.metric_time__extract_year AS metric_time__extract_year
              , nr_subq_8.metric_time__extract_quarter AS metric_time__extract_quarter
              , nr_subq_8.metric_time__extract_month AS metric_time__extract_month
              , nr_subq_8.metric_time__extract_day AS metric_time__extract_day
              , nr_subq_8.metric_time__extract_dow AS metric_time__extract_dow
              , nr_subq_8.metric_time__extract_doy AS metric_time__extract_doy
              , nr_subq_8.listing AS listing
              , nr_subq_8.guest AS guest
              , nr_subq_8.host AS host
              , nr_subq_8.booking__listing AS booking__listing
              , nr_subq_8.booking__guest AS booking__guest
              , nr_subq_8.booking__host AS booking__host
              , nr_subq_8.is_instant AS is_instant
              , nr_subq_8.booking__is_instant AS booking__is_instant
              , nr_subq_8.bookings AS bookings
              , nr_subq_8.instant_bookings AS instant_bookings
              , nr_subq_8.booking_value AS booking_value
              , nr_subq_8.max_booking_value AS max_booking_value
              , nr_subq_8.min_booking_value AS min_booking_value
              , nr_subq_8.bookers AS bookers
              , nr_subq_8.average_booking_value AS average_booking_value
              , nr_subq_8.referred_bookings AS referred_bookings
              , nr_subq_8.median_booking_value AS median_booking_value
              , nr_subq_8.booking_value_p99 AS booking_value_p99
              , nr_subq_8.discrete_booking_value_p99 AS discrete_booking_value_p99
              , nr_subq_8.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
              , nr_subq_8.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
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
            ) nr_subq_8
            LEFT OUTER JOIN (
              -- Pass Only Elements: ['is_lux_latest', 'listing']
              SELECT
                nr_subq_9.listing
                , nr_subq_9.is_lux_latest
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
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS ds__day
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS ds__week
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS ds__month
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS ds__quarter
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS ds__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
                    , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS ds__extract_dow
                    , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS ds__extract_doy
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS created_at__day
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS created_at__week
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS created_at__month
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS created_at__quarter
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS created_at__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
                    , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS created_at__extract_dow
                    , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
                    , listings_latest_src_28000.country AS country_latest
                    , listings_latest_src_28000.is_lux AS is_lux_latest
                    , listings_latest_src_28000.capacity AS capacity_latest
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS listing__ds__day
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__ds__week
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS listing__ds__month
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__ds__quarter
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS listing__ds__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
                    , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS listing__ds__extract_dow
                    , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS listing__created_at__day
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__created_at__week
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS listing__created_at__month
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__created_at__quarter
                    , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS listing__created_at__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_day
                    , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS listing__created_at__extract_dow
                    , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                    , listings_latest_src_28000.country AS listing__country_latest
                    , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                    , listings_latest_src_28000.capacity AS listing__capacity_latest
                    , listings_latest_src_28000.listing_id AS listing
                    , listings_latest_src_28000.user_id AS user
                    , listings_latest_src_28000.user_id AS listing__user
                  FROM ***************************.dim_listings_latest listings_latest_src_28000
                ) nr_subq_28007
              ) nr_subq_9
            ) nr_subq_10
            ON
              nr_subq_8.listing = nr_subq_10.listing
          ) nr_subq_11
          WHERE listing__is_lux_latest
        ) nr_subq_12
      ) nr_subq_13
      GROUP BY
        metric_time__day
    ) nr_subq_14
  ) nr_subq_15
  ON
    nr_subq_7.metric_time__day = nr_subq_15.metric_time__day
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      nr_subq_18.metric_time__day
      , nr_subq_18.booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        nr_subq_17.metric_time__day
        , SUM(nr_subq_17.booking_value) AS booking_value
      FROM (
        -- Pass Only Elements: ['booking_value', 'metric_time__day']
        SELECT
          nr_subq_16.metric_time__day
          , nr_subq_16.booking_value
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
        ) nr_subq_16
      ) nr_subq_17
      GROUP BY
        metric_time__day
    ) nr_subq_18
  ) nr_subq_19
  ON
    COALESCE(nr_subq_7.metric_time__day, nr_subq_15.metric_time__day) = nr_subq_19.metric_time__day
  GROUP BY
    metric_time__day
) nr_subq_20
