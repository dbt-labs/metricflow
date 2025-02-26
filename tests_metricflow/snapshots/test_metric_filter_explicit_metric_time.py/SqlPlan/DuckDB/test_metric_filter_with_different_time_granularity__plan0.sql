test_name: test_metric_filter_with_different_time_granularity
test_filename: test_metric_filter_explicit_metric_time.py
docstring:
  Tests a query with a metric filter where the parent query has a different time granularity.

      This test verifies that the parent query's time granularity is respected even when the filter
      doesn't explicitly include metric_time in its group_by list.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  subq_14.metric_time__day
  , subq_14.bookings
FROM (
  -- Aggregate Measures
  SELECT
    subq_13.metric_time__day
    , SUM(subq_13.bookings) AS bookings
  FROM (
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    SELECT
      subq_12.metric_time__day
      , subq_12.bookings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_11.ds__day
        , subq_11.ds__week
        , subq_11.ds__month
        , subq_11.ds__quarter
        , subq_11.ds__year
        , subq_11.ds__extract_year
        , subq_11.ds__extract_quarter
        , subq_11.ds__extract_month
        , subq_11.ds__extract_day
        , subq_11.ds__extract_dow
        , subq_11.ds__extract_doy
        , subq_11.ds_partitioned__day
        , subq_11.ds_partitioned__week
        , subq_11.ds_partitioned__month
        , subq_11.ds_partitioned__quarter
        , subq_11.ds_partitioned__year
        , subq_11.ds_partitioned__extract_year
        , subq_11.ds_partitioned__extract_quarter
        , subq_11.ds_partitioned__extract_month
        , subq_11.ds_partitioned__extract_day
        , subq_11.ds_partitioned__extract_dow
        , subq_11.ds_partitioned__extract_doy
        , subq_11.paid_at__day
        , subq_11.paid_at__week
        , subq_11.paid_at__month
        , subq_11.paid_at__quarter
        , subq_11.paid_at__year
        , subq_11.paid_at__extract_year
        , subq_11.paid_at__extract_quarter
        , subq_11.paid_at__extract_month
        , subq_11.paid_at__extract_day
        , subq_11.paid_at__extract_dow
        , subq_11.paid_at__extract_doy
        , subq_11.booking__ds__day
        , subq_11.booking__ds__week
        , subq_11.booking__ds__month
        , subq_11.booking__ds__quarter
        , subq_11.booking__ds__year
        , subq_11.booking__ds__extract_year
        , subq_11.booking__ds__extract_quarter
        , subq_11.booking__ds__extract_month
        , subq_11.booking__ds__extract_day
        , subq_11.booking__ds__extract_dow
        , subq_11.booking__ds__extract_doy
        , subq_11.booking__ds_partitioned__day
        , subq_11.booking__ds_partitioned__week
        , subq_11.booking__ds_partitioned__month
        , subq_11.booking__ds_partitioned__quarter
        , subq_11.booking__ds_partitioned__year
        , subq_11.booking__ds_partitioned__extract_year
        , subq_11.booking__ds_partitioned__extract_quarter
        , subq_11.booking__ds_partitioned__extract_month
        , subq_11.booking__ds_partitioned__extract_day
        , subq_11.booking__ds_partitioned__extract_dow
        , subq_11.booking__ds_partitioned__extract_doy
        , subq_11.booking__paid_at__day
        , subq_11.booking__paid_at__week
        , subq_11.booking__paid_at__month
        , subq_11.booking__paid_at__quarter
        , subq_11.booking__paid_at__year
        , subq_11.booking__paid_at__extract_year
        , subq_11.booking__paid_at__extract_quarter
        , subq_11.booking__paid_at__extract_month
        , subq_11.booking__paid_at__extract_day
        , subq_11.booking__paid_at__extract_dow
        , subq_11.booking__paid_at__extract_doy
        , subq_11.metric_time__day
        , subq_11.metric_time__week
        , subq_11.metric_time__month
        , subq_11.metric_time__quarter
        , subq_11.metric_time__year
        , subq_11.metric_time__extract_year
        , subq_11.metric_time__extract_quarter
        , subq_11.metric_time__extract_month
        , subq_11.metric_time__extract_day
        , subq_11.metric_time__extract_dow
        , subq_11.metric_time__extract_doy
        , subq_11.listing
        , subq_11.guest
        , subq_11.host
        , subq_11.booking__listing
        , subq_11.booking__guest
        , subq_11.booking__host
        , subq_11.is_instant
        , subq_11.booking__is_instant
        , subq_11.listing__listings
        , subq_11.bookings
        , subq_11.instant_bookings
        , subq_11.booking_value
        , subq_11.max_booking_value
        , subq_11.min_booking_value
        , subq_11.bookers
        , subq_11.average_booking_value
        , subq_11.referred_bookings
        , subq_11.median_booking_value
        , subq_11.booking_value_p99
        , subq_11.discrete_booking_value_p99
        , subq_11.approximate_continuous_booking_value_p99
        , subq_11.approximate_discrete_booking_value_p99
      FROM (
        -- Join Standard Outputs
        SELECT
          subq_10.listing__listings AS listing__listings
          , subq_4.ds__day AS ds__day
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
          , subq_4.metric_time__day AS metric_time__day
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
          -- Metric Time Dimension 'ds'
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
            , subq_3.ds__day AS metric_time__day
            , subq_3.ds__week AS metric_time__week
            , subq_3.ds__month AS metric_time__month
            , subq_3.ds__quarter AS metric_time__quarter
            , subq_3.ds__year AS metric_time__year
            , subq_3.ds__extract_year AS metric_time__extract_year
            , subq_3.ds__extract_quarter AS metric_time__extract_quarter
            , subq_3.ds__extract_month AS metric_time__extract_month
            , subq_3.ds__extract_day AS metric_time__extract_day
            , subq_3.ds__extract_dow AS metric_time__extract_dow
            , subq_3.ds__extract_doy AS metric_time__extract_doy
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
              , EXTRACT(isodow FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
              , EXTRACT(isodow FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
              , EXTRACT(isodow FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
              , EXTRACT(isodow FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
              , EXTRACT(isodow FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
              , EXTRACT(isodow FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
              , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
              , bookings_source_src_28000.listing_id AS listing
              , bookings_source_src_28000.guest_id AS guest
              , bookings_source_src_28000.host_id AS host
              , bookings_source_src_28000.listing_id AS booking__listing
              , bookings_source_src_28000.guest_id AS booking__guest
              , bookings_source_src_28000.host_id AS booking__host
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) subq_3
        ) subq_4
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['listing', 'listing__listings']
          SELECT
            subq_9.listing
            , subq_9.listing__listings
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              subq_8.listing
              , subq_8.listings AS listing__listings
            FROM (
              -- Aggregate Measures
              SELECT
                subq_7.listing
                , SUM(subq_7.listings) AS listings
              FROM (
                -- Pass Only Elements: ['listings', 'listing']
                SELECT
                  subq_6.listing
                  , subq_6.listings
                FROM (
                  -- Metric Time Dimension 'ds'
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
                    , subq_5.created_at__day
                    , subq_5.created_at__week
                    , subq_5.created_at__month
                    , subq_5.created_at__quarter
                    , subq_5.created_at__year
                    , subq_5.created_at__extract_year
                    , subq_5.created_at__extract_quarter
                    , subq_5.created_at__extract_month
                    , subq_5.created_at__extract_day
                    , subq_5.created_at__extract_dow
                    , subq_5.created_at__extract_doy
                    , subq_5.listing__ds__day
                    , subq_5.listing__ds__week
                    , subq_5.listing__ds__month
                    , subq_5.listing__ds__quarter
                    , subq_5.listing__ds__year
                    , subq_5.listing__ds__extract_year
                    , subq_5.listing__ds__extract_quarter
                    , subq_5.listing__ds__extract_month
                    , subq_5.listing__ds__extract_day
                    , subq_5.listing__ds__extract_dow
                    , subq_5.listing__ds__extract_doy
                    , subq_5.listing__created_at__day
                    , subq_5.listing__created_at__week
                    , subq_5.listing__created_at__month
                    , subq_5.listing__created_at__quarter
                    , subq_5.listing__created_at__year
                    , subq_5.listing__created_at__extract_year
                    , subq_5.listing__created_at__extract_quarter
                    , subq_5.listing__created_at__extract_month
                    , subq_5.listing__created_at__extract_day
                    , subq_5.listing__created_at__extract_dow
                    , subq_5.listing__created_at__extract_doy
                    , subq_5.ds__day AS metric_time__day
                    , subq_5.ds__week AS metric_time__week
                    , subq_5.ds__month AS metric_time__month
                    , subq_5.ds__quarter AS metric_time__quarter
                    , subq_5.ds__year AS metric_time__year
                    , subq_5.ds__extract_year AS metric_time__extract_year
                    , subq_5.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_5.ds__extract_month AS metric_time__extract_month
                    , subq_5.ds__extract_day AS metric_time__extract_day
                    , subq_5.ds__extract_dow AS metric_time__extract_dow
                    , subq_5.ds__extract_doy AS metric_time__extract_doy
                    , subq_5.listing
                    , subq_5.user
                    , subq_5.listing__user
                    , subq_5.country_latest
                    , subq_5.is_lux_latest
                    , subq_5.capacity_latest
                    , subq_5.listing__country_latest
                    , subq_5.listing__is_lux_latest
                    , subq_5.listing__capacity_latest
                    , subq_5.listings
                    , subq_5.largest_listing
                    , subq_5.smallest_listing
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
                      , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS ds__extract_dow
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
                      , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
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
                      , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
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
                      , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                      , listings_latest_src_28000.country AS listing__country_latest
                      , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                      , listings_latest_src_28000.capacity AS listing__capacity_latest
                      , listings_latest_src_28000.listing_id AS listing
                      , listings_latest_src_28000.user_id AS user
                      , listings_latest_src_28000.user_id AS listing__user
                    FROM ***************************.dim_listings_latest listings_latest_src_28000
                  ) subq_5
                ) subq_6
              ) subq_7
              GROUP BY
                subq_7.listing
            ) subq_8
          ) subq_9
        ) subq_10
        ON
          subq_4.listing = subq_10.listing
      ) subq_11
      WHERE listing__listings > 0
    ) subq_12
  ) subq_13
  GROUP BY
    subq_13.metric_time__day
) subq_14
