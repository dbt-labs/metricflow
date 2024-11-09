test_name: test_compute_metrics_node_ratio_from_multiple_semantic_models
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests the combine metrics node for ratio type metrics.
---
-- Compute Metrics via Expressions
SELECT
  subq_18.ds__day
  , subq_18.listing__country_latest
  , CAST(subq_18.bookings AS DOUBLE PRECISION) / CAST(NULLIF(subq_18.views, 0) AS DOUBLE PRECISION) AS bookings_per_view
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_8.ds__day, subq_17.ds__day) AS ds__day
    , COALESCE(subq_8.listing__country_latest, subq_17.listing__country_latest) AS listing__country_latest
    , MAX(subq_8.bookings) AS bookings
    , MAX(subq_17.views) AS views
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_7.ds__day
      , subq_7.listing__country_latest
      , subq_7.bookings
    FROM (
      -- Aggregate Measures
      SELECT
        subq_6.ds__day
        , subq_6.listing__country_latest
        , SUM(subq_6.bookings) AS bookings
      FROM (
        -- Pass Only Elements: ['bookings', 'listing__country_latest', 'ds__day']
        SELECT
          subq_5.ds__day
          , subq_5.listing__country_latest
          , subq_5.bookings
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
                , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds) END AS ds__extract_dow
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
                , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) END AS ds_partitioned__extract_dow
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
                , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.paid_at) END AS paid_at__extract_dow
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
                , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds) END AS booking__ds__extract_dow
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
                , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) END AS booking__ds_partitioned__extract_dow
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
                , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.paid_at) END AS booking__paid_at__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS ds__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS created_at__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__ds__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__created_at__extract_dow
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
      ) subq_6
      GROUP BY
        subq_6.ds__day
        , subq_6.listing__country_latest
    ) subq_7
  ) subq_8
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_16.ds__day
      , subq_16.listing__country_latest
      , subq_16.views
    FROM (
      -- Aggregate Measures
      SELECT
        subq_15.ds__day
        , subq_15.listing__country_latest
        , SUM(subq_15.views) AS views
      FROM (
        -- Pass Only Elements: ['views', 'listing__country_latest', 'ds__day']
        SELECT
          subq_14.ds__day
          , subq_14.listing__country_latest
          , subq_14.views
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_13.country_latest AS listing__country_latest
            , subq_10.ds__day AS ds__day
            , subq_10.ds__week AS ds__week
            , subq_10.ds__month AS ds__month
            , subq_10.ds__quarter AS ds__quarter
            , subq_10.ds__year AS ds__year
            , subq_10.ds__extract_year AS ds__extract_year
            , subq_10.ds__extract_quarter AS ds__extract_quarter
            , subq_10.ds__extract_month AS ds__extract_month
            , subq_10.ds__extract_day AS ds__extract_day
            , subq_10.ds__extract_dow AS ds__extract_dow
            , subq_10.ds__extract_doy AS ds__extract_doy
            , subq_10.ds_partitioned__day AS ds_partitioned__day
            , subq_10.ds_partitioned__week AS ds_partitioned__week
            , subq_10.ds_partitioned__month AS ds_partitioned__month
            , subq_10.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_10.ds_partitioned__year AS ds_partitioned__year
            , subq_10.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , subq_10.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , subq_10.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , subq_10.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , subq_10.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , subq_10.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , subq_10.view__ds__day AS view__ds__day
            , subq_10.view__ds__week AS view__ds__week
            , subq_10.view__ds__month AS view__ds__month
            , subq_10.view__ds__quarter AS view__ds__quarter
            , subq_10.view__ds__year AS view__ds__year
            , subq_10.view__ds__extract_year AS view__ds__extract_year
            , subq_10.view__ds__extract_quarter AS view__ds__extract_quarter
            , subq_10.view__ds__extract_month AS view__ds__extract_month
            , subq_10.view__ds__extract_day AS view__ds__extract_day
            , subq_10.view__ds__extract_dow AS view__ds__extract_dow
            , subq_10.view__ds__extract_doy AS view__ds__extract_doy
            , subq_10.view__ds_partitioned__day AS view__ds_partitioned__day
            , subq_10.view__ds_partitioned__week AS view__ds_partitioned__week
            , subq_10.view__ds_partitioned__month AS view__ds_partitioned__month
            , subq_10.view__ds_partitioned__quarter AS view__ds_partitioned__quarter
            , subq_10.view__ds_partitioned__year AS view__ds_partitioned__year
            , subq_10.view__ds_partitioned__extract_year AS view__ds_partitioned__extract_year
            , subq_10.view__ds_partitioned__extract_quarter AS view__ds_partitioned__extract_quarter
            , subq_10.view__ds_partitioned__extract_month AS view__ds_partitioned__extract_month
            , subq_10.view__ds_partitioned__extract_day AS view__ds_partitioned__extract_day
            , subq_10.view__ds_partitioned__extract_dow AS view__ds_partitioned__extract_dow
            , subq_10.view__ds_partitioned__extract_doy AS view__ds_partitioned__extract_doy
            , subq_10.metric_time__day AS metric_time__day
            , subq_10.metric_time__week AS metric_time__week
            , subq_10.metric_time__month AS metric_time__month
            , subq_10.metric_time__quarter AS metric_time__quarter
            , subq_10.metric_time__year AS metric_time__year
            , subq_10.metric_time__extract_year AS metric_time__extract_year
            , subq_10.metric_time__extract_quarter AS metric_time__extract_quarter
            , subq_10.metric_time__extract_month AS metric_time__extract_month
            , subq_10.metric_time__extract_day AS metric_time__extract_day
            , subq_10.metric_time__extract_dow AS metric_time__extract_dow
            , subq_10.metric_time__extract_doy AS metric_time__extract_doy
            , subq_10.listing AS listing
            , subq_10.user AS user
            , subq_10.view__listing AS view__listing
            , subq_10.view__user AS view__user
            , subq_10.views AS views
          FROM (
            -- Metric Time Dimension 'ds'
            SELECT
              subq_9.ds__day
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
              , subq_9.ds_partitioned__day
              , subq_9.ds_partitioned__week
              , subq_9.ds_partitioned__month
              , subq_9.ds_partitioned__quarter
              , subq_9.ds_partitioned__year
              , subq_9.ds_partitioned__extract_year
              , subq_9.ds_partitioned__extract_quarter
              , subq_9.ds_partitioned__extract_month
              , subq_9.ds_partitioned__extract_day
              , subq_9.ds_partitioned__extract_dow
              , subq_9.ds_partitioned__extract_doy
              , subq_9.view__ds__day
              , subq_9.view__ds__week
              , subq_9.view__ds__month
              , subq_9.view__ds__quarter
              , subq_9.view__ds__year
              , subq_9.view__ds__extract_year
              , subq_9.view__ds__extract_quarter
              , subq_9.view__ds__extract_month
              , subq_9.view__ds__extract_day
              , subq_9.view__ds__extract_dow
              , subq_9.view__ds__extract_doy
              , subq_9.view__ds_partitioned__day
              , subq_9.view__ds_partitioned__week
              , subq_9.view__ds_partitioned__month
              , subq_9.view__ds_partitioned__quarter
              , subq_9.view__ds_partitioned__year
              , subq_9.view__ds_partitioned__extract_year
              , subq_9.view__ds_partitioned__extract_quarter
              , subq_9.view__ds_partitioned__extract_month
              , subq_9.view__ds_partitioned__extract_day
              , subq_9.view__ds_partitioned__extract_dow
              , subq_9.view__ds_partitioned__extract_doy
              , subq_9.ds__day AS metric_time__day
              , subq_9.ds__week AS metric_time__week
              , subq_9.ds__month AS metric_time__month
              , subq_9.ds__quarter AS metric_time__quarter
              , subq_9.ds__year AS metric_time__year
              , subq_9.ds__extract_year AS metric_time__extract_year
              , subq_9.ds__extract_quarter AS metric_time__extract_quarter
              , subq_9.ds__extract_month AS metric_time__extract_month
              , subq_9.ds__extract_day AS metric_time__extract_day
              , subq_9.ds__extract_dow AS metric_time__extract_dow
              , subq_9.ds__extract_doy AS metric_time__extract_doy
              , subq_9.listing
              , subq_9.user
              , subq_9.view__listing
              , subq_9.view__user
              , subq_9.views
            FROM (
              -- Read Elements From Semantic Model 'views_source'
              SELECT
                1 AS views
                , DATE_TRUNC('day', views_source_src_28000.ds) AS ds__day
                , DATE_TRUNC('week', views_source_src_28000.ds) AS ds__week
                , DATE_TRUNC('month', views_source_src_28000.ds) AS ds__month
                , DATE_TRUNC('quarter', views_source_src_28000.ds) AS ds__quarter
                , DATE_TRUNC('year', views_source_src_28000.ds) AS ds__year
                , EXTRACT(year FROM views_source_src_28000.ds) AS ds__extract_year
                , EXTRACT(quarter FROM views_source_src_28000.ds) AS ds__extract_quarter
                , EXTRACT(month FROM views_source_src_28000.ds) AS ds__extract_month
                , EXTRACT(day FROM views_source_src_28000.ds) AS ds__extract_day
                , CASE WHEN EXTRACT(dow FROM views_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM views_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM views_source_src_28000.ds) END AS ds__extract_dow
                , EXTRACT(doy FROM views_source_src_28000.ds) AS ds__extract_doy
                , DATE_TRUNC('day', views_source_src_28000.ds_partitioned) AS ds_partitioned__day
                , DATE_TRUNC('week', views_source_src_28000.ds_partitioned) AS ds_partitioned__week
                , DATE_TRUNC('month', views_source_src_28000.ds_partitioned) AS ds_partitioned__month
                , DATE_TRUNC('quarter', views_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                , DATE_TRUNC('year', views_source_src_28000.ds_partitioned) AS ds_partitioned__year
                , EXTRACT(year FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                , EXTRACT(quarter FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                , EXTRACT(month FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                , EXTRACT(day FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                , CASE WHEN EXTRACT(dow FROM views_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM views_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM views_source_src_28000.ds_partitioned) END AS ds_partitioned__extract_dow
                , EXTRACT(doy FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                , DATE_TRUNC('day', views_source_src_28000.ds) AS view__ds__day
                , DATE_TRUNC('week', views_source_src_28000.ds) AS view__ds__week
                , DATE_TRUNC('month', views_source_src_28000.ds) AS view__ds__month
                , DATE_TRUNC('quarter', views_source_src_28000.ds) AS view__ds__quarter
                , DATE_TRUNC('year', views_source_src_28000.ds) AS view__ds__year
                , EXTRACT(year FROM views_source_src_28000.ds) AS view__ds__extract_year
                , EXTRACT(quarter FROM views_source_src_28000.ds) AS view__ds__extract_quarter
                , EXTRACT(month FROM views_source_src_28000.ds) AS view__ds__extract_month
                , EXTRACT(day FROM views_source_src_28000.ds) AS view__ds__extract_day
                , CASE WHEN EXTRACT(dow FROM views_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM views_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM views_source_src_28000.ds) END AS view__ds__extract_dow
                , EXTRACT(doy FROM views_source_src_28000.ds) AS view__ds__extract_doy
                , DATE_TRUNC('day', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__day
                , DATE_TRUNC('week', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__week
                , DATE_TRUNC('month', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__month
                , DATE_TRUNC('quarter', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__quarter
                , DATE_TRUNC('year', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__year
                , EXTRACT(year FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_year
                , EXTRACT(quarter FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_quarter
                , EXTRACT(month FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_month
                , EXTRACT(day FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_day
                , CASE WHEN EXTRACT(dow FROM views_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM views_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM views_source_src_28000.ds_partitioned) END AS view__ds_partitioned__extract_dow
                , EXTRACT(doy FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_doy
                , views_source_src_28000.listing_id AS listing
                , views_source_src_28000.user_id AS user
                , views_source_src_28000.listing_id AS view__listing
                , views_source_src_28000.user_id AS view__user
              FROM ***************************.fct_views views_source_src_28000
            ) subq_9
          ) subq_10
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['country_latest', 'listing']
            SELECT
              subq_12.listing
              , subq_12.country_latest
            FROM (
              -- Metric Time Dimension 'ds'
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
                , subq_11.created_at__day
                , subq_11.created_at__week
                , subq_11.created_at__month
                , subq_11.created_at__quarter
                , subq_11.created_at__year
                , subq_11.created_at__extract_year
                , subq_11.created_at__extract_quarter
                , subq_11.created_at__extract_month
                , subq_11.created_at__extract_day
                , subq_11.created_at__extract_dow
                , subq_11.created_at__extract_doy
                , subq_11.listing__ds__day
                , subq_11.listing__ds__week
                , subq_11.listing__ds__month
                , subq_11.listing__ds__quarter
                , subq_11.listing__ds__year
                , subq_11.listing__ds__extract_year
                , subq_11.listing__ds__extract_quarter
                , subq_11.listing__ds__extract_month
                , subq_11.listing__ds__extract_day
                , subq_11.listing__ds__extract_dow
                , subq_11.listing__ds__extract_doy
                , subq_11.listing__created_at__day
                , subq_11.listing__created_at__week
                , subq_11.listing__created_at__month
                , subq_11.listing__created_at__quarter
                , subq_11.listing__created_at__year
                , subq_11.listing__created_at__extract_year
                , subq_11.listing__created_at__extract_quarter
                , subq_11.listing__created_at__extract_month
                , subq_11.listing__created_at__extract_day
                , subq_11.listing__created_at__extract_dow
                , subq_11.listing__created_at__extract_doy
                , subq_11.ds__day AS metric_time__day
                , subq_11.ds__week AS metric_time__week
                , subq_11.ds__month AS metric_time__month
                , subq_11.ds__quarter AS metric_time__quarter
                , subq_11.ds__year AS metric_time__year
                , subq_11.ds__extract_year AS metric_time__extract_year
                , subq_11.ds__extract_quarter AS metric_time__extract_quarter
                , subq_11.ds__extract_month AS metric_time__extract_month
                , subq_11.ds__extract_day AS metric_time__extract_day
                , subq_11.ds__extract_dow AS metric_time__extract_dow
                , subq_11.ds__extract_doy AS metric_time__extract_doy
                , subq_11.listing
                , subq_11.user
                , subq_11.listing__user
                , subq_11.country_latest
                , subq_11.is_lux_latest
                , subq_11.capacity_latest
                , subq_11.listing__country_latest
                , subq_11.listing__is_lux_latest
                , subq_11.listing__capacity_latest
                , subq_11.listings
                , subq_11.largest_listing
                , subq_11.smallest_listing
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
                  , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS ds__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS created_at__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__ds__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__created_at__extract_dow
                  , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                  , listings_latest_src_28000.country AS listing__country_latest
                  , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                  , listings_latest_src_28000.capacity AS listing__capacity_latest
                  , listings_latest_src_28000.listing_id AS listing
                  , listings_latest_src_28000.user_id AS user
                  , listings_latest_src_28000.user_id AS listing__user
                FROM ***************************.dim_listings_latest listings_latest_src_28000
              ) subq_11
            ) subq_12
          ) subq_13
          ON
            subq_10.listing = subq_13.listing
        ) subq_14
      ) subq_15
      GROUP BY
        subq_15.ds__day
        , subq_15.listing__country_latest
    ) subq_16
  ) subq_17
  ON
    (
      subq_8.listing__country_latest = subq_17.listing__country_latest
    ) AND (
      subq_8.ds__day = subq_17.ds__day
    )
  GROUP BY
    COALESCE(subq_8.ds__day, subq_17.ds__day)
    , COALESCE(subq_8.listing__country_latest, subq_17.listing__country_latest)
) subq_18
