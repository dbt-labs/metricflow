test_name: test_query_with_multiple_metrics_in_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with 2 simple metrics in the query-level where filter.
sql_engine: Databricks
---
-- Compute Metrics via Expressions
SELECT
  subq_17.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_16.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      subq_15.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_14.ds__day
        , subq_14.ds__week
        , subq_14.ds__month
        , subq_14.ds__quarter
        , subq_14.ds__year
        , subq_14.ds__extract_year
        , subq_14.ds__extract_quarter
        , subq_14.ds__extract_month
        , subq_14.ds__extract_day
        , subq_14.ds__extract_dow
        , subq_14.ds__extract_doy
        , subq_14.created_at__day
        , subq_14.created_at__week
        , subq_14.created_at__month
        , subq_14.created_at__quarter
        , subq_14.created_at__year
        , subq_14.created_at__extract_year
        , subq_14.created_at__extract_quarter
        , subq_14.created_at__extract_month
        , subq_14.created_at__extract_day
        , subq_14.created_at__extract_dow
        , subq_14.created_at__extract_doy
        , subq_14.listing__ds__day
        , subq_14.listing__ds__week
        , subq_14.listing__ds__month
        , subq_14.listing__ds__quarter
        , subq_14.listing__ds__year
        , subq_14.listing__ds__extract_year
        , subq_14.listing__ds__extract_quarter
        , subq_14.listing__ds__extract_month
        , subq_14.listing__ds__extract_day
        , subq_14.listing__ds__extract_dow
        , subq_14.listing__ds__extract_doy
        , subq_14.listing__created_at__day
        , subq_14.listing__created_at__week
        , subq_14.listing__created_at__month
        , subq_14.listing__created_at__quarter
        , subq_14.listing__created_at__year
        , subq_14.listing__created_at__extract_year
        , subq_14.listing__created_at__extract_quarter
        , subq_14.listing__created_at__extract_month
        , subq_14.listing__created_at__extract_day
        , subq_14.listing__created_at__extract_dow
        , subq_14.listing__created_at__extract_doy
        , subq_14.metric_time__day
        , subq_14.metric_time__week
        , subq_14.metric_time__month
        , subq_14.metric_time__quarter
        , subq_14.metric_time__year
        , subq_14.metric_time__extract_year
        , subq_14.metric_time__extract_quarter
        , subq_14.metric_time__extract_month
        , subq_14.metric_time__extract_day
        , subq_14.metric_time__extract_dow
        , subq_14.metric_time__extract_doy
        , subq_14.listing
        , subq_14.user
        , subq_14.listing__user
        , subq_14.country_latest
        , subq_14.is_lux_latest
        , subq_14.capacity_latest
        , subq_14.listing__country_latest
        , subq_14.listing__is_lux_latest
        , subq_14.listing__capacity_latest
        , subq_14.listing__bookings
        , subq_14.listing__bookers
        , subq_14.listings
        , subq_14.largest_listing
        , subq_14.smallest_listing
      FROM (
        -- Join Standard Outputs
        SELECT
          subq_7.listing__bookings AS listing__bookings
          , subq_13.listing__bookers AS listing__bookers
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
          , subq_1.created_at__day AS created_at__day
          , subq_1.created_at__week AS created_at__week
          , subq_1.created_at__month AS created_at__month
          , subq_1.created_at__quarter AS created_at__quarter
          , subq_1.created_at__year AS created_at__year
          , subq_1.created_at__extract_year AS created_at__extract_year
          , subq_1.created_at__extract_quarter AS created_at__extract_quarter
          , subq_1.created_at__extract_month AS created_at__extract_month
          , subq_1.created_at__extract_day AS created_at__extract_day
          , subq_1.created_at__extract_dow AS created_at__extract_dow
          , subq_1.created_at__extract_doy AS created_at__extract_doy
          , subq_1.listing__ds__day AS listing__ds__day
          , subq_1.listing__ds__week AS listing__ds__week
          , subq_1.listing__ds__month AS listing__ds__month
          , subq_1.listing__ds__quarter AS listing__ds__quarter
          , subq_1.listing__ds__year AS listing__ds__year
          , subq_1.listing__ds__extract_year AS listing__ds__extract_year
          , subq_1.listing__ds__extract_quarter AS listing__ds__extract_quarter
          , subq_1.listing__ds__extract_month AS listing__ds__extract_month
          , subq_1.listing__ds__extract_day AS listing__ds__extract_day
          , subq_1.listing__ds__extract_dow AS listing__ds__extract_dow
          , subq_1.listing__ds__extract_doy AS listing__ds__extract_doy
          , subq_1.listing__created_at__day AS listing__created_at__day
          , subq_1.listing__created_at__week AS listing__created_at__week
          , subq_1.listing__created_at__month AS listing__created_at__month
          , subq_1.listing__created_at__quarter AS listing__created_at__quarter
          , subq_1.listing__created_at__year AS listing__created_at__year
          , subq_1.listing__created_at__extract_year AS listing__created_at__extract_year
          , subq_1.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
          , subq_1.listing__created_at__extract_month AS listing__created_at__extract_month
          , subq_1.listing__created_at__extract_day AS listing__created_at__extract_day
          , subq_1.listing__created_at__extract_dow AS listing__created_at__extract_dow
          , subq_1.listing__created_at__extract_doy AS listing__created_at__extract_doy
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
          , subq_1.user AS user
          , subq_1.listing__user AS listing__user
          , subq_1.country_latest AS country_latest
          , subq_1.is_lux_latest AS is_lux_latest
          , subq_1.capacity_latest AS capacity_latest
          , subq_1.listing__country_latest AS listing__country_latest
          , subq_1.listing__is_lux_latest AS listing__is_lux_latest
          , subq_1.listing__capacity_latest AS listing__capacity_latest
          , subq_1.listings AS listings
          , subq_1.largest_listing AS largest_listing
          , subq_1.smallest_listing AS smallest_listing
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
            , subq_0.created_at__day
            , subq_0.created_at__week
            , subq_0.created_at__month
            , subq_0.created_at__quarter
            , subq_0.created_at__year
            , subq_0.created_at__extract_year
            , subq_0.created_at__extract_quarter
            , subq_0.created_at__extract_month
            , subq_0.created_at__extract_day
            , subq_0.created_at__extract_dow
            , subq_0.created_at__extract_doy
            , subq_0.listing__ds__day
            , subq_0.listing__ds__week
            , subq_0.listing__ds__month
            , subq_0.listing__ds__quarter
            , subq_0.listing__ds__year
            , subq_0.listing__ds__extract_year
            , subq_0.listing__ds__extract_quarter
            , subq_0.listing__ds__extract_month
            , subq_0.listing__ds__extract_day
            , subq_0.listing__ds__extract_dow
            , subq_0.listing__ds__extract_doy
            , subq_0.listing__created_at__day
            , subq_0.listing__created_at__week
            , subq_0.listing__created_at__month
            , subq_0.listing__created_at__quarter
            , subq_0.listing__created_at__year
            , subq_0.listing__created_at__extract_year
            , subq_0.listing__created_at__extract_quarter
            , subq_0.listing__created_at__extract_month
            , subq_0.listing__created_at__extract_day
            , subq_0.listing__created_at__extract_dow
            , subq_0.listing__created_at__extract_doy
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
            , subq_0.user
            , subq_0.listing__user
            , subq_0.country_latest
            , subq_0.is_lux_latest
            , subq_0.capacity_latest
            , subq_0.listing__country_latest
            , subq_0.listing__is_lux_latest
            , subq_0.listing__capacity_latest
            , subq_0.listings
            , subq_0.largest_listing
            , subq_0.smallest_listing
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
          ) subq_0
        ) subq_1
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['listing', 'listing__bookings']
          SELECT
            subq_6.listing
            , subq_6.listing__bookings
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              subq_5.listing
              , subq_5.bookings AS listing__bookings
            FROM (
              -- Aggregate Measures
              SELECT
                subq_4.listing
                , SUM(subq_4.bookings) AS bookings
              FROM (
                -- Pass Only Elements: ['bookings', 'listing']
                SELECT
                  subq_3.listing
                  , subq_3.bookings
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
                  ) subq_2
                ) subq_3
              ) subq_4
              GROUP BY
                subq_4.listing
            ) subq_5
          ) subq_6
        ) subq_7
        ON
          subq_1.listing = subq_7.listing
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['listing', 'listing__bookers']
          SELECT
            subq_12.listing
            , subq_12.listing__bookers
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              subq_11.listing
              , subq_11.bookers AS listing__bookers
            FROM (
              -- Aggregate Measures
              SELECT
                subq_10.listing
                , COUNT(DISTINCT subq_10.bookers) AS bookers
              FROM (
                -- Pass Only Elements: ['bookers', 'listing']
                SELECT
                  subq_9.listing
                  , subq_9.bookers
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_8.ds__day
                    , subq_8.ds__week
                    , subq_8.ds__month
                    , subq_8.ds__quarter
                    , subq_8.ds__year
                    , subq_8.ds__extract_year
                    , subq_8.ds__extract_quarter
                    , subq_8.ds__extract_month
                    , subq_8.ds__extract_day
                    , subq_8.ds__extract_dow
                    , subq_8.ds__extract_doy
                    , subq_8.ds_partitioned__day
                    , subq_8.ds_partitioned__week
                    , subq_8.ds_partitioned__month
                    , subq_8.ds_partitioned__quarter
                    , subq_8.ds_partitioned__year
                    , subq_8.ds_partitioned__extract_year
                    , subq_8.ds_partitioned__extract_quarter
                    , subq_8.ds_partitioned__extract_month
                    , subq_8.ds_partitioned__extract_day
                    , subq_8.ds_partitioned__extract_dow
                    , subq_8.ds_partitioned__extract_doy
                    , subq_8.paid_at__day
                    , subq_8.paid_at__week
                    , subq_8.paid_at__month
                    , subq_8.paid_at__quarter
                    , subq_8.paid_at__year
                    , subq_8.paid_at__extract_year
                    , subq_8.paid_at__extract_quarter
                    , subq_8.paid_at__extract_month
                    , subq_8.paid_at__extract_day
                    , subq_8.paid_at__extract_dow
                    , subq_8.paid_at__extract_doy
                    , subq_8.booking__ds__day
                    , subq_8.booking__ds__week
                    , subq_8.booking__ds__month
                    , subq_8.booking__ds__quarter
                    , subq_8.booking__ds__year
                    , subq_8.booking__ds__extract_year
                    , subq_8.booking__ds__extract_quarter
                    , subq_8.booking__ds__extract_month
                    , subq_8.booking__ds__extract_day
                    , subq_8.booking__ds__extract_dow
                    , subq_8.booking__ds__extract_doy
                    , subq_8.booking__ds_partitioned__day
                    , subq_8.booking__ds_partitioned__week
                    , subq_8.booking__ds_partitioned__month
                    , subq_8.booking__ds_partitioned__quarter
                    , subq_8.booking__ds_partitioned__year
                    , subq_8.booking__ds_partitioned__extract_year
                    , subq_8.booking__ds_partitioned__extract_quarter
                    , subq_8.booking__ds_partitioned__extract_month
                    , subq_8.booking__ds_partitioned__extract_day
                    , subq_8.booking__ds_partitioned__extract_dow
                    , subq_8.booking__ds_partitioned__extract_doy
                    , subq_8.booking__paid_at__day
                    , subq_8.booking__paid_at__week
                    , subq_8.booking__paid_at__month
                    , subq_8.booking__paid_at__quarter
                    , subq_8.booking__paid_at__year
                    , subq_8.booking__paid_at__extract_year
                    , subq_8.booking__paid_at__extract_quarter
                    , subq_8.booking__paid_at__extract_month
                    , subq_8.booking__paid_at__extract_day
                    , subq_8.booking__paid_at__extract_dow
                    , subq_8.booking__paid_at__extract_doy
                    , subq_8.ds__day AS metric_time__day
                    , subq_8.ds__week AS metric_time__week
                    , subq_8.ds__month AS metric_time__month
                    , subq_8.ds__quarter AS metric_time__quarter
                    , subq_8.ds__year AS metric_time__year
                    , subq_8.ds__extract_year AS metric_time__extract_year
                    , subq_8.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_8.ds__extract_month AS metric_time__extract_month
                    , subq_8.ds__extract_day AS metric_time__extract_day
                    , subq_8.ds__extract_dow AS metric_time__extract_dow
                    , subq_8.ds__extract_doy AS metric_time__extract_doy
                    , subq_8.listing
                    , subq_8.guest
                    , subq_8.host
                    , subq_8.booking__listing
                    , subq_8.booking__guest
                    , subq_8.booking__host
                    , subq_8.is_instant
                    , subq_8.booking__is_instant
                    , subq_8.bookings
                    , subq_8.instant_bookings
                    , subq_8.booking_value
                    , subq_8.max_booking_value
                    , subq_8.min_booking_value
                    , subq_8.bookers
                    , subq_8.average_booking_value
                    , subq_8.referred_bookings
                    , subq_8.median_booking_value
                    , subq_8.booking_value_p99
                    , subq_8.discrete_booking_value_p99
                    , subq_8.approximate_continuous_booking_value_p99
                    , subq_8.approximate_discrete_booking_value_p99
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
                  ) subq_8
                ) subq_9
              ) subq_10
              GROUP BY
                subq_10.listing
            ) subq_11
          ) subq_12
        ) subq_13
        ON
          subq_1.listing = subq_13.listing
      ) subq_14
      WHERE listing__bookings > 2 AND listing__bookers > 1
    ) subq_15
  ) subq_16
) subq_17
