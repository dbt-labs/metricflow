test_name: test_query_with_multiple_metrics_in_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with 2 simple metrics in the query-level where filter.
sql_engine: Postgres
---
-- Compute Metrics via Expressions
SELECT
  subq_22.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_21.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      subq_20.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_19.ds__day
        , subq_19.ds__week
        , subq_19.ds__month
        , subq_19.ds__quarter
        , subq_19.ds__year
        , subq_19.ds__extract_year
        , subq_19.ds__extract_quarter
        , subq_19.ds__extract_month
        , subq_19.ds__extract_day
        , subq_19.ds__extract_dow
        , subq_19.ds__extract_doy
        , subq_19.created_at__day
        , subq_19.created_at__week
        , subq_19.created_at__month
        , subq_19.created_at__quarter
        , subq_19.created_at__year
        , subq_19.created_at__extract_year
        , subq_19.created_at__extract_quarter
        , subq_19.created_at__extract_month
        , subq_19.created_at__extract_day
        , subq_19.created_at__extract_dow
        , subq_19.created_at__extract_doy
        , subq_19.listing__ds__day
        , subq_19.listing__ds__week
        , subq_19.listing__ds__month
        , subq_19.listing__ds__quarter
        , subq_19.listing__ds__year
        , subq_19.listing__ds__extract_year
        , subq_19.listing__ds__extract_quarter
        , subq_19.listing__ds__extract_month
        , subq_19.listing__ds__extract_day
        , subq_19.listing__ds__extract_dow
        , subq_19.listing__ds__extract_doy
        , subq_19.listing__created_at__day
        , subq_19.listing__created_at__week
        , subq_19.listing__created_at__month
        , subq_19.listing__created_at__quarter
        , subq_19.listing__created_at__year
        , subq_19.listing__created_at__extract_year
        , subq_19.listing__created_at__extract_quarter
        , subq_19.listing__created_at__extract_month
        , subq_19.listing__created_at__extract_day
        , subq_19.listing__created_at__extract_dow
        , subq_19.listing__created_at__extract_doy
        , subq_19.metric_time__day
        , subq_19.metric_time__week
        , subq_19.metric_time__month
        , subq_19.metric_time__quarter
        , subq_19.metric_time__year
        , subq_19.metric_time__extract_year
        , subq_19.metric_time__extract_quarter
        , subq_19.metric_time__extract_month
        , subq_19.metric_time__extract_day
        , subq_19.metric_time__extract_dow
        , subq_19.metric_time__extract_doy
        , subq_19.listing
        , subq_19.user
        , subq_19.listing__user
        , subq_19.country_latest
        , subq_19.is_lux_latest
        , subq_19.capacity_latest
        , subq_19.listing__country_latest
        , subq_19.listing__is_lux_latest
        , subq_19.listing__capacity_latest
        , subq_19.listing__bookings
        , subq_19.listing__bookers
        , subq_19.listings
        , subq_19.largest_listing
        , subq_19.smallest_listing
      FROM (
        -- Join Standard Outputs
        SELECT
          subq_13.listing__bookings AS listing__bookings
          , subq_18.listing__bookers AS listing__bookers
          , subq_7.ds__day AS ds__day
          , subq_7.ds__week AS ds__week
          , subq_7.ds__month AS ds__month
          , subq_7.ds__quarter AS ds__quarter
          , subq_7.ds__year AS ds__year
          , subq_7.ds__extract_year AS ds__extract_year
          , subq_7.ds__extract_quarter AS ds__extract_quarter
          , subq_7.ds__extract_month AS ds__extract_month
          , subq_7.ds__extract_day AS ds__extract_day
          , subq_7.ds__extract_dow AS ds__extract_dow
          , subq_7.ds__extract_doy AS ds__extract_doy
          , subq_7.created_at__day AS created_at__day
          , subq_7.created_at__week AS created_at__week
          , subq_7.created_at__month AS created_at__month
          , subq_7.created_at__quarter AS created_at__quarter
          , subq_7.created_at__year AS created_at__year
          , subq_7.created_at__extract_year AS created_at__extract_year
          , subq_7.created_at__extract_quarter AS created_at__extract_quarter
          , subq_7.created_at__extract_month AS created_at__extract_month
          , subq_7.created_at__extract_day AS created_at__extract_day
          , subq_7.created_at__extract_dow AS created_at__extract_dow
          , subq_7.created_at__extract_doy AS created_at__extract_doy
          , subq_7.listing__ds__day AS listing__ds__day
          , subq_7.listing__ds__week AS listing__ds__week
          , subq_7.listing__ds__month AS listing__ds__month
          , subq_7.listing__ds__quarter AS listing__ds__quarter
          , subq_7.listing__ds__year AS listing__ds__year
          , subq_7.listing__ds__extract_year AS listing__ds__extract_year
          , subq_7.listing__ds__extract_quarter AS listing__ds__extract_quarter
          , subq_7.listing__ds__extract_month AS listing__ds__extract_month
          , subq_7.listing__ds__extract_day AS listing__ds__extract_day
          , subq_7.listing__ds__extract_dow AS listing__ds__extract_dow
          , subq_7.listing__ds__extract_doy AS listing__ds__extract_doy
          , subq_7.listing__created_at__day AS listing__created_at__day
          , subq_7.listing__created_at__week AS listing__created_at__week
          , subq_7.listing__created_at__month AS listing__created_at__month
          , subq_7.listing__created_at__quarter AS listing__created_at__quarter
          , subq_7.listing__created_at__year AS listing__created_at__year
          , subq_7.listing__created_at__extract_year AS listing__created_at__extract_year
          , subq_7.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
          , subq_7.listing__created_at__extract_month AS listing__created_at__extract_month
          , subq_7.listing__created_at__extract_day AS listing__created_at__extract_day
          , subq_7.listing__created_at__extract_dow AS listing__created_at__extract_dow
          , subq_7.listing__created_at__extract_doy AS listing__created_at__extract_doy
          , subq_7.metric_time__day AS metric_time__day
          , subq_7.metric_time__week AS metric_time__week
          , subq_7.metric_time__month AS metric_time__month
          , subq_7.metric_time__quarter AS metric_time__quarter
          , subq_7.metric_time__year AS metric_time__year
          , subq_7.metric_time__extract_year AS metric_time__extract_year
          , subq_7.metric_time__extract_quarter AS metric_time__extract_quarter
          , subq_7.metric_time__extract_month AS metric_time__extract_month
          , subq_7.metric_time__extract_day AS metric_time__extract_day
          , subq_7.metric_time__extract_dow AS metric_time__extract_dow
          , subq_7.metric_time__extract_doy AS metric_time__extract_doy
          , subq_7.listing AS listing
          , subq_7.user AS user
          , subq_7.listing__user AS listing__user
          , subq_7.country_latest AS country_latest
          , subq_7.is_lux_latest AS is_lux_latest
          , subq_7.capacity_latest AS capacity_latest
          , subq_7.listing__country_latest AS listing__country_latest
          , subq_7.listing__is_lux_latest AS listing__is_lux_latest
          , subq_7.listing__capacity_latest AS listing__capacity_latest
          , subq_7.listings AS listings
          , subq_7.largest_listing AS largest_listing
          , subq_7.smallest_listing AS smallest_listing
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_6.ds__day
            , subq_6.ds__week
            , subq_6.ds__month
            , subq_6.ds__quarter
            , subq_6.ds__year
            , subq_6.ds__extract_year
            , subq_6.ds__extract_quarter
            , subq_6.ds__extract_month
            , subq_6.ds__extract_day
            , subq_6.ds__extract_dow
            , subq_6.ds__extract_doy
            , subq_6.created_at__day
            , subq_6.created_at__week
            , subq_6.created_at__month
            , subq_6.created_at__quarter
            , subq_6.created_at__year
            , subq_6.created_at__extract_year
            , subq_6.created_at__extract_quarter
            , subq_6.created_at__extract_month
            , subq_6.created_at__extract_day
            , subq_6.created_at__extract_dow
            , subq_6.created_at__extract_doy
            , subq_6.listing__ds__day
            , subq_6.listing__ds__week
            , subq_6.listing__ds__month
            , subq_6.listing__ds__quarter
            , subq_6.listing__ds__year
            , subq_6.listing__ds__extract_year
            , subq_6.listing__ds__extract_quarter
            , subq_6.listing__ds__extract_month
            , subq_6.listing__ds__extract_day
            , subq_6.listing__ds__extract_dow
            , subq_6.listing__ds__extract_doy
            , subq_6.listing__created_at__day
            , subq_6.listing__created_at__week
            , subq_6.listing__created_at__month
            , subq_6.listing__created_at__quarter
            , subq_6.listing__created_at__year
            , subq_6.listing__created_at__extract_year
            , subq_6.listing__created_at__extract_quarter
            , subq_6.listing__created_at__extract_month
            , subq_6.listing__created_at__extract_day
            , subq_6.listing__created_at__extract_dow
            , subq_6.listing__created_at__extract_doy
            , subq_6.ds__day AS metric_time__day
            , subq_6.ds__week AS metric_time__week
            , subq_6.ds__month AS metric_time__month
            , subq_6.ds__quarter AS metric_time__quarter
            , subq_6.ds__year AS metric_time__year
            , subq_6.ds__extract_year AS metric_time__extract_year
            , subq_6.ds__extract_quarter AS metric_time__extract_quarter
            , subq_6.ds__extract_month AS metric_time__extract_month
            , subq_6.ds__extract_day AS metric_time__extract_day
            , subq_6.ds__extract_dow AS metric_time__extract_dow
            , subq_6.ds__extract_doy AS metric_time__extract_doy
            , subq_6.listing
            , subq_6.user
            , subq_6.listing__user
            , subq_6.country_latest
            , subq_6.is_lux_latest
            , subq_6.capacity_latest
            , subq_6.listing__country_latest
            , subq_6.listing__is_lux_latest
            , subq_6.listing__capacity_latest
            , subq_6.listings
            , subq_6.largest_listing
            , subq_6.smallest_listing
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
          ) subq_6
        ) subq_7
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['listing', 'listing__bookings']
          SELECT
            subq_12.listing
            , subq_12.listing__bookings
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              subq_11.listing
              , subq_11.bookings AS listing__bookings
            FROM (
              -- Aggregate Measures
              SELECT
                subq_10.listing
                , SUM(subq_10.bookings) AS bookings
              FROM (
                -- Pass Only Elements: ['bookings', 'listing']
                SELECT
                  subq_9.listing
                  , subq_9.bookings
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
                  ) subq_8
                ) subq_9
              ) subq_10
              GROUP BY
                subq_10.listing
            ) subq_11
          ) subq_12
        ) subq_13
        ON
          subq_7.listing = subq_13.listing
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['listing', 'listing__bookers']
          SELECT
            subq_17.listing
            , subq_17.listing__bookers
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              subq_16.listing
              , subq_16.bookers AS listing__bookers
            FROM (
              -- Aggregate Measures
              SELECT
                subq_15.listing
                , COUNT(DISTINCT subq_15.bookers) AS bookers
              FROM (
                -- Pass Only Elements: ['bookers', 'listing']
                SELECT
                  subq_14.listing
                  , subq_14.bookers
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
                  ) subq_8
                ) subq_14
              ) subq_15
              GROUP BY
                subq_15.listing
            ) subq_16
          ) subq_17
        ) subq_18
        ON
          subq_7.listing = subq_18.listing
      ) subq_19
      WHERE listing__bookings > 2 AND listing__bookers > 1
    ) subq_20
  ) subq_21
) subq_22
