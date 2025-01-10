test_name: test_query_with_ratio_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a ratio metric in the query-level where filter.
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
SELECT
  subq_18.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_17.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      subq_16.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_15.ds__day
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
        , subq_15.created_at__day
        , subq_15.created_at__week
        , subq_15.created_at__month
        , subq_15.created_at__quarter
        , subq_15.created_at__year
        , subq_15.created_at__extract_year
        , subq_15.created_at__extract_quarter
        , subq_15.created_at__extract_month
        , subq_15.created_at__extract_day
        , subq_15.created_at__extract_dow
        , subq_15.created_at__extract_doy
        , subq_15.listing__ds__day
        , subq_15.listing__ds__week
        , subq_15.listing__ds__month
        , subq_15.listing__ds__quarter
        , subq_15.listing__ds__year
        , subq_15.listing__ds__extract_year
        , subq_15.listing__ds__extract_quarter
        , subq_15.listing__ds__extract_month
        , subq_15.listing__ds__extract_day
        , subq_15.listing__ds__extract_dow
        , subq_15.listing__ds__extract_doy
        , subq_15.listing__created_at__day
        , subq_15.listing__created_at__week
        , subq_15.listing__created_at__month
        , subq_15.listing__created_at__quarter
        , subq_15.listing__created_at__year
        , subq_15.listing__created_at__extract_year
        , subq_15.listing__created_at__extract_quarter
        , subq_15.listing__created_at__extract_month
        , subq_15.listing__created_at__extract_day
        , subq_15.listing__created_at__extract_dow
        , subq_15.listing__created_at__extract_doy
        , subq_15.metric_time__day
        , subq_15.metric_time__week
        , subq_15.metric_time__month
        , subq_15.metric_time__quarter
        , subq_15.metric_time__year
        , subq_15.metric_time__extract_year
        , subq_15.metric_time__extract_quarter
        , subq_15.metric_time__extract_month
        , subq_15.metric_time__extract_day
        , subq_15.metric_time__extract_dow
        , subq_15.metric_time__extract_doy
        , subq_15.listing
        , subq_15.user
        , subq_15.listing__user
        , subq_15.country_latest
        , subq_15.is_lux_latest
        , subq_15.capacity_latest
        , subq_15.listing__country_latest
        , subq_15.listing__is_lux_latest
        , subq_15.listing__capacity_latest
        , subq_15.listing__bookings_per_booker
        , subq_15.listings
        , subq_15.largest_listing
        , subq_15.smallest_listing
      FROM (
        -- Join Standard Outputs
        SELECT
          subq_14.listing__bookings_per_booker AS listing__bookings_per_booker
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
              , EXTRACT(dayofweekiso FROM listings_latest_src_28000.created_at) AS ds__extract_dow
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
              , EXTRACT(dayofweekiso FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
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
              , EXTRACT(dayofweekiso FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
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
              , EXTRACT(dayofweekiso FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
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
          -- Pass Only Elements: ['listing', 'listing__bookings_per_booker']
          SELECT
            subq_13.listing
            , subq_13.listing__bookings_per_booker
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              subq_12.listing
              , CAST(subq_12.bookings AS DOUBLE) / CAST(NULLIF(subq_12.bookers, 0) AS DOUBLE) AS listing__bookings_per_booker
            FROM (
              -- Combine Aggregated Outputs
              SELECT
                COALESCE(subq_6.listing, subq_11.listing) AS listing
                , MAX(subq_6.bookings) AS bookings
                , MAX(subq_11.bookers) AS bookers
              FROM (
                -- Compute Metrics via Expressions
                SELECT
                  subq_5.listing
                  , subq_5.bookings
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
                      ) subq_2
                    ) subq_3
                  ) subq_4
                  GROUP BY
                    subq_4.listing
                ) subq_5
              ) subq_6
              FULL OUTER JOIN (
                -- Compute Metrics via Expressions
                SELECT
                  subq_10.listing
                  , subq_10.bookers
                FROM (
                  -- Aggregate Measures
                  SELECT
                    subq_9.listing
                    , COUNT(DISTINCT subq_9.bookers) AS bookers
                  FROM (
                    -- Pass Only Elements: ['bookers', 'listing']
                    SELECT
                      subq_8.listing
                      , subq_8.bookers
                    FROM (
                      -- Metric Time Dimension 'ds'
                      SELECT
                        subq_7.ds__day
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
                        , subq_7.ds__day AS metric_time__day
                        , subq_7.ds__week AS metric_time__week
                        , subq_7.ds__month AS metric_time__month
                        , subq_7.ds__quarter AS metric_time__quarter
                        , subq_7.ds__year AS metric_time__year
                        , subq_7.ds__extract_year AS metric_time__extract_year
                        , subq_7.ds__extract_quarter AS metric_time__extract_quarter
                        , subq_7.ds__extract_month AS metric_time__extract_month
                        , subq_7.ds__extract_day AS metric_time__extract_day
                        , subq_7.ds__extract_dow AS metric_time__extract_dow
                        , subq_7.ds__extract_doy AS metric_time__extract_doy
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
                      ) subq_7
                    ) subq_8
                  ) subq_9
                  GROUP BY
                    subq_9.listing
                ) subq_10
              ) subq_11
              ON
                subq_6.listing = subq_11.listing
              GROUP BY
                COALESCE(subq_6.listing, subq_11.listing)
            ) subq_12
          ) subq_13
        ) subq_14
        ON
          subq_1.listing = subq_14.listing
      ) subq_15
      WHERE listing__bookings_per_booker > 1
    ) subq_16
  ) subq_17
) subq_18