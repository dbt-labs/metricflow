test_name: test_query_with_simple_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a simple metric in the query-level where filter.
sql_engine: Trino
---
-- Write to DataTable
SELECT
  subq_18.listings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_17.__listings AS listings
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      SUM(subq_16.__listings) AS __listings
    FROM (
      -- Pass Only Elements: ['__listings']
      SELECT
        subq_15.__listings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_14.listings AS __listings
          , subq_14.listing__bookings
        FROM (
          -- Pass Only Elements: ['__listings', 'listing__bookings']
          SELECT
            subq_13.listing__bookings
            , subq_13.__listings AS listings
          FROM (
            -- Join Standard Outputs
            SELECT
              subq_12.listing__bookings AS listing__bookings
              , subq_5.ds__day AS ds__day
              , subq_5.ds__week AS ds__week
              , subq_5.ds__month AS ds__month
              , subq_5.ds__quarter AS ds__quarter
              , subq_5.ds__year AS ds__year
              , subq_5.ds__extract_year AS ds__extract_year
              , subq_5.ds__extract_quarter AS ds__extract_quarter
              , subq_5.ds__extract_month AS ds__extract_month
              , subq_5.ds__extract_day AS ds__extract_day
              , subq_5.ds__extract_dow AS ds__extract_dow
              , subq_5.ds__extract_doy AS ds__extract_doy
              , subq_5.created_at__day AS created_at__day
              , subq_5.created_at__week AS created_at__week
              , subq_5.created_at__month AS created_at__month
              , subq_5.created_at__quarter AS created_at__quarter
              , subq_5.created_at__year AS created_at__year
              , subq_5.created_at__extract_year AS created_at__extract_year
              , subq_5.created_at__extract_quarter AS created_at__extract_quarter
              , subq_5.created_at__extract_month AS created_at__extract_month
              , subq_5.created_at__extract_day AS created_at__extract_day
              , subq_5.created_at__extract_dow AS created_at__extract_dow
              , subq_5.created_at__extract_doy AS created_at__extract_doy
              , subq_5.listing__ds__day AS listing__ds__day
              , subq_5.listing__ds__week AS listing__ds__week
              , subq_5.listing__ds__month AS listing__ds__month
              , subq_5.listing__ds__quarter AS listing__ds__quarter
              , subq_5.listing__ds__year AS listing__ds__year
              , subq_5.listing__ds__extract_year AS listing__ds__extract_year
              , subq_5.listing__ds__extract_quarter AS listing__ds__extract_quarter
              , subq_5.listing__ds__extract_month AS listing__ds__extract_month
              , subq_5.listing__ds__extract_day AS listing__ds__extract_day
              , subq_5.listing__ds__extract_dow AS listing__ds__extract_dow
              , subq_5.listing__ds__extract_doy AS listing__ds__extract_doy
              , subq_5.listing__created_at__day AS listing__created_at__day
              , subq_5.listing__created_at__week AS listing__created_at__week
              , subq_5.listing__created_at__month AS listing__created_at__month
              , subq_5.listing__created_at__quarter AS listing__created_at__quarter
              , subq_5.listing__created_at__year AS listing__created_at__year
              , subq_5.listing__created_at__extract_year AS listing__created_at__extract_year
              , subq_5.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
              , subq_5.listing__created_at__extract_month AS listing__created_at__extract_month
              , subq_5.listing__created_at__extract_day AS listing__created_at__extract_day
              , subq_5.listing__created_at__extract_dow AS listing__created_at__extract_dow
              , subq_5.listing__created_at__extract_doy AS listing__created_at__extract_doy
              , subq_5.metric_time__day AS metric_time__day
              , subq_5.metric_time__week AS metric_time__week
              , subq_5.metric_time__month AS metric_time__month
              , subq_5.metric_time__quarter AS metric_time__quarter
              , subq_5.metric_time__year AS metric_time__year
              , subq_5.metric_time__extract_year AS metric_time__extract_year
              , subq_5.metric_time__extract_quarter AS metric_time__extract_quarter
              , subq_5.metric_time__extract_month AS metric_time__extract_month
              , subq_5.metric_time__extract_day AS metric_time__extract_day
              , subq_5.metric_time__extract_dow AS metric_time__extract_dow
              , subq_5.metric_time__extract_doy AS metric_time__extract_doy
              , subq_5.listing AS listing
              , subq_5.user AS user
              , subq_5.listing__user AS listing__user
              , subq_5.country_latest AS country_latest
              , subq_5.is_lux_latest AS is_lux_latest
              , subq_5.capacity_latest AS capacity_latest
              , subq_5.listing__country_latest AS listing__country_latest
              , subq_5.listing__is_lux_latest AS listing__is_lux_latest
              , subq_5.listing__capacity_latest AS listing__capacity_latest
              , subq_5.__listings AS __listings
              , subq_5.__lux_listings AS __lux_listings
              , subq_5.__smallest_listing AS __smallest_listing
              , subq_5.__largest_listing AS __largest_listing
              , subq_5.__active_listings AS __active_listings
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_4.ds__day
                , subq_4.ds__week
                , subq_4.ds__month
                , subq_4.ds__quarter
                , subq_4.ds__year
                , subq_4.ds__extract_year
                , subq_4.ds__extract_quarter
                , subq_4.ds__extract_month
                , subq_4.ds__extract_day
                , subq_4.ds__extract_dow
                , subq_4.ds__extract_doy
                , subq_4.created_at__day
                , subq_4.created_at__week
                , subq_4.created_at__month
                , subq_4.created_at__quarter
                , subq_4.created_at__year
                , subq_4.created_at__extract_year
                , subq_4.created_at__extract_quarter
                , subq_4.created_at__extract_month
                , subq_4.created_at__extract_day
                , subq_4.created_at__extract_dow
                , subq_4.created_at__extract_doy
                , subq_4.listing__ds__day
                , subq_4.listing__ds__week
                , subq_4.listing__ds__month
                , subq_4.listing__ds__quarter
                , subq_4.listing__ds__year
                , subq_4.listing__ds__extract_year
                , subq_4.listing__ds__extract_quarter
                , subq_4.listing__ds__extract_month
                , subq_4.listing__ds__extract_day
                , subq_4.listing__ds__extract_dow
                , subq_4.listing__ds__extract_doy
                , subq_4.listing__created_at__day
                , subq_4.listing__created_at__week
                , subq_4.listing__created_at__month
                , subq_4.listing__created_at__quarter
                , subq_4.listing__created_at__year
                , subq_4.listing__created_at__extract_year
                , subq_4.listing__created_at__extract_quarter
                , subq_4.listing__created_at__extract_month
                , subq_4.listing__created_at__extract_day
                , subq_4.listing__created_at__extract_dow
                , subq_4.listing__created_at__extract_doy
                , subq_4.ds__day AS metric_time__day
                , subq_4.ds__week AS metric_time__week
                , subq_4.ds__month AS metric_time__month
                , subq_4.ds__quarter AS metric_time__quarter
                , subq_4.ds__year AS metric_time__year
                , subq_4.ds__extract_year AS metric_time__extract_year
                , subq_4.ds__extract_quarter AS metric_time__extract_quarter
                , subq_4.ds__extract_month AS metric_time__extract_month
                , subq_4.ds__extract_day AS metric_time__extract_day
                , subq_4.ds__extract_dow AS metric_time__extract_dow
                , subq_4.ds__extract_doy AS metric_time__extract_doy
                , subq_4.listing
                , subq_4.user
                , subq_4.listing__user
                , subq_4.country_latest
                , subq_4.is_lux_latest
                , subq_4.capacity_latest
                , subq_4.listing__country_latest
                , subq_4.listing__is_lux_latest
                , subq_4.listing__capacity_latest
                , subq_4.__listings
                , subq_4.__lux_listings
                , subq_4.__smallest_listing
                , subq_4.__largest_listing
                , subq_4.__active_listings
              FROM (
                -- Read Elements From Semantic Model 'listings_latest'
                SELECT
                  1 AS __listings
                  , 1 AS __lux_listings
                  , listings_latest_src_28000.capacity AS __smallest_listing
                  , listings_latest_src_28000.capacity AS __largest_listing
                  , 1 AS __active_listings
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
              ) subq_4
            ) subq_5
            LEFT OUTER JOIN (
              -- Pass Only Elements: ['listing', 'listing__bookings']
              SELECT
                subq_11.listing
                , subq_11.listing__bookings
              FROM (
                -- Compute Metrics via Expressions
                SELECT
                  subq_10.listing
                  , subq_10.__bookings AS listing__bookings
                FROM (
                  -- Aggregate Inputs for Simple Metrics
                  SELECT
                    subq_9.listing
                    , SUM(subq_9.__bookings) AS __bookings
                  FROM (
                    -- Pass Only Elements: ['__bookings', 'listing']
                    SELECT
                      subq_8.listing
                      , subq_8.__bookings
                    FROM (
                      -- Pass Only Elements: ['__bookings', 'listing']
                      SELECT
                        subq_7.listing
                        , subq_7.__bookings
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
                          , subq_6.ds_partitioned__day
                          , subq_6.ds_partitioned__week
                          , subq_6.ds_partitioned__month
                          , subq_6.ds_partitioned__quarter
                          , subq_6.ds_partitioned__year
                          , subq_6.ds_partitioned__extract_year
                          , subq_6.ds_partitioned__extract_quarter
                          , subq_6.ds_partitioned__extract_month
                          , subq_6.ds_partitioned__extract_day
                          , subq_6.ds_partitioned__extract_dow
                          , subq_6.ds_partitioned__extract_doy
                          , subq_6.paid_at__day
                          , subq_6.paid_at__week
                          , subq_6.paid_at__month
                          , subq_6.paid_at__quarter
                          , subq_6.paid_at__year
                          , subq_6.paid_at__extract_year
                          , subq_6.paid_at__extract_quarter
                          , subq_6.paid_at__extract_month
                          , subq_6.paid_at__extract_day
                          , subq_6.paid_at__extract_dow
                          , subq_6.paid_at__extract_doy
                          , subq_6.booking__ds__day
                          , subq_6.booking__ds__week
                          , subq_6.booking__ds__month
                          , subq_6.booking__ds__quarter
                          , subq_6.booking__ds__year
                          , subq_6.booking__ds__extract_year
                          , subq_6.booking__ds__extract_quarter
                          , subq_6.booking__ds__extract_month
                          , subq_6.booking__ds__extract_day
                          , subq_6.booking__ds__extract_dow
                          , subq_6.booking__ds__extract_doy
                          , subq_6.booking__ds_partitioned__day
                          , subq_6.booking__ds_partitioned__week
                          , subq_6.booking__ds_partitioned__month
                          , subq_6.booking__ds_partitioned__quarter
                          , subq_6.booking__ds_partitioned__year
                          , subq_6.booking__ds_partitioned__extract_year
                          , subq_6.booking__ds_partitioned__extract_quarter
                          , subq_6.booking__ds_partitioned__extract_month
                          , subq_6.booking__ds_partitioned__extract_day
                          , subq_6.booking__ds_partitioned__extract_dow
                          , subq_6.booking__ds_partitioned__extract_doy
                          , subq_6.booking__paid_at__day
                          , subq_6.booking__paid_at__week
                          , subq_6.booking__paid_at__month
                          , subq_6.booking__paid_at__quarter
                          , subq_6.booking__paid_at__year
                          , subq_6.booking__paid_at__extract_year
                          , subq_6.booking__paid_at__extract_quarter
                          , subq_6.booking__paid_at__extract_month
                          , subq_6.booking__paid_at__extract_day
                          , subq_6.booking__paid_at__extract_dow
                          , subq_6.booking__paid_at__extract_doy
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
                          , subq_6.guest
                          , subq_6.host
                          , subq_6.booking__listing
                          , subq_6.booking__guest
                          , subq_6.booking__host
                          , subq_6.is_instant
                          , subq_6.booking__is_instant
                          , subq_6.__bookings
                          , subq_6.__average_booking_value
                          , subq_6.__instant_bookings
                          , subq_6.__booking_value
                          , subq_6.__max_booking_value
                          , subq_6.__min_booking_value
                          , subq_6.__instant_booking_value
                          , subq_6.__average_instant_booking_value
                          , subq_6.__booking_value_for_non_null_listing_id
                          , subq_6.__bookers
                          , subq_6.__referred_bookings
                          , subq_6.__median_booking_value
                          , subq_6.__booking_value_p99
                          , subq_6.__discrete_booking_value_p99
                          , subq_6.__approximate_continuous_booking_value_p99
                          , subq_6.__approximate_discrete_booking_value_p99
                          , subq_6.__bookings_join_to_time_spine
                          , subq_6.__bookings_fill_nulls_with_0_without_time_spine
                          , subq_6.__bookings_fill_nulls_with_0
                          , subq_6.__instant_bookings_with_measure_filter
                          , subq_6.__bookings_join_to_time_spine_with_tiered_filters
                          , subq_6.__bookers_fill_nulls_with_0_join_to_timespine
                        FROM (
                          -- Read Elements From Semantic Model 'bookings_source'
                          SELECT
                            1 AS __bookings
                            , bookings_source_src_28000.booking_value AS __average_booking_value
                            , CASE WHEN is_instant THEN 1 ELSE 0 END AS __instant_bookings
                            , bookings_source_src_28000.booking_value AS __booking_value
                            , bookings_source_src_28000.booking_value AS __max_booking_value
                            , bookings_source_src_28000.booking_value AS __min_booking_value
                            , bookings_source_src_28000.booking_value AS __instant_booking_value
                            , bookings_source_src_28000.booking_value AS __average_instant_booking_value
                            , bookings_source_src_28000.booking_value AS __booking_value_for_non_null_listing_id
                            , bookings_source_src_28000.guest_id AS __bookers
                            , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS __referred_bookings
                            , bookings_source_src_28000.booking_value AS __median_booking_value
                            , bookings_source_src_28000.booking_value AS __booking_value_p99
                            , bookings_source_src_28000.booking_value AS __discrete_booking_value_p99
                            , bookings_source_src_28000.booking_value AS __approximate_continuous_booking_value_p99
                            , bookings_source_src_28000.booking_value AS __approximate_discrete_booking_value_p99
                            , 1 AS __bookings_join_to_time_spine
                            , 1 AS __bookings_fill_nulls_with_0_without_time_spine
                            , 1 AS __bookings_fill_nulls_with_0
                            , 1 AS __instant_bookings_with_measure_filter
                            , 1 AS __bookings_join_to_time_spine_with_tiered_filters
                            , bookings_source_src_28000.guest_id AS __bookers_fill_nulls_with_0_join_to_timespine
                            , bookings_source_src_28000.booking_value AS __booking_payments
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
                        ) subq_6
                      ) subq_7
                    ) subq_8
                  ) subq_9
                  GROUP BY
                    subq_9.listing
                ) subq_10
              ) subq_11
            ) subq_12
            ON
              subq_5.listing = subq_12.listing
          ) subq_13
        ) subq_14
        WHERE listing__bookings > 2
      ) subq_15
    ) subq_16
  ) subq_17
) subq_18
