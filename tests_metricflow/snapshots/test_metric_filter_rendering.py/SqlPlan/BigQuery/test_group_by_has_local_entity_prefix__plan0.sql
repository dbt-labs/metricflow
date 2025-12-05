test_name: test_group_by_has_local_entity_prefix
test_filename: test_metric_filter_rendering.py
sql_engine: BigQuery
---
-- Write to DataTable
SELECT
  subq_24.listings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_23.__listings AS listings
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      SUM(subq_22.__listings) AS __listings
    FROM (
      -- Pass Only Elements: ['__listings']
      SELECT
        subq_21.__listings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_20.listings AS __listings
          , subq_20.user__listing__user__average_booking_value
        FROM (
          -- Pass Only Elements: ['__listings', 'user__listing__user__average_booking_value']
          SELECT
            subq_19.user__listing__user__average_booking_value
            , subq_19.__listings AS listings
          FROM (
            -- Join Standard Outputs
            SELECT
              subq_18.listing__user AS user__listing__user
              , subq_18.listing__user__average_booking_value AS user__listing__user__average_booking_value
              , subq_8.ds__day AS ds__day
              , subq_8.ds__week AS ds__week
              , subq_8.ds__month AS ds__month
              , subq_8.ds__quarter AS ds__quarter
              , subq_8.ds__year AS ds__year
              , subq_8.ds__extract_year AS ds__extract_year
              , subq_8.ds__extract_quarter AS ds__extract_quarter
              , subq_8.ds__extract_month AS ds__extract_month
              , subq_8.ds__extract_day AS ds__extract_day
              , subq_8.ds__extract_dow AS ds__extract_dow
              , subq_8.ds__extract_doy AS ds__extract_doy
              , subq_8.created_at__day AS created_at__day
              , subq_8.created_at__week AS created_at__week
              , subq_8.created_at__month AS created_at__month
              , subq_8.created_at__quarter AS created_at__quarter
              , subq_8.created_at__year AS created_at__year
              , subq_8.created_at__extract_year AS created_at__extract_year
              , subq_8.created_at__extract_quarter AS created_at__extract_quarter
              , subq_8.created_at__extract_month AS created_at__extract_month
              , subq_8.created_at__extract_day AS created_at__extract_day
              , subq_8.created_at__extract_dow AS created_at__extract_dow
              , subq_8.created_at__extract_doy AS created_at__extract_doy
              , subq_8.listing__ds__day AS listing__ds__day
              , subq_8.listing__ds__week AS listing__ds__week
              , subq_8.listing__ds__month AS listing__ds__month
              , subq_8.listing__ds__quarter AS listing__ds__quarter
              , subq_8.listing__ds__year AS listing__ds__year
              , subq_8.listing__ds__extract_year AS listing__ds__extract_year
              , subq_8.listing__ds__extract_quarter AS listing__ds__extract_quarter
              , subq_8.listing__ds__extract_month AS listing__ds__extract_month
              , subq_8.listing__ds__extract_day AS listing__ds__extract_day
              , subq_8.listing__ds__extract_dow AS listing__ds__extract_dow
              , subq_8.listing__ds__extract_doy AS listing__ds__extract_doy
              , subq_8.listing__created_at__day AS listing__created_at__day
              , subq_8.listing__created_at__week AS listing__created_at__week
              , subq_8.listing__created_at__month AS listing__created_at__month
              , subq_8.listing__created_at__quarter AS listing__created_at__quarter
              , subq_8.listing__created_at__year AS listing__created_at__year
              , subq_8.listing__created_at__extract_year AS listing__created_at__extract_year
              , subq_8.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
              , subq_8.listing__created_at__extract_month AS listing__created_at__extract_month
              , subq_8.listing__created_at__extract_day AS listing__created_at__extract_day
              , subq_8.listing__created_at__extract_dow AS listing__created_at__extract_dow
              , subq_8.listing__created_at__extract_doy AS listing__created_at__extract_doy
              , subq_8.metric_time__day AS metric_time__day
              , subq_8.metric_time__week AS metric_time__week
              , subq_8.metric_time__month AS metric_time__month
              , subq_8.metric_time__quarter AS metric_time__quarter
              , subq_8.metric_time__year AS metric_time__year
              , subq_8.metric_time__extract_year AS metric_time__extract_year
              , subq_8.metric_time__extract_quarter AS metric_time__extract_quarter
              , subq_8.metric_time__extract_month AS metric_time__extract_month
              , subq_8.metric_time__extract_day AS metric_time__extract_day
              , subq_8.metric_time__extract_dow AS metric_time__extract_dow
              , subq_8.metric_time__extract_doy AS metric_time__extract_doy
              , subq_8.listing AS listing
              , subq_8.user AS user
              , subq_8.listing__user AS listing__user
              , subq_8.country_latest AS country_latest
              , subq_8.is_lux_latest AS is_lux_latest
              , subq_8.capacity_latest AS capacity_latest
              , subq_8.listing__country_latest AS listing__country_latest
              , subq_8.listing__is_lux_latest AS listing__is_lux_latest
              , subq_8.listing__capacity_latest AS listing__capacity_latest
              , subq_8.__listings AS __listings
              , subq_8.__lux_listings AS __lux_listings
              , subq_8.__smallest_listing AS __smallest_listing
              , subq_8.__largest_listing AS __largest_listing
              , subq_8.__active_listings AS __active_listings
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
                , subq_7.created_at__day
                , subq_7.created_at__week
                , subq_7.created_at__month
                , subq_7.created_at__quarter
                , subq_7.created_at__year
                , subq_7.created_at__extract_year
                , subq_7.created_at__extract_quarter
                , subq_7.created_at__extract_month
                , subq_7.created_at__extract_day
                , subq_7.created_at__extract_dow
                , subq_7.created_at__extract_doy
                , subq_7.listing__ds__day
                , subq_7.listing__ds__week
                , subq_7.listing__ds__month
                , subq_7.listing__ds__quarter
                , subq_7.listing__ds__year
                , subq_7.listing__ds__extract_year
                , subq_7.listing__ds__extract_quarter
                , subq_7.listing__ds__extract_month
                , subq_7.listing__ds__extract_day
                , subq_7.listing__ds__extract_dow
                , subq_7.listing__ds__extract_doy
                , subq_7.listing__created_at__day
                , subq_7.listing__created_at__week
                , subq_7.listing__created_at__month
                , subq_7.listing__created_at__quarter
                , subq_7.listing__created_at__year
                , subq_7.listing__created_at__extract_year
                , subq_7.listing__created_at__extract_quarter
                , subq_7.listing__created_at__extract_month
                , subq_7.listing__created_at__extract_day
                , subq_7.listing__created_at__extract_dow
                , subq_7.listing__created_at__extract_doy
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
                , subq_7.user
                , subq_7.listing__user
                , subq_7.country_latest
                , subq_7.is_lux_latest
                , subq_7.capacity_latest
                , subq_7.listing__country_latest
                , subq_7.listing__is_lux_latest
                , subq_7.listing__capacity_latest
                , subq_7.__listings
                , subq_7.__lux_listings
                , subq_7.__smallest_listing
                , subq_7.__largest_listing
                , subq_7.__active_listings
              FROM (
                -- Read Elements From Semantic Model 'listings_latest'
                SELECT
                  1 AS __listings
                  , 1 AS __lux_listings
                  , listings_latest_src_28000.capacity AS __smallest_listing
                  , listings_latest_src_28000.capacity AS __largest_listing
                  , 1 AS __active_listings
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
              ) subq_7
            ) subq_8
            LEFT OUTER JOIN (
              -- Pass Only Elements: ['listing__user', 'listing__user__average_booking_value']
              SELECT
                subq_17.listing__user
                , subq_17.listing__user__average_booking_value
              FROM (
                -- Compute Metrics via Expressions
                SELECT
                  subq_16.listing__user
                  , subq_16.__average_booking_value AS listing__user__average_booking_value
                FROM (
                  -- Aggregate Inputs for Simple Metrics
                  SELECT
                    subq_15.listing__user
                    , AVG(subq_15.__average_booking_value) AS __average_booking_value
                  FROM (
                    -- Pass Only Elements: ['__average_booking_value', 'listing__user']
                    SELECT
                      subq_14.listing__user
                      , subq_14.__average_booking_value
                    FROM (
                      -- Pass Only Elements: ['__average_booking_value', 'listing__user']
                      SELECT
                        subq_13.listing__user
                        , subq_13.__average_booking_value
                      FROM (
                        -- Join Standard Outputs
                        SELECT
                          subq_12.user AS listing__user
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
                          , subq_10.paid_at__day AS paid_at__day
                          , subq_10.paid_at__week AS paid_at__week
                          , subq_10.paid_at__month AS paid_at__month
                          , subq_10.paid_at__quarter AS paid_at__quarter
                          , subq_10.paid_at__year AS paid_at__year
                          , subq_10.paid_at__extract_year AS paid_at__extract_year
                          , subq_10.paid_at__extract_quarter AS paid_at__extract_quarter
                          , subq_10.paid_at__extract_month AS paid_at__extract_month
                          , subq_10.paid_at__extract_day AS paid_at__extract_day
                          , subq_10.paid_at__extract_dow AS paid_at__extract_dow
                          , subq_10.paid_at__extract_doy AS paid_at__extract_doy
                          , subq_10.booking__ds__day AS booking__ds__day
                          , subq_10.booking__ds__week AS booking__ds__week
                          , subq_10.booking__ds__month AS booking__ds__month
                          , subq_10.booking__ds__quarter AS booking__ds__quarter
                          , subq_10.booking__ds__year AS booking__ds__year
                          , subq_10.booking__ds__extract_year AS booking__ds__extract_year
                          , subq_10.booking__ds__extract_quarter AS booking__ds__extract_quarter
                          , subq_10.booking__ds__extract_month AS booking__ds__extract_month
                          , subq_10.booking__ds__extract_day AS booking__ds__extract_day
                          , subq_10.booking__ds__extract_dow AS booking__ds__extract_dow
                          , subq_10.booking__ds__extract_doy AS booking__ds__extract_doy
                          , subq_10.booking__ds_partitioned__day AS booking__ds_partitioned__day
                          , subq_10.booking__ds_partitioned__week AS booking__ds_partitioned__week
                          , subq_10.booking__ds_partitioned__month AS booking__ds_partitioned__month
                          , subq_10.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                          , subq_10.booking__ds_partitioned__year AS booking__ds_partitioned__year
                          , subq_10.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                          , subq_10.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                          , subq_10.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                          , subq_10.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                          , subq_10.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                          , subq_10.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                          , subq_10.booking__paid_at__day AS booking__paid_at__day
                          , subq_10.booking__paid_at__week AS booking__paid_at__week
                          , subq_10.booking__paid_at__month AS booking__paid_at__month
                          , subq_10.booking__paid_at__quarter AS booking__paid_at__quarter
                          , subq_10.booking__paid_at__year AS booking__paid_at__year
                          , subq_10.booking__paid_at__extract_year AS booking__paid_at__extract_year
                          , subq_10.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                          , subq_10.booking__paid_at__extract_month AS booking__paid_at__extract_month
                          , subq_10.booking__paid_at__extract_day AS booking__paid_at__extract_day
                          , subq_10.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                          , subq_10.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
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
                          , subq_10.guest AS guest
                          , subq_10.host AS host
                          , subq_10.booking__listing AS booking__listing
                          , subq_10.booking__guest AS booking__guest
                          , subq_10.booking__host AS booking__host
                          , subq_10.is_instant AS is_instant
                          , subq_10.booking__is_instant AS booking__is_instant
                          , subq_10.__bookings AS __bookings
                          , subq_10.__average_booking_value AS __average_booking_value
                          , subq_10.__instant_bookings AS __instant_bookings
                          , subq_10.__booking_value AS __booking_value
                          , subq_10.__max_booking_value AS __max_booking_value
                          , subq_10.__min_booking_value AS __min_booking_value
                          , subq_10.__instant_booking_value AS __instant_booking_value
                          , subq_10.__average_instant_booking_value AS __average_instant_booking_value
                          , subq_10.__booking_value_for_non_null_listing_id AS __booking_value_for_non_null_listing_id
                          , subq_10.__bookers AS __bookers
                          , subq_10.__referred_bookings AS __referred_bookings
                          , subq_10.__median_booking_value AS __median_booking_value
                          , subq_10.__booking_value_p99 AS __booking_value_p99
                          , subq_10.__discrete_booking_value_p99 AS __discrete_booking_value_p99
                          , subq_10.__approximate_continuous_booking_value_p99 AS __approximate_continuous_booking_value_p99
                          , subq_10.__approximate_discrete_booking_value_p99 AS __approximate_discrete_booking_value_p99
                          , subq_10.__bookings_join_to_time_spine AS __bookings_join_to_time_spine
                          , subq_10.__bookings_fill_nulls_with_0_without_time_spine AS __bookings_fill_nulls_with_0_without_time_spine
                          , subq_10.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
                          , subq_10.__instant_bookings_with_measure_filter AS __instant_bookings_with_measure_filter
                          , subq_10.__bookings_join_to_time_spine_with_tiered_filters AS __bookings_join_to_time_spine_with_tiered_filters
                          , subq_10.__bookers_fill_nulls_with_0_join_to_timespine AS __bookers_fill_nulls_with_0_join_to_timespine
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
                            , subq_9.paid_at__day
                            , subq_9.paid_at__week
                            , subq_9.paid_at__month
                            , subq_9.paid_at__quarter
                            , subq_9.paid_at__year
                            , subq_9.paid_at__extract_year
                            , subq_9.paid_at__extract_quarter
                            , subq_9.paid_at__extract_month
                            , subq_9.paid_at__extract_day
                            , subq_9.paid_at__extract_dow
                            , subq_9.paid_at__extract_doy
                            , subq_9.booking__ds__day
                            , subq_9.booking__ds__week
                            , subq_9.booking__ds__month
                            , subq_9.booking__ds__quarter
                            , subq_9.booking__ds__year
                            , subq_9.booking__ds__extract_year
                            , subq_9.booking__ds__extract_quarter
                            , subq_9.booking__ds__extract_month
                            , subq_9.booking__ds__extract_day
                            , subq_9.booking__ds__extract_dow
                            , subq_9.booking__ds__extract_doy
                            , subq_9.booking__ds_partitioned__day
                            , subq_9.booking__ds_partitioned__week
                            , subq_9.booking__ds_partitioned__month
                            , subq_9.booking__ds_partitioned__quarter
                            , subq_9.booking__ds_partitioned__year
                            , subq_9.booking__ds_partitioned__extract_year
                            , subq_9.booking__ds_partitioned__extract_quarter
                            , subq_9.booking__ds_partitioned__extract_month
                            , subq_9.booking__ds_partitioned__extract_day
                            , subq_9.booking__ds_partitioned__extract_dow
                            , subq_9.booking__ds_partitioned__extract_doy
                            , subq_9.booking__paid_at__day
                            , subq_9.booking__paid_at__week
                            , subq_9.booking__paid_at__month
                            , subq_9.booking__paid_at__quarter
                            , subq_9.booking__paid_at__year
                            , subq_9.booking__paid_at__extract_year
                            , subq_9.booking__paid_at__extract_quarter
                            , subq_9.booking__paid_at__extract_month
                            , subq_9.booking__paid_at__extract_day
                            , subq_9.booking__paid_at__extract_dow
                            , subq_9.booking__paid_at__extract_doy
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
                            , subq_9.guest
                            , subq_9.host
                            , subq_9.booking__listing
                            , subq_9.booking__guest
                            , subq_9.booking__host
                            , subq_9.is_instant
                            , subq_9.booking__is_instant
                            , subq_9.__bookings
                            , subq_9.__average_booking_value
                            , subq_9.__instant_bookings
                            , subq_9.__booking_value
                            , subq_9.__max_booking_value
                            , subq_9.__min_booking_value
                            , subq_9.__instant_booking_value
                            , subq_9.__average_instant_booking_value
                            , subq_9.__booking_value_for_non_null_listing_id
                            , subq_9.__bookers
                            , subq_9.__referred_bookings
                            , subq_9.__median_booking_value
                            , subq_9.__booking_value_p99
                            , subq_9.__discrete_booking_value_p99
                            , subq_9.__approximate_continuous_booking_value_p99
                            , subq_9.__approximate_discrete_booking_value_p99
                            , subq_9.__bookings_join_to_time_spine
                            , subq_9.__bookings_fill_nulls_with_0_without_time_spine
                            , subq_9.__bookings_fill_nulls_with_0
                            , subq_9.__instant_bookings_with_measure_filter
                            , subq_9.__bookings_join_to_time_spine_with_tiered_filters
                            , subq_9.__bookers_fill_nulls_with_0_join_to_timespine
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
                          ) subq_9
                        ) subq_10
                        LEFT OUTER JOIN (
                          -- Pass Only Elements: ['listing', 'user']
                          SELECT
                            subq_11.listing
                            , subq_11.user
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
                              , subq_7.created_at__day
                              , subq_7.created_at__week
                              , subq_7.created_at__month
                              , subq_7.created_at__quarter
                              , subq_7.created_at__year
                              , subq_7.created_at__extract_year
                              , subq_7.created_at__extract_quarter
                              , subq_7.created_at__extract_month
                              , subq_7.created_at__extract_day
                              , subq_7.created_at__extract_dow
                              , subq_7.created_at__extract_doy
                              , subq_7.listing__ds__day
                              , subq_7.listing__ds__week
                              , subq_7.listing__ds__month
                              , subq_7.listing__ds__quarter
                              , subq_7.listing__ds__year
                              , subq_7.listing__ds__extract_year
                              , subq_7.listing__ds__extract_quarter
                              , subq_7.listing__ds__extract_month
                              , subq_7.listing__ds__extract_day
                              , subq_7.listing__ds__extract_dow
                              , subq_7.listing__ds__extract_doy
                              , subq_7.listing__created_at__day
                              , subq_7.listing__created_at__week
                              , subq_7.listing__created_at__month
                              , subq_7.listing__created_at__quarter
                              , subq_7.listing__created_at__year
                              , subq_7.listing__created_at__extract_year
                              , subq_7.listing__created_at__extract_quarter
                              , subq_7.listing__created_at__extract_month
                              , subq_7.listing__created_at__extract_day
                              , subq_7.listing__created_at__extract_dow
                              , subq_7.listing__created_at__extract_doy
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
                              , subq_7.user
                              , subq_7.listing__user
                              , subq_7.country_latest
                              , subq_7.is_lux_latest
                              , subq_7.capacity_latest
                              , subq_7.listing__country_latest
                              , subq_7.listing__is_lux_latest
                              , subq_7.listing__capacity_latest
                              , subq_7.__listings
                              , subq_7.__lux_listings
                              , subq_7.__smallest_listing
                              , subq_7.__largest_listing
                              , subq_7.__active_listings
                            FROM (
                              -- Read Elements From Semantic Model 'listings_latest'
                              SELECT
                                1 AS __listings
                                , 1 AS __lux_listings
                                , listings_latest_src_28000.capacity AS __smallest_listing
                                , listings_latest_src_28000.capacity AS __largest_listing
                                , 1 AS __active_listings
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
                            ) subq_7
                          ) subq_11
                        ) subq_12
                        ON
                          subq_10.listing = subq_12.listing
                      ) subq_13
                    ) subq_14
                  ) subq_15
                  GROUP BY
                    listing__user
                ) subq_16
              ) subq_17
            ) subq_18
            ON
              subq_8.user = subq_18.listing__user
          ) subq_19
        ) subq_20
        WHERE user__listing__user__average_booking_value > 1
      ) subq_21
    ) subq_22
  ) subq_23
) subq_24
