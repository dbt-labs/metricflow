test_name: test_group_by_has_local_entity_prefix
test_filename: test_metric_filter_rendering.py
sql_engine: Databricks
---
-- Write to DataTable
SELECT
  subq_21.listings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_20.listings
  FROM (
    -- Aggregate Measures
    SELECT
      SUM(subq_19.listings) AS listings
    FROM (
      -- Pass Only Elements: ['listings']
      SELECT
        subq_18.listings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_17.ds__day
          , subq_17.ds__week
          , subq_17.ds__month
          , subq_17.ds__quarter
          , subq_17.ds__year
          , subq_17.ds__extract_year
          , subq_17.ds__extract_quarter
          , subq_17.ds__extract_month
          , subq_17.ds__extract_day
          , subq_17.ds__extract_dow
          , subq_17.ds__extract_doy
          , subq_17.created_at__day
          , subq_17.created_at__week
          , subq_17.created_at__month
          , subq_17.created_at__quarter
          , subq_17.created_at__year
          , subq_17.created_at__extract_year
          , subq_17.created_at__extract_quarter
          , subq_17.created_at__extract_month
          , subq_17.created_at__extract_day
          , subq_17.created_at__extract_dow
          , subq_17.created_at__extract_doy
          , subq_17.listing__ds__day
          , subq_17.listing__ds__week
          , subq_17.listing__ds__month
          , subq_17.listing__ds__quarter
          , subq_17.listing__ds__year
          , subq_17.listing__ds__extract_year
          , subq_17.listing__ds__extract_quarter
          , subq_17.listing__ds__extract_month
          , subq_17.listing__ds__extract_day
          , subq_17.listing__ds__extract_dow
          , subq_17.listing__ds__extract_doy
          , subq_17.listing__created_at__day
          , subq_17.listing__created_at__week
          , subq_17.listing__created_at__month
          , subq_17.listing__created_at__quarter
          , subq_17.listing__created_at__year
          , subq_17.listing__created_at__extract_year
          , subq_17.listing__created_at__extract_quarter
          , subq_17.listing__created_at__extract_month
          , subq_17.listing__created_at__extract_day
          , subq_17.listing__created_at__extract_dow
          , subq_17.listing__created_at__extract_doy
          , subq_17.metric_time__day
          , subq_17.metric_time__week
          , subq_17.metric_time__month
          , subq_17.metric_time__quarter
          , subq_17.metric_time__year
          , subq_17.metric_time__extract_year
          , subq_17.metric_time__extract_quarter
          , subq_17.metric_time__extract_month
          , subq_17.metric_time__extract_day
          , subq_17.metric_time__extract_dow
          , subq_17.metric_time__extract_doy
          , subq_17.listing
          , subq_17.user
          , subq_17.listing__user
          , subq_17.user__listing__user
          , subq_17.country_latest
          , subq_17.is_lux_latest
          , subq_17.capacity_latest
          , subq_17.listing__country_latest
          , subq_17.listing__is_lux_latest
          , subq_17.listing__capacity_latest
          , subq_17.user__listing__user__average_booking_value
          , subq_17.listings
          , subq_17.largest_listing
          , subq_17.smallest_listing
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_16.listing__user AS user__listing__user
            , subq_16.listing__user__average_booking_value AS user__listing__user__average_booking_value
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
            ) subq_6
          ) subq_7
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['listing__user', 'listing__user__average_booking_value']
            SELECT
              subq_15.listing__user
              , subq_15.listing__user__average_booking_value
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_14.listing__user
                , subq_14.average_booking_value AS listing__user__average_booking_value
              FROM (
                -- Aggregate Measures
                SELECT
                  subq_13.listing__user
                  , AVG(subq_13.average_booking_value) AS average_booking_value
                FROM (
                  -- Pass Only Elements: ['average_booking_value', 'listing__user']
                  SELECT
                    subq_12.listing__user
                    , subq_12.average_booking_value
                  FROM (
                    -- Join Standard Outputs
                    SELECT
                      subq_11.user AS listing__user
                      , subq_9.ds__day AS ds__day
                      , subq_9.ds__week AS ds__week
                      , subq_9.ds__month AS ds__month
                      , subq_9.ds__quarter AS ds__quarter
                      , subq_9.ds__year AS ds__year
                      , subq_9.ds__extract_year AS ds__extract_year
                      , subq_9.ds__extract_quarter AS ds__extract_quarter
                      , subq_9.ds__extract_month AS ds__extract_month
                      , subq_9.ds__extract_day AS ds__extract_day
                      , subq_9.ds__extract_dow AS ds__extract_dow
                      , subq_9.ds__extract_doy AS ds__extract_doy
                      , subq_9.ds_partitioned__day AS ds_partitioned__day
                      , subq_9.ds_partitioned__week AS ds_partitioned__week
                      , subq_9.ds_partitioned__month AS ds_partitioned__month
                      , subq_9.ds_partitioned__quarter AS ds_partitioned__quarter
                      , subq_9.ds_partitioned__year AS ds_partitioned__year
                      , subq_9.ds_partitioned__extract_year AS ds_partitioned__extract_year
                      , subq_9.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                      , subq_9.ds_partitioned__extract_month AS ds_partitioned__extract_month
                      , subq_9.ds_partitioned__extract_day AS ds_partitioned__extract_day
                      , subq_9.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                      , subq_9.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                      , subq_9.paid_at__day AS paid_at__day
                      , subq_9.paid_at__week AS paid_at__week
                      , subq_9.paid_at__month AS paid_at__month
                      , subq_9.paid_at__quarter AS paid_at__quarter
                      , subq_9.paid_at__year AS paid_at__year
                      , subq_9.paid_at__extract_year AS paid_at__extract_year
                      , subq_9.paid_at__extract_quarter AS paid_at__extract_quarter
                      , subq_9.paid_at__extract_month AS paid_at__extract_month
                      , subq_9.paid_at__extract_day AS paid_at__extract_day
                      , subq_9.paid_at__extract_dow AS paid_at__extract_dow
                      , subq_9.paid_at__extract_doy AS paid_at__extract_doy
                      , subq_9.booking__ds__day AS booking__ds__day
                      , subq_9.booking__ds__week AS booking__ds__week
                      , subq_9.booking__ds__month AS booking__ds__month
                      , subq_9.booking__ds__quarter AS booking__ds__quarter
                      , subq_9.booking__ds__year AS booking__ds__year
                      , subq_9.booking__ds__extract_year AS booking__ds__extract_year
                      , subq_9.booking__ds__extract_quarter AS booking__ds__extract_quarter
                      , subq_9.booking__ds__extract_month AS booking__ds__extract_month
                      , subq_9.booking__ds__extract_day AS booking__ds__extract_day
                      , subq_9.booking__ds__extract_dow AS booking__ds__extract_dow
                      , subq_9.booking__ds__extract_doy AS booking__ds__extract_doy
                      , subq_9.booking__ds_partitioned__day AS booking__ds_partitioned__day
                      , subq_9.booking__ds_partitioned__week AS booking__ds_partitioned__week
                      , subq_9.booking__ds_partitioned__month AS booking__ds_partitioned__month
                      , subq_9.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                      , subq_9.booking__ds_partitioned__year AS booking__ds_partitioned__year
                      , subq_9.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                      , subq_9.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                      , subq_9.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                      , subq_9.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                      , subq_9.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                      , subq_9.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                      , subq_9.booking__paid_at__day AS booking__paid_at__day
                      , subq_9.booking__paid_at__week AS booking__paid_at__week
                      , subq_9.booking__paid_at__month AS booking__paid_at__month
                      , subq_9.booking__paid_at__quarter AS booking__paid_at__quarter
                      , subq_9.booking__paid_at__year AS booking__paid_at__year
                      , subq_9.booking__paid_at__extract_year AS booking__paid_at__extract_year
                      , subq_9.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                      , subq_9.booking__paid_at__extract_month AS booking__paid_at__extract_month
                      , subq_9.booking__paid_at__extract_day AS booking__paid_at__extract_day
                      , subq_9.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                      , subq_9.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
                      , subq_9.metric_time__day AS metric_time__day
                      , subq_9.metric_time__week AS metric_time__week
                      , subq_9.metric_time__month AS metric_time__month
                      , subq_9.metric_time__quarter AS metric_time__quarter
                      , subq_9.metric_time__year AS metric_time__year
                      , subq_9.metric_time__extract_year AS metric_time__extract_year
                      , subq_9.metric_time__extract_quarter AS metric_time__extract_quarter
                      , subq_9.metric_time__extract_month AS metric_time__extract_month
                      , subq_9.metric_time__extract_day AS metric_time__extract_day
                      , subq_9.metric_time__extract_dow AS metric_time__extract_dow
                      , subq_9.metric_time__extract_doy AS metric_time__extract_doy
                      , subq_9.listing AS listing
                      , subq_9.guest AS guest
                      , subq_9.host AS host
                      , subq_9.booking__listing AS booking__listing
                      , subq_9.booking__guest AS booking__guest
                      , subq_9.booking__host AS booking__host
                      , subq_9.is_instant AS is_instant
                      , subq_9.booking__is_instant AS booking__is_instant
                      , subq_9.bookings AS bookings
                      , subq_9.instant_bookings AS instant_bookings
                      , subq_9.booking_value AS booking_value
                      , subq_9.max_booking_value AS max_booking_value
                      , subq_9.min_booking_value AS min_booking_value
                      , subq_9.bookers AS bookers
                      , subq_9.average_booking_value AS average_booking_value
                      , subq_9.referred_bookings AS referred_bookings
                      , subq_9.median_booking_value AS median_booking_value
                      , subq_9.booking_value_p99 AS booking_value_p99
                      , subq_9.discrete_booking_value_p99 AS discrete_booking_value_p99
                      , subq_9.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
                      , subq_9.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
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
                    LEFT OUTER JOIN (
                      -- Pass Only Elements: ['listing', 'user']
                      SELECT
                        subq_10.listing
                        , subq_10.user
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
                        ) subq_6
                      ) subq_10
                    ) subq_11
                    ON
                      subq_9.listing = subq_11.listing
                  ) subq_12
                ) subq_13
                GROUP BY
                  subq_13.listing__user
              ) subq_14
            ) subq_15
          ) subq_16
          ON
            subq_7.user = subq_16.listing__user
        ) subq_17
        WHERE user__listing__user__average_booking_value > 1
      ) subq_18
    ) subq_19
  ) subq_20
) subq_21
