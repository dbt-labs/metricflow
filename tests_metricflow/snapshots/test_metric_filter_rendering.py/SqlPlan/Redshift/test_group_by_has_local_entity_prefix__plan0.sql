test_name: test_group_by_has_local_entity_prefix
test_filename: test_metric_filter_rendering.py
sql_engine: Redshift
---
-- Compute Metrics via Expressions
SELECT
  subq_15.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_14.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      subq_13.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_12.ds__day
        , subq_12.ds__week
        , subq_12.ds__month
        , subq_12.ds__quarter
        , subq_12.ds__year
        , subq_12.ds__extract_year
        , subq_12.ds__extract_quarter
        , subq_12.ds__extract_month
        , subq_12.ds__extract_day
        , subq_12.ds__extract_dow
        , subq_12.ds__extract_doy
        , subq_12.created_at__day
        , subq_12.created_at__week
        , subq_12.created_at__month
        , subq_12.created_at__quarter
        , subq_12.created_at__year
        , subq_12.created_at__extract_year
        , subq_12.created_at__extract_quarter
        , subq_12.created_at__extract_month
        , subq_12.created_at__extract_day
        , subq_12.created_at__extract_dow
        , subq_12.created_at__extract_doy
        , subq_12.listing__ds__day
        , subq_12.listing__ds__week
        , subq_12.listing__ds__month
        , subq_12.listing__ds__quarter
        , subq_12.listing__ds__year
        , subq_12.listing__ds__extract_year
        , subq_12.listing__ds__extract_quarter
        , subq_12.listing__ds__extract_month
        , subq_12.listing__ds__extract_day
        , subq_12.listing__ds__extract_dow
        , subq_12.listing__ds__extract_doy
        , subq_12.listing__created_at__day
        , subq_12.listing__created_at__week
        , subq_12.listing__created_at__month
        , subq_12.listing__created_at__quarter
        , subq_12.listing__created_at__year
        , subq_12.listing__created_at__extract_year
        , subq_12.listing__created_at__extract_quarter
        , subq_12.listing__created_at__extract_month
        , subq_12.listing__created_at__extract_day
        , subq_12.listing__created_at__extract_dow
        , subq_12.listing__created_at__extract_doy
        , subq_12.metric_time__day
        , subq_12.metric_time__week
        , subq_12.metric_time__month
        , subq_12.metric_time__quarter
        , subq_12.metric_time__year
        , subq_12.metric_time__extract_year
        , subq_12.metric_time__extract_quarter
        , subq_12.metric_time__extract_month
        , subq_12.metric_time__extract_day
        , subq_12.metric_time__extract_dow
        , subq_12.metric_time__extract_doy
        , subq_12.listing
        , subq_12.user
        , subq_12.listing__user
        , subq_12.user__listing__user
        , subq_12.country_latest
        , subq_12.is_lux_latest
        , subq_12.capacity_latest
        , subq_12.listing__country_latest
        , subq_12.listing__is_lux_latest
        , subq_12.listing__capacity_latest
        , subq_12.user__listing__user__average_booking_value
        , subq_12.listings
        , subq_12.largest_listing
        , subq_12.smallest_listing
      FROM (
        -- Join Standard Outputs
        SELECT
          subq_11.listing__user AS user__listing__user
          , subq_11.listing__user__average_booking_value AS user__listing__user__average_booking_value
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
          ) subq_0
        ) subq_1
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['listing__user', 'listing__user__average_booking_value']
          SELECT
            subq_10.listing__user
            , subq_10.listing__user__average_booking_value
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              subq_9.listing__user
              , subq_9.average_booking_value AS listing__user__average_booking_value
            FROM (
              -- Aggregate Measures
              SELECT
                subq_8.listing__user
                , AVG(subq_8.average_booking_value) AS average_booking_value
              FROM (
                -- Pass Only Elements: ['average_booking_value', 'listing__user']
                SELECT
                  subq_7.listing__user
                  , subq_7.average_booking_value
                FROM (
                  -- Join Standard Outputs
                  SELECT
                    subq_6.user AS listing__user
                    , subq_3.ds__day AS ds__day
                    , subq_3.ds__week AS ds__week
                    , subq_3.ds__month AS ds__month
                    , subq_3.ds__quarter AS ds__quarter
                    , subq_3.ds__year AS ds__year
                    , subq_3.ds__extract_year AS ds__extract_year
                    , subq_3.ds__extract_quarter AS ds__extract_quarter
                    , subq_3.ds__extract_month AS ds__extract_month
                    , subq_3.ds__extract_day AS ds__extract_day
                    , subq_3.ds__extract_dow AS ds__extract_dow
                    , subq_3.ds__extract_doy AS ds__extract_doy
                    , subq_3.ds_partitioned__day AS ds_partitioned__day
                    , subq_3.ds_partitioned__week AS ds_partitioned__week
                    , subq_3.ds_partitioned__month AS ds_partitioned__month
                    , subq_3.ds_partitioned__quarter AS ds_partitioned__quarter
                    , subq_3.ds_partitioned__year AS ds_partitioned__year
                    , subq_3.ds_partitioned__extract_year AS ds_partitioned__extract_year
                    , subq_3.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                    , subq_3.ds_partitioned__extract_month AS ds_partitioned__extract_month
                    , subq_3.ds_partitioned__extract_day AS ds_partitioned__extract_day
                    , subq_3.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                    , subq_3.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                    , subq_3.paid_at__day AS paid_at__day
                    , subq_3.paid_at__week AS paid_at__week
                    , subq_3.paid_at__month AS paid_at__month
                    , subq_3.paid_at__quarter AS paid_at__quarter
                    , subq_3.paid_at__year AS paid_at__year
                    , subq_3.paid_at__extract_year AS paid_at__extract_year
                    , subq_3.paid_at__extract_quarter AS paid_at__extract_quarter
                    , subq_3.paid_at__extract_month AS paid_at__extract_month
                    , subq_3.paid_at__extract_day AS paid_at__extract_day
                    , subq_3.paid_at__extract_dow AS paid_at__extract_dow
                    , subq_3.paid_at__extract_doy AS paid_at__extract_doy
                    , subq_3.booking__ds__day AS booking__ds__day
                    , subq_3.booking__ds__week AS booking__ds__week
                    , subq_3.booking__ds__month AS booking__ds__month
                    , subq_3.booking__ds__quarter AS booking__ds__quarter
                    , subq_3.booking__ds__year AS booking__ds__year
                    , subq_3.booking__ds__extract_year AS booking__ds__extract_year
                    , subq_3.booking__ds__extract_quarter AS booking__ds__extract_quarter
                    , subq_3.booking__ds__extract_month AS booking__ds__extract_month
                    , subq_3.booking__ds__extract_day AS booking__ds__extract_day
                    , subq_3.booking__ds__extract_dow AS booking__ds__extract_dow
                    , subq_3.booking__ds__extract_doy AS booking__ds__extract_doy
                    , subq_3.booking__ds_partitioned__day AS booking__ds_partitioned__day
                    , subq_3.booking__ds_partitioned__week AS booking__ds_partitioned__week
                    , subq_3.booking__ds_partitioned__month AS booking__ds_partitioned__month
                    , subq_3.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                    , subq_3.booking__ds_partitioned__year AS booking__ds_partitioned__year
                    , subq_3.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                    , subq_3.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                    , subq_3.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                    , subq_3.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                    , subq_3.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                    , subq_3.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                    , subq_3.booking__paid_at__day AS booking__paid_at__day
                    , subq_3.booking__paid_at__week AS booking__paid_at__week
                    , subq_3.booking__paid_at__month AS booking__paid_at__month
                    , subq_3.booking__paid_at__quarter AS booking__paid_at__quarter
                    , subq_3.booking__paid_at__year AS booking__paid_at__year
                    , subq_3.booking__paid_at__extract_year AS booking__paid_at__extract_year
                    , subq_3.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                    , subq_3.booking__paid_at__extract_month AS booking__paid_at__extract_month
                    , subq_3.booking__paid_at__extract_day AS booking__paid_at__extract_day
                    , subq_3.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                    , subq_3.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
                    , subq_3.metric_time__day AS metric_time__day
                    , subq_3.metric_time__week AS metric_time__week
                    , subq_3.metric_time__month AS metric_time__month
                    , subq_3.metric_time__quarter AS metric_time__quarter
                    , subq_3.metric_time__year AS metric_time__year
                    , subq_3.metric_time__extract_year AS metric_time__extract_year
                    , subq_3.metric_time__extract_quarter AS metric_time__extract_quarter
                    , subq_3.metric_time__extract_month AS metric_time__extract_month
                    , subq_3.metric_time__extract_day AS metric_time__extract_day
                    , subq_3.metric_time__extract_dow AS metric_time__extract_dow
                    , subq_3.metric_time__extract_doy AS metric_time__extract_doy
                    , subq_3.listing AS listing
                    , subq_3.guest AS guest
                    , subq_3.host AS host
                    , subq_3.booking__listing AS booking__listing
                    , subq_3.booking__guest AS booking__guest
                    , subq_3.booking__host AS booking__host
                    , subq_3.is_instant AS is_instant
                    , subq_3.booking__is_instant AS booking__is_instant
                    , subq_3.bookings AS bookings
                    , subq_3.instant_bookings AS instant_bookings
                    , subq_3.booking_value AS booking_value
                    , subq_3.max_booking_value AS max_booking_value
                    , subq_3.min_booking_value AS min_booking_value
                    , subq_3.bookers AS bookers
                    , subq_3.average_booking_value AS average_booking_value
                    , subq_3.referred_bookings AS referred_bookings
                    , subq_3.median_booking_value AS median_booking_value
                    , subq_3.booking_value_p99 AS booking_value_p99
                    , subq_3.discrete_booking_value_p99 AS discrete_booking_value_p99
                    , subq_3.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
                    , subq_3.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
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
                    ) subq_2
                  ) subq_3
                  LEFT OUTER JOIN (
                    -- Pass Only Elements: ['listing', 'user']
                    SELECT
                      subq_5.listing
                      , subq_5.user
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
                        , subq_4.listings
                        , subq_4.largest_listing
                        , subq_4.smallest_listing
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
                      ) subq_4
                    ) subq_5
                  ) subq_6
                  ON
                    subq_3.listing = subq_6.listing
                ) subq_7
              ) subq_8
              GROUP BY
                subq_8.listing__user
            ) subq_9
          ) subq_10
        ) subq_11
        ON
          subq_1.user = subq_11.listing__user
      ) subq_12
      WHERE user__listing__user__average_booking_value > 1
    ) subq_13
  ) subq_14
) subq_15
