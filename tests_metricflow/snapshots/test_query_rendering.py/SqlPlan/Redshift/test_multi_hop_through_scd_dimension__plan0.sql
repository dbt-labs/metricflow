test_name: test_multi_hop_through_scd_dimension
test_filename: test_query_rendering.py
docstring:
  Tests conversion of a plan using a dimension that is reached through an SCD table.
sql_engine: Redshift
---
-- Write to DataTable
SELECT
  subq_17.metric_time__day
  , subq_17.listing__user__home_state_latest
  , subq_17.bookings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_16.metric_time__day
    , subq_16.listing__user__home_state_latest
    , subq_16.__bookings AS bookings
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_15.metric_time__day
      , subq_15.listing__user__home_state_latest
      , SUM(subq_15.__bookings) AS __bookings
    FROM (
      -- Pass Only Elements: ['__bookings', 'listing__user__home_state_latest', 'metric_time__day']
      SELECT
        subq_14.metric_time__day
        , subq_14.listing__user__home_state_latest
        , subq_14.__bookings
      FROM (
        -- Pass Only Elements: ['__bookings', 'listing__user__home_state_latest', 'metric_time__day']
        SELECT
          subq_13.metric_time__day
          , subq_13.listing__user__home_state_latest
          , subq_13.__bookings
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_12.user__home_state_latest AS listing__user__home_state_latest
            , subq_12.window_start__day AS listing__window_start__day
            , subq_12.window_end__day AS listing__window_end__day
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
            , subq_7.ds_partitioned__day AS ds_partitioned__day
            , subq_7.ds_partitioned__week AS ds_partitioned__week
            , subq_7.ds_partitioned__month AS ds_partitioned__month
            , subq_7.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_7.ds_partitioned__year AS ds_partitioned__year
            , subq_7.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , subq_7.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , subq_7.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , subq_7.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , subq_7.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , subq_7.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , subq_7.paid_at__day AS paid_at__day
            , subq_7.paid_at__week AS paid_at__week
            , subq_7.paid_at__month AS paid_at__month
            , subq_7.paid_at__quarter AS paid_at__quarter
            , subq_7.paid_at__year AS paid_at__year
            , subq_7.paid_at__extract_year AS paid_at__extract_year
            , subq_7.paid_at__extract_quarter AS paid_at__extract_quarter
            , subq_7.paid_at__extract_month AS paid_at__extract_month
            , subq_7.paid_at__extract_day AS paid_at__extract_day
            , subq_7.paid_at__extract_dow AS paid_at__extract_dow
            , subq_7.paid_at__extract_doy AS paid_at__extract_doy
            , subq_7.booking__ds__day AS booking__ds__day
            , subq_7.booking__ds__week AS booking__ds__week
            , subq_7.booking__ds__month AS booking__ds__month
            , subq_7.booking__ds__quarter AS booking__ds__quarter
            , subq_7.booking__ds__year AS booking__ds__year
            , subq_7.booking__ds__extract_year AS booking__ds__extract_year
            , subq_7.booking__ds__extract_quarter AS booking__ds__extract_quarter
            , subq_7.booking__ds__extract_month AS booking__ds__extract_month
            , subq_7.booking__ds__extract_day AS booking__ds__extract_day
            , subq_7.booking__ds__extract_dow AS booking__ds__extract_dow
            , subq_7.booking__ds__extract_doy AS booking__ds__extract_doy
            , subq_7.booking__ds_partitioned__day AS booking__ds_partitioned__day
            , subq_7.booking__ds_partitioned__week AS booking__ds_partitioned__week
            , subq_7.booking__ds_partitioned__month AS booking__ds_partitioned__month
            , subq_7.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
            , subq_7.booking__ds_partitioned__year AS booking__ds_partitioned__year
            , subq_7.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
            , subq_7.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
            , subq_7.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
            , subq_7.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
            , subq_7.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
            , subq_7.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
            , subq_7.booking__paid_at__day AS booking__paid_at__day
            , subq_7.booking__paid_at__week AS booking__paid_at__week
            , subq_7.booking__paid_at__month AS booking__paid_at__month
            , subq_7.booking__paid_at__quarter AS booking__paid_at__quarter
            , subq_7.booking__paid_at__year AS booking__paid_at__year
            , subq_7.booking__paid_at__extract_year AS booking__paid_at__extract_year
            , subq_7.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
            , subq_7.booking__paid_at__extract_month AS booking__paid_at__extract_month
            , subq_7.booking__paid_at__extract_day AS booking__paid_at__extract_day
            , subq_7.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
            , subq_7.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
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
            , subq_7.guest AS guest
            , subq_7.host AS host
            , subq_7.user AS user
            , subq_7.booking__listing AS booking__listing
            , subq_7.booking__guest AS booking__guest
            , subq_7.booking__host AS booking__host
            , subq_7.booking__user AS booking__user
            , subq_7.is_instant AS is_instant
            , subq_7.booking__is_instant AS booking__is_instant
            , subq_7.__bookings AS __bookings
            , subq_7.__family_bookings AS __family_bookings
            , subq_7.__potentially_lux_bookings AS __potentially_lux_bookings
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
              , subq_6.user
              , subq_6.booking__listing
              , subq_6.booking__guest
              , subq_6.booking__host
              , subq_6.booking__user
              , subq_6.is_instant
              , subq_6.booking__is_instant
              , subq_6.__bookings
              , subq_6.__family_bookings
              , subq_6.__potentially_lux_bookings
            FROM (
              -- Read Elements From Semantic Model 'bookings_source'
              SELECT
                1 AS __bookings
                , 1 AS __family_bookings
                , 1 AS __potentially_lux_bookings
                , bookings_source_src_26000.is_instant
                , DATE_TRUNC('day', bookings_source_src_26000.ds) AS ds__day
                , DATE_TRUNC('week', bookings_source_src_26000.ds) AS ds__week
                , DATE_TRUNC('month', bookings_source_src_26000.ds) AS ds__month
                , DATE_TRUNC('quarter', bookings_source_src_26000.ds) AS ds__quarter
                , DATE_TRUNC('year', bookings_source_src_26000.ds) AS ds__year
                , EXTRACT(year FROM bookings_source_src_26000.ds) AS ds__extract_year
                , EXTRACT(quarter FROM bookings_source_src_26000.ds) AS ds__extract_quarter
                , EXTRACT(month FROM bookings_source_src_26000.ds) AS ds__extract_month
                , EXTRACT(day FROM bookings_source_src_26000.ds) AS ds__extract_day
                , CASE WHEN EXTRACT(dow FROM bookings_source_src_26000.ds) = 0 THEN EXTRACT(dow FROM bookings_source_src_26000.ds) + 7 ELSE EXTRACT(dow FROM bookings_source_src_26000.ds) END AS ds__extract_dow
                , EXTRACT(doy FROM bookings_source_src_26000.ds) AS ds__extract_doy
                , DATE_TRUNC('day', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__day
                , DATE_TRUNC('week', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__week
                , DATE_TRUNC('month', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__month
                , DATE_TRUNC('quarter', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__quarter
                , DATE_TRUNC('year', bookings_source_src_26000.ds_partitioned) AS ds_partitioned__year
                , EXTRACT(year FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_year
                , EXTRACT(quarter FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_quarter
                , EXTRACT(month FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_month
                , EXTRACT(day FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_day
                , CASE WHEN EXTRACT(dow FROM bookings_source_src_26000.ds_partitioned) = 0 THEN EXTRACT(dow FROM bookings_source_src_26000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM bookings_source_src_26000.ds_partitioned) END AS ds_partitioned__extract_dow
                , EXTRACT(doy FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_doy
                , DATE_TRUNC('day', bookings_source_src_26000.paid_at) AS paid_at__day
                , DATE_TRUNC('week', bookings_source_src_26000.paid_at) AS paid_at__week
                , DATE_TRUNC('month', bookings_source_src_26000.paid_at) AS paid_at__month
                , DATE_TRUNC('quarter', bookings_source_src_26000.paid_at) AS paid_at__quarter
                , DATE_TRUNC('year', bookings_source_src_26000.paid_at) AS paid_at__year
                , EXTRACT(year FROM bookings_source_src_26000.paid_at) AS paid_at__extract_year
                , EXTRACT(quarter FROM bookings_source_src_26000.paid_at) AS paid_at__extract_quarter
                , EXTRACT(month FROM bookings_source_src_26000.paid_at) AS paid_at__extract_month
                , EXTRACT(day FROM bookings_source_src_26000.paid_at) AS paid_at__extract_day
                , CASE WHEN EXTRACT(dow FROM bookings_source_src_26000.paid_at) = 0 THEN EXTRACT(dow FROM bookings_source_src_26000.paid_at) + 7 ELSE EXTRACT(dow FROM bookings_source_src_26000.paid_at) END AS paid_at__extract_dow
                , EXTRACT(doy FROM bookings_source_src_26000.paid_at) AS paid_at__extract_doy
                , bookings_source_src_26000.is_instant AS booking__is_instant
                , DATE_TRUNC('day', bookings_source_src_26000.ds) AS booking__ds__day
                , DATE_TRUNC('week', bookings_source_src_26000.ds) AS booking__ds__week
                , DATE_TRUNC('month', bookings_source_src_26000.ds) AS booking__ds__month
                , DATE_TRUNC('quarter', bookings_source_src_26000.ds) AS booking__ds__quarter
                , DATE_TRUNC('year', bookings_source_src_26000.ds) AS booking__ds__year
                , EXTRACT(year FROM bookings_source_src_26000.ds) AS booking__ds__extract_year
                , EXTRACT(quarter FROM bookings_source_src_26000.ds) AS booking__ds__extract_quarter
                , EXTRACT(month FROM bookings_source_src_26000.ds) AS booking__ds__extract_month
                , EXTRACT(day FROM bookings_source_src_26000.ds) AS booking__ds__extract_day
                , CASE WHEN EXTRACT(dow FROM bookings_source_src_26000.ds) = 0 THEN EXTRACT(dow FROM bookings_source_src_26000.ds) + 7 ELSE EXTRACT(dow FROM bookings_source_src_26000.ds) END AS booking__ds__extract_dow
                , EXTRACT(doy FROM bookings_source_src_26000.ds) AS booking__ds__extract_doy
                , DATE_TRUNC('day', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__day
                , DATE_TRUNC('week', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__week
                , DATE_TRUNC('month', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__month
                , DATE_TRUNC('quarter', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__quarter
                , DATE_TRUNC('year', bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__year
                , EXTRACT(year FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_year
                , EXTRACT(quarter FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                , EXTRACT(month FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_month
                , EXTRACT(day FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_day
                , CASE WHEN EXTRACT(dow FROM bookings_source_src_26000.ds_partitioned) = 0 THEN EXTRACT(dow FROM bookings_source_src_26000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM bookings_source_src_26000.ds_partitioned) END AS booking__ds_partitioned__extract_dow
                , EXTRACT(doy FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_doy
                , DATE_TRUNC('day', bookings_source_src_26000.paid_at) AS booking__paid_at__day
                , DATE_TRUNC('week', bookings_source_src_26000.paid_at) AS booking__paid_at__week
                , DATE_TRUNC('month', bookings_source_src_26000.paid_at) AS booking__paid_at__month
                , DATE_TRUNC('quarter', bookings_source_src_26000.paid_at) AS booking__paid_at__quarter
                , DATE_TRUNC('year', bookings_source_src_26000.paid_at) AS booking__paid_at__year
                , EXTRACT(year FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_year
                , EXTRACT(quarter FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_quarter
                , EXTRACT(month FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_month
                , EXTRACT(day FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_day
                , CASE WHEN EXTRACT(dow FROM bookings_source_src_26000.paid_at) = 0 THEN EXTRACT(dow FROM bookings_source_src_26000.paid_at) + 7 ELSE EXTRACT(dow FROM bookings_source_src_26000.paid_at) END AS booking__paid_at__extract_dow
                , EXTRACT(doy FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_doy
                , bookings_source_src_26000.listing_id AS listing
                , bookings_source_src_26000.guest_id AS guest
                , bookings_source_src_26000.host_id AS host
                , bookings_source_src_26000.guest_id AS user
                , bookings_source_src_26000.listing_id AS booking__listing
                , bookings_source_src_26000.guest_id AS booking__guest
                , bookings_source_src_26000.host_id AS booking__host
                , bookings_source_src_26000.guest_id AS booking__user
              FROM ***************************.fct_bookings bookings_source_src_26000
            ) subq_6
          ) subq_7
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['user__home_state_latest', 'window_start__day', 'window_end__day', 'listing']
            SELECT
              subq_11.window_start__day
              , subq_11.window_end__day
              , subq_11.listing
              , subq_11.user__home_state_latest
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_10.home_state_latest AS user__home_state_latest
                , subq_10.ds__day AS user__ds__day
                , subq_10.ds__week AS user__ds__week
                , subq_10.ds__month AS user__ds__month
                , subq_10.ds__quarter AS user__ds__quarter
                , subq_10.ds__year AS user__ds__year
                , subq_10.ds__extract_year AS user__ds__extract_year
                , subq_10.ds__extract_quarter AS user__ds__extract_quarter
                , subq_10.ds__extract_month AS user__ds__extract_month
                , subq_10.ds__extract_day AS user__ds__extract_day
                , subq_10.ds__extract_dow AS user__ds__extract_dow
                , subq_10.ds__extract_doy AS user__ds__extract_doy
                , subq_8.window_start__day AS window_start__day
                , subq_8.window_start__week AS window_start__week
                , subq_8.window_start__month AS window_start__month
                , subq_8.window_start__quarter AS window_start__quarter
                , subq_8.window_start__year AS window_start__year
                , subq_8.window_start__extract_year AS window_start__extract_year
                , subq_8.window_start__extract_quarter AS window_start__extract_quarter
                , subq_8.window_start__extract_month AS window_start__extract_month
                , subq_8.window_start__extract_day AS window_start__extract_day
                , subq_8.window_start__extract_dow AS window_start__extract_dow
                , subq_8.window_start__extract_doy AS window_start__extract_doy
                , subq_8.window_end__day AS window_end__day
                , subq_8.window_end__week AS window_end__week
                , subq_8.window_end__month AS window_end__month
                , subq_8.window_end__quarter AS window_end__quarter
                , subq_8.window_end__year AS window_end__year
                , subq_8.window_end__extract_year AS window_end__extract_year
                , subq_8.window_end__extract_quarter AS window_end__extract_quarter
                , subq_8.window_end__extract_month AS window_end__extract_month
                , subq_8.window_end__extract_day AS window_end__extract_day
                , subq_8.window_end__extract_dow AS window_end__extract_dow
                , subq_8.window_end__extract_doy AS window_end__extract_doy
                , subq_8.listing__window_start__day AS listing__window_start__day
                , subq_8.listing__window_start__week AS listing__window_start__week
                , subq_8.listing__window_start__month AS listing__window_start__month
                , subq_8.listing__window_start__quarter AS listing__window_start__quarter
                , subq_8.listing__window_start__year AS listing__window_start__year
                , subq_8.listing__window_start__extract_year AS listing__window_start__extract_year
                , subq_8.listing__window_start__extract_quarter AS listing__window_start__extract_quarter
                , subq_8.listing__window_start__extract_month AS listing__window_start__extract_month
                , subq_8.listing__window_start__extract_day AS listing__window_start__extract_day
                , subq_8.listing__window_start__extract_dow AS listing__window_start__extract_dow
                , subq_8.listing__window_start__extract_doy AS listing__window_start__extract_doy
                , subq_8.listing__window_end__day AS listing__window_end__day
                , subq_8.listing__window_end__week AS listing__window_end__week
                , subq_8.listing__window_end__month AS listing__window_end__month
                , subq_8.listing__window_end__quarter AS listing__window_end__quarter
                , subq_8.listing__window_end__year AS listing__window_end__year
                , subq_8.listing__window_end__extract_year AS listing__window_end__extract_year
                , subq_8.listing__window_end__extract_quarter AS listing__window_end__extract_quarter
                , subq_8.listing__window_end__extract_month AS listing__window_end__extract_month
                , subq_8.listing__window_end__extract_day AS listing__window_end__extract_day
                , subq_8.listing__window_end__extract_dow AS listing__window_end__extract_dow
                , subq_8.listing__window_end__extract_doy AS listing__window_end__extract_doy
                , subq_8.listing AS listing
                , subq_8.user AS user
                , subq_8.listing__user AS listing__user
                , subq_8.country AS country
                , subq_8.is_lux AS is_lux
                , subq_8.capacity AS capacity
                , subq_8.listing__country AS listing__country
                , subq_8.listing__is_lux AS listing__is_lux
                , subq_8.listing__capacity AS listing__capacity
              FROM (
                -- Read Elements From Semantic Model 'listings'
                SELECT
                  listings_src_26000.active_from AS window_start__day
                  , DATE_TRUNC('week', listings_src_26000.active_from) AS window_start__week
                  , DATE_TRUNC('month', listings_src_26000.active_from) AS window_start__month
                  , DATE_TRUNC('quarter', listings_src_26000.active_from) AS window_start__quarter
                  , DATE_TRUNC('year', listings_src_26000.active_from) AS window_start__year
                  , EXTRACT(year FROM listings_src_26000.active_from) AS window_start__extract_year
                  , EXTRACT(quarter FROM listings_src_26000.active_from) AS window_start__extract_quarter
                  , EXTRACT(month FROM listings_src_26000.active_from) AS window_start__extract_month
                  , EXTRACT(day FROM listings_src_26000.active_from) AS window_start__extract_day
                  , CASE WHEN EXTRACT(dow FROM listings_src_26000.active_from) = 0 THEN EXTRACT(dow FROM listings_src_26000.active_from) + 7 ELSE EXTRACT(dow FROM listings_src_26000.active_from) END AS window_start__extract_dow
                  , EXTRACT(doy FROM listings_src_26000.active_from) AS window_start__extract_doy
                  , listings_src_26000.active_to AS window_end__day
                  , DATE_TRUNC('week', listings_src_26000.active_to) AS window_end__week
                  , DATE_TRUNC('month', listings_src_26000.active_to) AS window_end__month
                  , DATE_TRUNC('quarter', listings_src_26000.active_to) AS window_end__quarter
                  , DATE_TRUNC('year', listings_src_26000.active_to) AS window_end__year
                  , EXTRACT(year FROM listings_src_26000.active_to) AS window_end__extract_year
                  , EXTRACT(quarter FROM listings_src_26000.active_to) AS window_end__extract_quarter
                  , EXTRACT(month FROM listings_src_26000.active_to) AS window_end__extract_month
                  , EXTRACT(day FROM listings_src_26000.active_to) AS window_end__extract_day
                  , CASE WHEN EXTRACT(dow FROM listings_src_26000.active_to) = 0 THEN EXTRACT(dow FROM listings_src_26000.active_to) + 7 ELSE EXTRACT(dow FROM listings_src_26000.active_to) END AS window_end__extract_dow
                  , EXTRACT(doy FROM listings_src_26000.active_to) AS window_end__extract_doy
                  , listings_src_26000.country
                  , listings_src_26000.is_lux
                  , listings_src_26000.capacity
                  , listings_src_26000.active_from AS listing__window_start__day
                  , DATE_TRUNC('week', listings_src_26000.active_from) AS listing__window_start__week
                  , DATE_TRUNC('month', listings_src_26000.active_from) AS listing__window_start__month
                  , DATE_TRUNC('quarter', listings_src_26000.active_from) AS listing__window_start__quarter
                  , DATE_TRUNC('year', listings_src_26000.active_from) AS listing__window_start__year
                  , EXTRACT(year FROM listings_src_26000.active_from) AS listing__window_start__extract_year
                  , EXTRACT(quarter FROM listings_src_26000.active_from) AS listing__window_start__extract_quarter
                  , EXTRACT(month FROM listings_src_26000.active_from) AS listing__window_start__extract_month
                  , EXTRACT(day FROM listings_src_26000.active_from) AS listing__window_start__extract_day
                  , CASE WHEN EXTRACT(dow FROM listings_src_26000.active_from) = 0 THEN EXTRACT(dow FROM listings_src_26000.active_from) + 7 ELSE EXTRACT(dow FROM listings_src_26000.active_from) END AS listing__window_start__extract_dow
                  , EXTRACT(doy FROM listings_src_26000.active_from) AS listing__window_start__extract_doy
                  , listings_src_26000.active_to AS listing__window_end__day
                  , DATE_TRUNC('week', listings_src_26000.active_to) AS listing__window_end__week
                  , DATE_TRUNC('month', listings_src_26000.active_to) AS listing__window_end__month
                  , DATE_TRUNC('quarter', listings_src_26000.active_to) AS listing__window_end__quarter
                  , DATE_TRUNC('year', listings_src_26000.active_to) AS listing__window_end__year
                  , EXTRACT(year FROM listings_src_26000.active_to) AS listing__window_end__extract_year
                  , EXTRACT(quarter FROM listings_src_26000.active_to) AS listing__window_end__extract_quarter
                  , EXTRACT(month FROM listings_src_26000.active_to) AS listing__window_end__extract_month
                  , EXTRACT(day FROM listings_src_26000.active_to) AS listing__window_end__extract_day
                  , CASE WHEN EXTRACT(dow FROM listings_src_26000.active_to) = 0 THEN EXTRACT(dow FROM listings_src_26000.active_to) + 7 ELSE EXTRACT(dow FROM listings_src_26000.active_to) END AS listing__window_end__extract_dow
                  , EXTRACT(doy FROM listings_src_26000.active_to) AS listing__window_end__extract_doy
                  , listings_src_26000.country AS listing__country
                  , listings_src_26000.is_lux AS listing__is_lux
                  , listings_src_26000.capacity AS listing__capacity
                  , listings_src_26000.listing_id AS listing
                  , listings_src_26000.user_id AS user
                  , listings_src_26000.user_id AS listing__user
                FROM ***************************.dim_listings listings_src_26000
              ) subq_8
              LEFT OUTER JOIN (
                -- Pass Only Elements: [
                --   'home_state_latest',
                --   'user__home_state_latest',
                --   'ds__day',
                --   'ds__week',
                --   'ds__month',
                --   'ds__quarter',
                --   'ds__year',
                --   'ds__extract_year',
                --   'ds__extract_quarter',
                --   'ds__extract_month',
                --   'ds__extract_day',
                --   'ds__extract_dow',
                --   'ds__extract_doy',
                --   'user__ds__day',
                --   'user__ds__week',
                --   'user__ds__month',
                --   'user__ds__quarter',
                --   'user__ds__year',
                --   'user__ds__extract_year',
                --   'user__ds__extract_quarter',
                --   'user__ds__extract_month',
                --   'user__ds__extract_day',
                --   'user__ds__extract_dow',
                --   'user__ds__extract_doy',
                --   'user',
                -- ]
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
                  , subq_9.user__ds__day
                  , subq_9.user__ds__week
                  , subq_9.user__ds__month
                  , subq_9.user__ds__quarter
                  , subq_9.user__ds__year
                  , subq_9.user__ds__extract_year
                  , subq_9.user__ds__extract_quarter
                  , subq_9.user__ds__extract_month
                  , subq_9.user__ds__extract_day
                  , subq_9.user__ds__extract_dow
                  , subq_9.user__ds__extract_doy
                  , subq_9.user
                  , subq_9.home_state_latest
                  , subq_9.user__home_state_latest
                FROM (
                  -- Read Elements From Semantic Model 'users_latest'
                  SELECT
                    DATE_TRUNC('day', users_latest_src_26000.ds) AS ds__day
                    , DATE_TRUNC('week', users_latest_src_26000.ds) AS ds__week
                    , DATE_TRUNC('month', users_latest_src_26000.ds) AS ds__month
                    , DATE_TRUNC('quarter', users_latest_src_26000.ds) AS ds__quarter
                    , DATE_TRUNC('year', users_latest_src_26000.ds) AS ds__year
                    , EXTRACT(year FROM users_latest_src_26000.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM users_latest_src_26000.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM users_latest_src_26000.ds) AS ds__extract_month
                    , EXTRACT(day FROM users_latest_src_26000.ds) AS ds__extract_day
                    , CASE WHEN EXTRACT(dow FROM users_latest_src_26000.ds) = 0 THEN EXTRACT(dow FROM users_latest_src_26000.ds) + 7 ELSE EXTRACT(dow FROM users_latest_src_26000.ds) END AS ds__extract_dow
                    , EXTRACT(doy FROM users_latest_src_26000.ds) AS ds__extract_doy
                    , users_latest_src_26000.home_state_latest
                    , DATE_TRUNC('day', users_latest_src_26000.ds) AS user__ds__day
                    , DATE_TRUNC('week', users_latest_src_26000.ds) AS user__ds__week
                    , DATE_TRUNC('month', users_latest_src_26000.ds) AS user__ds__month
                    , DATE_TRUNC('quarter', users_latest_src_26000.ds) AS user__ds__quarter
                    , DATE_TRUNC('year', users_latest_src_26000.ds) AS user__ds__year
                    , EXTRACT(year FROM users_latest_src_26000.ds) AS user__ds__extract_year
                    , EXTRACT(quarter FROM users_latest_src_26000.ds) AS user__ds__extract_quarter
                    , EXTRACT(month FROM users_latest_src_26000.ds) AS user__ds__extract_month
                    , EXTRACT(day FROM users_latest_src_26000.ds) AS user__ds__extract_day
                    , CASE WHEN EXTRACT(dow FROM users_latest_src_26000.ds) = 0 THEN EXTRACT(dow FROM users_latest_src_26000.ds) + 7 ELSE EXTRACT(dow FROM users_latest_src_26000.ds) END AS user__ds__extract_dow
                    , EXTRACT(doy FROM users_latest_src_26000.ds) AS user__ds__extract_doy
                    , users_latest_src_26000.home_state_latest AS user__home_state_latest
                    , users_latest_src_26000.user_id AS user
                  FROM ***************************.dim_users_latest users_latest_src_26000
                ) subq_9
              ) subq_10
              ON
                subq_8.user = subq_10.user
            ) subq_11
          ) subq_12
          ON
            (
              subq_7.listing = subq_12.listing
            ) AND (
              (
                subq_7.metric_time__day >= subq_12.window_start__day
              ) AND (
                (
                  subq_7.metric_time__day < subq_12.window_end__day
                ) OR (
                  subq_12.window_end__day IS NULL
                )
              )
            )
        ) subq_13
      ) subq_14
    ) subq_15
    GROUP BY
      subq_15.metric_time__day
      , subq_15.listing__user__home_state_latest
  ) subq_16
) subq_17
