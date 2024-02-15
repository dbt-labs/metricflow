-- Compute Metrics via Expressions
SELECT
  subq_21.metric_time__day
  , subq_21.listing__user__home_state_latest
  , subq_21.bookings
FROM (
  -- Aggregate Measures
  SELECT
    subq_20.metric_time__day
    , subq_20.listing__user__home_state_latest
    , SUM(subq_20.bookings) AS bookings
  FROM (
    -- Pass Only Elements: ['bookings', 'listing__user__home_state_latest', 'metric_time__day']
    SELECT
      subq_19.metric_time__day
      , subq_19.listing__user__home_state_latest
      , subq_19.bookings
    FROM (
      -- Join Standard Outputs
      SELECT
        subq_13.metric_time__day AS metric_time__day
        , subq_18.window_start__day AS listing__window_start__day
        , subq_18.window_end__day AS listing__window_end__day
        , subq_13.listing AS listing
        , subq_18.user__home_state_latest AS listing__user__home_state_latest
        , subq_13.bookings AS bookings
      FROM (
        -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
        SELECT
          subq_12.metric_time__day
          , subq_12.listing
          , subq_12.bookings
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
            , subq_11.guest
            , subq_11.host
            , subq_11.user
            , subq_11.booking__listing
            , subq_11.booking__guest
            , subq_11.booking__host
            , subq_11.booking__user
            , subq_11.is_instant
            , subq_11.booking__is_instant
            , subq_11.bookings
            , subq_11.instant_bookings
            , subq_11.booking_value
            , subq_11.bookers
            , subq_11.average_booking_value
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
            SELECT
              1 AS bookings
              , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
              , bookings_source_src_26000.booking_value
              , bookings_source_src_26000.guest_id AS bookers
              , bookings_source_src_26000.booking_value AS average_booking_value
              , bookings_source_src_26000.booking_value AS booking_payments
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
          ) subq_11
        ) subq_12
      ) subq_13
      LEFT OUTER JOIN (
        -- Pass Only Elements: ['user__home_state_latest', 'window_start__day', 'window_end__day', 'listing']
        SELECT
          subq_17.window_start__day
          , subq_17.window_end__day
          , subq_17.listing
          , subq_17.user__home_state_latest
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_14.window_start__day AS window_start__day
            , subq_14.window_start__week AS window_start__week
            , subq_14.window_start__month AS window_start__month
            , subq_14.window_start__quarter AS window_start__quarter
            , subq_14.window_start__year AS window_start__year
            , subq_14.window_start__extract_year AS window_start__extract_year
            , subq_14.window_start__extract_quarter AS window_start__extract_quarter
            , subq_14.window_start__extract_month AS window_start__extract_month
            , subq_14.window_start__extract_day AS window_start__extract_day
            , subq_14.window_start__extract_dow AS window_start__extract_dow
            , subq_14.window_start__extract_doy AS window_start__extract_doy
            , subq_14.window_end__day AS window_end__day
            , subq_14.window_end__week AS window_end__week
            , subq_14.window_end__month AS window_end__month
            , subq_14.window_end__quarter AS window_end__quarter
            , subq_14.window_end__year AS window_end__year
            , subq_14.window_end__extract_year AS window_end__extract_year
            , subq_14.window_end__extract_quarter AS window_end__extract_quarter
            , subq_14.window_end__extract_month AS window_end__extract_month
            , subq_14.window_end__extract_day AS window_end__extract_day
            , subq_14.window_end__extract_dow AS window_end__extract_dow
            , subq_14.window_end__extract_doy AS window_end__extract_doy
            , subq_14.listing__window_start__day AS listing__window_start__day
            , subq_14.listing__window_start__week AS listing__window_start__week
            , subq_14.listing__window_start__month AS listing__window_start__month
            , subq_14.listing__window_start__quarter AS listing__window_start__quarter
            , subq_14.listing__window_start__year AS listing__window_start__year
            , subq_14.listing__window_start__extract_year AS listing__window_start__extract_year
            , subq_14.listing__window_start__extract_quarter AS listing__window_start__extract_quarter
            , subq_14.listing__window_start__extract_month AS listing__window_start__extract_month
            , subq_14.listing__window_start__extract_day AS listing__window_start__extract_day
            , subq_14.listing__window_start__extract_dow AS listing__window_start__extract_dow
            , subq_14.listing__window_start__extract_doy AS listing__window_start__extract_doy
            , subq_14.listing__window_end__day AS listing__window_end__day
            , subq_14.listing__window_end__week AS listing__window_end__week
            , subq_14.listing__window_end__month AS listing__window_end__month
            , subq_14.listing__window_end__quarter AS listing__window_end__quarter
            , subq_14.listing__window_end__year AS listing__window_end__year
            , subq_14.listing__window_end__extract_year AS listing__window_end__extract_year
            , subq_14.listing__window_end__extract_quarter AS listing__window_end__extract_quarter
            , subq_14.listing__window_end__extract_month AS listing__window_end__extract_month
            , subq_14.listing__window_end__extract_day AS listing__window_end__extract_day
            , subq_14.listing__window_end__extract_dow AS listing__window_end__extract_dow
            , subq_14.listing__window_end__extract_doy AS listing__window_end__extract_doy
            , subq_16.ds__day AS user__ds__day
            , subq_16.ds__week AS user__ds__week
            , subq_16.ds__month AS user__ds__month
            , subq_16.ds__quarter AS user__ds__quarter
            , subq_16.ds__year AS user__ds__year
            , subq_16.ds__extract_year AS user__ds__extract_year
            , subq_16.ds__extract_quarter AS user__ds__extract_quarter
            , subq_16.ds__extract_month AS user__ds__extract_month
            , subq_16.ds__extract_day AS user__ds__extract_day
            , subq_16.ds__extract_dow AS user__ds__extract_dow
            , subq_16.ds__extract_doy AS user__ds__extract_doy
            , subq_14.listing AS listing
            , subq_14.user AS user
            , subq_14.listing__user AS listing__user
            , subq_14.country AS country
            , subq_14.is_lux AS is_lux
            , subq_14.capacity AS capacity
            , subq_14.listing__country AS listing__country
            , subq_14.listing__is_lux AS listing__is_lux
            , subq_14.listing__capacity AS listing__capacity
            , subq_16.home_state_latest AS user__home_state_latest
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
          ) subq_14
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
              , subq_15.user__ds__day
              , subq_15.user__ds__week
              , subq_15.user__ds__month
              , subq_15.user__ds__quarter
              , subq_15.user__ds__year
              , subq_15.user__ds__extract_year
              , subq_15.user__ds__extract_quarter
              , subq_15.user__ds__extract_month
              , subq_15.user__ds__extract_day
              , subq_15.user__ds__extract_dow
              , subq_15.user__ds__extract_doy
              , subq_15.user
              , subq_15.home_state_latest
              , subq_15.user__home_state_latest
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
            ) subq_15
          ) subq_16
          ON
            subq_14.user = subq_16.user
        ) subq_17
      ) subq_18
      ON
        (
          subq_13.listing = subq_18.listing
        ) AND (
          (
            subq_13.metric_time__day >= subq_18.window_start__day
          ) AND (
            (
              subq_13.metric_time__day < subq_18.window_end__day
            ) OR (
              subq_18.window_end__day IS NULL
            )
          )
        )
    ) subq_19
  ) subq_20
  GROUP BY
    subq_20.metric_time__day
    , subq_20.listing__user__home_state_latest
) subq_21
