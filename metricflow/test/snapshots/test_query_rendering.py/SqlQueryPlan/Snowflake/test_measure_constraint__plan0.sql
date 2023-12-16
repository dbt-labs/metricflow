-- Compute Metrics via Expressions
SELECT
  subq_29.metric_time__day
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_11.metric_time__day, subq_23.metric_time__day, subq_28.metric_time__day) AS metric_time__day
    , MAX(subq_11.average_booking_value) AS average_booking_value
    , MAX(subq_23.bookings) AS bookings
    , MAX(subq_28.booking_value) AS booking_value
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_10.metric_time__day
      , subq_10.average_booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        subq_9.metric_time__day
        , AVG(subq_9.average_booking_value) AS average_booking_value
      FROM (
        -- Pass Only Elements:
        --   ['average_booking_value', 'metric_time__day']
        SELECT
          subq_8.metric_time__day
          , subq_8.average_booking_value
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_7.metric_time__day
            , subq_7.listing__is_lux_latest
            , subq_7.average_booking_value
          FROM (
            -- Pass Only Elements:
            --   ['average_booking_value', 'listing__is_lux_latest', 'metric_time__day']
            SELECT
              subq_6.metric_time__day
              , subq_6.listing__is_lux_latest
              , subq_6.average_booking_value
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_2.metric_time__day AS metric_time__day
                , subq_2.listing AS listing
                , subq_5.is_lux_latest AS listing__is_lux_latest
                , subq_2.average_booking_value AS average_booking_value
              FROM (
                -- Pass Only Elements:
                --   ['average_booking_value', 'metric_time__day', 'listing']
                SELECT
                  subq_1.metric_time__day
                  , subq_1.listing
                  , subq_1.average_booking_value
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
                      , bookings_source_src_10001.booking_value
                      , bookings_source_src_10001.booking_value AS max_booking_value
                      , bookings_source_src_10001.booking_value AS min_booking_value
                      , bookings_source_src_10001.guest_id AS bookers
                      , bookings_source_src_10001.booking_value AS average_booking_value
                      , bookings_source_src_10001.booking_value AS booking_payments
                      , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
                      , bookings_source_src_10001.booking_value AS median_booking_value
                      , bookings_source_src_10001.booking_value AS booking_value_p99
                      , bookings_source_src_10001.booking_value AS discrete_booking_value_p99
                      , bookings_source_src_10001.booking_value AS approximate_continuous_booking_value_p99
                      , bookings_source_src_10001.booking_value AS approximate_discrete_booking_value_p99
                      , bookings_source_src_10001.is_instant
                      , DATE_TRUNC('day', bookings_source_src_10001.ds) AS ds__day
                      , DATE_TRUNC('week', bookings_source_src_10001.ds) AS ds__week
                      , DATE_TRUNC('month', bookings_source_src_10001.ds) AS ds__month
                      , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS ds__quarter
                      , DATE_TRUNC('year', bookings_source_src_10001.ds) AS ds__year
                      , EXTRACT(year FROM bookings_source_src_10001.ds) AS ds__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS ds__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_10001.ds) AS ds__extract_month
                      , EXTRACT(day FROM bookings_source_src_10001.ds) AS ds__extract_day
                      , EXTRACT(dayofweekiso FROM bookings_source_src_10001.ds) AS ds__extract_dow
                      , EXTRACT(doy FROM bookings_source_src_10001.ds) AS ds__extract_doy
                      , DATE_TRUNC('day', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__day
                      , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__week
                      , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__month
                      , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__quarter
                      , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__year
                      , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_month
                      , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_day
                      , EXTRACT(dayofweekiso FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_dow
                      , EXTRACT(doy FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_doy
                      , DATE_TRUNC('day', bookings_source_src_10001.paid_at) AS paid_at__day
                      , DATE_TRUNC('week', bookings_source_src_10001.paid_at) AS paid_at__week
                      , DATE_TRUNC('month', bookings_source_src_10001.paid_at) AS paid_at__month
                      , DATE_TRUNC('quarter', bookings_source_src_10001.paid_at) AS paid_at__quarter
                      , DATE_TRUNC('year', bookings_source_src_10001.paid_at) AS paid_at__year
                      , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS paid_at__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS paid_at__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS paid_at__extract_month
                      , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS paid_at__extract_day
                      , EXTRACT(dayofweekiso FROM bookings_source_src_10001.paid_at) AS paid_at__extract_dow
                      , EXTRACT(doy FROM bookings_source_src_10001.paid_at) AS paid_at__extract_doy
                      , bookings_source_src_10001.is_instant AS booking__is_instant
                      , DATE_TRUNC('day', bookings_source_src_10001.ds) AS booking__ds__day
                      , DATE_TRUNC('week', bookings_source_src_10001.ds) AS booking__ds__week
                      , DATE_TRUNC('month', bookings_source_src_10001.ds) AS booking__ds__month
                      , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS booking__ds__quarter
                      , DATE_TRUNC('year', bookings_source_src_10001.ds) AS booking__ds__year
                      , EXTRACT(year FROM bookings_source_src_10001.ds) AS booking__ds__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS booking__ds__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_10001.ds) AS booking__ds__extract_month
                      , EXTRACT(day FROM bookings_source_src_10001.ds) AS booking__ds__extract_day
                      , EXTRACT(dayofweekiso FROM bookings_source_src_10001.ds) AS booking__ds__extract_dow
                      , EXTRACT(doy FROM bookings_source_src_10001.ds) AS booking__ds__extract_doy
                      , DATE_TRUNC('day', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__day
                      , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__week
                      , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__month
                      , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__quarter
                      , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__year
                      , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_month
                      , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_day
                      , EXTRACT(dayofweekiso FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_dow
                      , EXTRACT(doy FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_doy
                      , DATE_TRUNC('day', bookings_source_src_10001.paid_at) AS booking__paid_at__day
                      , DATE_TRUNC('week', bookings_source_src_10001.paid_at) AS booking__paid_at__week
                      , DATE_TRUNC('month', bookings_source_src_10001.paid_at) AS booking__paid_at__month
                      , DATE_TRUNC('quarter', bookings_source_src_10001.paid_at) AS booking__paid_at__quarter
                      , DATE_TRUNC('year', bookings_source_src_10001.paid_at) AS booking__paid_at__year
                      , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_month
                      , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_day
                      , EXTRACT(dayofweekiso FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_dow
                      , EXTRACT(doy FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_doy
                      , bookings_source_src_10001.listing_id AS listing
                      , bookings_source_src_10001.guest_id AS guest
                      , bookings_source_src_10001.host_id AS host
                      , bookings_source_src_10001.listing_id AS booking__listing
                      , bookings_source_src_10001.guest_id AS booking__guest
                      , bookings_source_src_10001.host_id AS booking__host
                    FROM ***************************.fct_bookings bookings_source_src_10001
                  ) subq_0
                ) subq_1
              ) subq_2
              LEFT OUTER JOIN (
                -- Pass Only Elements:
                --   ['is_lux_latest', 'listing']
                SELECT
                  subq_4.listing
                  , subq_4.is_lux_latest
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_3.ds__day
                    , subq_3.ds__week
                    , subq_3.ds__month
                    , subq_3.ds__quarter
                    , subq_3.ds__year
                    , subq_3.ds__extract_year
                    , subq_3.ds__extract_quarter
                    , subq_3.ds__extract_month
                    , subq_3.ds__extract_day
                    , subq_3.ds__extract_dow
                    , subq_3.ds__extract_doy
                    , subq_3.created_at__day
                    , subq_3.created_at__week
                    , subq_3.created_at__month
                    , subq_3.created_at__quarter
                    , subq_3.created_at__year
                    , subq_3.created_at__extract_year
                    , subq_3.created_at__extract_quarter
                    , subq_3.created_at__extract_month
                    , subq_3.created_at__extract_day
                    , subq_3.created_at__extract_dow
                    , subq_3.created_at__extract_doy
                    , subq_3.listing__ds__day
                    , subq_3.listing__ds__week
                    , subq_3.listing__ds__month
                    , subq_3.listing__ds__quarter
                    , subq_3.listing__ds__year
                    , subq_3.listing__ds__extract_year
                    , subq_3.listing__ds__extract_quarter
                    , subq_3.listing__ds__extract_month
                    , subq_3.listing__ds__extract_day
                    , subq_3.listing__ds__extract_dow
                    , subq_3.listing__ds__extract_doy
                    , subq_3.listing__created_at__day
                    , subq_3.listing__created_at__week
                    , subq_3.listing__created_at__month
                    , subq_3.listing__created_at__quarter
                    , subq_3.listing__created_at__year
                    , subq_3.listing__created_at__extract_year
                    , subq_3.listing__created_at__extract_quarter
                    , subq_3.listing__created_at__extract_month
                    , subq_3.listing__created_at__extract_day
                    , subq_3.listing__created_at__extract_dow
                    , subq_3.listing__created_at__extract_doy
                    , subq_3.ds__day AS metric_time__day
                    , subq_3.ds__week AS metric_time__week
                    , subq_3.ds__month AS metric_time__month
                    , subq_3.ds__quarter AS metric_time__quarter
                    , subq_3.ds__year AS metric_time__year
                    , subq_3.ds__extract_year AS metric_time__extract_year
                    , subq_3.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_3.ds__extract_month AS metric_time__extract_month
                    , subq_3.ds__extract_day AS metric_time__extract_day
                    , subq_3.ds__extract_dow AS metric_time__extract_dow
                    , subq_3.ds__extract_doy AS metric_time__extract_doy
                    , subq_3.listing
                    , subq_3.user
                    , subq_3.listing__user
                    , subq_3.country_latest
                    , subq_3.is_lux_latest
                    , subq_3.capacity_latest
                    , subq_3.listing__country_latest
                    , subq_3.listing__is_lux_latest
                    , subq_3.listing__capacity_latest
                    , subq_3.listings
                    , subq_3.largest_listing
                    , subq_3.smallest_listing
                  FROM (
                    -- Read Elements From Semantic Model 'listings_latest'
                    SELECT
                      1 AS listings
                      , listings_latest_src_10005.capacity AS largest_listing
                      , listings_latest_src_10005.capacity AS smallest_listing
                      , DATE_TRUNC('day', listings_latest_src_10005.created_at) AS ds__day
                      , DATE_TRUNC('week', listings_latest_src_10005.created_at) AS ds__week
                      , DATE_TRUNC('month', listings_latest_src_10005.created_at) AS ds__month
                      , DATE_TRUNC('quarter', listings_latest_src_10005.created_at) AS ds__quarter
                      , DATE_TRUNC('year', listings_latest_src_10005.created_at) AS ds__year
                      , EXTRACT(year FROM listings_latest_src_10005.created_at) AS ds__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_10005.created_at) AS ds__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_10005.created_at) AS ds__extract_month
                      , EXTRACT(day FROM listings_latest_src_10005.created_at) AS ds__extract_day
                      , EXTRACT(dayofweekiso FROM listings_latest_src_10005.created_at) AS ds__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_10005.created_at) AS ds__extract_doy
                      , DATE_TRUNC('day', listings_latest_src_10005.created_at) AS created_at__day
                      , DATE_TRUNC('week', listings_latest_src_10005.created_at) AS created_at__week
                      , DATE_TRUNC('month', listings_latest_src_10005.created_at) AS created_at__month
                      , DATE_TRUNC('quarter', listings_latest_src_10005.created_at) AS created_at__quarter
                      , DATE_TRUNC('year', listings_latest_src_10005.created_at) AS created_at__year
                      , EXTRACT(year FROM listings_latest_src_10005.created_at) AS created_at__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_10005.created_at) AS created_at__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_10005.created_at) AS created_at__extract_month
                      , EXTRACT(day FROM listings_latest_src_10005.created_at) AS created_at__extract_day
                      , EXTRACT(dayofweekiso FROM listings_latest_src_10005.created_at) AS created_at__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_10005.created_at) AS created_at__extract_doy
                      , listings_latest_src_10005.country AS country_latest
                      , listings_latest_src_10005.is_lux AS is_lux_latest
                      , listings_latest_src_10005.capacity AS capacity_latest
                      , DATE_TRUNC('day', listings_latest_src_10005.created_at) AS listing__ds__day
                      , DATE_TRUNC('week', listings_latest_src_10005.created_at) AS listing__ds__week
                      , DATE_TRUNC('month', listings_latest_src_10005.created_at) AS listing__ds__month
                      , DATE_TRUNC('quarter', listings_latest_src_10005.created_at) AS listing__ds__quarter
                      , DATE_TRUNC('year', listings_latest_src_10005.created_at) AS listing__ds__year
                      , EXTRACT(year FROM listings_latest_src_10005.created_at) AS listing__ds__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_10005.created_at) AS listing__ds__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_10005.created_at) AS listing__ds__extract_month
                      , EXTRACT(day FROM listings_latest_src_10005.created_at) AS listing__ds__extract_day
                      , EXTRACT(dayofweekiso FROM listings_latest_src_10005.created_at) AS listing__ds__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_10005.created_at) AS listing__ds__extract_doy
                      , DATE_TRUNC('day', listings_latest_src_10005.created_at) AS listing__created_at__day
                      , DATE_TRUNC('week', listings_latest_src_10005.created_at) AS listing__created_at__week
                      , DATE_TRUNC('month', listings_latest_src_10005.created_at) AS listing__created_at__month
                      , DATE_TRUNC('quarter', listings_latest_src_10005.created_at) AS listing__created_at__quarter
                      , DATE_TRUNC('year', listings_latest_src_10005.created_at) AS listing__created_at__year
                      , EXTRACT(year FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_month
                      , EXTRACT(day FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_day
                      , EXTRACT(dayofweekiso FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_doy
                      , listings_latest_src_10005.country AS listing__country_latest
                      , listings_latest_src_10005.is_lux AS listing__is_lux_latest
                      , listings_latest_src_10005.capacity AS listing__capacity_latest
                      , listings_latest_src_10005.listing_id AS listing
                      , listings_latest_src_10005.user_id AS user
                      , listings_latest_src_10005.user_id AS listing__user
                    FROM ***************************.dim_listings_latest listings_latest_src_10005
                  ) subq_3
                ) subq_4
              ) subq_5
              ON
                subq_2.listing = subq_5.listing
            ) subq_6
          ) subq_7
          WHERE listing__is_lux_latest
        ) subq_8
      ) subq_9
      GROUP BY
        subq_9.metric_time__day
    ) subq_10
  ) subq_11
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_22.metric_time__day
      , subq_22.bookings
    FROM (
      -- Aggregate Measures
      SELECT
        subq_21.metric_time__day
        , SUM(subq_21.bookings) AS bookings
      FROM (
        -- Pass Only Elements:
        --   ['bookings', 'metric_time__day']
        SELECT
          subq_20.metric_time__day
          , subq_20.bookings
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_19.metric_time__day
            , subq_19.listing__is_lux_latest
            , subq_19.bookings
          FROM (
            -- Pass Only Elements:
            --   ['bookings', 'listing__is_lux_latest', 'metric_time__day']
            SELECT
              subq_18.metric_time__day
              , subq_18.listing__is_lux_latest
              , subq_18.bookings
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_14.metric_time__day AS metric_time__day
                , subq_14.listing AS listing
                , subq_17.is_lux_latest AS listing__is_lux_latest
                , subq_14.bookings AS bookings
              FROM (
                -- Pass Only Elements:
                --   ['bookings', 'metric_time__day', 'listing']
                SELECT
                  subq_13.metric_time__day
                  , subq_13.listing
                  , subq_13.bookings
                FROM (
                  -- Metric Time Dimension 'ds'
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
                    , subq_12.ds_partitioned__day
                    , subq_12.ds_partitioned__week
                    , subq_12.ds_partitioned__month
                    , subq_12.ds_partitioned__quarter
                    , subq_12.ds_partitioned__year
                    , subq_12.ds_partitioned__extract_year
                    , subq_12.ds_partitioned__extract_quarter
                    , subq_12.ds_partitioned__extract_month
                    , subq_12.ds_partitioned__extract_day
                    , subq_12.ds_partitioned__extract_dow
                    , subq_12.ds_partitioned__extract_doy
                    , subq_12.paid_at__day
                    , subq_12.paid_at__week
                    , subq_12.paid_at__month
                    , subq_12.paid_at__quarter
                    , subq_12.paid_at__year
                    , subq_12.paid_at__extract_year
                    , subq_12.paid_at__extract_quarter
                    , subq_12.paid_at__extract_month
                    , subq_12.paid_at__extract_day
                    , subq_12.paid_at__extract_dow
                    , subq_12.paid_at__extract_doy
                    , subq_12.booking__ds__day
                    , subq_12.booking__ds__week
                    , subq_12.booking__ds__month
                    , subq_12.booking__ds__quarter
                    , subq_12.booking__ds__year
                    , subq_12.booking__ds__extract_year
                    , subq_12.booking__ds__extract_quarter
                    , subq_12.booking__ds__extract_month
                    , subq_12.booking__ds__extract_day
                    , subq_12.booking__ds__extract_dow
                    , subq_12.booking__ds__extract_doy
                    , subq_12.booking__ds_partitioned__day
                    , subq_12.booking__ds_partitioned__week
                    , subq_12.booking__ds_partitioned__month
                    , subq_12.booking__ds_partitioned__quarter
                    , subq_12.booking__ds_partitioned__year
                    , subq_12.booking__ds_partitioned__extract_year
                    , subq_12.booking__ds_partitioned__extract_quarter
                    , subq_12.booking__ds_partitioned__extract_month
                    , subq_12.booking__ds_partitioned__extract_day
                    , subq_12.booking__ds_partitioned__extract_dow
                    , subq_12.booking__ds_partitioned__extract_doy
                    , subq_12.booking__paid_at__day
                    , subq_12.booking__paid_at__week
                    , subq_12.booking__paid_at__month
                    , subq_12.booking__paid_at__quarter
                    , subq_12.booking__paid_at__year
                    , subq_12.booking__paid_at__extract_year
                    , subq_12.booking__paid_at__extract_quarter
                    , subq_12.booking__paid_at__extract_month
                    , subq_12.booking__paid_at__extract_day
                    , subq_12.booking__paid_at__extract_dow
                    , subq_12.booking__paid_at__extract_doy
                    , subq_12.ds__day AS metric_time__day
                    , subq_12.ds__week AS metric_time__week
                    , subq_12.ds__month AS metric_time__month
                    , subq_12.ds__quarter AS metric_time__quarter
                    , subq_12.ds__year AS metric_time__year
                    , subq_12.ds__extract_year AS metric_time__extract_year
                    , subq_12.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_12.ds__extract_month AS metric_time__extract_month
                    , subq_12.ds__extract_day AS metric_time__extract_day
                    , subq_12.ds__extract_dow AS metric_time__extract_dow
                    , subq_12.ds__extract_doy AS metric_time__extract_doy
                    , subq_12.listing
                    , subq_12.guest
                    , subq_12.host
                    , subq_12.booking__listing
                    , subq_12.booking__guest
                    , subq_12.booking__host
                    , subq_12.is_instant
                    , subq_12.booking__is_instant
                    , subq_12.bookings
                    , subq_12.instant_bookings
                    , subq_12.booking_value
                    , subq_12.max_booking_value
                    , subq_12.min_booking_value
                    , subq_12.bookers
                    , subq_12.average_booking_value
                    , subq_12.referred_bookings
                    , subq_12.median_booking_value
                    , subq_12.booking_value_p99
                    , subq_12.discrete_booking_value_p99
                    , subq_12.approximate_continuous_booking_value_p99
                    , subq_12.approximate_discrete_booking_value_p99
                  FROM (
                    -- Read Elements From Semantic Model 'bookings_source'
                    SELECT
                      1 AS bookings
                      , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
                      , bookings_source_src_10001.booking_value
                      , bookings_source_src_10001.booking_value AS max_booking_value
                      , bookings_source_src_10001.booking_value AS min_booking_value
                      , bookings_source_src_10001.guest_id AS bookers
                      , bookings_source_src_10001.booking_value AS average_booking_value
                      , bookings_source_src_10001.booking_value AS booking_payments
                      , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
                      , bookings_source_src_10001.booking_value AS median_booking_value
                      , bookings_source_src_10001.booking_value AS booking_value_p99
                      , bookings_source_src_10001.booking_value AS discrete_booking_value_p99
                      , bookings_source_src_10001.booking_value AS approximate_continuous_booking_value_p99
                      , bookings_source_src_10001.booking_value AS approximate_discrete_booking_value_p99
                      , bookings_source_src_10001.is_instant
                      , DATE_TRUNC('day', bookings_source_src_10001.ds) AS ds__day
                      , DATE_TRUNC('week', bookings_source_src_10001.ds) AS ds__week
                      , DATE_TRUNC('month', bookings_source_src_10001.ds) AS ds__month
                      , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS ds__quarter
                      , DATE_TRUNC('year', bookings_source_src_10001.ds) AS ds__year
                      , EXTRACT(year FROM bookings_source_src_10001.ds) AS ds__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS ds__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_10001.ds) AS ds__extract_month
                      , EXTRACT(day FROM bookings_source_src_10001.ds) AS ds__extract_day
                      , EXTRACT(dayofweekiso FROM bookings_source_src_10001.ds) AS ds__extract_dow
                      , EXTRACT(doy FROM bookings_source_src_10001.ds) AS ds__extract_doy
                      , DATE_TRUNC('day', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__day
                      , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__week
                      , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__month
                      , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__quarter
                      , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__year
                      , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_month
                      , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_day
                      , EXTRACT(dayofweekiso FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_dow
                      , EXTRACT(doy FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_doy
                      , DATE_TRUNC('day', bookings_source_src_10001.paid_at) AS paid_at__day
                      , DATE_TRUNC('week', bookings_source_src_10001.paid_at) AS paid_at__week
                      , DATE_TRUNC('month', bookings_source_src_10001.paid_at) AS paid_at__month
                      , DATE_TRUNC('quarter', bookings_source_src_10001.paid_at) AS paid_at__quarter
                      , DATE_TRUNC('year', bookings_source_src_10001.paid_at) AS paid_at__year
                      , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS paid_at__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS paid_at__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS paid_at__extract_month
                      , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS paid_at__extract_day
                      , EXTRACT(dayofweekiso FROM bookings_source_src_10001.paid_at) AS paid_at__extract_dow
                      , EXTRACT(doy FROM bookings_source_src_10001.paid_at) AS paid_at__extract_doy
                      , bookings_source_src_10001.is_instant AS booking__is_instant
                      , DATE_TRUNC('day', bookings_source_src_10001.ds) AS booking__ds__day
                      , DATE_TRUNC('week', bookings_source_src_10001.ds) AS booking__ds__week
                      , DATE_TRUNC('month', bookings_source_src_10001.ds) AS booking__ds__month
                      , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS booking__ds__quarter
                      , DATE_TRUNC('year', bookings_source_src_10001.ds) AS booking__ds__year
                      , EXTRACT(year FROM bookings_source_src_10001.ds) AS booking__ds__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS booking__ds__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_10001.ds) AS booking__ds__extract_month
                      , EXTRACT(day FROM bookings_source_src_10001.ds) AS booking__ds__extract_day
                      , EXTRACT(dayofweekiso FROM bookings_source_src_10001.ds) AS booking__ds__extract_dow
                      , EXTRACT(doy FROM bookings_source_src_10001.ds) AS booking__ds__extract_doy
                      , DATE_TRUNC('day', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__day
                      , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__week
                      , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__month
                      , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__quarter
                      , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__year
                      , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_month
                      , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_day
                      , EXTRACT(dayofweekiso FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_dow
                      , EXTRACT(doy FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_doy
                      , DATE_TRUNC('day', bookings_source_src_10001.paid_at) AS booking__paid_at__day
                      , DATE_TRUNC('week', bookings_source_src_10001.paid_at) AS booking__paid_at__week
                      , DATE_TRUNC('month', bookings_source_src_10001.paid_at) AS booking__paid_at__month
                      , DATE_TRUNC('quarter', bookings_source_src_10001.paid_at) AS booking__paid_at__quarter
                      , DATE_TRUNC('year', bookings_source_src_10001.paid_at) AS booking__paid_at__year
                      , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_month
                      , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_day
                      , EXTRACT(dayofweekiso FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_dow
                      , EXTRACT(doy FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_doy
                      , bookings_source_src_10001.listing_id AS listing
                      , bookings_source_src_10001.guest_id AS guest
                      , bookings_source_src_10001.host_id AS host
                      , bookings_source_src_10001.listing_id AS booking__listing
                      , bookings_source_src_10001.guest_id AS booking__guest
                      , bookings_source_src_10001.host_id AS booking__host
                    FROM ***************************.fct_bookings bookings_source_src_10001
                  ) subq_12
                ) subq_13
              ) subq_14
              LEFT OUTER JOIN (
                -- Pass Only Elements:
                --   ['is_lux_latest', 'listing']
                SELECT
                  subq_16.listing
                  , subq_16.is_lux_latest
                FROM (
                  -- Metric Time Dimension 'ds'
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
                    , subq_15.ds__day AS metric_time__day
                    , subq_15.ds__week AS metric_time__week
                    , subq_15.ds__month AS metric_time__month
                    , subq_15.ds__quarter AS metric_time__quarter
                    , subq_15.ds__year AS metric_time__year
                    , subq_15.ds__extract_year AS metric_time__extract_year
                    , subq_15.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_15.ds__extract_month AS metric_time__extract_month
                    , subq_15.ds__extract_day AS metric_time__extract_day
                    , subq_15.ds__extract_dow AS metric_time__extract_dow
                    , subq_15.ds__extract_doy AS metric_time__extract_doy
                    , subq_15.listing
                    , subq_15.user
                    , subq_15.listing__user
                    , subq_15.country_latest
                    , subq_15.is_lux_latest
                    , subq_15.capacity_latest
                    , subq_15.listing__country_latest
                    , subq_15.listing__is_lux_latest
                    , subq_15.listing__capacity_latest
                    , subq_15.listings
                    , subq_15.largest_listing
                    , subq_15.smallest_listing
                  FROM (
                    -- Read Elements From Semantic Model 'listings_latest'
                    SELECT
                      1 AS listings
                      , listings_latest_src_10005.capacity AS largest_listing
                      , listings_latest_src_10005.capacity AS smallest_listing
                      , DATE_TRUNC('day', listings_latest_src_10005.created_at) AS ds__day
                      , DATE_TRUNC('week', listings_latest_src_10005.created_at) AS ds__week
                      , DATE_TRUNC('month', listings_latest_src_10005.created_at) AS ds__month
                      , DATE_TRUNC('quarter', listings_latest_src_10005.created_at) AS ds__quarter
                      , DATE_TRUNC('year', listings_latest_src_10005.created_at) AS ds__year
                      , EXTRACT(year FROM listings_latest_src_10005.created_at) AS ds__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_10005.created_at) AS ds__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_10005.created_at) AS ds__extract_month
                      , EXTRACT(day FROM listings_latest_src_10005.created_at) AS ds__extract_day
                      , EXTRACT(dayofweekiso FROM listings_latest_src_10005.created_at) AS ds__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_10005.created_at) AS ds__extract_doy
                      , DATE_TRUNC('day', listings_latest_src_10005.created_at) AS created_at__day
                      , DATE_TRUNC('week', listings_latest_src_10005.created_at) AS created_at__week
                      , DATE_TRUNC('month', listings_latest_src_10005.created_at) AS created_at__month
                      , DATE_TRUNC('quarter', listings_latest_src_10005.created_at) AS created_at__quarter
                      , DATE_TRUNC('year', listings_latest_src_10005.created_at) AS created_at__year
                      , EXTRACT(year FROM listings_latest_src_10005.created_at) AS created_at__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_10005.created_at) AS created_at__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_10005.created_at) AS created_at__extract_month
                      , EXTRACT(day FROM listings_latest_src_10005.created_at) AS created_at__extract_day
                      , EXTRACT(dayofweekiso FROM listings_latest_src_10005.created_at) AS created_at__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_10005.created_at) AS created_at__extract_doy
                      , listings_latest_src_10005.country AS country_latest
                      , listings_latest_src_10005.is_lux AS is_lux_latest
                      , listings_latest_src_10005.capacity AS capacity_latest
                      , DATE_TRUNC('day', listings_latest_src_10005.created_at) AS listing__ds__day
                      , DATE_TRUNC('week', listings_latest_src_10005.created_at) AS listing__ds__week
                      , DATE_TRUNC('month', listings_latest_src_10005.created_at) AS listing__ds__month
                      , DATE_TRUNC('quarter', listings_latest_src_10005.created_at) AS listing__ds__quarter
                      , DATE_TRUNC('year', listings_latest_src_10005.created_at) AS listing__ds__year
                      , EXTRACT(year FROM listings_latest_src_10005.created_at) AS listing__ds__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_10005.created_at) AS listing__ds__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_10005.created_at) AS listing__ds__extract_month
                      , EXTRACT(day FROM listings_latest_src_10005.created_at) AS listing__ds__extract_day
                      , EXTRACT(dayofweekiso FROM listings_latest_src_10005.created_at) AS listing__ds__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_10005.created_at) AS listing__ds__extract_doy
                      , DATE_TRUNC('day', listings_latest_src_10005.created_at) AS listing__created_at__day
                      , DATE_TRUNC('week', listings_latest_src_10005.created_at) AS listing__created_at__week
                      , DATE_TRUNC('month', listings_latest_src_10005.created_at) AS listing__created_at__month
                      , DATE_TRUNC('quarter', listings_latest_src_10005.created_at) AS listing__created_at__quarter
                      , DATE_TRUNC('year', listings_latest_src_10005.created_at) AS listing__created_at__year
                      , EXTRACT(year FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_year
                      , EXTRACT(quarter FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_quarter
                      , EXTRACT(month FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_month
                      , EXTRACT(day FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_day
                      , EXTRACT(dayofweekiso FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_10005.created_at) AS listing__created_at__extract_doy
                      , listings_latest_src_10005.country AS listing__country_latest
                      , listings_latest_src_10005.is_lux AS listing__is_lux_latest
                      , listings_latest_src_10005.capacity AS listing__capacity_latest
                      , listings_latest_src_10005.listing_id AS listing
                      , listings_latest_src_10005.user_id AS user
                      , listings_latest_src_10005.user_id AS listing__user
                    FROM ***************************.dim_listings_latest listings_latest_src_10005
                  ) subq_15
                ) subq_16
              ) subq_17
              ON
                subq_14.listing = subq_17.listing
            ) subq_18
          ) subq_19
          WHERE listing__is_lux_latest
        ) subq_20
      ) subq_21
      GROUP BY
        subq_21.metric_time__day
    ) subq_22
  ) subq_23
  ON
    subq_11.metric_time__day = subq_23.metric_time__day
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_27.metric_time__day
      , subq_27.booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        subq_26.metric_time__day
        , SUM(subq_26.booking_value) AS booking_value
      FROM (
        -- Pass Only Elements:
        --   ['booking_value', 'metric_time__day']
        SELECT
          subq_25.metric_time__day
          , subq_25.booking_value
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_24.ds__day
            , subq_24.ds__week
            , subq_24.ds__month
            , subq_24.ds__quarter
            , subq_24.ds__year
            , subq_24.ds__extract_year
            , subq_24.ds__extract_quarter
            , subq_24.ds__extract_month
            , subq_24.ds__extract_day
            , subq_24.ds__extract_dow
            , subq_24.ds__extract_doy
            , subq_24.ds_partitioned__day
            , subq_24.ds_partitioned__week
            , subq_24.ds_partitioned__month
            , subq_24.ds_partitioned__quarter
            , subq_24.ds_partitioned__year
            , subq_24.ds_partitioned__extract_year
            , subq_24.ds_partitioned__extract_quarter
            , subq_24.ds_partitioned__extract_month
            , subq_24.ds_partitioned__extract_day
            , subq_24.ds_partitioned__extract_dow
            , subq_24.ds_partitioned__extract_doy
            , subq_24.paid_at__day
            , subq_24.paid_at__week
            , subq_24.paid_at__month
            , subq_24.paid_at__quarter
            , subq_24.paid_at__year
            , subq_24.paid_at__extract_year
            , subq_24.paid_at__extract_quarter
            , subq_24.paid_at__extract_month
            , subq_24.paid_at__extract_day
            , subq_24.paid_at__extract_dow
            , subq_24.paid_at__extract_doy
            , subq_24.booking__ds__day
            , subq_24.booking__ds__week
            , subq_24.booking__ds__month
            , subq_24.booking__ds__quarter
            , subq_24.booking__ds__year
            , subq_24.booking__ds__extract_year
            , subq_24.booking__ds__extract_quarter
            , subq_24.booking__ds__extract_month
            , subq_24.booking__ds__extract_day
            , subq_24.booking__ds__extract_dow
            , subq_24.booking__ds__extract_doy
            , subq_24.booking__ds_partitioned__day
            , subq_24.booking__ds_partitioned__week
            , subq_24.booking__ds_partitioned__month
            , subq_24.booking__ds_partitioned__quarter
            , subq_24.booking__ds_partitioned__year
            , subq_24.booking__ds_partitioned__extract_year
            , subq_24.booking__ds_partitioned__extract_quarter
            , subq_24.booking__ds_partitioned__extract_month
            , subq_24.booking__ds_partitioned__extract_day
            , subq_24.booking__ds_partitioned__extract_dow
            , subq_24.booking__ds_partitioned__extract_doy
            , subq_24.booking__paid_at__day
            , subq_24.booking__paid_at__week
            , subq_24.booking__paid_at__month
            , subq_24.booking__paid_at__quarter
            , subq_24.booking__paid_at__year
            , subq_24.booking__paid_at__extract_year
            , subq_24.booking__paid_at__extract_quarter
            , subq_24.booking__paid_at__extract_month
            , subq_24.booking__paid_at__extract_day
            , subq_24.booking__paid_at__extract_dow
            , subq_24.booking__paid_at__extract_doy
            , subq_24.ds__day AS metric_time__day
            , subq_24.ds__week AS metric_time__week
            , subq_24.ds__month AS metric_time__month
            , subq_24.ds__quarter AS metric_time__quarter
            , subq_24.ds__year AS metric_time__year
            , subq_24.ds__extract_year AS metric_time__extract_year
            , subq_24.ds__extract_quarter AS metric_time__extract_quarter
            , subq_24.ds__extract_month AS metric_time__extract_month
            , subq_24.ds__extract_day AS metric_time__extract_day
            , subq_24.ds__extract_dow AS metric_time__extract_dow
            , subq_24.ds__extract_doy AS metric_time__extract_doy
            , subq_24.listing
            , subq_24.guest
            , subq_24.host
            , subq_24.booking__listing
            , subq_24.booking__guest
            , subq_24.booking__host
            , subq_24.is_instant
            , subq_24.booking__is_instant
            , subq_24.bookings
            , subq_24.instant_bookings
            , subq_24.booking_value
            , subq_24.max_booking_value
            , subq_24.min_booking_value
            , subq_24.bookers
            , subq_24.average_booking_value
            , subq_24.referred_bookings
            , subq_24.median_booking_value
            , subq_24.booking_value_p99
            , subq_24.discrete_booking_value_p99
            , subq_24.approximate_continuous_booking_value_p99
            , subq_24.approximate_discrete_booking_value_p99
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
            SELECT
              1 AS bookings
              , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
              , bookings_source_src_10001.booking_value
              , bookings_source_src_10001.booking_value AS max_booking_value
              , bookings_source_src_10001.booking_value AS min_booking_value
              , bookings_source_src_10001.guest_id AS bookers
              , bookings_source_src_10001.booking_value AS average_booking_value
              , bookings_source_src_10001.booking_value AS booking_payments
              , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
              , bookings_source_src_10001.booking_value AS median_booking_value
              , bookings_source_src_10001.booking_value AS booking_value_p99
              , bookings_source_src_10001.booking_value AS discrete_booking_value_p99
              , bookings_source_src_10001.booking_value AS approximate_continuous_booking_value_p99
              , bookings_source_src_10001.booking_value AS approximate_discrete_booking_value_p99
              , bookings_source_src_10001.is_instant
              , DATE_TRUNC('day', bookings_source_src_10001.ds) AS ds__day
              , DATE_TRUNC('week', bookings_source_src_10001.ds) AS ds__week
              , DATE_TRUNC('month', bookings_source_src_10001.ds) AS ds__month
              , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS ds__quarter
              , DATE_TRUNC('year', bookings_source_src_10001.ds) AS ds__year
              , EXTRACT(year FROM bookings_source_src_10001.ds) AS ds__extract_year
              , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS ds__extract_quarter
              , EXTRACT(month FROM bookings_source_src_10001.ds) AS ds__extract_month
              , EXTRACT(day FROM bookings_source_src_10001.ds) AS ds__extract_day
              , EXTRACT(dayofweekiso FROM bookings_source_src_10001.ds) AS ds__extract_dow
              , EXTRACT(doy FROM bookings_source_src_10001.ds) AS ds__extract_doy
              , DATE_TRUNC('day', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__day
              , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__week
              , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__month
              , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__quarter
              , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__year
              , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_year
              , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_quarter
              , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_month
              , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_day
              , EXTRACT(dayofweekiso FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_dow
              , EXTRACT(doy FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_doy
              , DATE_TRUNC('day', bookings_source_src_10001.paid_at) AS paid_at__day
              , DATE_TRUNC('week', bookings_source_src_10001.paid_at) AS paid_at__week
              , DATE_TRUNC('month', bookings_source_src_10001.paid_at) AS paid_at__month
              , DATE_TRUNC('quarter', bookings_source_src_10001.paid_at) AS paid_at__quarter
              , DATE_TRUNC('year', bookings_source_src_10001.paid_at) AS paid_at__year
              , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS paid_at__extract_year
              , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS paid_at__extract_quarter
              , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS paid_at__extract_month
              , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS paid_at__extract_day
              , EXTRACT(dayofweekiso FROM bookings_source_src_10001.paid_at) AS paid_at__extract_dow
              , EXTRACT(doy FROM bookings_source_src_10001.paid_at) AS paid_at__extract_doy
              , bookings_source_src_10001.is_instant AS booking__is_instant
              , DATE_TRUNC('day', bookings_source_src_10001.ds) AS booking__ds__day
              , DATE_TRUNC('week', bookings_source_src_10001.ds) AS booking__ds__week
              , DATE_TRUNC('month', bookings_source_src_10001.ds) AS booking__ds__month
              , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS booking__ds__quarter
              , DATE_TRUNC('year', bookings_source_src_10001.ds) AS booking__ds__year
              , EXTRACT(year FROM bookings_source_src_10001.ds) AS booking__ds__extract_year
              , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS booking__ds__extract_quarter
              , EXTRACT(month FROM bookings_source_src_10001.ds) AS booking__ds__extract_month
              , EXTRACT(day FROM bookings_source_src_10001.ds) AS booking__ds__extract_day
              , EXTRACT(dayofweekiso FROM bookings_source_src_10001.ds) AS booking__ds__extract_dow
              , EXTRACT(doy FROM bookings_source_src_10001.ds) AS booking__ds__extract_doy
              , DATE_TRUNC('day', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__day
              , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__week
              , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__month
              , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__quarter
              , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__year
              , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_year
              , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_quarter
              , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_month
              , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_day
              , EXTRACT(dayofweekiso FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_dow
              , EXTRACT(doy FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_doy
              , DATE_TRUNC('day', bookings_source_src_10001.paid_at) AS booking__paid_at__day
              , DATE_TRUNC('week', bookings_source_src_10001.paid_at) AS booking__paid_at__week
              , DATE_TRUNC('month', bookings_source_src_10001.paid_at) AS booking__paid_at__month
              , DATE_TRUNC('quarter', bookings_source_src_10001.paid_at) AS booking__paid_at__quarter
              , DATE_TRUNC('year', bookings_source_src_10001.paid_at) AS booking__paid_at__year
              , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_year
              , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_quarter
              , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_month
              , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_day
              , EXTRACT(dayofweekiso FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_dow
              , EXTRACT(doy FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_doy
              , bookings_source_src_10001.listing_id AS listing
              , bookings_source_src_10001.guest_id AS guest
              , bookings_source_src_10001.host_id AS host
              , bookings_source_src_10001.listing_id AS booking__listing
              , bookings_source_src_10001.guest_id AS booking__guest
              , bookings_source_src_10001.host_id AS booking__host
            FROM ***************************.fct_bookings bookings_source_src_10001
          ) subq_24
        ) subq_25
      ) subq_26
      GROUP BY
        subq_26.metric_time__day
    ) subq_27
  ) subq_28
  ON
    COALESCE(subq_11.metric_time__day, subq_23.metric_time__day) = subq_28.metric_time__day
  GROUP BY
    COALESCE(subq_11.metric_time__day, subq_23.metric_time__day, subq_28.metric_time__day)
) subq_29
