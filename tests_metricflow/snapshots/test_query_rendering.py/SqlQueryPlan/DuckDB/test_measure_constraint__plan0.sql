-- Compute Metrics via Expressions
SELECT
  subq_20.metric_time__day
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_7.metric_time__day, subq_15.metric_time__day, subq_19.metric_time__day) AS metric_time__day
    , MAX(subq_7.average_booking_value) AS average_booking_value
    , MAX(subq_15.bookings) AS bookings
    , MAX(subq_19.booking_value) AS booking_value
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_6.metric_time__day
      , subq_6.average_booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        subq_5.metric_time__day
        , AVG(subq_5.average_booking_value) AS average_booking_value
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['average_booking_value', 'metric_time__day']
        SELECT
          subq_4.metric_time__day
          , subq_4.average_booking_value
        FROM (
          -- Join Standard Outputs
          -- Pass Only Elements: ['average_booking_value', 'listing__is_lux_latest', 'metric_time__day']
          SELECT
            subq_1.metric_time__day AS metric_time__day
            , subq_1.average_booking_value AS average_booking_value
          FROM (
            -- Metric Time Dimension 'ds'
            -- Pass Only Elements: ['average_booking_value', 'metric_time__day', 'listing']
            SELECT
              subq_0.ds__day AS metric_time__day
              , subq_0.listing
              , subq_0.average_booking_value
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
            ) subq_0
          ) subq_1
          LEFT OUTER JOIN (
            -- Metric Time Dimension 'ds'
            -- Pass Only Elements: ['is_lux_latest', 'listing']
            SELECT
              subq_2.listing
              , subq_2.is_lux_latest
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
            ) subq_2
          ) subq_3
          ON
            subq_1.listing = subq_3.listing
        ) subq_4
        WHERE listing__is_lux_latest
      ) subq_5
      GROUP BY
        subq_5.metric_time__day
    ) subq_6
  ) subq_7
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_14.metric_time__day
      , subq_14.bookings
    FROM (
      -- Aggregate Measures
      SELECT
        subq_13.metric_time__day
        , SUM(subq_13.bookings) AS bookings
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['bookings', 'metric_time__day']
        SELECT
          subq_12.metric_time__day
          , subq_12.bookings
        FROM (
          -- Join Standard Outputs
          -- Pass Only Elements: ['bookings', 'listing__is_lux_latest', 'metric_time__day']
          SELECT
            subq_9.metric_time__day AS metric_time__day
            , subq_9.bookings AS bookings
          FROM (
            -- Metric Time Dimension 'ds'
            -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
            SELECT
              subq_8.ds__day AS metric_time__day
              , subq_8.listing
              , subq_8.bookings
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
          LEFT OUTER JOIN (
            -- Metric Time Dimension 'ds'
            -- Pass Only Elements: ['is_lux_latest', 'listing']
            SELECT
              subq_10.listing
              , subq_10.is_lux_latest
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
            ) subq_10
          ) subq_11
          ON
            subq_9.listing = subq_11.listing
        ) subq_12
        WHERE listing__is_lux_latest
      ) subq_13
      GROUP BY
        subq_13.metric_time__day
    ) subq_14
  ) subq_15
  ON
    subq_7.metric_time__day = subq_15.metric_time__day
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_18.metric_time__day
      , subq_18.booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        subq_17.metric_time__day
        , SUM(subq_17.booking_value) AS booking_value
      FROM (
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['booking_value', 'metric_time__day']
        SELECT
          subq_16.ds__day AS metric_time__day
          , subq_16.booking_value
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
        ) subq_16
      ) subq_17
      GROUP BY
        subq_17.metric_time__day
    ) subq_18
  ) subq_19
  ON
    COALESCE(subq_7.metric_time__day, subq_15.metric_time__day) = subq_19.metric_time__day
  GROUP BY
    COALESCE(subq_7.metric_time__day, subq_15.metric_time__day, subq_19.metric_time__day)
) subq_20
