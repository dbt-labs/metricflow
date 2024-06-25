-- Compute Metrics via Expressions
SELECT
  instant_lux_booking_value_rate AS instant_lux_booking_value_rate
FROM (
  -- Compute Metrics via Expressions
  SELECT
    average_booking_value * bookings / NULLIF(booking_value, 0) AS instant_lux_booking_value_rate
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      MAX(subq_18.average_booking_value) AS average_booking_value
      , MAX(subq_31.bookings) AS bookings
      , MAX(subq_39.booking_value) AS booking_value
    FROM (
      -- Compute Metrics via Expressions
      SELECT
        subq_17.average_booking_value
      FROM (
        -- Aggregate Measures
        SELECT
          AVG(subq_16.average_booking_value) AS average_booking_value
        FROM (
          -- Pass Only Elements: ['average_booking_value',]
          SELECT
            subq_15.average_booking_value
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_14.booking__is_instant
              , subq_14.listing__is_lux_latest
              , subq_14.average_booking_value
            FROM (
              -- Pass Only Elements: ['average_booking_value', 'listing__is_lux_latest', 'booking__is_instant']
              SELECT
                subq_13.booking__is_instant
                , subq_13.listing__is_lux_latest
                , subq_13.average_booking_value
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_9.listing AS listing
                  , subq_9.booking__is_instant AS booking__is_instant
                  , subq_12.is_lux_latest AS listing__is_lux_latest
                  , subq_9.average_booking_value AS average_booking_value
                FROM (
                  -- Pass Only Elements: ['average_booking_value', 'booking__is_instant', 'listing']
                  SELECT
                    subq_8.listing
                    , subq_8.booking__is_instant
                    , subq_8.average_booking_value
                  FROM (
                    -- Constrain Output with WHERE
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
                      , subq_7.metric_time__day
                      , subq_7.metric_time__week
                      , subq_7.metric_time__month
                      , subq_7.metric_time__quarter
                      , subq_7.metric_time__year
                      , subq_7.metric_time__extract_year
                      , subq_7.metric_time__extract_quarter
                      , subq_7.metric_time__extract_month
                      , subq_7.metric_time__extract_day
                      , subq_7.metric_time__extract_dow
                      , subq_7.metric_time__extract_doy
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
                        , subq_6.bookings
                        , subq_6.instant_bookings
                        , subq_6.booking_value
                        , subq_6.max_booking_value
                        , subq_6.min_booking_value
                        , subq_6.bookers
                        , subq_6.average_booking_value
                        , subq_6.referred_bookings
                        , subq_6.median_booking_value
                        , subq_6.booking_value_p99
                        , subq_6.discrete_booking_value_p99
                        , subq_6.approximate_continuous_booking_value_p99
                        , subq_6.approximate_discrete_booking_value_p99
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
                      ) subq_6
                    ) subq_7
                    WHERE booking__is_instant
                  ) subq_8
                ) subq_9
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['is_lux_latest', 'listing']
                  SELECT
                    subq_11.listing
                    , subq_11.is_lux_latest
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      subq_10.ds__day
                      , subq_10.ds__week
                      , subq_10.ds__month
                      , subq_10.ds__quarter
                      , subq_10.ds__year
                      , subq_10.ds__extract_year
                      , subq_10.ds__extract_quarter
                      , subq_10.ds__extract_month
                      , subq_10.ds__extract_day
                      , subq_10.ds__extract_dow
                      , subq_10.ds__extract_doy
                      , subq_10.created_at__day
                      , subq_10.created_at__week
                      , subq_10.created_at__month
                      , subq_10.created_at__quarter
                      , subq_10.created_at__year
                      , subq_10.created_at__extract_year
                      , subq_10.created_at__extract_quarter
                      , subq_10.created_at__extract_month
                      , subq_10.created_at__extract_day
                      , subq_10.created_at__extract_dow
                      , subq_10.created_at__extract_doy
                      , subq_10.listing__ds__day
                      , subq_10.listing__ds__week
                      , subq_10.listing__ds__month
                      , subq_10.listing__ds__quarter
                      , subq_10.listing__ds__year
                      , subq_10.listing__ds__extract_year
                      , subq_10.listing__ds__extract_quarter
                      , subq_10.listing__ds__extract_month
                      , subq_10.listing__ds__extract_day
                      , subq_10.listing__ds__extract_dow
                      , subq_10.listing__ds__extract_doy
                      , subq_10.listing__created_at__day
                      , subq_10.listing__created_at__week
                      , subq_10.listing__created_at__month
                      , subq_10.listing__created_at__quarter
                      , subq_10.listing__created_at__year
                      , subq_10.listing__created_at__extract_year
                      , subq_10.listing__created_at__extract_quarter
                      , subq_10.listing__created_at__extract_month
                      , subq_10.listing__created_at__extract_day
                      , subq_10.listing__created_at__extract_dow
                      , subq_10.listing__created_at__extract_doy
                      , subq_10.ds__day AS metric_time__day
                      , subq_10.ds__week AS metric_time__week
                      , subq_10.ds__month AS metric_time__month
                      , subq_10.ds__quarter AS metric_time__quarter
                      , subq_10.ds__year AS metric_time__year
                      , subq_10.ds__extract_year AS metric_time__extract_year
                      , subq_10.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_10.ds__extract_month AS metric_time__extract_month
                      , subq_10.ds__extract_day AS metric_time__extract_day
                      , subq_10.ds__extract_dow AS metric_time__extract_dow
                      , subq_10.ds__extract_doy AS metric_time__extract_doy
                      , subq_10.listing
                      , subq_10.user
                      , subq_10.listing__user
                      , subq_10.country_latest
                      , subq_10.is_lux_latest
                      , subq_10.capacity_latest
                      , subq_10.listing__country_latest
                      , subq_10.listing__is_lux_latest
                      , subq_10.listing__capacity_latest
                      , subq_10.listings
                      , subq_10.largest_listing
                      , subq_10.smallest_listing
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
                ) subq_12
                ON
                  subq_9.listing = subq_12.listing
              ) subq_13
            ) subq_14
            WHERE (listing__is_lux_latest) AND (booking__is_instant)
          ) subq_15
        ) subq_16
      ) subq_17
    ) subq_18
    CROSS JOIN (
      -- Compute Metrics via Expressions
      SELECT
        subq_30.bookings
      FROM (
        -- Aggregate Measures
        SELECT
          SUM(subq_29.bookings) AS bookings
        FROM (
          -- Pass Only Elements: ['bookings',]
          SELECT
            subq_28.bookings
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_27.booking__is_instant
              , subq_27.listing__is_lux_latest
              , subq_27.bookings
            FROM (
              -- Pass Only Elements: ['bookings', 'listing__is_lux_latest', 'booking__is_instant']
              SELECT
                subq_26.booking__is_instant
                , subq_26.listing__is_lux_latest
                , subq_26.bookings
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_22.listing AS listing
                  , subq_22.booking__is_instant AS booking__is_instant
                  , subq_25.is_lux_latest AS listing__is_lux_latest
                  , subq_22.bookings AS bookings
                FROM (
                  -- Pass Only Elements: ['bookings', 'booking__is_instant', 'listing']
                  SELECT
                    subq_21.listing
                    , subq_21.booking__is_instant
                    , subq_21.bookings
                  FROM (
                    -- Constrain Output with WHERE
                    SELECT
                      subq_20.ds__day
                      , subq_20.ds__week
                      , subq_20.ds__month
                      , subq_20.ds__quarter
                      , subq_20.ds__year
                      , subq_20.ds__extract_year
                      , subq_20.ds__extract_quarter
                      , subq_20.ds__extract_month
                      , subq_20.ds__extract_day
                      , subq_20.ds__extract_dow
                      , subq_20.ds__extract_doy
                      , subq_20.ds_partitioned__day
                      , subq_20.ds_partitioned__week
                      , subq_20.ds_partitioned__month
                      , subq_20.ds_partitioned__quarter
                      , subq_20.ds_partitioned__year
                      , subq_20.ds_partitioned__extract_year
                      , subq_20.ds_partitioned__extract_quarter
                      , subq_20.ds_partitioned__extract_month
                      , subq_20.ds_partitioned__extract_day
                      , subq_20.ds_partitioned__extract_dow
                      , subq_20.ds_partitioned__extract_doy
                      , subq_20.paid_at__day
                      , subq_20.paid_at__week
                      , subq_20.paid_at__month
                      , subq_20.paid_at__quarter
                      , subq_20.paid_at__year
                      , subq_20.paid_at__extract_year
                      , subq_20.paid_at__extract_quarter
                      , subq_20.paid_at__extract_month
                      , subq_20.paid_at__extract_day
                      , subq_20.paid_at__extract_dow
                      , subq_20.paid_at__extract_doy
                      , subq_20.booking__ds__day
                      , subq_20.booking__ds__week
                      , subq_20.booking__ds__month
                      , subq_20.booking__ds__quarter
                      , subq_20.booking__ds__year
                      , subq_20.booking__ds__extract_year
                      , subq_20.booking__ds__extract_quarter
                      , subq_20.booking__ds__extract_month
                      , subq_20.booking__ds__extract_day
                      , subq_20.booking__ds__extract_dow
                      , subq_20.booking__ds__extract_doy
                      , subq_20.booking__ds_partitioned__day
                      , subq_20.booking__ds_partitioned__week
                      , subq_20.booking__ds_partitioned__month
                      , subq_20.booking__ds_partitioned__quarter
                      , subq_20.booking__ds_partitioned__year
                      , subq_20.booking__ds_partitioned__extract_year
                      , subq_20.booking__ds_partitioned__extract_quarter
                      , subq_20.booking__ds_partitioned__extract_month
                      , subq_20.booking__ds_partitioned__extract_day
                      , subq_20.booking__ds_partitioned__extract_dow
                      , subq_20.booking__ds_partitioned__extract_doy
                      , subq_20.booking__paid_at__day
                      , subq_20.booking__paid_at__week
                      , subq_20.booking__paid_at__month
                      , subq_20.booking__paid_at__quarter
                      , subq_20.booking__paid_at__year
                      , subq_20.booking__paid_at__extract_year
                      , subq_20.booking__paid_at__extract_quarter
                      , subq_20.booking__paid_at__extract_month
                      , subq_20.booking__paid_at__extract_day
                      , subq_20.booking__paid_at__extract_dow
                      , subq_20.booking__paid_at__extract_doy
                      , subq_20.metric_time__day
                      , subq_20.metric_time__week
                      , subq_20.metric_time__month
                      , subq_20.metric_time__quarter
                      , subq_20.metric_time__year
                      , subq_20.metric_time__extract_year
                      , subq_20.metric_time__extract_quarter
                      , subq_20.metric_time__extract_month
                      , subq_20.metric_time__extract_day
                      , subq_20.metric_time__extract_dow
                      , subq_20.metric_time__extract_doy
                      , subq_20.listing
                      , subq_20.guest
                      , subq_20.host
                      , subq_20.booking__listing
                      , subq_20.booking__guest
                      , subq_20.booking__host
                      , subq_20.is_instant
                      , subq_20.booking__is_instant
                      , subq_20.bookings
                      , subq_20.instant_bookings
                      , subq_20.booking_value
                      , subq_20.max_booking_value
                      , subq_20.min_booking_value
                      , subq_20.bookers
                      , subq_20.average_booking_value
                      , subq_20.referred_bookings
                      , subq_20.median_booking_value
                      , subq_20.booking_value_p99
                      , subq_20.discrete_booking_value_p99
                      , subq_20.approximate_continuous_booking_value_p99
                      , subq_20.approximate_discrete_booking_value_p99
                    FROM (
                      -- Metric Time Dimension 'ds'
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
                        , subq_19.ds_partitioned__day
                        , subq_19.ds_partitioned__week
                        , subq_19.ds_partitioned__month
                        , subq_19.ds_partitioned__quarter
                        , subq_19.ds_partitioned__year
                        , subq_19.ds_partitioned__extract_year
                        , subq_19.ds_partitioned__extract_quarter
                        , subq_19.ds_partitioned__extract_month
                        , subq_19.ds_partitioned__extract_day
                        , subq_19.ds_partitioned__extract_dow
                        , subq_19.ds_partitioned__extract_doy
                        , subq_19.paid_at__day
                        , subq_19.paid_at__week
                        , subq_19.paid_at__month
                        , subq_19.paid_at__quarter
                        , subq_19.paid_at__year
                        , subq_19.paid_at__extract_year
                        , subq_19.paid_at__extract_quarter
                        , subq_19.paid_at__extract_month
                        , subq_19.paid_at__extract_day
                        , subq_19.paid_at__extract_dow
                        , subq_19.paid_at__extract_doy
                        , subq_19.booking__ds__day
                        , subq_19.booking__ds__week
                        , subq_19.booking__ds__month
                        , subq_19.booking__ds__quarter
                        , subq_19.booking__ds__year
                        , subq_19.booking__ds__extract_year
                        , subq_19.booking__ds__extract_quarter
                        , subq_19.booking__ds__extract_month
                        , subq_19.booking__ds__extract_day
                        , subq_19.booking__ds__extract_dow
                        , subq_19.booking__ds__extract_doy
                        , subq_19.booking__ds_partitioned__day
                        , subq_19.booking__ds_partitioned__week
                        , subq_19.booking__ds_partitioned__month
                        , subq_19.booking__ds_partitioned__quarter
                        , subq_19.booking__ds_partitioned__year
                        , subq_19.booking__ds_partitioned__extract_year
                        , subq_19.booking__ds_partitioned__extract_quarter
                        , subq_19.booking__ds_partitioned__extract_month
                        , subq_19.booking__ds_partitioned__extract_day
                        , subq_19.booking__ds_partitioned__extract_dow
                        , subq_19.booking__ds_partitioned__extract_doy
                        , subq_19.booking__paid_at__day
                        , subq_19.booking__paid_at__week
                        , subq_19.booking__paid_at__month
                        , subq_19.booking__paid_at__quarter
                        , subq_19.booking__paid_at__year
                        , subq_19.booking__paid_at__extract_year
                        , subq_19.booking__paid_at__extract_quarter
                        , subq_19.booking__paid_at__extract_month
                        , subq_19.booking__paid_at__extract_day
                        , subq_19.booking__paid_at__extract_dow
                        , subq_19.booking__paid_at__extract_doy
                        , subq_19.ds__day AS metric_time__day
                        , subq_19.ds__week AS metric_time__week
                        , subq_19.ds__month AS metric_time__month
                        , subq_19.ds__quarter AS metric_time__quarter
                        , subq_19.ds__year AS metric_time__year
                        , subq_19.ds__extract_year AS metric_time__extract_year
                        , subq_19.ds__extract_quarter AS metric_time__extract_quarter
                        , subq_19.ds__extract_month AS metric_time__extract_month
                        , subq_19.ds__extract_day AS metric_time__extract_day
                        , subq_19.ds__extract_dow AS metric_time__extract_dow
                        , subq_19.ds__extract_doy AS metric_time__extract_doy
                        , subq_19.listing
                        , subq_19.guest
                        , subq_19.host
                        , subq_19.booking__listing
                        , subq_19.booking__guest
                        , subq_19.booking__host
                        , subq_19.is_instant
                        , subq_19.booking__is_instant
                        , subq_19.bookings
                        , subq_19.instant_bookings
                        , subq_19.booking_value
                        , subq_19.max_booking_value
                        , subq_19.min_booking_value
                        , subq_19.bookers
                        , subq_19.average_booking_value
                        , subq_19.referred_bookings
                        , subq_19.median_booking_value
                        , subq_19.booking_value_p99
                        , subq_19.discrete_booking_value_p99
                        , subq_19.approximate_continuous_booking_value_p99
                        , subq_19.approximate_discrete_booking_value_p99
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
                      ) subq_19
                    ) subq_20
                    WHERE booking__is_instant
                  ) subq_21
                ) subq_22
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['is_lux_latest', 'listing']
                  SELECT
                    subq_24.listing
                    , subq_24.is_lux_latest
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      subq_23.ds__day
                      , subq_23.ds__week
                      , subq_23.ds__month
                      , subq_23.ds__quarter
                      , subq_23.ds__year
                      , subq_23.ds__extract_year
                      , subq_23.ds__extract_quarter
                      , subq_23.ds__extract_month
                      , subq_23.ds__extract_day
                      , subq_23.ds__extract_dow
                      , subq_23.ds__extract_doy
                      , subq_23.created_at__day
                      , subq_23.created_at__week
                      , subq_23.created_at__month
                      , subq_23.created_at__quarter
                      , subq_23.created_at__year
                      , subq_23.created_at__extract_year
                      , subq_23.created_at__extract_quarter
                      , subq_23.created_at__extract_month
                      , subq_23.created_at__extract_day
                      , subq_23.created_at__extract_dow
                      , subq_23.created_at__extract_doy
                      , subq_23.listing__ds__day
                      , subq_23.listing__ds__week
                      , subq_23.listing__ds__month
                      , subq_23.listing__ds__quarter
                      , subq_23.listing__ds__year
                      , subq_23.listing__ds__extract_year
                      , subq_23.listing__ds__extract_quarter
                      , subq_23.listing__ds__extract_month
                      , subq_23.listing__ds__extract_day
                      , subq_23.listing__ds__extract_dow
                      , subq_23.listing__ds__extract_doy
                      , subq_23.listing__created_at__day
                      , subq_23.listing__created_at__week
                      , subq_23.listing__created_at__month
                      , subq_23.listing__created_at__quarter
                      , subq_23.listing__created_at__year
                      , subq_23.listing__created_at__extract_year
                      , subq_23.listing__created_at__extract_quarter
                      , subq_23.listing__created_at__extract_month
                      , subq_23.listing__created_at__extract_day
                      , subq_23.listing__created_at__extract_dow
                      , subq_23.listing__created_at__extract_doy
                      , subq_23.ds__day AS metric_time__day
                      , subq_23.ds__week AS metric_time__week
                      , subq_23.ds__month AS metric_time__month
                      , subq_23.ds__quarter AS metric_time__quarter
                      , subq_23.ds__year AS metric_time__year
                      , subq_23.ds__extract_year AS metric_time__extract_year
                      , subq_23.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_23.ds__extract_month AS metric_time__extract_month
                      , subq_23.ds__extract_day AS metric_time__extract_day
                      , subq_23.ds__extract_dow AS metric_time__extract_dow
                      , subq_23.ds__extract_doy AS metric_time__extract_doy
                      , subq_23.listing
                      , subq_23.user
                      , subq_23.listing__user
                      , subq_23.country_latest
                      , subq_23.is_lux_latest
                      , subq_23.capacity_latest
                      , subq_23.listing__country_latest
                      , subq_23.listing__is_lux_latest
                      , subq_23.listing__capacity_latest
                      , subq_23.listings
                      , subq_23.largest_listing
                      , subq_23.smallest_listing
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
                    ) subq_23
                  ) subq_24
                ) subq_25
                ON
                  subq_22.listing = subq_25.listing
              ) subq_26
            ) subq_27
            WHERE (listing__is_lux_latest) AND (booking__is_instant)
          ) subq_28
        ) subq_29
      ) subq_30
    ) subq_31
    CROSS JOIN (
      -- Compute Metrics via Expressions
      SELECT
        subq_38.booking_value
      FROM (
        -- Aggregate Measures
        SELECT
          SUM(subq_37.booking_value) AS booking_value
        FROM (
          -- Pass Only Elements: ['booking_value',]
          SELECT
            subq_36.booking_value
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_35.booking__is_instant
              , subq_35.booking_value
            FROM (
              -- Pass Only Elements: ['booking_value', 'booking__is_instant']
              SELECT
                subq_34.booking__is_instant
                , subq_34.booking_value
              FROM (
                -- Constrain Output with WHERE
                SELECT
                  subq_33.ds__day
                  , subq_33.ds__week
                  , subq_33.ds__month
                  , subq_33.ds__quarter
                  , subq_33.ds__year
                  , subq_33.ds__extract_year
                  , subq_33.ds__extract_quarter
                  , subq_33.ds__extract_month
                  , subq_33.ds__extract_day
                  , subq_33.ds__extract_dow
                  , subq_33.ds__extract_doy
                  , subq_33.ds_partitioned__day
                  , subq_33.ds_partitioned__week
                  , subq_33.ds_partitioned__month
                  , subq_33.ds_partitioned__quarter
                  , subq_33.ds_partitioned__year
                  , subq_33.ds_partitioned__extract_year
                  , subq_33.ds_partitioned__extract_quarter
                  , subq_33.ds_partitioned__extract_month
                  , subq_33.ds_partitioned__extract_day
                  , subq_33.ds_partitioned__extract_dow
                  , subq_33.ds_partitioned__extract_doy
                  , subq_33.paid_at__day
                  , subq_33.paid_at__week
                  , subq_33.paid_at__month
                  , subq_33.paid_at__quarter
                  , subq_33.paid_at__year
                  , subq_33.paid_at__extract_year
                  , subq_33.paid_at__extract_quarter
                  , subq_33.paid_at__extract_month
                  , subq_33.paid_at__extract_day
                  , subq_33.paid_at__extract_dow
                  , subq_33.paid_at__extract_doy
                  , subq_33.booking__ds__day
                  , subq_33.booking__ds__week
                  , subq_33.booking__ds__month
                  , subq_33.booking__ds__quarter
                  , subq_33.booking__ds__year
                  , subq_33.booking__ds__extract_year
                  , subq_33.booking__ds__extract_quarter
                  , subq_33.booking__ds__extract_month
                  , subq_33.booking__ds__extract_day
                  , subq_33.booking__ds__extract_dow
                  , subq_33.booking__ds__extract_doy
                  , subq_33.booking__ds_partitioned__day
                  , subq_33.booking__ds_partitioned__week
                  , subq_33.booking__ds_partitioned__month
                  , subq_33.booking__ds_partitioned__quarter
                  , subq_33.booking__ds_partitioned__year
                  , subq_33.booking__ds_partitioned__extract_year
                  , subq_33.booking__ds_partitioned__extract_quarter
                  , subq_33.booking__ds_partitioned__extract_month
                  , subq_33.booking__ds_partitioned__extract_day
                  , subq_33.booking__ds_partitioned__extract_dow
                  , subq_33.booking__ds_partitioned__extract_doy
                  , subq_33.booking__paid_at__day
                  , subq_33.booking__paid_at__week
                  , subq_33.booking__paid_at__month
                  , subq_33.booking__paid_at__quarter
                  , subq_33.booking__paid_at__year
                  , subq_33.booking__paid_at__extract_year
                  , subq_33.booking__paid_at__extract_quarter
                  , subq_33.booking__paid_at__extract_month
                  , subq_33.booking__paid_at__extract_day
                  , subq_33.booking__paid_at__extract_dow
                  , subq_33.booking__paid_at__extract_doy
                  , subq_33.metric_time__day
                  , subq_33.metric_time__week
                  , subq_33.metric_time__month
                  , subq_33.metric_time__quarter
                  , subq_33.metric_time__year
                  , subq_33.metric_time__extract_year
                  , subq_33.metric_time__extract_quarter
                  , subq_33.metric_time__extract_month
                  , subq_33.metric_time__extract_day
                  , subq_33.metric_time__extract_dow
                  , subq_33.metric_time__extract_doy
                  , subq_33.listing
                  , subq_33.guest
                  , subq_33.host
                  , subq_33.booking__listing
                  , subq_33.booking__guest
                  , subq_33.booking__host
                  , subq_33.is_instant
                  , subq_33.booking__is_instant
                  , subq_33.bookings
                  , subq_33.instant_bookings
                  , subq_33.booking_value
                  , subq_33.max_booking_value
                  , subq_33.min_booking_value
                  , subq_33.bookers
                  , subq_33.average_booking_value
                  , subq_33.referred_bookings
                  , subq_33.median_booking_value
                  , subq_33.booking_value_p99
                  , subq_33.discrete_booking_value_p99
                  , subq_33.approximate_continuous_booking_value_p99
                  , subq_33.approximate_discrete_booking_value_p99
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_32.ds__day
                    , subq_32.ds__week
                    , subq_32.ds__month
                    , subq_32.ds__quarter
                    , subq_32.ds__year
                    , subq_32.ds__extract_year
                    , subq_32.ds__extract_quarter
                    , subq_32.ds__extract_month
                    , subq_32.ds__extract_day
                    , subq_32.ds__extract_dow
                    , subq_32.ds__extract_doy
                    , subq_32.ds_partitioned__day
                    , subq_32.ds_partitioned__week
                    , subq_32.ds_partitioned__month
                    , subq_32.ds_partitioned__quarter
                    , subq_32.ds_partitioned__year
                    , subq_32.ds_partitioned__extract_year
                    , subq_32.ds_partitioned__extract_quarter
                    , subq_32.ds_partitioned__extract_month
                    , subq_32.ds_partitioned__extract_day
                    , subq_32.ds_partitioned__extract_dow
                    , subq_32.ds_partitioned__extract_doy
                    , subq_32.paid_at__day
                    , subq_32.paid_at__week
                    , subq_32.paid_at__month
                    , subq_32.paid_at__quarter
                    , subq_32.paid_at__year
                    , subq_32.paid_at__extract_year
                    , subq_32.paid_at__extract_quarter
                    , subq_32.paid_at__extract_month
                    , subq_32.paid_at__extract_day
                    , subq_32.paid_at__extract_dow
                    , subq_32.paid_at__extract_doy
                    , subq_32.booking__ds__day
                    , subq_32.booking__ds__week
                    , subq_32.booking__ds__month
                    , subq_32.booking__ds__quarter
                    , subq_32.booking__ds__year
                    , subq_32.booking__ds__extract_year
                    , subq_32.booking__ds__extract_quarter
                    , subq_32.booking__ds__extract_month
                    , subq_32.booking__ds__extract_day
                    , subq_32.booking__ds__extract_dow
                    , subq_32.booking__ds__extract_doy
                    , subq_32.booking__ds_partitioned__day
                    , subq_32.booking__ds_partitioned__week
                    , subq_32.booking__ds_partitioned__month
                    , subq_32.booking__ds_partitioned__quarter
                    , subq_32.booking__ds_partitioned__year
                    , subq_32.booking__ds_partitioned__extract_year
                    , subq_32.booking__ds_partitioned__extract_quarter
                    , subq_32.booking__ds_partitioned__extract_month
                    , subq_32.booking__ds_partitioned__extract_day
                    , subq_32.booking__ds_partitioned__extract_dow
                    , subq_32.booking__ds_partitioned__extract_doy
                    , subq_32.booking__paid_at__day
                    , subq_32.booking__paid_at__week
                    , subq_32.booking__paid_at__month
                    , subq_32.booking__paid_at__quarter
                    , subq_32.booking__paid_at__year
                    , subq_32.booking__paid_at__extract_year
                    , subq_32.booking__paid_at__extract_quarter
                    , subq_32.booking__paid_at__extract_month
                    , subq_32.booking__paid_at__extract_day
                    , subq_32.booking__paid_at__extract_dow
                    , subq_32.booking__paid_at__extract_doy
                    , subq_32.ds__day AS metric_time__day
                    , subq_32.ds__week AS metric_time__week
                    , subq_32.ds__month AS metric_time__month
                    , subq_32.ds__quarter AS metric_time__quarter
                    , subq_32.ds__year AS metric_time__year
                    , subq_32.ds__extract_year AS metric_time__extract_year
                    , subq_32.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_32.ds__extract_month AS metric_time__extract_month
                    , subq_32.ds__extract_day AS metric_time__extract_day
                    , subq_32.ds__extract_dow AS metric_time__extract_dow
                    , subq_32.ds__extract_doy AS metric_time__extract_doy
                    , subq_32.listing
                    , subq_32.guest
                    , subq_32.host
                    , subq_32.booking__listing
                    , subq_32.booking__guest
                    , subq_32.booking__host
                    , subq_32.is_instant
                    , subq_32.booking__is_instant
                    , subq_32.bookings
                    , subq_32.instant_bookings
                    , subq_32.booking_value
                    , subq_32.max_booking_value
                    , subq_32.min_booking_value
                    , subq_32.bookers
                    , subq_32.average_booking_value
                    , subq_32.referred_bookings
                    , subq_32.median_booking_value
                    , subq_32.booking_value_p99
                    , subq_32.discrete_booking_value_p99
                    , subq_32.approximate_continuous_booking_value_p99
                    , subq_32.approximate_discrete_booking_value_p99
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
                  ) subq_32
                ) subq_33
                WHERE booking__is_instant
              ) subq_34
            ) subq_35
            WHERE booking__is_instant
          ) subq_36
        ) subq_37
      ) subq_38
    ) subq_39
  ) subq_40
) subq_41
