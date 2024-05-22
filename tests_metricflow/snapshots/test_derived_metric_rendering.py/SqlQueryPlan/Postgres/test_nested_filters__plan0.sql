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
      MAX(subq_30.average_booking_value) AS average_booking_value
      , MAX(subq_43.bookings) AS bookings
      , MAX(subq_51.booking_value) AS booking_value
    FROM (
      -- Compute Metrics via Expressions
      SELECT
        subq_29.average_booking_value
      FROM (
        -- Aggregate Measures
        SELECT
          AVG(subq_28.average_booking_value) AS average_booking_value
        FROM (
          -- Pass Only Elements: ['average_booking_value',]
          SELECT
            subq_27.average_booking_value
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_26.booking__is_instant
              , subq_26.listing__is_lux_latest
              , subq_26.average_booking_value
            FROM (
              -- Pass Only Elements: ['average_booking_value', 'listing__is_lux_latest', 'booking__is_instant']
              SELECT
                subq_25.booking__is_instant
                , subq_25.listing__is_lux_latest
                , subq_25.average_booking_value
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_21.listing AS listing
                  , subq_21.booking__is_instant AS booking__is_instant
                  , subq_24.is_lux_latest AS listing__is_lux_latest
                  , subq_21.average_booking_value AS average_booking_value
                FROM (
                  -- Pass Only Elements: ['average_booking_value', 'booking__is_instant', 'listing']
                  SELECT
                    subq_20.listing
                    , subq_20.booking__is_instant
                    , subq_20.average_booking_value
                  FROM (
                    -- Constrain Output with WHERE
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
                      , subq_19.metric_time__day
                      , subq_19.metric_time__week
                      , subq_19.metric_time__month
                      , subq_19.metric_time__quarter
                      , subq_19.metric_time__year
                      , subq_19.metric_time__extract_year
                      , subq_19.metric_time__extract_quarter
                      , subq_19.metric_time__extract_month
                      , subq_19.metric_time__extract_day
                      , subq_19.metric_time__extract_dow
                      , subq_19.metric_time__extract_doy
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
                      -- Metric Time Dimension 'ds'
                      SELECT
                        subq_18.ds__day
                        , subq_18.ds__week
                        , subq_18.ds__month
                        , subq_18.ds__quarter
                        , subq_18.ds__year
                        , subq_18.ds__extract_year
                        , subq_18.ds__extract_quarter
                        , subq_18.ds__extract_month
                        , subq_18.ds__extract_day
                        , subq_18.ds__extract_dow
                        , subq_18.ds__extract_doy
                        , subq_18.ds_partitioned__day
                        , subq_18.ds_partitioned__week
                        , subq_18.ds_partitioned__month
                        , subq_18.ds_partitioned__quarter
                        , subq_18.ds_partitioned__year
                        , subq_18.ds_partitioned__extract_year
                        , subq_18.ds_partitioned__extract_quarter
                        , subq_18.ds_partitioned__extract_month
                        , subq_18.ds_partitioned__extract_day
                        , subq_18.ds_partitioned__extract_dow
                        , subq_18.ds_partitioned__extract_doy
                        , subq_18.paid_at__day
                        , subq_18.paid_at__week
                        , subq_18.paid_at__month
                        , subq_18.paid_at__quarter
                        , subq_18.paid_at__year
                        , subq_18.paid_at__extract_year
                        , subq_18.paid_at__extract_quarter
                        , subq_18.paid_at__extract_month
                        , subq_18.paid_at__extract_day
                        , subq_18.paid_at__extract_dow
                        , subq_18.paid_at__extract_doy
                        , subq_18.booking__ds__day
                        , subq_18.booking__ds__week
                        , subq_18.booking__ds__month
                        , subq_18.booking__ds__quarter
                        , subq_18.booking__ds__year
                        , subq_18.booking__ds__extract_year
                        , subq_18.booking__ds__extract_quarter
                        , subq_18.booking__ds__extract_month
                        , subq_18.booking__ds__extract_day
                        , subq_18.booking__ds__extract_dow
                        , subq_18.booking__ds__extract_doy
                        , subq_18.booking__ds_partitioned__day
                        , subq_18.booking__ds_partitioned__week
                        , subq_18.booking__ds_partitioned__month
                        , subq_18.booking__ds_partitioned__quarter
                        , subq_18.booking__ds_partitioned__year
                        , subq_18.booking__ds_partitioned__extract_year
                        , subq_18.booking__ds_partitioned__extract_quarter
                        , subq_18.booking__ds_partitioned__extract_month
                        , subq_18.booking__ds_partitioned__extract_day
                        , subq_18.booking__ds_partitioned__extract_dow
                        , subq_18.booking__ds_partitioned__extract_doy
                        , subq_18.booking__paid_at__day
                        , subq_18.booking__paid_at__week
                        , subq_18.booking__paid_at__month
                        , subq_18.booking__paid_at__quarter
                        , subq_18.booking__paid_at__year
                        , subq_18.booking__paid_at__extract_year
                        , subq_18.booking__paid_at__extract_quarter
                        , subq_18.booking__paid_at__extract_month
                        , subq_18.booking__paid_at__extract_day
                        , subq_18.booking__paid_at__extract_dow
                        , subq_18.booking__paid_at__extract_doy
                        , subq_18.ds__day AS metric_time__day
                        , subq_18.ds__week AS metric_time__week
                        , subq_18.ds__month AS metric_time__month
                        , subq_18.ds__quarter AS metric_time__quarter
                        , subq_18.ds__year AS metric_time__year
                        , subq_18.ds__extract_year AS metric_time__extract_year
                        , subq_18.ds__extract_quarter AS metric_time__extract_quarter
                        , subq_18.ds__extract_month AS metric_time__extract_month
                        , subq_18.ds__extract_day AS metric_time__extract_day
                        , subq_18.ds__extract_dow AS metric_time__extract_dow
                        , subq_18.ds__extract_doy AS metric_time__extract_doy
                        , subq_18.listing
                        , subq_18.guest
                        , subq_18.host
                        , subq_18.booking__listing
                        , subq_18.booking__guest
                        , subq_18.booking__host
                        , subq_18.is_instant
                        , subq_18.booking__is_instant
                        , subq_18.bookings
                        , subq_18.instant_bookings
                        , subq_18.booking_value
                        , subq_18.max_booking_value
                        , subq_18.min_booking_value
                        , subq_18.bookers
                        , subq_18.average_booking_value
                        , subq_18.referred_bookings
                        , subq_18.median_booking_value
                        , subq_18.booking_value_p99
                        , subq_18.discrete_booking_value_p99
                        , subq_18.approximate_continuous_booking_value_p99
                        , subq_18.approximate_discrete_booking_value_p99
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
                      ) subq_18
                    ) subq_19
                    WHERE booking__is_instant
                  ) subq_20
                ) subq_21
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['is_lux_latest', 'listing']
                  SELECT
                    subq_23.listing
                    , subq_23.is_lux_latest
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      subq_22.ds__day
                      , subq_22.ds__week
                      , subq_22.ds__month
                      , subq_22.ds__quarter
                      , subq_22.ds__year
                      , subq_22.ds__extract_year
                      , subq_22.ds__extract_quarter
                      , subq_22.ds__extract_month
                      , subq_22.ds__extract_day
                      , subq_22.ds__extract_dow
                      , subq_22.ds__extract_doy
                      , subq_22.created_at__day
                      , subq_22.created_at__week
                      , subq_22.created_at__month
                      , subq_22.created_at__quarter
                      , subq_22.created_at__year
                      , subq_22.created_at__extract_year
                      , subq_22.created_at__extract_quarter
                      , subq_22.created_at__extract_month
                      , subq_22.created_at__extract_day
                      , subq_22.created_at__extract_dow
                      , subq_22.created_at__extract_doy
                      , subq_22.listing__ds__day
                      , subq_22.listing__ds__week
                      , subq_22.listing__ds__month
                      , subq_22.listing__ds__quarter
                      , subq_22.listing__ds__year
                      , subq_22.listing__ds__extract_year
                      , subq_22.listing__ds__extract_quarter
                      , subq_22.listing__ds__extract_month
                      , subq_22.listing__ds__extract_day
                      , subq_22.listing__ds__extract_dow
                      , subq_22.listing__ds__extract_doy
                      , subq_22.listing__created_at__day
                      , subq_22.listing__created_at__week
                      , subq_22.listing__created_at__month
                      , subq_22.listing__created_at__quarter
                      , subq_22.listing__created_at__year
                      , subq_22.listing__created_at__extract_year
                      , subq_22.listing__created_at__extract_quarter
                      , subq_22.listing__created_at__extract_month
                      , subq_22.listing__created_at__extract_day
                      , subq_22.listing__created_at__extract_dow
                      , subq_22.listing__created_at__extract_doy
                      , subq_22.ds__day AS metric_time__day
                      , subq_22.ds__week AS metric_time__week
                      , subq_22.ds__month AS metric_time__month
                      , subq_22.ds__quarter AS metric_time__quarter
                      , subq_22.ds__year AS metric_time__year
                      , subq_22.ds__extract_year AS metric_time__extract_year
                      , subq_22.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_22.ds__extract_month AS metric_time__extract_month
                      , subq_22.ds__extract_day AS metric_time__extract_day
                      , subq_22.ds__extract_dow AS metric_time__extract_dow
                      , subq_22.ds__extract_doy AS metric_time__extract_doy
                      , subq_22.listing
                      , subq_22.user
                      , subq_22.listing__user
                      , subq_22.country_latest
                      , subq_22.is_lux_latest
                      , subq_22.capacity_latest
                      , subq_22.listing__country_latest
                      , subq_22.listing__is_lux_latest
                      , subq_22.listing__capacity_latest
                      , subq_22.listings
                      , subq_22.largest_listing
                      , subq_22.smallest_listing
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
                    ) subq_22
                  ) subq_23
                ) subq_24
                ON
                  subq_21.listing = subq_24.listing
              ) subq_25
            ) subq_26
            WHERE (listing__is_lux_latest) AND (booking__is_instant)
          ) subq_27
        ) subq_28
      ) subq_29
    ) subq_30
    CROSS JOIN (
      -- Compute Metrics via Expressions
      SELECT
        subq_42.bookings
      FROM (
        -- Aggregate Measures
        SELECT
          SUM(subq_41.bookings) AS bookings
        FROM (
          -- Pass Only Elements: ['bookings',]
          SELECT
            subq_40.bookings
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_39.booking__is_instant
              , subq_39.listing__is_lux_latest
              , subq_39.bookings
            FROM (
              -- Pass Only Elements: ['bookings', 'listing__is_lux_latest', 'booking__is_instant']
              SELECT
                subq_38.booking__is_instant
                , subq_38.listing__is_lux_latest
                , subq_38.bookings
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_34.listing AS listing
                  , subq_34.booking__is_instant AS booking__is_instant
                  , subq_37.is_lux_latest AS listing__is_lux_latest
                  , subq_34.bookings AS bookings
                FROM (
                  -- Pass Only Elements: ['bookings', 'booking__is_instant', 'listing']
                  SELECT
                    subq_33.listing
                    , subq_33.booking__is_instant
                    , subq_33.bookings
                  FROM (
                    -- Constrain Output with WHERE
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
                      , subq_32.metric_time__day
                      , subq_32.metric_time__week
                      , subq_32.metric_time__month
                      , subq_32.metric_time__quarter
                      , subq_32.metric_time__year
                      , subq_32.metric_time__extract_year
                      , subq_32.metric_time__extract_quarter
                      , subq_32.metric_time__extract_month
                      , subq_32.metric_time__extract_day
                      , subq_32.metric_time__extract_dow
                      , subq_32.metric_time__extract_doy
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
                      -- Metric Time Dimension 'ds'
                      SELECT
                        subq_31.ds__day
                        , subq_31.ds__week
                        , subq_31.ds__month
                        , subq_31.ds__quarter
                        , subq_31.ds__year
                        , subq_31.ds__extract_year
                        , subq_31.ds__extract_quarter
                        , subq_31.ds__extract_month
                        , subq_31.ds__extract_day
                        , subq_31.ds__extract_dow
                        , subq_31.ds__extract_doy
                        , subq_31.ds_partitioned__day
                        , subq_31.ds_partitioned__week
                        , subq_31.ds_partitioned__month
                        , subq_31.ds_partitioned__quarter
                        , subq_31.ds_partitioned__year
                        , subq_31.ds_partitioned__extract_year
                        , subq_31.ds_partitioned__extract_quarter
                        , subq_31.ds_partitioned__extract_month
                        , subq_31.ds_partitioned__extract_day
                        , subq_31.ds_partitioned__extract_dow
                        , subq_31.ds_partitioned__extract_doy
                        , subq_31.paid_at__day
                        , subq_31.paid_at__week
                        , subq_31.paid_at__month
                        , subq_31.paid_at__quarter
                        , subq_31.paid_at__year
                        , subq_31.paid_at__extract_year
                        , subq_31.paid_at__extract_quarter
                        , subq_31.paid_at__extract_month
                        , subq_31.paid_at__extract_day
                        , subq_31.paid_at__extract_dow
                        , subq_31.paid_at__extract_doy
                        , subq_31.booking__ds__day
                        , subq_31.booking__ds__week
                        , subq_31.booking__ds__month
                        , subq_31.booking__ds__quarter
                        , subq_31.booking__ds__year
                        , subq_31.booking__ds__extract_year
                        , subq_31.booking__ds__extract_quarter
                        , subq_31.booking__ds__extract_month
                        , subq_31.booking__ds__extract_day
                        , subq_31.booking__ds__extract_dow
                        , subq_31.booking__ds__extract_doy
                        , subq_31.booking__ds_partitioned__day
                        , subq_31.booking__ds_partitioned__week
                        , subq_31.booking__ds_partitioned__month
                        , subq_31.booking__ds_partitioned__quarter
                        , subq_31.booking__ds_partitioned__year
                        , subq_31.booking__ds_partitioned__extract_year
                        , subq_31.booking__ds_partitioned__extract_quarter
                        , subq_31.booking__ds_partitioned__extract_month
                        , subq_31.booking__ds_partitioned__extract_day
                        , subq_31.booking__ds_partitioned__extract_dow
                        , subq_31.booking__ds_partitioned__extract_doy
                        , subq_31.booking__paid_at__day
                        , subq_31.booking__paid_at__week
                        , subq_31.booking__paid_at__month
                        , subq_31.booking__paid_at__quarter
                        , subq_31.booking__paid_at__year
                        , subq_31.booking__paid_at__extract_year
                        , subq_31.booking__paid_at__extract_quarter
                        , subq_31.booking__paid_at__extract_month
                        , subq_31.booking__paid_at__extract_day
                        , subq_31.booking__paid_at__extract_dow
                        , subq_31.booking__paid_at__extract_doy
                        , subq_31.ds__day AS metric_time__day
                        , subq_31.ds__week AS metric_time__week
                        , subq_31.ds__month AS metric_time__month
                        , subq_31.ds__quarter AS metric_time__quarter
                        , subq_31.ds__year AS metric_time__year
                        , subq_31.ds__extract_year AS metric_time__extract_year
                        , subq_31.ds__extract_quarter AS metric_time__extract_quarter
                        , subq_31.ds__extract_month AS metric_time__extract_month
                        , subq_31.ds__extract_day AS metric_time__extract_day
                        , subq_31.ds__extract_dow AS metric_time__extract_dow
                        , subq_31.ds__extract_doy AS metric_time__extract_doy
                        , subq_31.listing
                        , subq_31.guest
                        , subq_31.host
                        , subq_31.booking__listing
                        , subq_31.booking__guest
                        , subq_31.booking__host
                        , subq_31.is_instant
                        , subq_31.booking__is_instant
                        , subq_31.bookings
                        , subq_31.instant_bookings
                        , subq_31.booking_value
                        , subq_31.max_booking_value
                        , subq_31.min_booking_value
                        , subq_31.bookers
                        , subq_31.average_booking_value
                        , subq_31.referred_bookings
                        , subq_31.median_booking_value
                        , subq_31.booking_value_p99
                        , subq_31.discrete_booking_value_p99
                        , subq_31.approximate_continuous_booking_value_p99
                        , subq_31.approximate_discrete_booking_value_p99
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
                      ) subq_31
                    ) subq_32
                    WHERE booking__is_instant
                  ) subq_33
                ) subq_34
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['is_lux_latest', 'listing']
                  SELECT
                    subq_36.listing
                    , subq_36.is_lux_latest
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      subq_35.ds__day
                      , subq_35.ds__week
                      , subq_35.ds__month
                      , subq_35.ds__quarter
                      , subq_35.ds__year
                      , subq_35.ds__extract_year
                      , subq_35.ds__extract_quarter
                      , subq_35.ds__extract_month
                      , subq_35.ds__extract_day
                      , subq_35.ds__extract_dow
                      , subq_35.ds__extract_doy
                      , subq_35.created_at__day
                      , subq_35.created_at__week
                      , subq_35.created_at__month
                      , subq_35.created_at__quarter
                      , subq_35.created_at__year
                      , subq_35.created_at__extract_year
                      , subq_35.created_at__extract_quarter
                      , subq_35.created_at__extract_month
                      , subq_35.created_at__extract_day
                      , subq_35.created_at__extract_dow
                      , subq_35.created_at__extract_doy
                      , subq_35.listing__ds__day
                      , subq_35.listing__ds__week
                      , subq_35.listing__ds__month
                      , subq_35.listing__ds__quarter
                      , subq_35.listing__ds__year
                      , subq_35.listing__ds__extract_year
                      , subq_35.listing__ds__extract_quarter
                      , subq_35.listing__ds__extract_month
                      , subq_35.listing__ds__extract_day
                      , subq_35.listing__ds__extract_dow
                      , subq_35.listing__ds__extract_doy
                      , subq_35.listing__created_at__day
                      , subq_35.listing__created_at__week
                      , subq_35.listing__created_at__month
                      , subq_35.listing__created_at__quarter
                      , subq_35.listing__created_at__year
                      , subq_35.listing__created_at__extract_year
                      , subq_35.listing__created_at__extract_quarter
                      , subq_35.listing__created_at__extract_month
                      , subq_35.listing__created_at__extract_day
                      , subq_35.listing__created_at__extract_dow
                      , subq_35.listing__created_at__extract_doy
                      , subq_35.ds__day AS metric_time__day
                      , subq_35.ds__week AS metric_time__week
                      , subq_35.ds__month AS metric_time__month
                      , subq_35.ds__quarter AS metric_time__quarter
                      , subq_35.ds__year AS metric_time__year
                      , subq_35.ds__extract_year AS metric_time__extract_year
                      , subq_35.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_35.ds__extract_month AS metric_time__extract_month
                      , subq_35.ds__extract_day AS metric_time__extract_day
                      , subq_35.ds__extract_dow AS metric_time__extract_dow
                      , subq_35.ds__extract_doy AS metric_time__extract_doy
                      , subq_35.listing
                      , subq_35.user
                      , subq_35.listing__user
                      , subq_35.country_latest
                      , subq_35.is_lux_latest
                      , subq_35.capacity_latest
                      , subq_35.listing__country_latest
                      , subq_35.listing__is_lux_latest
                      , subq_35.listing__capacity_latest
                      , subq_35.listings
                      , subq_35.largest_listing
                      , subq_35.smallest_listing
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
                    ) subq_35
                  ) subq_36
                ) subq_37
                ON
                  subq_34.listing = subq_37.listing
              ) subq_38
            ) subq_39
            WHERE (listing__is_lux_latest) AND (booking__is_instant)
          ) subq_40
        ) subq_41
      ) subq_42
    ) subq_43
    CROSS JOIN (
      -- Compute Metrics via Expressions
      SELECT
        subq_50.booking_value
      FROM (
        -- Aggregate Measures
        SELECT
          SUM(subq_49.booking_value) AS booking_value
        FROM (
          -- Pass Only Elements: ['booking_value',]
          SELECT
            subq_48.booking_value
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_47.booking__is_instant
              , subq_47.booking_value
            FROM (
              -- Pass Only Elements: ['booking_value', 'booking__is_instant']
              SELECT
                subq_46.booking__is_instant
                , subq_46.booking_value
              FROM (
                -- Constrain Output with WHERE
                SELECT
                  subq_45.ds__day
                  , subq_45.ds__week
                  , subq_45.ds__month
                  , subq_45.ds__quarter
                  , subq_45.ds__year
                  , subq_45.ds__extract_year
                  , subq_45.ds__extract_quarter
                  , subq_45.ds__extract_month
                  , subq_45.ds__extract_day
                  , subq_45.ds__extract_dow
                  , subq_45.ds__extract_doy
                  , subq_45.ds_partitioned__day
                  , subq_45.ds_partitioned__week
                  , subq_45.ds_partitioned__month
                  , subq_45.ds_partitioned__quarter
                  , subq_45.ds_partitioned__year
                  , subq_45.ds_partitioned__extract_year
                  , subq_45.ds_partitioned__extract_quarter
                  , subq_45.ds_partitioned__extract_month
                  , subq_45.ds_partitioned__extract_day
                  , subq_45.ds_partitioned__extract_dow
                  , subq_45.ds_partitioned__extract_doy
                  , subq_45.paid_at__day
                  , subq_45.paid_at__week
                  , subq_45.paid_at__month
                  , subq_45.paid_at__quarter
                  , subq_45.paid_at__year
                  , subq_45.paid_at__extract_year
                  , subq_45.paid_at__extract_quarter
                  , subq_45.paid_at__extract_month
                  , subq_45.paid_at__extract_day
                  , subq_45.paid_at__extract_dow
                  , subq_45.paid_at__extract_doy
                  , subq_45.booking__ds__day
                  , subq_45.booking__ds__week
                  , subq_45.booking__ds__month
                  , subq_45.booking__ds__quarter
                  , subq_45.booking__ds__year
                  , subq_45.booking__ds__extract_year
                  , subq_45.booking__ds__extract_quarter
                  , subq_45.booking__ds__extract_month
                  , subq_45.booking__ds__extract_day
                  , subq_45.booking__ds__extract_dow
                  , subq_45.booking__ds__extract_doy
                  , subq_45.booking__ds_partitioned__day
                  , subq_45.booking__ds_partitioned__week
                  , subq_45.booking__ds_partitioned__month
                  , subq_45.booking__ds_partitioned__quarter
                  , subq_45.booking__ds_partitioned__year
                  , subq_45.booking__ds_partitioned__extract_year
                  , subq_45.booking__ds_partitioned__extract_quarter
                  , subq_45.booking__ds_partitioned__extract_month
                  , subq_45.booking__ds_partitioned__extract_day
                  , subq_45.booking__ds_partitioned__extract_dow
                  , subq_45.booking__ds_partitioned__extract_doy
                  , subq_45.booking__paid_at__day
                  , subq_45.booking__paid_at__week
                  , subq_45.booking__paid_at__month
                  , subq_45.booking__paid_at__quarter
                  , subq_45.booking__paid_at__year
                  , subq_45.booking__paid_at__extract_year
                  , subq_45.booking__paid_at__extract_quarter
                  , subq_45.booking__paid_at__extract_month
                  , subq_45.booking__paid_at__extract_day
                  , subq_45.booking__paid_at__extract_dow
                  , subq_45.booking__paid_at__extract_doy
                  , subq_45.metric_time__day
                  , subq_45.metric_time__week
                  , subq_45.metric_time__month
                  , subq_45.metric_time__quarter
                  , subq_45.metric_time__year
                  , subq_45.metric_time__extract_year
                  , subq_45.metric_time__extract_quarter
                  , subq_45.metric_time__extract_month
                  , subq_45.metric_time__extract_day
                  , subq_45.metric_time__extract_dow
                  , subq_45.metric_time__extract_doy
                  , subq_45.listing
                  , subq_45.guest
                  , subq_45.host
                  , subq_45.booking__listing
                  , subq_45.booking__guest
                  , subq_45.booking__host
                  , subq_45.is_instant
                  , subq_45.booking__is_instant
                  , subq_45.bookings
                  , subq_45.instant_bookings
                  , subq_45.booking_value
                  , subq_45.max_booking_value
                  , subq_45.min_booking_value
                  , subq_45.bookers
                  , subq_45.average_booking_value
                  , subq_45.referred_bookings
                  , subq_45.median_booking_value
                  , subq_45.booking_value_p99
                  , subq_45.discrete_booking_value_p99
                  , subq_45.approximate_continuous_booking_value_p99
                  , subq_45.approximate_discrete_booking_value_p99
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_44.ds__day
                    , subq_44.ds__week
                    , subq_44.ds__month
                    , subq_44.ds__quarter
                    , subq_44.ds__year
                    , subq_44.ds__extract_year
                    , subq_44.ds__extract_quarter
                    , subq_44.ds__extract_month
                    , subq_44.ds__extract_day
                    , subq_44.ds__extract_dow
                    , subq_44.ds__extract_doy
                    , subq_44.ds_partitioned__day
                    , subq_44.ds_partitioned__week
                    , subq_44.ds_partitioned__month
                    , subq_44.ds_partitioned__quarter
                    , subq_44.ds_partitioned__year
                    , subq_44.ds_partitioned__extract_year
                    , subq_44.ds_partitioned__extract_quarter
                    , subq_44.ds_partitioned__extract_month
                    , subq_44.ds_partitioned__extract_day
                    , subq_44.ds_partitioned__extract_dow
                    , subq_44.ds_partitioned__extract_doy
                    , subq_44.paid_at__day
                    , subq_44.paid_at__week
                    , subq_44.paid_at__month
                    , subq_44.paid_at__quarter
                    , subq_44.paid_at__year
                    , subq_44.paid_at__extract_year
                    , subq_44.paid_at__extract_quarter
                    , subq_44.paid_at__extract_month
                    , subq_44.paid_at__extract_day
                    , subq_44.paid_at__extract_dow
                    , subq_44.paid_at__extract_doy
                    , subq_44.booking__ds__day
                    , subq_44.booking__ds__week
                    , subq_44.booking__ds__month
                    , subq_44.booking__ds__quarter
                    , subq_44.booking__ds__year
                    , subq_44.booking__ds__extract_year
                    , subq_44.booking__ds__extract_quarter
                    , subq_44.booking__ds__extract_month
                    , subq_44.booking__ds__extract_day
                    , subq_44.booking__ds__extract_dow
                    , subq_44.booking__ds__extract_doy
                    , subq_44.booking__ds_partitioned__day
                    , subq_44.booking__ds_partitioned__week
                    , subq_44.booking__ds_partitioned__month
                    , subq_44.booking__ds_partitioned__quarter
                    , subq_44.booking__ds_partitioned__year
                    , subq_44.booking__ds_partitioned__extract_year
                    , subq_44.booking__ds_partitioned__extract_quarter
                    , subq_44.booking__ds_partitioned__extract_month
                    , subq_44.booking__ds_partitioned__extract_day
                    , subq_44.booking__ds_partitioned__extract_dow
                    , subq_44.booking__ds_partitioned__extract_doy
                    , subq_44.booking__paid_at__day
                    , subq_44.booking__paid_at__week
                    , subq_44.booking__paid_at__month
                    , subq_44.booking__paid_at__quarter
                    , subq_44.booking__paid_at__year
                    , subq_44.booking__paid_at__extract_year
                    , subq_44.booking__paid_at__extract_quarter
                    , subq_44.booking__paid_at__extract_month
                    , subq_44.booking__paid_at__extract_day
                    , subq_44.booking__paid_at__extract_dow
                    , subq_44.booking__paid_at__extract_doy
                    , subq_44.ds__day AS metric_time__day
                    , subq_44.ds__week AS metric_time__week
                    , subq_44.ds__month AS metric_time__month
                    , subq_44.ds__quarter AS metric_time__quarter
                    , subq_44.ds__year AS metric_time__year
                    , subq_44.ds__extract_year AS metric_time__extract_year
                    , subq_44.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_44.ds__extract_month AS metric_time__extract_month
                    , subq_44.ds__extract_day AS metric_time__extract_day
                    , subq_44.ds__extract_dow AS metric_time__extract_dow
                    , subq_44.ds__extract_doy AS metric_time__extract_doy
                    , subq_44.listing
                    , subq_44.guest
                    , subq_44.host
                    , subq_44.booking__listing
                    , subq_44.booking__guest
                    , subq_44.booking__host
                    , subq_44.is_instant
                    , subq_44.booking__is_instant
                    , subq_44.bookings
                    , subq_44.instant_bookings
                    , subq_44.booking_value
                    , subq_44.max_booking_value
                    , subq_44.min_booking_value
                    , subq_44.bookers
                    , subq_44.average_booking_value
                    , subq_44.referred_bookings
                    , subq_44.median_booking_value
                    , subq_44.booking_value_p99
                    , subq_44.discrete_booking_value_p99
                    , subq_44.approximate_continuous_booking_value_p99
                    , subq_44.approximate_discrete_booking_value_p99
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
                  ) subq_44
                ) subq_45
                WHERE booking__is_instant
              ) subq_46
            ) subq_47
            WHERE booking__is_instant
          ) subq_48
        ) subq_49
      ) subq_50
    ) subq_51
  ) subq_52
) subq_53
