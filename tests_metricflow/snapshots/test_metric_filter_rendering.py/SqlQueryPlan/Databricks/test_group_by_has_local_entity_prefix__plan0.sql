-- Compute Metrics via Expressions
SELECT
  subq_27.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_26.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      subq_25.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_24.user__listing__user__average_booking_value
        , subq_24.listings
      FROM (
        -- Pass Only Elements: ['listings', 'user__listing__user__average_booking_value']
        SELECT
          subq_23.user__listing__user__average_booking_value
          , subq_23.listings
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_11.user AS user
            , subq_22.listing__user AS user__listing__user
            , subq_22.listing__user__average_booking_value AS user__listing__user__average_booking_value
            , subq_11.listings AS listings
          FROM (
            -- Pass Only Elements: ['listings', 'user']
            SELECT
              subq_10.user
              , subq_10.listings
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
                , subq_9.created_at__day
                , subq_9.created_at__week
                , subq_9.created_at__month
                , subq_9.created_at__quarter
                , subq_9.created_at__year
                , subq_9.created_at__extract_year
                , subq_9.created_at__extract_quarter
                , subq_9.created_at__extract_month
                , subq_9.created_at__extract_day
                , subq_9.created_at__extract_dow
                , subq_9.created_at__extract_doy
                , subq_9.listing__ds__day
                , subq_9.listing__ds__week
                , subq_9.listing__ds__month
                , subq_9.listing__ds__quarter
                , subq_9.listing__ds__year
                , subq_9.listing__ds__extract_year
                , subq_9.listing__ds__extract_quarter
                , subq_9.listing__ds__extract_month
                , subq_9.listing__ds__extract_day
                , subq_9.listing__ds__extract_dow
                , subq_9.listing__ds__extract_doy
                , subq_9.listing__created_at__day
                , subq_9.listing__created_at__week
                , subq_9.listing__created_at__month
                , subq_9.listing__created_at__quarter
                , subq_9.listing__created_at__year
                , subq_9.listing__created_at__extract_year
                , subq_9.listing__created_at__extract_quarter
                , subq_9.listing__created_at__extract_month
                , subq_9.listing__created_at__extract_day
                , subq_9.listing__created_at__extract_dow
                , subq_9.listing__created_at__extract_doy
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
                , subq_9.user
                , subq_9.listing__user
                , subq_9.country_latest
                , subq_9.is_lux_latest
                , subq_9.capacity_latest
                , subq_9.listing__country_latest
                , subq_9.listing__is_lux_latest
                , subq_9.listing__capacity_latest
                , subq_9.listings
                , subq_9.largest_listing
                , subq_9.smallest_listing
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
              ) subq_9
            ) subq_10
          ) subq_11
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['listing__user', 'listing__user__average_booking_value']
            SELECT
              subq_21.listing__user
              , subq_21.listing__user__average_booking_value
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_20.listing__user
                , subq_20.average_booking_value AS listing__user__average_booking_value
              FROM (
                -- Aggregate Measures
                SELECT
                  subq_19.listing__user
                  , AVG(subq_19.average_booking_value) AS average_booking_value
                FROM (
                  -- Pass Only Elements: ['average_booking_value', 'listing__user']
                  SELECT
                    subq_18.listing__user
                    , subq_18.average_booking_value
                  FROM (
                    -- Join Standard Outputs
                    SELECT
                      subq_14.listing AS listing
                      , subq_17.user AS listing__user
                      , subq_14.average_booking_value AS average_booking_value
                    FROM (
                      -- Pass Only Elements: ['average_booking_value', 'listing']
                      SELECT
                        subq_13.listing
                        , subq_13.average_booking_value
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
                        ) subq_12
                      ) subq_13
                    ) subq_14
                    LEFT OUTER JOIN (
                      -- Pass Only Elements: ['listing', 'user']
                      SELECT
                        subq_16.listing
                        , subq_16.user
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
                        ) subq_15
                      ) subq_16
                    ) subq_17
                    ON
                      subq_14.listing = subq_17.listing
                  ) subq_18
                ) subq_19
                GROUP BY
                  subq_19.listing__user
              ) subq_20
            ) subq_21
          ) subq_22
          ON
            subq_11.user = subq_22.listing__user
        ) subq_23
      ) subq_24
      WHERE user__listing__user__average_booking_value > 1
    ) subq_25
  ) subq_26
) subq_27
