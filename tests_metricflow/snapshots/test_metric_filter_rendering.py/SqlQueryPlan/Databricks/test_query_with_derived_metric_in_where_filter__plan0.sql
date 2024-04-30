-- Compute Metrics via Expressions
SELECT
  subq_31.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_30.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      subq_29.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_28.listing__views_times_booking_value
        , subq_28.listings
      FROM (
        -- Pass Only Elements: ['listings', 'listing__views_times_booking_value']
        SELECT
          subq_27.listing__views_times_booking_value
          , subq_27.listings
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_13.listing AS listing
            , subq_26.views_times_booking_value AS listing__views_times_booking_value
            , subq_13.listings AS listings
          FROM (
            -- Pass Only Elements: ['listings', 'listing']
            SELECT
              subq_12.listing
              , subq_12.listings
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
                , subq_11.created_at__day
                , subq_11.created_at__week
                , subq_11.created_at__month
                , subq_11.created_at__quarter
                , subq_11.created_at__year
                , subq_11.created_at__extract_year
                , subq_11.created_at__extract_quarter
                , subq_11.created_at__extract_month
                , subq_11.created_at__extract_day
                , subq_11.created_at__extract_dow
                , subq_11.created_at__extract_doy
                , subq_11.listing__ds__day
                , subq_11.listing__ds__week
                , subq_11.listing__ds__month
                , subq_11.listing__ds__quarter
                , subq_11.listing__ds__year
                , subq_11.listing__ds__extract_year
                , subq_11.listing__ds__extract_quarter
                , subq_11.listing__ds__extract_month
                , subq_11.listing__ds__extract_day
                , subq_11.listing__ds__extract_dow
                , subq_11.listing__ds__extract_doy
                , subq_11.listing__created_at__day
                , subq_11.listing__created_at__week
                , subq_11.listing__created_at__month
                , subq_11.listing__created_at__quarter
                , subq_11.listing__created_at__year
                , subq_11.listing__created_at__extract_year
                , subq_11.listing__created_at__extract_quarter
                , subq_11.listing__created_at__extract_month
                , subq_11.listing__created_at__extract_day
                , subq_11.listing__created_at__extract_dow
                , subq_11.listing__created_at__extract_doy
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
                , subq_11.user
                , subq_11.listing__user
                , subq_11.country_latest
                , subq_11.is_lux_latest
                , subq_11.capacity_latest
                , subq_11.listing__country_latest
                , subq_11.listing__is_lux_latest
                , subq_11.listing__capacity_latest
                , subq_11.listings
                , subq_11.largest_listing
                , subq_11.smallest_listing
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
              ) subq_11
            ) subq_12
          ) subq_13
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['listing', 'listing__views_times_booking_value']
            SELECT
              subq_25.listing
              , subq_25.views_times_booking_value
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_24.listing
                , booking_value * views AS views_times_booking_value
              FROM (
                -- Combine Aggregated Outputs
                SELECT
                  COALESCE(subq_18.listing, subq_23.listing) AS listing
                  , MAX(subq_18.booking_value) AS booking_value
                  , MAX(subq_23.views) AS views
                FROM (
                  -- Compute Metrics via Expressions
                  SELECT
                    subq_17.listing
                    , subq_17.booking_value
                  FROM (
                    -- Aggregate Measures
                    SELECT
                      subq_16.listing
                      , SUM(subq_16.booking_value) AS booking_value
                    FROM (
                      -- Pass Only Elements: ['booking_value', 'listing']
                      SELECT
                        subq_15.listing
                        , subq_15.booking_value
                      FROM (
                        -- Metric Time Dimension 'ds'
                        SELECT
                          subq_14.ds__day
                          , subq_14.ds__week
                          , subq_14.ds__month
                          , subq_14.ds__quarter
                          , subq_14.ds__year
                          , subq_14.ds__extract_year
                          , subq_14.ds__extract_quarter
                          , subq_14.ds__extract_month
                          , subq_14.ds__extract_day
                          , subq_14.ds__extract_dow
                          , subq_14.ds__extract_doy
                          , subq_14.ds_partitioned__day
                          , subq_14.ds_partitioned__week
                          , subq_14.ds_partitioned__month
                          , subq_14.ds_partitioned__quarter
                          , subq_14.ds_partitioned__year
                          , subq_14.ds_partitioned__extract_year
                          , subq_14.ds_partitioned__extract_quarter
                          , subq_14.ds_partitioned__extract_month
                          , subq_14.ds_partitioned__extract_day
                          , subq_14.ds_partitioned__extract_dow
                          , subq_14.ds_partitioned__extract_doy
                          , subq_14.paid_at__day
                          , subq_14.paid_at__week
                          , subq_14.paid_at__month
                          , subq_14.paid_at__quarter
                          , subq_14.paid_at__year
                          , subq_14.paid_at__extract_year
                          , subq_14.paid_at__extract_quarter
                          , subq_14.paid_at__extract_month
                          , subq_14.paid_at__extract_day
                          , subq_14.paid_at__extract_dow
                          , subq_14.paid_at__extract_doy
                          , subq_14.booking__ds__day
                          , subq_14.booking__ds__week
                          , subq_14.booking__ds__month
                          , subq_14.booking__ds__quarter
                          , subq_14.booking__ds__year
                          , subq_14.booking__ds__extract_year
                          , subq_14.booking__ds__extract_quarter
                          , subq_14.booking__ds__extract_month
                          , subq_14.booking__ds__extract_day
                          , subq_14.booking__ds__extract_dow
                          , subq_14.booking__ds__extract_doy
                          , subq_14.booking__ds_partitioned__day
                          , subq_14.booking__ds_partitioned__week
                          , subq_14.booking__ds_partitioned__month
                          , subq_14.booking__ds_partitioned__quarter
                          , subq_14.booking__ds_partitioned__year
                          , subq_14.booking__ds_partitioned__extract_year
                          , subq_14.booking__ds_partitioned__extract_quarter
                          , subq_14.booking__ds_partitioned__extract_month
                          , subq_14.booking__ds_partitioned__extract_day
                          , subq_14.booking__ds_partitioned__extract_dow
                          , subq_14.booking__ds_partitioned__extract_doy
                          , subq_14.booking__paid_at__day
                          , subq_14.booking__paid_at__week
                          , subq_14.booking__paid_at__month
                          , subq_14.booking__paid_at__quarter
                          , subq_14.booking__paid_at__year
                          , subq_14.booking__paid_at__extract_year
                          , subq_14.booking__paid_at__extract_quarter
                          , subq_14.booking__paid_at__extract_month
                          , subq_14.booking__paid_at__extract_day
                          , subq_14.booking__paid_at__extract_dow
                          , subq_14.booking__paid_at__extract_doy
                          , subq_14.ds__day AS metric_time__day
                          , subq_14.ds__week AS metric_time__week
                          , subq_14.ds__month AS metric_time__month
                          , subq_14.ds__quarter AS metric_time__quarter
                          , subq_14.ds__year AS metric_time__year
                          , subq_14.ds__extract_year AS metric_time__extract_year
                          , subq_14.ds__extract_quarter AS metric_time__extract_quarter
                          , subq_14.ds__extract_month AS metric_time__extract_month
                          , subq_14.ds__extract_day AS metric_time__extract_day
                          , subq_14.ds__extract_dow AS metric_time__extract_dow
                          , subq_14.ds__extract_doy AS metric_time__extract_doy
                          , subq_14.listing
                          , subq_14.guest
                          , subq_14.host
                          , subq_14.booking__listing
                          , subq_14.booking__guest
                          , subq_14.booking__host
                          , subq_14.is_instant
                          , subq_14.booking__is_instant
                          , subq_14.bookings
                          , subq_14.instant_bookings
                          , subq_14.booking_value
                          , subq_14.max_booking_value
                          , subq_14.min_booking_value
                          , subq_14.bookers
                          , subq_14.average_booking_value
                          , subq_14.referred_bookings
                          , subq_14.median_booking_value
                          , subq_14.booking_value_p99
                          , subq_14.discrete_booking_value_p99
                          , subq_14.approximate_continuous_booking_value_p99
                          , subq_14.approximate_discrete_booking_value_p99
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
                        ) subq_14
                      ) subq_15
                    ) subq_16
                    GROUP BY
                      subq_16.listing
                  ) subq_17
                ) subq_18
                FULL OUTER JOIN (
                  -- Compute Metrics via Expressions
                  SELECT
                    subq_22.listing
                    , subq_22.views
                  FROM (
                    -- Aggregate Measures
                    SELECT
                      subq_21.listing
                      , SUM(subq_21.views) AS views
                    FROM (
                      -- Pass Only Elements: ['views', 'listing']
                      SELECT
                        subq_20.listing
                        , subq_20.views
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
                          , subq_19.view__ds__day
                          , subq_19.view__ds__week
                          , subq_19.view__ds__month
                          , subq_19.view__ds__quarter
                          , subq_19.view__ds__year
                          , subq_19.view__ds__extract_year
                          , subq_19.view__ds__extract_quarter
                          , subq_19.view__ds__extract_month
                          , subq_19.view__ds__extract_day
                          , subq_19.view__ds__extract_dow
                          , subq_19.view__ds__extract_doy
                          , subq_19.view__ds_partitioned__day
                          , subq_19.view__ds_partitioned__week
                          , subq_19.view__ds_partitioned__month
                          , subq_19.view__ds_partitioned__quarter
                          , subq_19.view__ds_partitioned__year
                          , subq_19.view__ds_partitioned__extract_year
                          , subq_19.view__ds_partitioned__extract_quarter
                          , subq_19.view__ds_partitioned__extract_month
                          , subq_19.view__ds_partitioned__extract_day
                          , subq_19.view__ds_partitioned__extract_dow
                          , subq_19.view__ds_partitioned__extract_doy
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
                          , subq_19.user
                          , subq_19.view__listing
                          , subq_19.view__user
                          , subq_19.views
                        FROM (
                          -- Read Elements From Semantic Model 'views_source'
                          SELECT
                            1 AS views
                            , DATE_TRUNC('day', views_source_src_28000.ds) AS ds__day
                            , DATE_TRUNC('week', views_source_src_28000.ds) AS ds__week
                            , DATE_TRUNC('month', views_source_src_28000.ds) AS ds__month
                            , DATE_TRUNC('quarter', views_source_src_28000.ds) AS ds__quarter
                            , DATE_TRUNC('year', views_source_src_28000.ds) AS ds__year
                            , EXTRACT(year FROM views_source_src_28000.ds) AS ds__extract_year
                            , EXTRACT(quarter FROM views_source_src_28000.ds) AS ds__extract_quarter
                            , EXTRACT(month FROM views_source_src_28000.ds) AS ds__extract_month
                            , EXTRACT(day FROM views_source_src_28000.ds) AS ds__extract_day
                            , EXTRACT(DAYOFWEEK_ISO FROM views_source_src_28000.ds) AS ds__extract_dow
                            , EXTRACT(doy FROM views_source_src_28000.ds) AS ds__extract_doy
                            , DATE_TRUNC('day', views_source_src_28000.ds_partitioned) AS ds_partitioned__day
                            , DATE_TRUNC('week', views_source_src_28000.ds_partitioned) AS ds_partitioned__week
                            , DATE_TRUNC('month', views_source_src_28000.ds_partitioned) AS ds_partitioned__month
                            , DATE_TRUNC('quarter', views_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                            , DATE_TRUNC('year', views_source_src_28000.ds_partitioned) AS ds_partitioned__year
                            , EXTRACT(year FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                            , EXTRACT(quarter FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                            , EXTRACT(month FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                            , EXTRACT(day FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                            , EXTRACT(DAYOFWEEK_ISO FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
                            , EXTRACT(doy FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                            , DATE_TRUNC('day', views_source_src_28000.ds) AS view__ds__day
                            , DATE_TRUNC('week', views_source_src_28000.ds) AS view__ds__week
                            , DATE_TRUNC('month', views_source_src_28000.ds) AS view__ds__month
                            , DATE_TRUNC('quarter', views_source_src_28000.ds) AS view__ds__quarter
                            , DATE_TRUNC('year', views_source_src_28000.ds) AS view__ds__year
                            , EXTRACT(year FROM views_source_src_28000.ds) AS view__ds__extract_year
                            , EXTRACT(quarter FROM views_source_src_28000.ds) AS view__ds__extract_quarter
                            , EXTRACT(month FROM views_source_src_28000.ds) AS view__ds__extract_month
                            , EXTRACT(day FROM views_source_src_28000.ds) AS view__ds__extract_day
                            , EXTRACT(DAYOFWEEK_ISO FROM views_source_src_28000.ds) AS view__ds__extract_dow
                            , EXTRACT(doy FROM views_source_src_28000.ds) AS view__ds__extract_doy
                            , DATE_TRUNC('day', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__day
                            , DATE_TRUNC('week', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__week
                            , DATE_TRUNC('month', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__month
                            , DATE_TRUNC('quarter', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__quarter
                            , DATE_TRUNC('year', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__year
                            , EXTRACT(year FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_year
                            , EXTRACT(quarter FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_quarter
                            , EXTRACT(month FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_month
                            , EXTRACT(day FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_day
                            , EXTRACT(DAYOFWEEK_ISO FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_dow
                            , EXTRACT(doy FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_doy
                            , views_source_src_28000.listing_id AS listing
                            , views_source_src_28000.user_id AS user
                            , views_source_src_28000.listing_id AS view__listing
                            , views_source_src_28000.user_id AS view__user
                          FROM ***************************.fct_views views_source_src_28000
                        ) subq_19
                      ) subq_20
                    ) subq_21
                    GROUP BY
                      subq_21.listing
                  ) subq_22
                ) subq_23
                ON
                  subq_18.listing = subq_23.listing
                GROUP BY
                  COALESCE(subq_18.listing, subq_23.listing)
              ) subq_24
            ) subq_25
          ) subq_26
          ON
            subq_13.listing = subq_26.listing
        ) subq_27
      ) subq_28
      WHERE listing__views_times_booking_value > 1
    ) subq_29
  ) subq_30
) subq_31
