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
        subq_24.listing__bookings
        , subq_24.listing__bookers
        , subq_24.listings
      FROM (
        -- Pass Only Elements: ['listings', 'listing__bookings', 'listing__bookers']
        SELECT
          subq_23.listing__bookings
          , subq_23.listing__bookers
          , subq_23.listings
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_10.listing AS listing
            , subq_16.listing__bookings AS listing__bookings
            , subq_22.listing__bookers AS listing__bookers
            , subq_10.listings AS listings
          FROM (
            -- Pass Only Elements: ['listings', 'listing', 'listing']
            SELECT
              subq_9.listing
              , subq_9.listings
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
                , subq_8.created_at__day
                , subq_8.created_at__week
                , subq_8.created_at__month
                , subq_8.created_at__quarter
                , subq_8.created_at__year
                , subq_8.created_at__extract_year
                , subq_8.created_at__extract_quarter
                , subq_8.created_at__extract_month
                , subq_8.created_at__extract_day
                , subq_8.created_at__extract_dow
                , subq_8.created_at__extract_doy
                , subq_8.listing__ds__day
                , subq_8.listing__ds__week
                , subq_8.listing__ds__month
                , subq_8.listing__ds__quarter
                , subq_8.listing__ds__year
                , subq_8.listing__ds__extract_year
                , subq_8.listing__ds__extract_quarter
                , subq_8.listing__ds__extract_month
                , subq_8.listing__ds__extract_day
                , subq_8.listing__ds__extract_dow
                , subq_8.listing__ds__extract_doy
                , subq_8.listing__created_at__day
                , subq_8.listing__created_at__week
                , subq_8.listing__created_at__month
                , subq_8.listing__created_at__quarter
                , subq_8.listing__created_at__year
                , subq_8.listing__created_at__extract_year
                , subq_8.listing__created_at__extract_quarter
                , subq_8.listing__created_at__extract_month
                , subq_8.listing__created_at__extract_day
                , subq_8.listing__created_at__extract_dow
                , subq_8.listing__created_at__extract_doy
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
                , subq_8.user
                , subq_8.listing__user
                , subq_8.country_latest
                , subq_8.is_lux_latest
                , subq_8.capacity_latest
                , subq_8.listing__country_latest
                , subq_8.listing__is_lux_latest
                , subq_8.listing__capacity_latest
                , subq_8.listings
                , subq_8.largest_listing
                , subq_8.smallest_listing
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
              ) subq_8
            ) subq_9
          ) subq_10
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['listing', 'listing__bookings']
            SELECT
              subq_15.listing
              , subq_15.listing__bookings
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_14.listing
                , subq_14.bookings AS listing__bookings
              FROM (
                -- Aggregate Measures
                SELECT
                  subq_13.listing
                  , SUM(subq_13.bookings) AS bookings
                FROM (
                  -- Pass Only Elements: ['bookings', 'listing']
                  SELECT
                    subq_12.listing
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
                      , subq_11.booking__listing
                      , subq_11.booking__guest
                      , subq_11.booking__host
                      , subq_11.is_instant
                      , subq_11.booking__is_instant
                      , subq_11.bookings
                      , subq_11.instant_bookings
                      , subq_11.booking_value
                      , subq_11.max_booking_value
                      , subq_11.min_booking_value
                      , subq_11.bookers
                      , subq_11.average_booking_value
                      , subq_11.referred_bookings
                      , subq_11.median_booking_value
                      , subq_11.booking_value_p99
                      , subq_11.discrete_booking_value_p99
                      , subq_11.approximate_continuous_booking_value_p99
                      , subq_11.approximate_discrete_booking_value_p99
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
                    ) subq_11
                  ) subq_12
                ) subq_13
                GROUP BY
                  subq_13.listing
              ) subq_14
            ) subq_15
          ) subq_16
          ON
            subq_10.listing = subq_16.listing
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['listing', 'listing__bookers']
            SELECT
              subq_21.listing
              , subq_21.listing__bookers
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_20.listing
                , subq_20.bookers AS listing__bookers
              FROM (
                -- Aggregate Measures
                SELECT
                  subq_19.listing
                  , COUNT(DISTINCT subq_19.bookers) AS bookers
                FROM (
                  -- Pass Only Elements: ['bookers', 'listing']
                  SELECT
                    subq_18.listing
                    , subq_18.bookers
                  FROM (
                    -- Metric Time Dimension 'ds'
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
                      , subq_17.ds_partitioned__day
                      , subq_17.ds_partitioned__week
                      , subq_17.ds_partitioned__month
                      , subq_17.ds_partitioned__quarter
                      , subq_17.ds_partitioned__year
                      , subq_17.ds_partitioned__extract_year
                      , subq_17.ds_partitioned__extract_quarter
                      , subq_17.ds_partitioned__extract_month
                      , subq_17.ds_partitioned__extract_day
                      , subq_17.ds_partitioned__extract_dow
                      , subq_17.ds_partitioned__extract_doy
                      , subq_17.paid_at__day
                      , subq_17.paid_at__week
                      , subq_17.paid_at__month
                      , subq_17.paid_at__quarter
                      , subq_17.paid_at__year
                      , subq_17.paid_at__extract_year
                      , subq_17.paid_at__extract_quarter
                      , subq_17.paid_at__extract_month
                      , subq_17.paid_at__extract_day
                      , subq_17.paid_at__extract_dow
                      , subq_17.paid_at__extract_doy
                      , subq_17.booking__ds__day
                      , subq_17.booking__ds__week
                      , subq_17.booking__ds__month
                      , subq_17.booking__ds__quarter
                      , subq_17.booking__ds__year
                      , subq_17.booking__ds__extract_year
                      , subq_17.booking__ds__extract_quarter
                      , subq_17.booking__ds__extract_month
                      , subq_17.booking__ds__extract_day
                      , subq_17.booking__ds__extract_dow
                      , subq_17.booking__ds__extract_doy
                      , subq_17.booking__ds_partitioned__day
                      , subq_17.booking__ds_partitioned__week
                      , subq_17.booking__ds_partitioned__month
                      , subq_17.booking__ds_partitioned__quarter
                      , subq_17.booking__ds_partitioned__year
                      , subq_17.booking__ds_partitioned__extract_year
                      , subq_17.booking__ds_partitioned__extract_quarter
                      , subq_17.booking__ds_partitioned__extract_month
                      , subq_17.booking__ds_partitioned__extract_day
                      , subq_17.booking__ds_partitioned__extract_dow
                      , subq_17.booking__ds_partitioned__extract_doy
                      , subq_17.booking__paid_at__day
                      , subq_17.booking__paid_at__week
                      , subq_17.booking__paid_at__month
                      , subq_17.booking__paid_at__quarter
                      , subq_17.booking__paid_at__year
                      , subq_17.booking__paid_at__extract_year
                      , subq_17.booking__paid_at__extract_quarter
                      , subq_17.booking__paid_at__extract_month
                      , subq_17.booking__paid_at__extract_day
                      , subq_17.booking__paid_at__extract_dow
                      , subq_17.booking__paid_at__extract_doy
                      , subq_17.ds__day AS metric_time__day
                      , subq_17.ds__week AS metric_time__week
                      , subq_17.ds__month AS metric_time__month
                      , subq_17.ds__quarter AS metric_time__quarter
                      , subq_17.ds__year AS metric_time__year
                      , subq_17.ds__extract_year AS metric_time__extract_year
                      , subq_17.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_17.ds__extract_month AS metric_time__extract_month
                      , subq_17.ds__extract_day AS metric_time__extract_day
                      , subq_17.ds__extract_dow AS metric_time__extract_dow
                      , subq_17.ds__extract_doy AS metric_time__extract_doy
                      , subq_17.listing
                      , subq_17.guest
                      , subq_17.host
                      , subq_17.booking__listing
                      , subq_17.booking__guest
                      , subq_17.booking__host
                      , subq_17.is_instant
                      , subq_17.booking__is_instant
                      , subq_17.bookings
                      , subq_17.instant_bookings
                      , subq_17.booking_value
                      , subq_17.max_booking_value
                      , subq_17.min_booking_value
                      , subq_17.bookers
                      , subq_17.average_booking_value
                      , subq_17.referred_bookings
                      , subq_17.median_booking_value
                      , subq_17.booking_value_p99
                      , subq_17.discrete_booking_value_p99
                      , subq_17.approximate_continuous_booking_value_p99
                      , subq_17.approximate_discrete_booking_value_p99
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
                    ) subq_17
                  ) subq_18
                ) subq_19
                GROUP BY
                  subq_19.listing
              ) subq_20
            ) subq_21
          ) subq_22
          ON
            subq_10.listing = subq_22.listing
        ) subq_23
      ) subq_24
      WHERE listing__bookings > 2 AND listing__bookers > 1
    ) subq_25
  ) subq_26
) subq_27
