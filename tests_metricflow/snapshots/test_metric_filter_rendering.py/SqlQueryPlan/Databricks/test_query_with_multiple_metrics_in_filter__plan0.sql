-- Compute Metrics via Expressions
SELECT
  subq_19.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_18.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      subq_17.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_16.listing__bookings
        , subq_16.listing__bookers
        , subq_16.listings
      FROM (
        -- Pass Only Elements: ['listings', 'listing__bookings', 'listing__bookers']
        SELECT
          subq_15.listing__bookings
          , subq_15.listing__bookers
          , subq_15.listings
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_2.listing AS listing
            , subq_8.listing__bookings AS listing__bookings
            , subq_14.listing__bookers AS listing__bookers
            , subq_2.listings AS listings
          FROM (
            -- Pass Only Elements: ['listings', 'listing', 'listing']
            SELECT
              subq_1.listing
              , subq_1.listings
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
              ) subq_0
            ) subq_1
          ) subq_2
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['listing', 'listing__bookings']
            SELECT
              subq_7.listing
              , subq_7.listing__bookings
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_6.listing
                , subq_6.bookings AS listing__bookings
              FROM (
                -- Aggregate Measures
                SELECT
                  subq_5.listing
                  , SUM(subq_5.bookings) AS bookings
                FROM (
                  -- Pass Only Elements: ['bookings', 'listing']
                  SELECT
                    subq_4.listing
                    , subq_4.bookings
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
                      , subq_3.ds_partitioned__day
                      , subq_3.ds_partitioned__week
                      , subq_3.ds_partitioned__month
                      , subq_3.ds_partitioned__quarter
                      , subq_3.ds_partitioned__year
                      , subq_3.ds_partitioned__extract_year
                      , subq_3.ds_partitioned__extract_quarter
                      , subq_3.ds_partitioned__extract_month
                      , subq_3.ds_partitioned__extract_day
                      , subq_3.ds_partitioned__extract_dow
                      , subq_3.ds_partitioned__extract_doy
                      , subq_3.paid_at__day
                      , subq_3.paid_at__week
                      , subq_3.paid_at__month
                      , subq_3.paid_at__quarter
                      , subq_3.paid_at__year
                      , subq_3.paid_at__extract_year
                      , subq_3.paid_at__extract_quarter
                      , subq_3.paid_at__extract_month
                      , subq_3.paid_at__extract_day
                      , subq_3.paid_at__extract_dow
                      , subq_3.paid_at__extract_doy
                      , subq_3.booking__ds__day
                      , subq_3.booking__ds__week
                      , subq_3.booking__ds__month
                      , subq_3.booking__ds__quarter
                      , subq_3.booking__ds__year
                      , subq_3.booking__ds__extract_year
                      , subq_3.booking__ds__extract_quarter
                      , subq_3.booking__ds__extract_month
                      , subq_3.booking__ds__extract_day
                      , subq_3.booking__ds__extract_dow
                      , subq_3.booking__ds__extract_doy
                      , subq_3.booking__ds_partitioned__day
                      , subq_3.booking__ds_partitioned__week
                      , subq_3.booking__ds_partitioned__month
                      , subq_3.booking__ds_partitioned__quarter
                      , subq_3.booking__ds_partitioned__year
                      , subq_3.booking__ds_partitioned__extract_year
                      , subq_3.booking__ds_partitioned__extract_quarter
                      , subq_3.booking__ds_partitioned__extract_month
                      , subq_3.booking__ds_partitioned__extract_day
                      , subq_3.booking__ds_partitioned__extract_dow
                      , subq_3.booking__ds_partitioned__extract_doy
                      , subq_3.booking__paid_at__day
                      , subq_3.booking__paid_at__week
                      , subq_3.booking__paid_at__month
                      , subq_3.booking__paid_at__quarter
                      , subq_3.booking__paid_at__year
                      , subq_3.booking__paid_at__extract_year
                      , subq_3.booking__paid_at__extract_quarter
                      , subq_3.booking__paid_at__extract_month
                      , subq_3.booking__paid_at__extract_day
                      , subq_3.booking__paid_at__extract_dow
                      , subq_3.booking__paid_at__extract_doy
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
                      , subq_3.guest
                      , subq_3.host
                      , subq_3.booking__listing
                      , subq_3.booking__guest
                      , subq_3.booking__host
                      , subq_3.is_instant
                      , subq_3.booking__is_instant
                      , subq_3.bookings
                      , subq_3.instant_bookings
                      , subq_3.booking_value
                      , subq_3.max_booking_value
                      , subq_3.min_booking_value
                      , subq_3.bookers
                      , subq_3.average_booking_value
                      , subq_3.referred_bookings
                      , subq_3.median_booking_value
                      , subq_3.booking_value_p99
                      , subq_3.discrete_booking_value_p99
                      , subq_3.approximate_continuous_booking_value_p99
                      , subq_3.approximate_discrete_booking_value_p99
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
                    ) subq_3
                  ) subq_4
                ) subq_5
                GROUP BY
                  subq_5.listing
              ) subq_6
            ) subq_7
          ) subq_8
          ON
            subq_2.listing = subq_8.listing
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['listing', 'listing__bookers']
            SELECT
              subq_13.listing
              , subq_13.listing__bookers
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_12.listing
                , subq_12.bookers AS listing__bookers
              FROM (
                -- Aggregate Measures
                SELECT
                  subq_11.listing
                  , COUNT(DISTINCT subq_11.bookers) AS bookers
                FROM (
                  -- Pass Only Elements: ['bookers', 'listing']
                  SELECT
                    subq_10.listing
                    , subq_10.bookers
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
                      , subq_9.ds_partitioned__day
                      , subq_9.ds_partitioned__week
                      , subq_9.ds_partitioned__month
                      , subq_9.ds_partitioned__quarter
                      , subq_9.ds_partitioned__year
                      , subq_9.ds_partitioned__extract_year
                      , subq_9.ds_partitioned__extract_quarter
                      , subq_9.ds_partitioned__extract_month
                      , subq_9.ds_partitioned__extract_day
                      , subq_9.ds_partitioned__extract_dow
                      , subq_9.ds_partitioned__extract_doy
                      , subq_9.paid_at__day
                      , subq_9.paid_at__week
                      , subq_9.paid_at__month
                      , subq_9.paid_at__quarter
                      , subq_9.paid_at__year
                      , subq_9.paid_at__extract_year
                      , subq_9.paid_at__extract_quarter
                      , subq_9.paid_at__extract_month
                      , subq_9.paid_at__extract_day
                      , subq_9.paid_at__extract_dow
                      , subq_9.paid_at__extract_doy
                      , subq_9.booking__ds__day
                      , subq_9.booking__ds__week
                      , subq_9.booking__ds__month
                      , subq_9.booking__ds__quarter
                      , subq_9.booking__ds__year
                      , subq_9.booking__ds__extract_year
                      , subq_9.booking__ds__extract_quarter
                      , subq_9.booking__ds__extract_month
                      , subq_9.booking__ds__extract_day
                      , subq_9.booking__ds__extract_dow
                      , subq_9.booking__ds__extract_doy
                      , subq_9.booking__ds_partitioned__day
                      , subq_9.booking__ds_partitioned__week
                      , subq_9.booking__ds_partitioned__month
                      , subq_9.booking__ds_partitioned__quarter
                      , subq_9.booking__ds_partitioned__year
                      , subq_9.booking__ds_partitioned__extract_year
                      , subq_9.booking__ds_partitioned__extract_quarter
                      , subq_9.booking__ds_partitioned__extract_month
                      , subq_9.booking__ds_partitioned__extract_day
                      , subq_9.booking__ds_partitioned__extract_dow
                      , subq_9.booking__ds_partitioned__extract_doy
                      , subq_9.booking__paid_at__day
                      , subq_9.booking__paid_at__week
                      , subq_9.booking__paid_at__month
                      , subq_9.booking__paid_at__quarter
                      , subq_9.booking__paid_at__year
                      , subq_9.booking__paid_at__extract_year
                      , subq_9.booking__paid_at__extract_quarter
                      , subq_9.booking__paid_at__extract_month
                      , subq_9.booking__paid_at__extract_day
                      , subq_9.booking__paid_at__extract_dow
                      , subq_9.booking__paid_at__extract_doy
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
                      , subq_9.guest
                      , subq_9.host
                      , subq_9.booking__listing
                      , subq_9.booking__guest
                      , subq_9.booking__host
                      , subq_9.is_instant
                      , subq_9.booking__is_instant
                      , subq_9.bookings
                      , subq_9.instant_bookings
                      , subq_9.booking_value
                      , subq_9.max_booking_value
                      , subq_9.min_booking_value
                      , subq_9.bookers
                      , subq_9.average_booking_value
                      , subq_9.referred_bookings
                      , subq_9.median_booking_value
                      , subq_9.booking_value_p99
                      , subq_9.discrete_booking_value_p99
                      , subq_9.approximate_continuous_booking_value_p99
                      , subq_9.approximate_discrete_booking_value_p99
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
                    ) subq_9
                  ) subq_10
                ) subq_11
                GROUP BY
                  subq_11.listing
              ) subq_12
            ) subq_13
          ) subq_14
          ON
            subq_2.listing = subq_14.listing
        ) subq_15
      ) subq_16
      WHERE listing__bookings > 2 AND listing__bookers > 1
    ) subq_17
  ) subq_18
) subq_19
