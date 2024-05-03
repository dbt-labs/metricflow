-- Compute Metrics via Expressions
SELECT
  subq_17.metric_time__day
  , subq_17.listings AS active_listings
FROM (
  -- Aggregate Measures
  SELECT
    subq_16.metric_time__day
    , SUM(subq_16.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings', 'metric_time__day']
    SELECT
      subq_15.metric_time__day
      , subq_15.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_14.metric_time__day
        , subq_14.listing__bookings
        , subq_14.listings
      FROM (
        -- Pass Only Elements: ['listings', 'metric_time__day', 'listing__bookings']
        SELECT
          subq_13.metric_time__day
          , subq_13.listing__bookings
          , subq_13.listings
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_6.metric_time__day AS metric_time__day
            , subq_6.listing AS listing
            , subq_12.listing__bookings AS listing__bookings
            , subq_6.listings AS listings
          FROM (
            -- Pass Only Elements: ['listings', 'metric_time__day', 'listing']
            SELECT
              subq_5.metric_time__day
              , subq_5.listing
              , subq_5.listings
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_4.ds__day
                , subq_4.ds__week
                , subq_4.ds__month
                , subq_4.ds__quarter
                , subq_4.ds__year
                , subq_4.ds__extract_year
                , subq_4.ds__extract_quarter
                , subq_4.ds__extract_month
                , subq_4.ds__extract_day
                , subq_4.ds__extract_dow
                , subq_4.ds__extract_doy
                , subq_4.created_at__day
                , subq_4.created_at__week
                , subq_4.created_at__month
                , subq_4.created_at__quarter
                , subq_4.created_at__year
                , subq_4.created_at__extract_year
                , subq_4.created_at__extract_quarter
                , subq_4.created_at__extract_month
                , subq_4.created_at__extract_day
                , subq_4.created_at__extract_dow
                , subq_4.created_at__extract_doy
                , subq_4.listing__ds__day
                , subq_4.listing__ds__week
                , subq_4.listing__ds__month
                , subq_4.listing__ds__quarter
                , subq_4.listing__ds__year
                , subq_4.listing__ds__extract_year
                , subq_4.listing__ds__extract_quarter
                , subq_4.listing__ds__extract_month
                , subq_4.listing__ds__extract_day
                , subq_4.listing__ds__extract_dow
                , subq_4.listing__ds__extract_doy
                , subq_4.listing__created_at__day
                , subq_4.listing__created_at__week
                , subq_4.listing__created_at__month
                , subq_4.listing__created_at__quarter
                , subq_4.listing__created_at__year
                , subq_4.listing__created_at__extract_year
                , subq_4.listing__created_at__extract_quarter
                , subq_4.listing__created_at__extract_month
                , subq_4.listing__created_at__extract_day
                , subq_4.listing__created_at__extract_dow
                , subq_4.listing__created_at__extract_doy
                , subq_4.ds__day AS metric_time__day
                , subq_4.ds__week AS metric_time__week
                , subq_4.ds__month AS metric_time__month
                , subq_4.ds__quarter AS metric_time__quarter
                , subq_4.ds__year AS metric_time__year
                , subq_4.ds__extract_year AS metric_time__extract_year
                , subq_4.ds__extract_quarter AS metric_time__extract_quarter
                , subq_4.ds__extract_month AS metric_time__extract_month
                , subq_4.ds__extract_day AS metric_time__extract_day
                , subq_4.ds__extract_dow AS metric_time__extract_dow
                , subq_4.ds__extract_doy AS metric_time__extract_doy
                , subq_4.listing
                , subq_4.user
                , subq_4.listing__user
                , subq_4.country_latest
                , subq_4.is_lux_latest
                , subq_4.capacity_latest
                , subq_4.listing__country_latest
                , subq_4.listing__is_lux_latest
                , subq_4.listing__capacity_latest
                , subq_4.listings
                , subq_4.largest_listing
                , subq_4.smallest_listing
              FROM (
                -- Read Elements From Semantic Model 'listings_latest'
                SELECT
                  1 AS listings
                  , listings_latest_src_28000.capacity AS largest_listing
                  , listings_latest_src_28000.capacity AS smallest_listing
                  , DATE_TRUNC(listings_latest_src_28000.created_at, day) AS ds__day
                  , DATE_TRUNC(listings_latest_src_28000.created_at, isoweek) AS ds__week
                  , DATE_TRUNC(listings_latest_src_28000.created_at, month) AS ds__month
                  , DATE_TRUNC(listings_latest_src_28000.created_at, quarter) AS ds__quarter
                  , DATE_TRUNC(listings_latest_src_28000.created_at, year) AS ds__year
                  , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
                  , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
                  , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
                  , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS ds__extract_dow
                  , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS ds__extract_doy
                  , DATE_TRUNC(listings_latest_src_28000.created_at, day) AS created_at__day
                  , DATE_TRUNC(listings_latest_src_28000.created_at, isoweek) AS created_at__week
                  , DATE_TRUNC(listings_latest_src_28000.created_at, month) AS created_at__month
                  , DATE_TRUNC(listings_latest_src_28000.created_at, quarter) AS created_at__quarter
                  , DATE_TRUNC(listings_latest_src_28000.created_at, year) AS created_at__year
                  , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
                  , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
                  , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
                  , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS created_at__extract_dow
                  , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
                  , listings_latest_src_28000.country AS country_latest
                  , listings_latest_src_28000.is_lux AS is_lux_latest
                  , listings_latest_src_28000.capacity AS capacity_latest
                  , DATE_TRUNC(listings_latest_src_28000.created_at, day) AS listing__ds__day
                  , DATE_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__ds__week
                  , DATE_TRUNC(listings_latest_src_28000.created_at, month) AS listing__ds__month
                  , DATE_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__ds__quarter
                  , DATE_TRUNC(listings_latest_src_28000.created_at, year) AS listing__ds__year
                  , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
                  , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                  , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
                  , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS listing__ds__extract_dow
                  , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                  , DATE_TRUNC(listings_latest_src_28000.created_at, day) AS listing__created_at__day
                  , DATE_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__created_at__week
                  , DATE_TRUNC(listings_latest_src_28000.created_at, month) AS listing__created_at__month
                  , DATE_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__created_at__quarter
                  , DATE_TRUNC(listings_latest_src_28000.created_at, year) AS listing__created_at__year
                  , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_year
                  , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
                  , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_month
                  , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS listing__created_at__extract_dow
                  , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                  , listings_latest_src_28000.country AS listing__country_latest
                  , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                  , listings_latest_src_28000.capacity AS listing__capacity_latest
                  , listings_latest_src_28000.listing_id AS listing
                  , listings_latest_src_28000.user_id AS user
                  , listings_latest_src_28000.user_id AS listing__user
                FROM ***************************.dim_listings_latest listings_latest_src_28000
              ) subq_4
            ) subq_5
          ) subq_6
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['listing', 'listing__bookings']
            SELECT
              subq_11.listing
              , subq_11.listing__bookings
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_10.listing
                , subq_10.bookings AS listing__bookings
              FROM (
                -- Aggregate Measures
                SELECT
                  subq_9.listing
                  , SUM(subq_9.bookings) AS bookings
                FROM (
                  -- Pass Only Elements: ['bookings', 'listing']
                  SELECT
                    subq_8.listing
                    , subq_8.bookings
                  FROM (
                    -- Metric Time Dimension 'ds'
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
                      , subq_7.ds__day AS metric_time__day
                      , subq_7.ds__week AS metric_time__week
                      , subq_7.ds__month AS metric_time__month
                      , subq_7.ds__quarter AS metric_time__quarter
                      , subq_7.ds__year AS metric_time__year
                      , subq_7.ds__extract_year AS metric_time__extract_year
                      , subq_7.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_7.ds__extract_month AS metric_time__extract_month
                      , subq_7.ds__extract_day AS metric_time__extract_day
                      , subq_7.ds__extract_dow AS metric_time__extract_dow
                      , subq_7.ds__extract_doy AS metric_time__extract_doy
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
                        , DATE_TRUNC(bookings_source_src_28000.ds, day) AS ds__day
                        , DATE_TRUNC(bookings_source_src_28000.ds, isoweek) AS ds__week
                        , DATE_TRUNC(bookings_source_src_28000.ds, month) AS ds__month
                        , DATE_TRUNC(bookings_source_src_28000.ds, quarter) AS ds__quarter
                        , DATE_TRUNC(bookings_source_src_28000.ds, year) AS ds__year
                        , EXTRACT(year FROM bookings_source_src_28000.ds) AS ds__extract_year
                        , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS ds__extract_quarter
                        , EXTRACT(month FROM bookings_source_src_28000.ds) AS ds__extract_month
                        , EXTRACT(day FROM bookings_source_src_28000.ds) AS ds__extract_day
                        , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds) - 1) AS ds__extract_dow
                        , EXTRACT(dayofyear FROM bookings_source_src_28000.ds) AS ds__extract_doy
                        , DATE_TRUNC(bookings_source_src_28000.ds_partitioned, day) AS ds_partitioned__day
                        , DATE_TRUNC(bookings_source_src_28000.ds_partitioned, isoweek) AS ds_partitioned__week
                        , DATE_TRUNC(bookings_source_src_28000.ds_partitioned, month) AS ds_partitioned__month
                        , DATE_TRUNC(bookings_source_src_28000.ds_partitioned, quarter) AS ds_partitioned__quarter
                        , DATE_TRUNC(bookings_source_src_28000.ds_partitioned, year) AS ds_partitioned__year
                        , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                        , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                        , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                        , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                        , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) - 1) AS ds_partitioned__extract_dow
                        , EXTRACT(dayofyear FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                        , DATE_TRUNC(bookings_source_src_28000.paid_at, day) AS paid_at__day
                        , DATE_TRUNC(bookings_source_src_28000.paid_at, isoweek) AS paid_at__week
                        , DATE_TRUNC(bookings_source_src_28000.paid_at, month) AS paid_at__month
                        , DATE_TRUNC(bookings_source_src_28000.paid_at, quarter) AS paid_at__quarter
                        , DATE_TRUNC(bookings_source_src_28000.paid_at, year) AS paid_at__year
                        , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS paid_at__extract_year
                        , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
                        , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS paid_at__extract_month
                        , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS paid_at__extract_day
                        , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) - 1) AS paid_at__extract_dow
                        , EXTRACT(dayofyear FROM bookings_source_src_28000.paid_at) AS paid_at__extract_doy
                        , bookings_source_src_28000.is_instant AS booking__is_instant
                        , DATE_TRUNC(bookings_source_src_28000.ds, day) AS booking__ds__day
                        , DATE_TRUNC(bookings_source_src_28000.ds, isoweek) AS booking__ds__week
                        , DATE_TRUNC(bookings_source_src_28000.ds, month) AS booking__ds__month
                        , DATE_TRUNC(bookings_source_src_28000.ds, quarter) AS booking__ds__quarter
                        , DATE_TRUNC(bookings_source_src_28000.ds, year) AS booking__ds__year
                        , EXTRACT(year FROM bookings_source_src_28000.ds) AS booking__ds__extract_year
                        , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS booking__ds__extract_quarter
                        , EXTRACT(month FROM bookings_source_src_28000.ds) AS booking__ds__extract_month
                        , EXTRACT(day FROM bookings_source_src_28000.ds) AS booking__ds__extract_day
                        , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds) - 1) AS booking__ds__extract_dow
                        , EXTRACT(dayofyear FROM bookings_source_src_28000.ds) AS booking__ds__extract_doy
                        , DATE_TRUNC(bookings_source_src_28000.ds_partitioned, day) AS booking__ds_partitioned__day
                        , DATE_TRUNC(bookings_source_src_28000.ds_partitioned, isoweek) AS booking__ds_partitioned__week
                        , DATE_TRUNC(bookings_source_src_28000.ds_partitioned, month) AS booking__ds_partitioned__month
                        , DATE_TRUNC(bookings_source_src_28000.ds_partitioned, quarter) AS booking__ds_partitioned__quarter
                        , DATE_TRUNC(bookings_source_src_28000.ds_partitioned, year) AS booking__ds_partitioned__year
                        , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
                        , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                        , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
                        , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
                        , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) - 1) AS booking__ds_partitioned__extract_dow
                        , EXTRACT(dayofyear FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
                        , DATE_TRUNC(bookings_source_src_28000.paid_at, day) AS booking__paid_at__day
                        , DATE_TRUNC(bookings_source_src_28000.paid_at, isoweek) AS booking__paid_at__week
                        , DATE_TRUNC(bookings_source_src_28000.paid_at, month) AS booking__paid_at__month
                        , DATE_TRUNC(bookings_source_src_28000.paid_at, quarter) AS booking__paid_at__quarter
                        , DATE_TRUNC(bookings_source_src_28000.paid_at, year) AS booking__paid_at__year
                        , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_year
                        , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_quarter
                        , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_month
                        , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_day
                        , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) - 1) AS booking__paid_at__extract_dow
                        , EXTRACT(dayofyear FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
                        , bookings_source_src_28000.listing_id AS listing
                        , bookings_source_src_28000.guest_id AS guest
                        , bookings_source_src_28000.host_id AS host
                        , bookings_source_src_28000.listing_id AS booking__listing
                        , bookings_source_src_28000.guest_id AS booking__guest
                        , bookings_source_src_28000.host_id AS booking__host
                      FROM ***************************.fct_bookings bookings_source_src_28000
                    ) subq_7
                  ) subq_8
                ) subq_9
                GROUP BY
                  listing
              ) subq_10
            ) subq_11
          ) subq_12
          ON
            subq_6.listing = subq_12.listing
        ) subq_13
      ) subq_14
      WHERE listing__bookings > 2
    ) subq_15
  ) subq_16
  GROUP BY
    metric_time__day
) subq_17
