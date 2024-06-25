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
      MAX(subq_12.average_booking_value) AS average_booking_value
      , MAX(subq_25.bookings) AS bookings
      , MAX(subq_33.booking_value) AS booking_value
    FROM (
      -- Compute Metrics via Expressions
      SELECT
        subq_11.average_booking_value
      FROM (
        -- Aggregate Measures
        SELECT
          AVG(subq_10.average_booking_value) AS average_booking_value
        FROM (
          -- Pass Only Elements: ['average_booking_value',]
          SELECT
            subq_9.average_booking_value
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_8.booking__is_instant
              , subq_8.listing__is_lux_latest
              , subq_8.average_booking_value
            FROM (
              -- Pass Only Elements: ['average_booking_value', 'listing__is_lux_latest', 'booking__is_instant']
              SELECT
                subq_7.booking__is_instant
                , subq_7.listing__is_lux_latest
                , subq_7.average_booking_value
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_3.listing AS listing
                  , subq_3.booking__is_instant AS booking__is_instant
                  , subq_6.is_lux_latest AS listing__is_lux_latest
                  , subq_3.average_booking_value AS average_booking_value
                FROM (
                  -- Pass Only Elements: ['average_booking_value', 'booking__is_instant', 'listing']
                  SELECT
                    subq_2.listing
                    , subq_2.booking__is_instant
                    , subq_2.average_booking_value
                  FROM (
                    -- Constrain Output with WHERE
                    SELECT
                      subq_1.ds__day
                      , subq_1.ds__week
                      , subq_1.ds__month
                      , subq_1.ds__quarter
                      , subq_1.ds__year
                      , subq_1.ds__extract_year
                      , subq_1.ds__extract_quarter
                      , subq_1.ds__extract_month
                      , subq_1.ds__extract_day
                      , subq_1.ds__extract_dow
                      , subq_1.ds__extract_doy
                      , subq_1.ds_partitioned__day
                      , subq_1.ds_partitioned__week
                      , subq_1.ds_partitioned__month
                      , subq_1.ds_partitioned__quarter
                      , subq_1.ds_partitioned__year
                      , subq_1.ds_partitioned__extract_year
                      , subq_1.ds_partitioned__extract_quarter
                      , subq_1.ds_partitioned__extract_month
                      , subq_1.ds_partitioned__extract_day
                      , subq_1.ds_partitioned__extract_dow
                      , subq_1.ds_partitioned__extract_doy
                      , subq_1.paid_at__day
                      , subq_1.paid_at__week
                      , subq_1.paid_at__month
                      , subq_1.paid_at__quarter
                      , subq_1.paid_at__year
                      , subq_1.paid_at__extract_year
                      , subq_1.paid_at__extract_quarter
                      , subq_1.paid_at__extract_month
                      , subq_1.paid_at__extract_day
                      , subq_1.paid_at__extract_dow
                      , subq_1.paid_at__extract_doy
                      , subq_1.booking__ds__day
                      , subq_1.booking__ds__week
                      , subq_1.booking__ds__month
                      , subq_1.booking__ds__quarter
                      , subq_1.booking__ds__year
                      , subq_1.booking__ds__extract_year
                      , subq_1.booking__ds__extract_quarter
                      , subq_1.booking__ds__extract_month
                      , subq_1.booking__ds__extract_day
                      , subq_1.booking__ds__extract_dow
                      , subq_1.booking__ds__extract_doy
                      , subq_1.booking__ds_partitioned__day
                      , subq_1.booking__ds_partitioned__week
                      , subq_1.booking__ds_partitioned__month
                      , subq_1.booking__ds_partitioned__quarter
                      , subq_1.booking__ds_partitioned__year
                      , subq_1.booking__ds_partitioned__extract_year
                      , subq_1.booking__ds_partitioned__extract_quarter
                      , subq_1.booking__ds_partitioned__extract_month
                      , subq_1.booking__ds_partitioned__extract_day
                      , subq_1.booking__ds_partitioned__extract_dow
                      , subq_1.booking__ds_partitioned__extract_doy
                      , subq_1.booking__paid_at__day
                      , subq_1.booking__paid_at__week
                      , subq_1.booking__paid_at__month
                      , subq_1.booking__paid_at__quarter
                      , subq_1.booking__paid_at__year
                      , subq_1.booking__paid_at__extract_year
                      , subq_1.booking__paid_at__extract_quarter
                      , subq_1.booking__paid_at__extract_month
                      , subq_1.booking__paid_at__extract_day
                      , subq_1.booking__paid_at__extract_dow
                      , subq_1.booking__paid_at__extract_doy
                      , subq_1.metric_time__day
                      , subq_1.metric_time__week
                      , subq_1.metric_time__month
                      , subq_1.metric_time__quarter
                      , subq_1.metric_time__year
                      , subq_1.metric_time__extract_year
                      , subq_1.metric_time__extract_quarter
                      , subq_1.metric_time__extract_month
                      , subq_1.metric_time__extract_day
                      , subq_1.metric_time__extract_dow
                      , subq_1.metric_time__extract_doy
                      , subq_1.listing
                      , subq_1.guest
                      , subq_1.host
                      , subq_1.booking__listing
                      , subq_1.booking__guest
                      , subq_1.booking__host
                      , subq_1.is_instant
                      , subq_1.booking__is_instant
                      , subq_1.bookings
                      , subq_1.instant_bookings
                      , subq_1.booking_value
                      , subq_1.max_booking_value
                      , subq_1.min_booking_value
                      , subq_1.bookers
                      , subq_1.average_booking_value
                      , subq_1.referred_bookings
                      , subq_1.median_booking_value
                      , subq_1.booking_value_p99
                      , subq_1.discrete_booking_value_p99
                      , subq_1.approximate_continuous_booking_value_p99
                      , subq_1.approximate_discrete_booking_value_p99
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
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, day) AS ds__day
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, isoweek) AS ds__week
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, month) AS ds__month
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, quarter) AS ds__quarter
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, year) AS ds__year
                          , EXTRACT(year FROM bookings_source_src_28000.ds) AS ds__extract_year
                          , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS ds__extract_quarter
                          , EXTRACT(month FROM bookings_source_src_28000.ds) AS ds__extract_month
                          , EXTRACT(day FROM bookings_source_src_28000.ds) AS ds__extract_day
                          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds) - 1) AS ds__extract_dow
                          , EXTRACT(dayofyear FROM bookings_source_src_28000.ds) AS ds__extract_doy
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, day) AS ds_partitioned__day
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, isoweek) AS ds_partitioned__week
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, month) AS ds_partitioned__month
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, quarter) AS ds_partitioned__quarter
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, year) AS ds_partitioned__year
                          , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                          , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                          , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                          , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) - 1) AS ds_partitioned__extract_dow
                          , EXTRACT(dayofyear FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, day) AS paid_at__day
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, isoweek) AS paid_at__week
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, month) AS paid_at__month
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, quarter) AS paid_at__quarter
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, year) AS paid_at__year
                          , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS paid_at__extract_year
                          , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
                          , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS paid_at__extract_month
                          , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS paid_at__extract_day
                          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) - 1) AS paid_at__extract_dow
                          , EXTRACT(dayofyear FROM bookings_source_src_28000.paid_at) AS paid_at__extract_doy
                          , bookings_source_src_28000.is_instant AS booking__is_instant
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, day) AS booking__ds__day
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, isoweek) AS booking__ds__week
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, month) AS booking__ds__month
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, quarter) AS booking__ds__quarter
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, year) AS booking__ds__year
                          , EXTRACT(year FROM bookings_source_src_28000.ds) AS booking__ds__extract_year
                          , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS booking__ds__extract_quarter
                          , EXTRACT(month FROM bookings_source_src_28000.ds) AS booking__ds__extract_month
                          , EXTRACT(day FROM bookings_source_src_28000.ds) AS booking__ds__extract_day
                          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds) - 1) AS booking__ds__extract_dow
                          , EXTRACT(dayofyear FROM bookings_source_src_28000.ds) AS booking__ds__extract_doy
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, day) AS booking__ds_partitioned__day
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, isoweek) AS booking__ds_partitioned__week
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, month) AS booking__ds_partitioned__month
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, quarter) AS booking__ds_partitioned__quarter
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, year) AS booking__ds_partitioned__year
                          , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
                          , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                          , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
                          , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
                          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) - 1) AS booking__ds_partitioned__extract_dow
                          , EXTRACT(dayofyear FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, day) AS booking__paid_at__day
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, isoweek) AS booking__paid_at__week
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, month) AS booking__paid_at__month
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, quarter) AS booking__paid_at__quarter
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, year) AS booking__paid_at__year
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
                      ) subq_0
                    ) subq_1
                    WHERE booking__is_instant
                  ) subq_2
                ) subq_3
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['is_lux_latest', 'listing']
                  SELECT
                    subq_5.listing
                    , subq_5.is_lux_latest
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
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS ds__day
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS ds__week
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS ds__month
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS ds__quarter
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS ds__year
                        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
                        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
                        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
                        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
                        , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS ds__extract_dow
                        , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS ds__extract_doy
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS created_at__day
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS created_at__week
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS created_at__month
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS created_at__quarter
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS created_at__year
                        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
                        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
                        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
                        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
                        , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS created_at__extract_dow
                        , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
                        , listings_latest_src_28000.country AS country_latest
                        , listings_latest_src_28000.is_lux AS is_lux_latest
                        , listings_latest_src_28000.capacity AS capacity_latest
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS listing__ds__day
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__ds__week
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS listing__ds__month
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__ds__quarter
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS listing__ds__year
                        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
                        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
                        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
                        , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS listing__ds__extract_dow
                        , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS listing__created_at__day
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__created_at__week
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS listing__created_at__month
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__created_at__quarter
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS listing__created_at__year
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
                ON
                  subq_3.listing = subq_6.listing
              ) subq_7
            ) subq_8
            WHERE (listing__is_lux_latest) AND (booking__is_instant)
          ) subq_9
        ) subq_10
      ) subq_11
    ) subq_12
    CROSS JOIN (
      -- Compute Metrics via Expressions
      SELECT
        subq_24.bookings
      FROM (
        -- Aggregate Measures
        SELECT
          SUM(subq_23.bookings) AS bookings
        FROM (
          -- Pass Only Elements: ['bookings',]
          SELECT
            subq_22.bookings
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_21.booking__is_instant
              , subq_21.listing__is_lux_latest
              , subq_21.bookings
            FROM (
              -- Pass Only Elements: ['bookings', 'listing__is_lux_latest', 'booking__is_instant']
              SELECT
                subq_20.booking__is_instant
                , subq_20.listing__is_lux_latest
                , subq_20.bookings
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_16.listing AS listing
                  , subq_16.booking__is_instant AS booking__is_instant
                  , subq_19.is_lux_latest AS listing__is_lux_latest
                  , subq_16.bookings AS bookings
                FROM (
                  -- Pass Only Elements: ['bookings', 'booking__is_instant', 'listing']
                  SELECT
                    subq_15.listing
                    , subq_15.booking__is_instant
                    , subq_15.bookings
                  FROM (
                    -- Constrain Output with WHERE
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
                      , subq_14.metric_time__day
                      , subq_14.metric_time__week
                      , subq_14.metric_time__month
                      , subq_14.metric_time__quarter
                      , subq_14.metric_time__year
                      , subq_14.metric_time__extract_year
                      , subq_14.metric_time__extract_quarter
                      , subq_14.metric_time__extract_month
                      , subq_14.metric_time__extract_day
                      , subq_14.metric_time__extract_dow
                      , subq_14.metric_time__extract_doy
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
                      -- Metric Time Dimension 'ds'
                      SELECT
                        subq_13.ds__day
                        , subq_13.ds__week
                        , subq_13.ds__month
                        , subq_13.ds__quarter
                        , subq_13.ds__year
                        , subq_13.ds__extract_year
                        , subq_13.ds__extract_quarter
                        , subq_13.ds__extract_month
                        , subq_13.ds__extract_day
                        , subq_13.ds__extract_dow
                        , subq_13.ds__extract_doy
                        , subq_13.ds_partitioned__day
                        , subq_13.ds_partitioned__week
                        , subq_13.ds_partitioned__month
                        , subq_13.ds_partitioned__quarter
                        , subq_13.ds_partitioned__year
                        , subq_13.ds_partitioned__extract_year
                        , subq_13.ds_partitioned__extract_quarter
                        , subq_13.ds_partitioned__extract_month
                        , subq_13.ds_partitioned__extract_day
                        , subq_13.ds_partitioned__extract_dow
                        , subq_13.ds_partitioned__extract_doy
                        , subq_13.paid_at__day
                        , subq_13.paid_at__week
                        , subq_13.paid_at__month
                        , subq_13.paid_at__quarter
                        , subq_13.paid_at__year
                        , subq_13.paid_at__extract_year
                        , subq_13.paid_at__extract_quarter
                        , subq_13.paid_at__extract_month
                        , subq_13.paid_at__extract_day
                        , subq_13.paid_at__extract_dow
                        , subq_13.paid_at__extract_doy
                        , subq_13.booking__ds__day
                        , subq_13.booking__ds__week
                        , subq_13.booking__ds__month
                        , subq_13.booking__ds__quarter
                        , subq_13.booking__ds__year
                        , subq_13.booking__ds__extract_year
                        , subq_13.booking__ds__extract_quarter
                        , subq_13.booking__ds__extract_month
                        , subq_13.booking__ds__extract_day
                        , subq_13.booking__ds__extract_dow
                        , subq_13.booking__ds__extract_doy
                        , subq_13.booking__ds_partitioned__day
                        , subq_13.booking__ds_partitioned__week
                        , subq_13.booking__ds_partitioned__month
                        , subq_13.booking__ds_partitioned__quarter
                        , subq_13.booking__ds_partitioned__year
                        , subq_13.booking__ds_partitioned__extract_year
                        , subq_13.booking__ds_partitioned__extract_quarter
                        , subq_13.booking__ds_partitioned__extract_month
                        , subq_13.booking__ds_partitioned__extract_day
                        , subq_13.booking__ds_partitioned__extract_dow
                        , subq_13.booking__ds_partitioned__extract_doy
                        , subq_13.booking__paid_at__day
                        , subq_13.booking__paid_at__week
                        , subq_13.booking__paid_at__month
                        , subq_13.booking__paid_at__quarter
                        , subq_13.booking__paid_at__year
                        , subq_13.booking__paid_at__extract_year
                        , subq_13.booking__paid_at__extract_quarter
                        , subq_13.booking__paid_at__extract_month
                        , subq_13.booking__paid_at__extract_day
                        , subq_13.booking__paid_at__extract_dow
                        , subq_13.booking__paid_at__extract_doy
                        , subq_13.ds__day AS metric_time__day
                        , subq_13.ds__week AS metric_time__week
                        , subq_13.ds__month AS metric_time__month
                        , subq_13.ds__quarter AS metric_time__quarter
                        , subq_13.ds__year AS metric_time__year
                        , subq_13.ds__extract_year AS metric_time__extract_year
                        , subq_13.ds__extract_quarter AS metric_time__extract_quarter
                        , subq_13.ds__extract_month AS metric_time__extract_month
                        , subq_13.ds__extract_day AS metric_time__extract_day
                        , subq_13.ds__extract_dow AS metric_time__extract_dow
                        , subq_13.ds__extract_doy AS metric_time__extract_doy
                        , subq_13.listing
                        , subq_13.guest
                        , subq_13.host
                        , subq_13.booking__listing
                        , subq_13.booking__guest
                        , subq_13.booking__host
                        , subq_13.is_instant
                        , subq_13.booking__is_instant
                        , subq_13.bookings
                        , subq_13.instant_bookings
                        , subq_13.booking_value
                        , subq_13.max_booking_value
                        , subq_13.min_booking_value
                        , subq_13.bookers
                        , subq_13.average_booking_value
                        , subq_13.referred_bookings
                        , subq_13.median_booking_value
                        , subq_13.booking_value_p99
                        , subq_13.discrete_booking_value_p99
                        , subq_13.approximate_continuous_booking_value_p99
                        , subq_13.approximate_discrete_booking_value_p99
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
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, day) AS ds__day
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, isoweek) AS ds__week
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, month) AS ds__month
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, quarter) AS ds__quarter
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, year) AS ds__year
                          , EXTRACT(year FROM bookings_source_src_28000.ds) AS ds__extract_year
                          , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS ds__extract_quarter
                          , EXTRACT(month FROM bookings_source_src_28000.ds) AS ds__extract_month
                          , EXTRACT(day FROM bookings_source_src_28000.ds) AS ds__extract_day
                          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds) - 1) AS ds__extract_dow
                          , EXTRACT(dayofyear FROM bookings_source_src_28000.ds) AS ds__extract_doy
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, day) AS ds_partitioned__day
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, isoweek) AS ds_partitioned__week
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, month) AS ds_partitioned__month
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, quarter) AS ds_partitioned__quarter
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, year) AS ds_partitioned__year
                          , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                          , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                          , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                          , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) - 1) AS ds_partitioned__extract_dow
                          , EXTRACT(dayofyear FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, day) AS paid_at__day
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, isoweek) AS paid_at__week
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, month) AS paid_at__month
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, quarter) AS paid_at__quarter
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, year) AS paid_at__year
                          , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS paid_at__extract_year
                          , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
                          , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS paid_at__extract_month
                          , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS paid_at__extract_day
                          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) - 1) AS paid_at__extract_dow
                          , EXTRACT(dayofyear FROM bookings_source_src_28000.paid_at) AS paid_at__extract_doy
                          , bookings_source_src_28000.is_instant AS booking__is_instant
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, day) AS booking__ds__day
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, isoweek) AS booking__ds__week
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, month) AS booking__ds__month
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, quarter) AS booking__ds__quarter
                          , DATETIME_TRUNC(bookings_source_src_28000.ds, year) AS booking__ds__year
                          , EXTRACT(year FROM bookings_source_src_28000.ds) AS booking__ds__extract_year
                          , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS booking__ds__extract_quarter
                          , EXTRACT(month FROM bookings_source_src_28000.ds) AS booking__ds__extract_month
                          , EXTRACT(day FROM bookings_source_src_28000.ds) AS booking__ds__extract_day
                          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds) - 1) AS booking__ds__extract_dow
                          , EXTRACT(dayofyear FROM bookings_source_src_28000.ds) AS booking__ds__extract_doy
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, day) AS booking__ds_partitioned__day
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, isoweek) AS booking__ds_partitioned__week
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, month) AS booking__ds_partitioned__month
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, quarter) AS booking__ds_partitioned__quarter
                          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, year) AS booking__ds_partitioned__year
                          , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
                          , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                          , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
                          , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
                          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) - 1) AS booking__ds_partitioned__extract_dow
                          , EXTRACT(dayofyear FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, day) AS booking__paid_at__day
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, isoweek) AS booking__paid_at__week
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, month) AS booking__paid_at__month
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, quarter) AS booking__paid_at__quarter
                          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, year) AS booking__paid_at__year
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
                      ) subq_13
                    ) subq_14
                    WHERE booking__is_instant
                  ) subq_15
                ) subq_16
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['is_lux_latest', 'listing']
                  SELECT
                    subq_18.listing
                    , subq_18.is_lux_latest
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
                      , subq_17.created_at__day
                      , subq_17.created_at__week
                      , subq_17.created_at__month
                      , subq_17.created_at__quarter
                      , subq_17.created_at__year
                      , subq_17.created_at__extract_year
                      , subq_17.created_at__extract_quarter
                      , subq_17.created_at__extract_month
                      , subq_17.created_at__extract_day
                      , subq_17.created_at__extract_dow
                      , subq_17.created_at__extract_doy
                      , subq_17.listing__ds__day
                      , subq_17.listing__ds__week
                      , subq_17.listing__ds__month
                      , subq_17.listing__ds__quarter
                      , subq_17.listing__ds__year
                      , subq_17.listing__ds__extract_year
                      , subq_17.listing__ds__extract_quarter
                      , subq_17.listing__ds__extract_month
                      , subq_17.listing__ds__extract_day
                      , subq_17.listing__ds__extract_dow
                      , subq_17.listing__ds__extract_doy
                      , subq_17.listing__created_at__day
                      , subq_17.listing__created_at__week
                      , subq_17.listing__created_at__month
                      , subq_17.listing__created_at__quarter
                      , subq_17.listing__created_at__year
                      , subq_17.listing__created_at__extract_year
                      , subq_17.listing__created_at__extract_quarter
                      , subq_17.listing__created_at__extract_month
                      , subq_17.listing__created_at__extract_day
                      , subq_17.listing__created_at__extract_dow
                      , subq_17.listing__created_at__extract_doy
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
                      , subq_17.user
                      , subq_17.listing__user
                      , subq_17.country_latest
                      , subq_17.is_lux_latest
                      , subq_17.capacity_latest
                      , subq_17.listing__country_latest
                      , subq_17.listing__is_lux_latest
                      , subq_17.listing__capacity_latest
                      , subq_17.listings
                      , subq_17.largest_listing
                      , subq_17.smallest_listing
                    FROM (
                      -- Read Elements From Semantic Model 'listings_latest'
                      SELECT
                        1 AS listings
                        , listings_latest_src_28000.capacity AS largest_listing
                        , listings_latest_src_28000.capacity AS smallest_listing
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS ds__day
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS ds__week
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS ds__month
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS ds__quarter
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS ds__year
                        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
                        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
                        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
                        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
                        , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS ds__extract_dow
                        , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS ds__extract_doy
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS created_at__day
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS created_at__week
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS created_at__month
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS created_at__quarter
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS created_at__year
                        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
                        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
                        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
                        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
                        , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS created_at__extract_dow
                        , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
                        , listings_latest_src_28000.country AS country_latest
                        , listings_latest_src_28000.is_lux AS is_lux_latest
                        , listings_latest_src_28000.capacity AS capacity_latest
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS listing__ds__day
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__ds__week
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS listing__ds__month
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__ds__quarter
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS listing__ds__year
                        , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
                        , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                        , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
                        , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
                        , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS listing__ds__extract_dow
                        , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS listing__created_at__day
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__created_at__week
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS listing__created_at__month
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__created_at__quarter
                        , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS listing__created_at__year
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
                    ) subq_17
                  ) subq_18
                ) subq_19
                ON
                  subq_16.listing = subq_19.listing
              ) subq_20
            ) subq_21
            WHERE (listing__is_lux_latest) AND (booking__is_instant)
          ) subq_22
        ) subq_23
      ) subq_24
    ) subq_25
    CROSS JOIN (
      -- Compute Metrics via Expressions
      SELECT
        subq_32.booking_value
      FROM (
        -- Aggregate Measures
        SELECT
          SUM(subq_31.booking_value) AS booking_value
        FROM (
          -- Pass Only Elements: ['booking_value',]
          SELECT
            subq_30.booking_value
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_29.booking__is_instant
              , subq_29.booking_value
            FROM (
              -- Pass Only Elements: ['booking_value', 'booking__is_instant']
              SELECT
                subq_28.booking__is_instant
                , subq_28.booking_value
              FROM (
                -- Constrain Output with WHERE
                SELECT
                  subq_27.ds__day
                  , subq_27.ds__week
                  , subq_27.ds__month
                  , subq_27.ds__quarter
                  , subq_27.ds__year
                  , subq_27.ds__extract_year
                  , subq_27.ds__extract_quarter
                  , subq_27.ds__extract_month
                  , subq_27.ds__extract_day
                  , subq_27.ds__extract_dow
                  , subq_27.ds__extract_doy
                  , subq_27.ds_partitioned__day
                  , subq_27.ds_partitioned__week
                  , subq_27.ds_partitioned__month
                  , subq_27.ds_partitioned__quarter
                  , subq_27.ds_partitioned__year
                  , subq_27.ds_partitioned__extract_year
                  , subq_27.ds_partitioned__extract_quarter
                  , subq_27.ds_partitioned__extract_month
                  , subq_27.ds_partitioned__extract_day
                  , subq_27.ds_partitioned__extract_dow
                  , subq_27.ds_partitioned__extract_doy
                  , subq_27.paid_at__day
                  , subq_27.paid_at__week
                  , subq_27.paid_at__month
                  , subq_27.paid_at__quarter
                  , subq_27.paid_at__year
                  , subq_27.paid_at__extract_year
                  , subq_27.paid_at__extract_quarter
                  , subq_27.paid_at__extract_month
                  , subq_27.paid_at__extract_day
                  , subq_27.paid_at__extract_dow
                  , subq_27.paid_at__extract_doy
                  , subq_27.booking__ds__day
                  , subq_27.booking__ds__week
                  , subq_27.booking__ds__month
                  , subq_27.booking__ds__quarter
                  , subq_27.booking__ds__year
                  , subq_27.booking__ds__extract_year
                  , subq_27.booking__ds__extract_quarter
                  , subq_27.booking__ds__extract_month
                  , subq_27.booking__ds__extract_day
                  , subq_27.booking__ds__extract_dow
                  , subq_27.booking__ds__extract_doy
                  , subq_27.booking__ds_partitioned__day
                  , subq_27.booking__ds_partitioned__week
                  , subq_27.booking__ds_partitioned__month
                  , subq_27.booking__ds_partitioned__quarter
                  , subq_27.booking__ds_partitioned__year
                  , subq_27.booking__ds_partitioned__extract_year
                  , subq_27.booking__ds_partitioned__extract_quarter
                  , subq_27.booking__ds_partitioned__extract_month
                  , subq_27.booking__ds_partitioned__extract_day
                  , subq_27.booking__ds_partitioned__extract_dow
                  , subq_27.booking__ds_partitioned__extract_doy
                  , subq_27.booking__paid_at__day
                  , subq_27.booking__paid_at__week
                  , subq_27.booking__paid_at__month
                  , subq_27.booking__paid_at__quarter
                  , subq_27.booking__paid_at__year
                  , subq_27.booking__paid_at__extract_year
                  , subq_27.booking__paid_at__extract_quarter
                  , subq_27.booking__paid_at__extract_month
                  , subq_27.booking__paid_at__extract_day
                  , subq_27.booking__paid_at__extract_dow
                  , subq_27.booking__paid_at__extract_doy
                  , subq_27.metric_time__day
                  , subq_27.metric_time__week
                  , subq_27.metric_time__month
                  , subq_27.metric_time__quarter
                  , subq_27.metric_time__year
                  , subq_27.metric_time__extract_year
                  , subq_27.metric_time__extract_quarter
                  , subq_27.metric_time__extract_month
                  , subq_27.metric_time__extract_day
                  , subq_27.metric_time__extract_dow
                  , subq_27.metric_time__extract_doy
                  , subq_27.listing
                  , subq_27.guest
                  , subq_27.host
                  , subq_27.booking__listing
                  , subq_27.booking__guest
                  , subq_27.booking__host
                  , subq_27.is_instant
                  , subq_27.booking__is_instant
                  , subq_27.bookings
                  , subq_27.instant_bookings
                  , subq_27.booking_value
                  , subq_27.max_booking_value
                  , subq_27.min_booking_value
                  , subq_27.bookers
                  , subq_27.average_booking_value
                  , subq_27.referred_bookings
                  , subq_27.median_booking_value
                  , subq_27.booking_value_p99
                  , subq_27.discrete_booking_value_p99
                  , subq_27.approximate_continuous_booking_value_p99
                  , subq_27.approximate_discrete_booking_value_p99
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_26.ds__day
                    , subq_26.ds__week
                    , subq_26.ds__month
                    , subq_26.ds__quarter
                    , subq_26.ds__year
                    , subq_26.ds__extract_year
                    , subq_26.ds__extract_quarter
                    , subq_26.ds__extract_month
                    , subq_26.ds__extract_day
                    , subq_26.ds__extract_dow
                    , subq_26.ds__extract_doy
                    , subq_26.ds_partitioned__day
                    , subq_26.ds_partitioned__week
                    , subq_26.ds_partitioned__month
                    , subq_26.ds_partitioned__quarter
                    , subq_26.ds_partitioned__year
                    , subq_26.ds_partitioned__extract_year
                    , subq_26.ds_partitioned__extract_quarter
                    , subq_26.ds_partitioned__extract_month
                    , subq_26.ds_partitioned__extract_day
                    , subq_26.ds_partitioned__extract_dow
                    , subq_26.ds_partitioned__extract_doy
                    , subq_26.paid_at__day
                    , subq_26.paid_at__week
                    , subq_26.paid_at__month
                    , subq_26.paid_at__quarter
                    , subq_26.paid_at__year
                    , subq_26.paid_at__extract_year
                    , subq_26.paid_at__extract_quarter
                    , subq_26.paid_at__extract_month
                    , subq_26.paid_at__extract_day
                    , subq_26.paid_at__extract_dow
                    , subq_26.paid_at__extract_doy
                    , subq_26.booking__ds__day
                    , subq_26.booking__ds__week
                    , subq_26.booking__ds__month
                    , subq_26.booking__ds__quarter
                    , subq_26.booking__ds__year
                    , subq_26.booking__ds__extract_year
                    , subq_26.booking__ds__extract_quarter
                    , subq_26.booking__ds__extract_month
                    , subq_26.booking__ds__extract_day
                    , subq_26.booking__ds__extract_dow
                    , subq_26.booking__ds__extract_doy
                    , subq_26.booking__ds_partitioned__day
                    , subq_26.booking__ds_partitioned__week
                    , subq_26.booking__ds_partitioned__month
                    , subq_26.booking__ds_partitioned__quarter
                    , subq_26.booking__ds_partitioned__year
                    , subq_26.booking__ds_partitioned__extract_year
                    , subq_26.booking__ds_partitioned__extract_quarter
                    , subq_26.booking__ds_partitioned__extract_month
                    , subq_26.booking__ds_partitioned__extract_day
                    , subq_26.booking__ds_partitioned__extract_dow
                    , subq_26.booking__ds_partitioned__extract_doy
                    , subq_26.booking__paid_at__day
                    , subq_26.booking__paid_at__week
                    , subq_26.booking__paid_at__month
                    , subq_26.booking__paid_at__quarter
                    , subq_26.booking__paid_at__year
                    , subq_26.booking__paid_at__extract_year
                    , subq_26.booking__paid_at__extract_quarter
                    , subq_26.booking__paid_at__extract_month
                    , subq_26.booking__paid_at__extract_day
                    , subq_26.booking__paid_at__extract_dow
                    , subq_26.booking__paid_at__extract_doy
                    , subq_26.ds__day AS metric_time__day
                    , subq_26.ds__week AS metric_time__week
                    , subq_26.ds__month AS metric_time__month
                    , subq_26.ds__quarter AS metric_time__quarter
                    , subq_26.ds__year AS metric_time__year
                    , subq_26.ds__extract_year AS metric_time__extract_year
                    , subq_26.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_26.ds__extract_month AS metric_time__extract_month
                    , subq_26.ds__extract_day AS metric_time__extract_day
                    , subq_26.ds__extract_dow AS metric_time__extract_dow
                    , subq_26.ds__extract_doy AS metric_time__extract_doy
                    , subq_26.listing
                    , subq_26.guest
                    , subq_26.host
                    , subq_26.booking__listing
                    , subq_26.booking__guest
                    , subq_26.booking__host
                    , subq_26.is_instant
                    , subq_26.booking__is_instant
                    , subq_26.bookings
                    , subq_26.instant_bookings
                    , subq_26.booking_value
                    , subq_26.max_booking_value
                    , subq_26.min_booking_value
                    , subq_26.bookers
                    , subq_26.average_booking_value
                    , subq_26.referred_bookings
                    , subq_26.median_booking_value
                    , subq_26.booking_value_p99
                    , subq_26.discrete_booking_value_p99
                    , subq_26.approximate_continuous_booking_value_p99
                    , subq_26.approximate_discrete_booking_value_p99
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
                      , DATETIME_TRUNC(bookings_source_src_28000.ds, day) AS ds__day
                      , DATETIME_TRUNC(bookings_source_src_28000.ds, isoweek) AS ds__week
                      , DATETIME_TRUNC(bookings_source_src_28000.ds, month) AS ds__month
                      , DATETIME_TRUNC(bookings_source_src_28000.ds, quarter) AS ds__quarter
                      , DATETIME_TRUNC(bookings_source_src_28000.ds, year) AS ds__year
                      , EXTRACT(year FROM bookings_source_src_28000.ds) AS ds__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS ds__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_28000.ds) AS ds__extract_month
                      , EXTRACT(day FROM bookings_source_src_28000.ds) AS ds__extract_day
                      , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds) - 1) AS ds__extract_dow
                      , EXTRACT(dayofyear FROM bookings_source_src_28000.ds) AS ds__extract_doy
                      , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, day) AS ds_partitioned__day
                      , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, isoweek) AS ds_partitioned__week
                      , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, month) AS ds_partitioned__month
                      , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, quarter) AS ds_partitioned__quarter
                      , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, year) AS ds_partitioned__year
                      , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                      , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                      , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) - 1) AS ds_partitioned__extract_dow
                      , EXTRACT(dayofyear FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                      , DATETIME_TRUNC(bookings_source_src_28000.paid_at, day) AS paid_at__day
                      , DATETIME_TRUNC(bookings_source_src_28000.paid_at, isoweek) AS paid_at__week
                      , DATETIME_TRUNC(bookings_source_src_28000.paid_at, month) AS paid_at__month
                      , DATETIME_TRUNC(bookings_source_src_28000.paid_at, quarter) AS paid_at__quarter
                      , DATETIME_TRUNC(bookings_source_src_28000.paid_at, year) AS paid_at__year
                      , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS paid_at__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS paid_at__extract_month
                      , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS paid_at__extract_day
                      , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) - 1) AS paid_at__extract_dow
                      , EXTRACT(dayofyear FROM bookings_source_src_28000.paid_at) AS paid_at__extract_doy
                      , bookings_source_src_28000.is_instant AS booking__is_instant
                      , DATETIME_TRUNC(bookings_source_src_28000.ds, day) AS booking__ds__day
                      , DATETIME_TRUNC(bookings_source_src_28000.ds, isoweek) AS booking__ds__week
                      , DATETIME_TRUNC(bookings_source_src_28000.ds, month) AS booking__ds__month
                      , DATETIME_TRUNC(bookings_source_src_28000.ds, quarter) AS booking__ds__quarter
                      , DATETIME_TRUNC(bookings_source_src_28000.ds, year) AS booking__ds__year
                      , EXTRACT(year FROM bookings_source_src_28000.ds) AS booking__ds__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS booking__ds__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_28000.ds) AS booking__ds__extract_month
                      , EXTRACT(day FROM bookings_source_src_28000.ds) AS booking__ds__extract_day
                      , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds) - 1) AS booking__ds__extract_dow
                      , EXTRACT(dayofyear FROM bookings_source_src_28000.ds) AS booking__ds__extract_doy
                      , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, day) AS booking__ds_partitioned__day
                      , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, isoweek) AS booking__ds_partitioned__week
                      , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, month) AS booking__ds_partitioned__month
                      , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, quarter) AS booking__ds_partitioned__quarter
                      , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, year) AS booking__ds_partitioned__year
                      , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
                      , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                      , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
                      , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
                      , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) - 1) AS booking__ds_partitioned__extract_dow
                      , EXTRACT(dayofyear FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
                      , DATETIME_TRUNC(bookings_source_src_28000.paid_at, day) AS booking__paid_at__day
                      , DATETIME_TRUNC(bookings_source_src_28000.paid_at, isoweek) AS booking__paid_at__week
                      , DATETIME_TRUNC(bookings_source_src_28000.paid_at, month) AS booking__paid_at__month
                      , DATETIME_TRUNC(bookings_source_src_28000.paid_at, quarter) AS booking__paid_at__quarter
                      , DATETIME_TRUNC(bookings_source_src_28000.paid_at, year) AS booking__paid_at__year
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
                  ) subq_26
                ) subq_27
                WHERE booking__is_instant
              ) subq_28
            ) subq_29
            WHERE booking__is_instant
          ) subq_30
        ) subq_31
      ) subq_32
    ) subq_33
  ) subq_34
) subq_35
