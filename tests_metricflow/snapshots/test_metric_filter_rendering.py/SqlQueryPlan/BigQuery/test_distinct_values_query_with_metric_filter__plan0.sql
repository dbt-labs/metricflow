-- Pass Only Elements: ['listing',]
SELECT
  subq_8.listing
FROM (
  -- Constrain Output with WHERE
  SELECT
    subq_7.listing
    , subq_7.lux_listing
    , subq_7.listing__lux_listing
    , subq_7.listing__bookings
  FROM (
    -- Join Standard Outputs
    SELECT
      subq_0.listing AS listing
      , subq_0.lux_listing AS lux_listing
      , subq_0.listing__lux_listing AS listing__lux_listing
      , subq_6.listing__bookings AS listing__bookings
    FROM (
      -- Read Elements From Semantic Model 'lux_listing_mapping'
      SELECT
        lux_listing_mapping_src_28000.listing_id AS listing
        , lux_listing_mapping_src_28000.lux_listing_id AS lux_listing
        , lux_listing_mapping_src_28000.lux_listing_id AS listing__lux_listing
      FROM ***************************.dim_lux_listing_id_mapping lux_listing_mapping_src_28000
    ) subq_0
    FULL OUTER JOIN (
      -- Pass Only Elements: ['listing', 'listing__bookings']
      SELECT
        subq_5.listing
        , subq_5.listing__bookings
      FROM (
        -- Compute Metrics via Expressions
        SELECT
          subq_4.listing
          , subq_4.bookings AS listing__bookings
        FROM (
          -- Aggregate Measures
          SELECT
            subq_3.listing
            , SUM(subq_3.bookings) AS bookings
          FROM (
            -- Pass Only Elements: ['bookings', 'listing']
            SELECT
              subq_2.listing
              , subq_2.bookings
            FROM (
              -- Metric Time Dimension 'ds'
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
                , subq_1.ds__day AS metric_time__day
                , subq_1.ds__week AS metric_time__week
                , subq_1.ds__month AS metric_time__month
                , subq_1.ds__quarter AS metric_time__quarter
                , subq_1.ds__year AS metric_time__year
                , subq_1.ds__extract_year AS metric_time__extract_year
                , subq_1.ds__extract_quarter AS metric_time__extract_quarter
                , subq_1.ds__extract_month AS metric_time__extract_month
                , subq_1.ds__extract_day AS metric_time__extract_day
                , subq_1.ds__extract_dow AS metric_time__extract_dow
                , subq_1.ds__extract_doy AS metric_time__extract_doy
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
              ) subq_1
            ) subq_2
          ) subq_3
          GROUP BY
            listing
        ) subq_4
      ) subq_5
    ) subq_6
    ON
      subq_0.listing = subq_6.listing
  ) subq_7
  WHERE listing__bookings > 2
) subq_8
GROUP BY
  listing
