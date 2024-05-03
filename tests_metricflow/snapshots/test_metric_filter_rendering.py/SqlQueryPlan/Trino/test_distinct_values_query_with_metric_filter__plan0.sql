-- Pass Only Elements: ['listing',]
SELECT
  subq_12.listing
FROM (
  -- Constrain Output with WHERE
  SELECT
    subq_11.listing
    , subq_11.lux_listing
    , subq_11.listing__lux_listing
    , subq_11.listing__bookings
  FROM (
    -- Join Standard Outputs
    SELECT
      subq_4.listing AS listing
      , subq_4.lux_listing AS lux_listing
      , subq_4.listing__lux_listing AS listing__lux_listing
      , subq_10.listing__bookings AS listing__bookings
    FROM (
      -- Read Elements From Semantic Model 'lux_listing_mapping'
      SELECT
        lux_listing_mapping_src_28000.listing_id AS listing
        , lux_listing_mapping_src_28000.lux_listing_id AS lux_listing
        , lux_listing_mapping_src_28000.lux_listing_id AS listing__lux_listing
      FROM ***************************.dim_lux_listing_id_mapping lux_listing_mapping_src_28000
    ) subq_4
    FULL OUTER JOIN (
      -- Pass Only Elements: ['listing', 'listing__bookings']
      SELECT
        subq_9.listing
        , subq_9.listing__bookings
      FROM (
        -- Compute Metrics via Expressions
        SELECT
          subq_8.listing
          , subq_8.bookings AS listing__bookings
        FROM (
          -- Aggregate Measures
          SELECT
            subq_7.listing
            , SUM(subq_7.bookings) AS bookings
          FROM (
            -- Pass Only Elements: ['bookings', 'listing']
            SELECT
              subq_6.listing
              , subq_6.bookings
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_5.ds__day
                , subq_5.ds__week
                , subq_5.ds__month
                , subq_5.ds__quarter
                , subq_5.ds__year
                , subq_5.ds__extract_year
                , subq_5.ds__extract_quarter
                , subq_5.ds__extract_month
                , subq_5.ds__extract_day
                , subq_5.ds__extract_dow
                , subq_5.ds__extract_doy
                , subq_5.ds_partitioned__day
                , subq_5.ds_partitioned__week
                , subq_5.ds_partitioned__month
                , subq_5.ds_partitioned__quarter
                , subq_5.ds_partitioned__year
                , subq_5.ds_partitioned__extract_year
                , subq_5.ds_partitioned__extract_quarter
                , subq_5.ds_partitioned__extract_month
                , subq_5.ds_partitioned__extract_day
                , subq_5.ds_partitioned__extract_dow
                , subq_5.ds_partitioned__extract_doy
                , subq_5.paid_at__day
                , subq_5.paid_at__week
                , subq_5.paid_at__month
                , subq_5.paid_at__quarter
                , subq_5.paid_at__year
                , subq_5.paid_at__extract_year
                , subq_5.paid_at__extract_quarter
                , subq_5.paid_at__extract_month
                , subq_5.paid_at__extract_day
                , subq_5.paid_at__extract_dow
                , subq_5.paid_at__extract_doy
                , subq_5.booking__ds__day
                , subq_5.booking__ds__week
                , subq_5.booking__ds__month
                , subq_5.booking__ds__quarter
                , subq_5.booking__ds__year
                , subq_5.booking__ds__extract_year
                , subq_5.booking__ds__extract_quarter
                , subq_5.booking__ds__extract_month
                , subq_5.booking__ds__extract_day
                , subq_5.booking__ds__extract_dow
                , subq_5.booking__ds__extract_doy
                , subq_5.booking__ds_partitioned__day
                , subq_5.booking__ds_partitioned__week
                , subq_5.booking__ds_partitioned__month
                , subq_5.booking__ds_partitioned__quarter
                , subq_5.booking__ds_partitioned__year
                , subq_5.booking__ds_partitioned__extract_year
                , subq_5.booking__ds_partitioned__extract_quarter
                , subq_5.booking__ds_partitioned__extract_month
                , subq_5.booking__ds_partitioned__extract_day
                , subq_5.booking__ds_partitioned__extract_dow
                , subq_5.booking__ds_partitioned__extract_doy
                , subq_5.booking__paid_at__day
                , subq_5.booking__paid_at__week
                , subq_5.booking__paid_at__month
                , subq_5.booking__paid_at__quarter
                , subq_5.booking__paid_at__year
                , subq_5.booking__paid_at__extract_year
                , subq_5.booking__paid_at__extract_quarter
                , subq_5.booking__paid_at__extract_month
                , subq_5.booking__paid_at__extract_day
                , subq_5.booking__paid_at__extract_dow
                , subq_5.booking__paid_at__extract_doy
                , subq_5.ds__day AS metric_time__day
                , subq_5.ds__week AS metric_time__week
                , subq_5.ds__month AS metric_time__month
                , subq_5.ds__quarter AS metric_time__quarter
                , subq_5.ds__year AS metric_time__year
                , subq_5.ds__extract_year AS metric_time__extract_year
                , subq_5.ds__extract_quarter AS metric_time__extract_quarter
                , subq_5.ds__extract_month AS metric_time__extract_month
                , subq_5.ds__extract_day AS metric_time__extract_day
                , subq_5.ds__extract_dow AS metric_time__extract_dow
                , subq_5.ds__extract_doy AS metric_time__extract_doy
                , subq_5.listing
                , subq_5.guest
                , subq_5.host
                , subq_5.booking__listing
                , subq_5.booking__guest
                , subq_5.booking__host
                , subq_5.is_instant
                , subq_5.booking__is_instant
                , subq_5.bookings
                , subq_5.instant_bookings
                , subq_5.booking_value
                , subq_5.max_booking_value
                , subq_5.min_booking_value
                , subq_5.bookers
                , subq_5.average_booking_value
                , subq_5.referred_bookings
                , subq_5.median_booking_value
                , subq_5.booking_value_p99
                , subq_5.discrete_booking_value_p99
                , subq_5.approximate_continuous_booking_value_p99
                , subq_5.approximate_discrete_booking_value_p99
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
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
                  , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
                  , bookings_source_src_28000.listing_id AS listing
                  , bookings_source_src_28000.guest_id AS guest
                  , bookings_source_src_28000.host_id AS host
                  , bookings_source_src_28000.listing_id AS booking__listing
                  , bookings_source_src_28000.guest_id AS booking__guest
                  , bookings_source_src_28000.host_id AS booking__host
                FROM ***************************.fct_bookings bookings_source_src_28000
              ) subq_5
            ) subq_6
          ) subq_7
          GROUP BY
            subq_7.listing
        ) subq_8
      ) subq_9
    ) subq_10
    ON
      subq_4.listing = subq_10.listing
  ) subq_11
  WHERE listing__bookings > 2
) subq_12
GROUP BY
  subq_12.listing
