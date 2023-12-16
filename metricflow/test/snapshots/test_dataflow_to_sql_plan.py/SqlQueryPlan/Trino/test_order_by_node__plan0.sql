-- Order By ['ds__day', 'bookings']
SELECT
  subq_3.ds__day
  , subq_3.is_instant
  , subq_3.bookings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_2.ds__day
    , subq_2.is_instant
    , subq_2.bookings
  FROM (
    -- Aggregate Measures
    SELECT
      subq_1.ds__day
      , subq_1.is_instant
      , SUM(subq_1.bookings) AS bookings
    FROM (
      -- Pass Only Elements:
      --   ['bookings', 'is_instant', 'ds__day']
      SELECT
        subq_0.ds__day
        , subq_0.is_instant
        , subq_0.bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        SELECT
          1 AS bookings
          , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
          , bookings_source_src_10001.booking_value
          , bookings_source_src_10001.booking_value AS max_booking_value
          , bookings_source_src_10001.booking_value AS min_booking_value
          , bookings_source_src_10001.guest_id AS bookers
          , bookings_source_src_10001.booking_value AS average_booking_value
          , bookings_source_src_10001.booking_value AS booking_payments
          , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
          , bookings_source_src_10001.booking_value AS median_booking_value
          , bookings_source_src_10001.booking_value AS booking_value_p99
          , bookings_source_src_10001.booking_value AS discrete_booking_value_p99
          , bookings_source_src_10001.booking_value AS approximate_continuous_booking_value_p99
          , bookings_source_src_10001.booking_value AS approximate_discrete_booking_value_p99
          , bookings_source_src_10001.is_instant
          , DATE_TRUNC('day', bookings_source_src_10001.ds) AS ds__day
          , DATE_TRUNC('week', bookings_source_src_10001.ds) AS ds__week
          , DATE_TRUNC('month', bookings_source_src_10001.ds) AS ds__month
          , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS ds__quarter
          , DATE_TRUNC('year', bookings_source_src_10001.ds) AS ds__year
          , EXTRACT(year FROM bookings_source_src_10001.ds) AS ds__extract_year
          , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS ds__extract_quarter
          , EXTRACT(month FROM bookings_source_src_10001.ds) AS ds__extract_month
          , EXTRACT(day FROM bookings_source_src_10001.ds) AS ds__extract_day
          , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.ds) AS ds__extract_dow
          , EXTRACT(doy FROM bookings_source_src_10001.ds) AS ds__extract_doy
          , DATE_TRUNC('day', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__day
          , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__week
          , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__month
          , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__quarter
          , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__year
          , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_year
          , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_quarter
          , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_month
          , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_day
          , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_dow
          , EXTRACT(doy FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_doy
          , DATE_TRUNC('day', bookings_source_src_10001.paid_at) AS paid_at__day
          , DATE_TRUNC('week', bookings_source_src_10001.paid_at) AS paid_at__week
          , DATE_TRUNC('month', bookings_source_src_10001.paid_at) AS paid_at__month
          , DATE_TRUNC('quarter', bookings_source_src_10001.paid_at) AS paid_at__quarter
          , DATE_TRUNC('year', bookings_source_src_10001.paid_at) AS paid_at__year
          , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS paid_at__extract_year
          , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS paid_at__extract_quarter
          , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS paid_at__extract_month
          , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS paid_at__extract_day
          , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.paid_at) AS paid_at__extract_dow
          , EXTRACT(doy FROM bookings_source_src_10001.paid_at) AS paid_at__extract_doy
          , bookings_source_src_10001.is_instant AS booking__is_instant
          , DATE_TRUNC('day', bookings_source_src_10001.ds) AS booking__ds__day
          , DATE_TRUNC('week', bookings_source_src_10001.ds) AS booking__ds__week
          , DATE_TRUNC('month', bookings_source_src_10001.ds) AS booking__ds__month
          , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS booking__ds__quarter
          , DATE_TRUNC('year', bookings_source_src_10001.ds) AS booking__ds__year
          , EXTRACT(year FROM bookings_source_src_10001.ds) AS booking__ds__extract_year
          , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS booking__ds__extract_quarter
          , EXTRACT(month FROM bookings_source_src_10001.ds) AS booking__ds__extract_month
          , EXTRACT(day FROM bookings_source_src_10001.ds) AS booking__ds__extract_day
          , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.ds) AS booking__ds__extract_dow
          , EXTRACT(doy FROM bookings_source_src_10001.ds) AS booking__ds__extract_doy
          , DATE_TRUNC('day', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__day
          , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__week
          , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__month
          , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__quarter
          , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__year
          , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_year
          , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_quarter
          , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_month
          , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_day
          , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_dow
          , EXTRACT(doy FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_doy
          , DATE_TRUNC('day', bookings_source_src_10001.paid_at) AS booking__paid_at__day
          , DATE_TRUNC('week', bookings_source_src_10001.paid_at) AS booking__paid_at__week
          , DATE_TRUNC('month', bookings_source_src_10001.paid_at) AS booking__paid_at__month
          , DATE_TRUNC('quarter', bookings_source_src_10001.paid_at) AS booking__paid_at__quarter
          , DATE_TRUNC('year', bookings_source_src_10001.paid_at) AS booking__paid_at__year
          , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_year
          , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_quarter
          , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_month
          , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_day
          , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_dow
          , EXTRACT(doy FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_doy
          , bookings_source_src_10001.listing_id AS listing
          , bookings_source_src_10001.guest_id AS guest
          , bookings_source_src_10001.host_id AS host
          , bookings_source_src_10001.listing_id AS booking__listing
          , bookings_source_src_10001.guest_id AS booking__guest
          , bookings_source_src_10001.host_id AS booking__host
        FROM ***************************.fct_bookings bookings_source_src_10001
      ) subq_0
    ) subq_1
    GROUP BY
      subq_1.ds__day
      , subq_1.is_instant
  ) subq_2
) subq_3
ORDER BY subq_3.ds__day, subq_3.bookings DESC
