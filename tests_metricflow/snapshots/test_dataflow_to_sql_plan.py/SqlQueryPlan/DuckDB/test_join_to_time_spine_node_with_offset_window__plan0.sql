-- Join to Time Spine Dataset
SELECT
  subq_4.metric_time__day AS metric_time__day
  , subq_3.listing AS listing
  , subq_3.booking_fees AS booking_fees
FROM (
  -- Time Spine
  SELECT
    subq_5.ds AS metric_time__day
  FROM ***************************.mf_time_spine subq_5
  WHERE subq_5.ds BETWEEN '2020-01-01' AND '2021-01-01'
) subq_4
INNER JOIN (
  -- Compute Metrics via Expressions
  SELECT
    subq_2.metric_time__day
    , subq_2.listing
    , booking_value * 0.05 AS booking_fees
  FROM (
    -- Aggregate Measures
    SELECT
      subq_1.metric_time__day
      , subq_1.listing
      , SUM(subq_1.booking_value) AS booking_value
    FROM (
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['booking_value', 'metric_time__day', 'listing']
      SELECT
        subq_0.ds__day AS metric_time__day
        , subq_0.listing
        , subq_0.booking_value
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
      ) subq_0
    ) subq_1
    GROUP BY
      subq_1.metric_time__day
      , subq_1.listing
  ) subq_2
) subq_3
ON
  subq_4.metric_time__day - INTERVAL 10 day = subq_3.metric_time__day
