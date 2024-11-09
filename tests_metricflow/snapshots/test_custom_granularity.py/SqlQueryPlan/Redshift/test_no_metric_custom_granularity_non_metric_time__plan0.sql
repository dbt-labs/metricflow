test_name: test_no_metric_custom_granularity_non_metric_time
test_filename: test_custom_granularity.py
---
-- Pass Only Elements: ['booking__ds__martian_day',]
SELECT
  subq_1.booking__ds__martian_day
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Join to Custom Granularity Dataset
  SELECT
    1 AS bookings
    , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
    , bookings_source_src_28000.booking_value AS booking_value
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
    , bookings_source_src_28000.is_instant AS is_instant
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
    , subq_0.martian_day AS booking__ds__martian_day
  FROM ***************************.fct_bookings bookings_source_src_28000
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_0
  ON
    DATE_TRUNC('day', bookings_source_src_28000.ds) = subq_0.ds
) subq_1
GROUP BY
  subq_1.booking__ds__martian_day
