-- Read Elements From Semantic Model 'bookings_source'
SELECT
  1 AS bookings
  , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
  , booking_value
  , booking_value AS max_booking_value
  , booking_value AS min_booking_value
  , guest_id AS bookers
  , booking_value AS average_booking_value
  , booking_value AS booking_payments
  , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
  , booking_value AS median_booking_value
  , booking_value AS booking_value_p99
  , booking_value AS discrete_booking_value_p99
  , booking_value AS approximate_continuous_booking_value_p99
  , booking_value AS approximate_discrete_booking_value_p99
  , is_instant
  , ds AS ds__day
  , DATE_TRUNC('week', ds) AS ds__week
  , DATE_TRUNC('month', ds) AS ds__month
  , DATE_TRUNC('quarter', ds) AS ds__quarter
  , DATE_TRUNC('year', ds) AS ds__year
  , EXTRACT(YEAR FROM ds) AS ds__extract_year
  , EXTRACT(QUARTER FROM ds) AS ds__extract_quarter
  , EXTRACT(MONTH FROM ds) AS ds__extract_month
  , EXTRACT(WEEK FROM ds) AS ds__extract_week
  , EXTRACT(DAY FROM ds) AS ds__extract_day
  , EXTRACT(DAYOFWEEK FROM ds) AS ds__extract_dayofweek
  , EXTRACT(DOY FROM ds) AS ds__extract_dayofyear
  , ds_partitioned AS ds_partitioned__day
  , DATE_TRUNC('week', ds_partitioned) AS ds_partitioned__week
  , DATE_TRUNC('month', ds_partitioned) AS ds_partitioned__month
  , DATE_TRUNC('quarter', ds_partitioned) AS ds_partitioned__quarter
  , DATE_TRUNC('year', ds_partitioned) AS ds_partitioned__year
  , EXTRACT(YEAR FROM ds_partitioned) AS ds_partitioned__extract_year
  , EXTRACT(QUARTER FROM ds_partitioned) AS ds_partitioned__extract_quarter
  , EXTRACT(MONTH FROM ds_partitioned) AS ds_partitioned__extract_month
  , EXTRACT(WEEK FROM ds_partitioned) AS ds_partitioned__extract_week
  , EXTRACT(DAY FROM ds_partitioned) AS ds_partitioned__extract_day
  , EXTRACT(DAYOFWEEK FROM ds_partitioned) AS ds_partitioned__extract_dayofweek
  , EXTRACT(DOY FROM ds_partitioned) AS ds_partitioned__extract_dayofyear
  , paid_at AS paid_at__day
  , DATE_TRUNC('week', paid_at) AS paid_at__week
  , DATE_TRUNC('month', paid_at) AS paid_at__month
  , DATE_TRUNC('quarter', paid_at) AS paid_at__quarter
  , DATE_TRUNC('year', paid_at) AS paid_at__year
  , EXTRACT(YEAR FROM paid_at) AS paid_at__extract_year
  , EXTRACT(QUARTER FROM paid_at) AS paid_at__extract_quarter
  , EXTRACT(MONTH FROM paid_at) AS paid_at__extract_month
  , EXTRACT(WEEK FROM paid_at) AS paid_at__extract_week
  , EXTRACT(DAY FROM paid_at) AS paid_at__extract_day
  , EXTRACT(DAYOFWEEK FROM paid_at) AS paid_at__extract_dayofweek
  , EXTRACT(DOY FROM paid_at) AS paid_at__extract_dayofyear
  , is_instant AS booking__is_instant
  , ds AS booking__ds__day
  , DATE_TRUNC('week', ds) AS booking__ds__week
  , DATE_TRUNC('month', ds) AS booking__ds__month
  , DATE_TRUNC('quarter', ds) AS booking__ds__quarter
  , DATE_TRUNC('year', ds) AS booking__ds__year
  , EXTRACT(YEAR FROM ds) AS booking__ds__extract_year
  , EXTRACT(QUARTER FROM ds) AS booking__ds__extract_quarter
  , EXTRACT(MONTH FROM ds) AS booking__ds__extract_month
  , EXTRACT(WEEK FROM ds) AS booking__ds__extract_week
  , EXTRACT(DAY FROM ds) AS booking__ds__extract_day
  , EXTRACT(DAYOFWEEK FROM ds) AS booking__ds__extract_dayofweek
  , EXTRACT(DOY FROM ds) AS booking__ds__extract_dayofyear
  , ds_partitioned AS booking__ds_partitioned__day
  , DATE_TRUNC('week', ds_partitioned) AS booking__ds_partitioned__week
  , DATE_TRUNC('month', ds_partitioned) AS booking__ds_partitioned__month
  , DATE_TRUNC('quarter', ds_partitioned) AS booking__ds_partitioned__quarter
  , DATE_TRUNC('year', ds_partitioned) AS booking__ds_partitioned__year
  , EXTRACT(YEAR FROM ds_partitioned) AS booking__ds_partitioned__extract_year
  , EXTRACT(QUARTER FROM ds_partitioned) AS booking__ds_partitioned__extract_quarter
  , EXTRACT(MONTH FROM ds_partitioned) AS booking__ds_partitioned__extract_month
  , EXTRACT(WEEK FROM ds_partitioned) AS booking__ds_partitioned__extract_week
  , EXTRACT(DAY FROM ds_partitioned) AS booking__ds_partitioned__extract_day
  , EXTRACT(DAYOFWEEK FROM ds_partitioned) AS booking__ds_partitioned__extract_dayofweek
  , EXTRACT(DOY FROM ds_partitioned) AS booking__ds_partitioned__extract_dayofyear
  , paid_at AS booking__paid_at__day
  , DATE_TRUNC('week', paid_at) AS booking__paid_at__week
  , DATE_TRUNC('month', paid_at) AS booking__paid_at__month
  , DATE_TRUNC('quarter', paid_at) AS booking__paid_at__quarter
  , DATE_TRUNC('year', paid_at) AS booking__paid_at__year
  , EXTRACT(YEAR FROM paid_at) AS booking__paid_at__extract_year
  , EXTRACT(QUARTER FROM paid_at) AS booking__paid_at__extract_quarter
  , EXTRACT(MONTH FROM paid_at) AS booking__paid_at__extract_month
  , EXTRACT(WEEK FROM paid_at) AS booking__paid_at__extract_week
  , EXTRACT(DAY FROM paid_at) AS booking__paid_at__extract_day
  , EXTRACT(DAYOFWEEK FROM paid_at) AS booking__paid_at__extract_dayofweek
  , EXTRACT(DOY FROM paid_at) AS booking__paid_at__extract_dayofyear
  , listing_id AS listing
  , guest_id AS guest
  , host_id AS host
  , listing_id AS booking__listing
  , guest_id AS booking__guest
  , host_id AS booking__host
FROM ***************************.fct_bookings bookings_source_src_10001
