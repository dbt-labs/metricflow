-- Read Elements From Semantic Model 'bookings_source'
-- Metric Time Dimension 'paid_at'
SELECT
  DATE_TRUNC('day', ds) AS ds__day
  , DATE_TRUNC('week', ds) AS ds__week
  , DATE_TRUNC('month', ds) AS ds__month
  , DATE_TRUNC('quarter', ds) AS ds__quarter
  , DATE_TRUNC('year', ds) AS ds__year
  , EXTRACT(year FROM ds) AS ds__extract_year
  , EXTRACT(quarter FROM ds) AS ds__extract_quarter
  , EXTRACT(month FROM ds) AS ds__extract_month
  , EXTRACT(week FROM ds) AS ds__extract_week
  , EXTRACT(day FROM ds) AS ds__extract_day
  , EXTRACT(dow FROM ds) AS ds__extract_dow
  , EXTRACT(doy FROM ds) AS ds__extract_doy
  , DATE_TRUNC('day', ds_partitioned) AS ds_partitioned__day
  , DATE_TRUNC('week', ds_partitioned) AS ds_partitioned__week
  , DATE_TRUNC('month', ds_partitioned) AS ds_partitioned__month
  , DATE_TRUNC('quarter', ds_partitioned) AS ds_partitioned__quarter
  , DATE_TRUNC('year', ds_partitioned) AS ds_partitioned__year
  , EXTRACT(year FROM ds_partitioned) AS ds_partitioned__extract_year
  , EXTRACT(quarter FROM ds_partitioned) AS ds_partitioned__extract_quarter
  , EXTRACT(month FROM ds_partitioned) AS ds_partitioned__extract_month
  , EXTRACT(week FROM ds_partitioned) AS ds_partitioned__extract_week
  , EXTRACT(day FROM ds_partitioned) AS ds_partitioned__extract_day
  , EXTRACT(dow FROM ds_partitioned) AS ds_partitioned__extract_dow
  , EXTRACT(doy FROM ds_partitioned) AS ds_partitioned__extract_doy
  , DATE_TRUNC('day', paid_at) AS paid_at__day
  , DATE_TRUNC('week', paid_at) AS paid_at__week
  , DATE_TRUNC('month', paid_at) AS paid_at__month
  , DATE_TRUNC('quarter', paid_at) AS paid_at__quarter
  , DATE_TRUNC('year', paid_at) AS paid_at__year
  , EXTRACT(year FROM paid_at) AS paid_at__extract_year
  , EXTRACT(quarter FROM paid_at) AS paid_at__extract_quarter
  , EXTRACT(month FROM paid_at) AS paid_at__extract_month
  , EXTRACT(week FROM paid_at) AS paid_at__extract_week
  , EXTRACT(day FROM paid_at) AS paid_at__extract_day
  , EXTRACT(dow FROM paid_at) AS paid_at__extract_dow
  , EXTRACT(doy FROM paid_at) AS paid_at__extract_doy
  , DATE_TRUNC('day', ds) AS booking__ds__day
  , DATE_TRUNC('week', ds) AS booking__ds__week
  , DATE_TRUNC('month', ds) AS booking__ds__month
  , DATE_TRUNC('quarter', ds) AS booking__ds__quarter
  , DATE_TRUNC('year', ds) AS booking__ds__year
  , EXTRACT(year FROM ds) AS booking__ds__extract_year
  , EXTRACT(quarter FROM ds) AS booking__ds__extract_quarter
  , EXTRACT(month FROM ds) AS booking__ds__extract_month
  , EXTRACT(week FROM ds) AS booking__ds__extract_week
  , EXTRACT(day FROM ds) AS booking__ds__extract_day
  , EXTRACT(dow FROM ds) AS booking__ds__extract_dow
  , EXTRACT(doy FROM ds) AS booking__ds__extract_doy
  , DATE_TRUNC('day', ds_partitioned) AS booking__ds_partitioned__day
  , DATE_TRUNC('week', ds_partitioned) AS booking__ds_partitioned__week
  , DATE_TRUNC('month', ds_partitioned) AS booking__ds_partitioned__month
  , DATE_TRUNC('quarter', ds_partitioned) AS booking__ds_partitioned__quarter
  , DATE_TRUNC('year', ds_partitioned) AS booking__ds_partitioned__year
  , EXTRACT(year FROM ds_partitioned) AS booking__ds_partitioned__extract_year
  , EXTRACT(quarter FROM ds_partitioned) AS booking__ds_partitioned__extract_quarter
  , EXTRACT(month FROM ds_partitioned) AS booking__ds_partitioned__extract_month
  , EXTRACT(week FROM ds_partitioned) AS booking__ds_partitioned__extract_week
  , EXTRACT(day FROM ds_partitioned) AS booking__ds_partitioned__extract_day
  , EXTRACT(dow FROM ds_partitioned) AS booking__ds_partitioned__extract_dow
  , EXTRACT(doy FROM ds_partitioned) AS booking__ds_partitioned__extract_doy
  , DATE_TRUNC('day', paid_at) AS booking__paid_at__day
  , DATE_TRUNC('week', paid_at) AS booking__paid_at__week
  , DATE_TRUNC('month', paid_at) AS booking__paid_at__month
  , DATE_TRUNC('quarter', paid_at) AS booking__paid_at__quarter
  , DATE_TRUNC('year', paid_at) AS booking__paid_at__year
  , EXTRACT(year FROM paid_at) AS booking__paid_at__extract_year
  , EXTRACT(quarter FROM paid_at) AS booking__paid_at__extract_quarter
  , EXTRACT(month FROM paid_at) AS booking__paid_at__extract_month
  , EXTRACT(week FROM paid_at) AS booking__paid_at__extract_week
  , EXTRACT(day FROM paid_at) AS booking__paid_at__extract_day
  , EXTRACT(dow FROM paid_at) AS booking__paid_at__extract_dow
  , EXTRACT(doy FROM paid_at) AS booking__paid_at__extract_doy
  , DATE_TRUNC('day', paid_at) AS metric_time__day
  , DATE_TRUNC('week', paid_at) AS metric_time__week
  , DATE_TRUNC('month', paid_at) AS metric_time__month
  , DATE_TRUNC('quarter', paid_at) AS metric_time__quarter
  , DATE_TRUNC('year', paid_at) AS metric_time__year
  , EXTRACT(year FROM paid_at) AS metric_time__extract_year
  , EXTRACT(quarter FROM paid_at) AS metric_time__extract_quarter
  , EXTRACT(month FROM paid_at) AS metric_time__extract_month
  , EXTRACT(week FROM paid_at) AS metric_time__extract_week
  , EXTRACT(day FROM paid_at) AS metric_time__extract_day
  , EXTRACT(dow FROM paid_at) AS metric_time__extract_dow
  , EXTRACT(doy FROM paid_at) AS metric_time__extract_doy
  , listing_id AS listing
  , guest_id AS guest
  , host_id AS host
  , listing_id AS booking__listing
  , guest_id AS booking__guest
  , host_id AS booking__host
  , is_instant
  , is_instant AS booking__is_instant
  , booking_value AS booking_payments
FROM ***************************.fct_bookings bookings_source_src_10001
