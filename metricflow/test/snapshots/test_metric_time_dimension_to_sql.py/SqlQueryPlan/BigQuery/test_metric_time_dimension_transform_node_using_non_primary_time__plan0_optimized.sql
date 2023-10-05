-- Read Elements From Semantic Model 'bookings_source'
-- Metric Time Dimension 'paid_at'
SELECT
  DATE_TRUNC(ds, day) AS ds__day
  , DATE_TRUNC(ds, isoweek) AS ds__week
  , DATE_TRUNC(ds, month) AS ds__month
  , DATE_TRUNC(ds, quarter) AS ds__quarter
  , DATE_TRUNC(ds, year) AS ds__year
  , EXTRACT(year FROM ds) AS ds__extract_year
  , EXTRACT(quarter FROM ds) AS ds__extract_quarter
  , EXTRACT(month FROM ds) AS ds__extract_month
  , EXTRACT(isoweek FROM ds) AS ds__extract_week
  , EXTRACT(day FROM ds) AS ds__extract_day
  , EXTRACT(dayofweek FROM ds) AS ds__extract_dow
  , EXTRACT(dayofyear FROM ds) AS ds__extract_doy
  , DATE_TRUNC(ds_partitioned, day) AS ds_partitioned__day
  , DATE_TRUNC(ds_partitioned, isoweek) AS ds_partitioned__week
  , DATE_TRUNC(ds_partitioned, month) AS ds_partitioned__month
  , DATE_TRUNC(ds_partitioned, quarter) AS ds_partitioned__quarter
  , DATE_TRUNC(ds_partitioned, year) AS ds_partitioned__year
  , EXTRACT(year FROM ds_partitioned) AS ds_partitioned__extract_year
  , EXTRACT(quarter FROM ds_partitioned) AS ds_partitioned__extract_quarter
  , EXTRACT(month FROM ds_partitioned) AS ds_partitioned__extract_month
  , EXTRACT(isoweek FROM ds_partitioned) AS ds_partitioned__extract_week
  , EXTRACT(day FROM ds_partitioned) AS ds_partitioned__extract_day
  , EXTRACT(dayofweek FROM ds_partitioned) AS ds_partitioned__extract_dow
  , EXTRACT(dayofyear FROM ds_partitioned) AS ds_partitioned__extract_doy
  , DATE_TRUNC(paid_at, day) AS paid_at__day
  , DATE_TRUNC(paid_at, isoweek) AS paid_at__week
  , DATE_TRUNC(paid_at, month) AS paid_at__month
  , DATE_TRUNC(paid_at, quarter) AS paid_at__quarter
  , DATE_TRUNC(paid_at, year) AS paid_at__year
  , EXTRACT(year FROM paid_at) AS paid_at__extract_year
  , EXTRACT(quarter FROM paid_at) AS paid_at__extract_quarter
  , EXTRACT(month FROM paid_at) AS paid_at__extract_month
  , EXTRACT(isoweek FROM paid_at) AS paid_at__extract_week
  , EXTRACT(day FROM paid_at) AS paid_at__extract_day
  , EXTRACT(dayofweek FROM paid_at) AS paid_at__extract_dow
  , EXTRACT(dayofyear FROM paid_at) AS paid_at__extract_doy
  , DATE_TRUNC(ds, day) AS booking__ds__day
  , DATE_TRUNC(ds, isoweek) AS booking__ds__week
  , DATE_TRUNC(ds, month) AS booking__ds__month
  , DATE_TRUNC(ds, quarter) AS booking__ds__quarter
  , DATE_TRUNC(ds, year) AS booking__ds__year
  , EXTRACT(year FROM ds) AS booking__ds__extract_year
  , EXTRACT(quarter FROM ds) AS booking__ds__extract_quarter
  , EXTRACT(month FROM ds) AS booking__ds__extract_month
  , EXTRACT(isoweek FROM ds) AS booking__ds__extract_week
  , EXTRACT(day FROM ds) AS booking__ds__extract_day
  , EXTRACT(dayofweek FROM ds) AS booking__ds__extract_dow
  , EXTRACT(dayofyear FROM ds) AS booking__ds__extract_doy
  , DATE_TRUNC(ds_partitioned, day) AS booking__ds_partitioned__day
  , DATE_TRUNC(ds_partitioned, isoweek) AS booking__ds_partitioned__week
  , DATE_TRUNC(ds_partitioned, month) AS booking__ds_partitioned__month
  , DATE_TRUNC(ds_partitioned, quarter) AS booking__ds_partitioned__quarter
  , DATE_TRUNC(ds_partitioned, year) AS booking__ds_partitioned__year
  , EXTRACT(year FROM ds_partitioned) AS booking__ds_partitioned__extract_year
  , EXTRACT(quarter FROM ds_partitioned) AS booking__ds_partitioned__extract_quarter
  , EXTRACT(month FROM ds_partitioned) AS booking__ds_partitioned__extract_month
  , EXTRACT(isoweek FROM ds_partitioned) AS booking__ds_partitioned__extract_week
  , EXTRACT(day FROM ds_partitioned) AS booking__ds_partitioned__extract_day
  , EXTRACT(dayofweek FROM ds_partitioned) AS booking__ds_partitioned__extract_dow
  , EXTRACT(dayofyear FROM ds_partitioned) AS booking__ds_partitioned__extract_doy
  , DATE_TRUNC(paid_at, day) AS booking__paid_at__day
  , DATE_TRUNC(paid_at, isoweek) AS booking__paid_at__week
  , DATE_TRUNC(paid_at, month) AS booking__paid_at__month
  , DATE_TRUNC(paid_at, quarter) AS booking__paid_at__quarter
  , DATE_TRUNC(paid_at, year) AS booking__paid_at__year
  , EXTRACT(year FROM paid_at) AS booking__paid_at__extract_year
  , EXTRACT(quarter FROM paid_at) AS booking__paid_at__extract_quarter
  , EXTRACT(month FROM paid_at) AS booking__paid_at__extract_month
  , EXTRACT(isoweek FROM paid_at) AS booking__paid_at__extract_week
  , EXTRACT(day FROM paid_at) AS booking__paid_at__extract_day
  , EXTRACT(dayofweek FROM paid_at) AS booking__paid_at__extract_dow
  , EXTRACT(dayofyear FROM paid_at) AS booking__paid_at__extract_doy
  , DATE_TRUNC(paid_at, day) AS metric_time__day
  , DATE_TRUNC(paid_at, isoweek) AS metric_time__week
  , DATE_TRUNC(paid_at, month) AS metric_time__month
  , DATE_TRUNC(paid_at, quarter) AS metric_time__quarter
  , DATE_TRUNC(paid_at, year) AS metric_time__year
  , EXTRACT(year FROM paid_at) AS metric_time__extract_year
  , EXTRACT(quarter FROM paid_at) AS metric_time__extract_quarter
  , EXTRACT(month FROM paid_at) AS metric_time__extract_month
  , EXTRACT(isoweek FROM paid_at) AS metric_time__extract_week
  , EXTRACT(day FROM paid_at) AS metric_time__extract_day
  , EXTRACT(dayofweek FROM paid_at) AS metric_time__extract_dow
  , EXTRACT(dayofyear FROM paid_at) AS metric_time__extract_doy
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
