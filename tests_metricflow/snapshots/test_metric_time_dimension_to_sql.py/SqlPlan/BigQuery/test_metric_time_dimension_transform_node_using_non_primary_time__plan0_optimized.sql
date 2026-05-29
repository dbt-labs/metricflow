test_name: test_metric_time_dimension_transform_node_using_non_primary_time
test_filename: test_metric_time_dimension_to_sql.py
docstring:
  Tests converting a PlotTimeDimensionTransform node using a non-primary time dimension to SQL.
sql_engine: BigQuery
---
-- Read Elements From Semantic Model 'bookings_source'
-- Metric Time Dimension 'paid_at'
SELECT
  DATETIME_TRUNC(ds, day) AS ds__day
  , DATETIME_TRUNC(ds, isoweek) AS ds__week
  , DATETIME_TRUNC(ds, month) AS ds__month
  , DATETIME_TRUNC(ds, quarter) AS ds__quarter
  , DATETIME_TRUNC(ds, year) AS ds__year
  , EXTRACT(year FROM ds) AS ds__extract_year
  , EXTRACT(quarter FROM ds) AS ds__extract_quarter
  , EXTRACT(month FROM ds) AS ds__extract_month
  , EXTRACT(day FROM ds) AS ds__extract_day
  , IF(EXTRACT(dayofweek FROM ds) = 1, 7, EXTRACT(dayofweek FROM ds) - 1) AS ds__extract_dow
  , EXTRACT(dayofyear FROM ds) AS ds__extract_doy
  , DATETIME_TRUNC(ds_partitioned, day) AS ds_partitioned__day
  , DATETIME_TRUNC(ds_partitioned, isoweek) AS ds_partitioned__week
  , DATETIME_TRUNC(ds_partitioned, month) AS ds_partitioned__month
  , DATETIME_TRUNC(ds_partitioned, quarter) AS ds_partitioned__quarter
  , DATETIME_TRUNC(ds_partitioned, year) AS ds_partitioned__year
  , EXTRACT(year FROM ds_partitioned) AS ds_partitioned__extract_year
  , EXTRACT(quarter FROM ds_partitioned) AS ds_partitioned__extract_quarter
  , EXTRACT(month FROM ds_partitioned) AS ds_partitioned__extract_month
  , EXTRACT(day FROM ds_partitioned) AS ds_partitioned__extract_day
  , IF(EXTRACT(dayofweek FROM ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM ds_partitioned) - 1) AS ds_partitioned__extract_dow
  , EXTRACT(dayofyear FROM ds_partitioned) AS ds_partitioned__extract_doy
  , DATETIME_TRUNC(paid_at, day) AS paid_at__day
  , DATETIME_TRUNC(paid_at, isoweek) AS paid_at__week
  , DATETIME_TRUNC(paid_at, month) AS paid_at__month
  , DATETIME_TRUNC(paid_at, quarter) AS paid_at__quarter
  , DATETIME_TRUNC(paid_at, year) AS paid_at__year
  , EXTRACT(year FROM paid_at) AS paid_at__extract_year
  , EXTRACT(quarter FROM paid_at) AS paid_at__extract_quarter
  , EXTRACT(month FROM paid_at) AS paid_at__extract_month
  , EXTRACT(day FROM paid_at) AS paid_at__extract_day
  , IF(EXTRACT(dayofweek FROM paid_at) = 1, 7, EXTRACT(dayofweek FROM paid_at) - 1) AS paid_at__extract_dow
  , EXTRACT(dayofyear FROM paid_at) AS paid_at__extract_doy
  , DATETIME_TRUNC(ds, day) AS booking__ds__day
  , DATETIME_TRUNC(ds, isoweek) AS booking__ds__week
  , DATETIME_TRUNC(ds, month) AS booking__ds__month
  , DATETIME_TRUNC(ds, quarter) AS booking__ds__quarter
  , DATETIME_TRUNC(ds, year) AS booking__ds__year
  , EXTRACT(year FROM ds) AS booking__ds__extract_year
  , EXTRACT(quarter FROM ds) AS booking__ds__extract_quarter
  , EXTRACT(month FROM ds) AS booking__ds__extract_month
  , EXTRACT(day FROM ds) AS booking__ds__extract_day
  , IF(EXTRACT(dayofweek FROM ds) = 1, 7, EXTRACT(dayofweek FROM ds) - 1) AS booking__ds__extract_dow
  , EXTRACT(dayofyear FROM ds) AS booking__ds__extract_doy
  , DATETIME_TRUNC(ds_partitioned, day) AS booking__ds_partitioned__day
  , DATETIME_TRUNC(ds_partitioned, isoweek) AS booking__ds_partitioned__week
  , DATETIME_TRUNC(ds_partitioned, month) AS booking__ds_partitioned__month
  , DATETIME_TRUNC(ds_partitioned, quarter) AS booking__ds_partitioned__quarter
  , DATETIME_TRUNC(ds_partitioned, year) AS booking__ds_partitioned__year
  , EXTRACT(year FROM ds_partitioned) AS booking__ds_partitioned__extract_year
  , EXTRACT(quarter FROM ds_partitioned) AS booking__ds_partitioned__extract_quarter
  , EXTRACT(month FROM ds_partitioned) AS booking__ds_partitioned__extract_month
  , EXTRACT(day FROM ds_partitioned) AS booking__ds_partitioned__extract_day
  , IF(EXTRACT(dayofweek FROM ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM ds_partitioned) - 1) AS booking__ds_partitioned__extract_dow
  , EXTRACT(dayofyear FROM ds_partitioned) AS booking__ds_partitioned__extract_doy
  , DATETIME_TRUNC(paid_at, day) AS booking__paid_at__day
  , DATETIME_TRUNC(paid_at, isoweek) AS booking__paid_at__week
  , DATETIME_TRUNC(paid_at, month) AS booking__paid_at__month
  , DATETIME_TRUNC(paid_at, quarter) AS booking__paid_at__quarter
  , DATETIME_TRUNC(paid_at, year) AS booking__paid_at__year
  , EXTRACT(year FROM paid_at) AS booking__paid_at__extract_year
  , EXTRACT(quarter FROM paid_at) AS booking__paid_at__extract_quarter
  , EXTRACT(month FROM paid_at) AS booking__paid_at__extract_month
  , EXTRACT(day FROM paid_at) AS booking__paid_at__extract_day
  , IF(EXTRACT(dayofweek FROM paid_at) = 1, 7, EXTRACT(dayofweek FROM paid_at) - 1) AS booking__paid_at__extract_dow
  , EXTRACT(dayofyear FROM paid_at) AS booking__paid_at__extract_doy
  , DATETIME_TRUNC(paid_at, day) AS metric_time__day
  , DATETIME_TRUNC(paid_at, isoweek) AS metric_time__week
  , DATETIME_TRUNC(paid_at, month) AS metric_time__month
  , DATETIME_TRUNC(paid_at, quarter) AS metric_time__quarter
  , DATETIME_TRUNC(paid_at, year) AS metric_time__year
  , EXTRACT(year FROM paid_at) AS metric_time__extract_year
  , EXTRACT(quarter FROM paid_at) AS metric_time__extract_quarter
  , EXTRACT(month FROM paid_at) AS metric_time__extract_month
  , EXTRACT(day FROM paid_at) AS metric_time__extract_day
  , IF(EXTRACT(dayofweek FROM paid_at) = 1, 7, EXTRACT(dayofweek FROM paid_at) - 1) AS metric_time__extract_dow
  , EXTRACT(dayofyear FROM paid_at) AS metric_time__extract_doy
  , listing_id AS listing
  , guest_id AS guest
  , host_id AS host
  , listing_id AS booking__listing
  , guest_id AS booking__guest
  , host_id AS booking__host
  , is_instant
  , is_instant AS booking__is_instant
  , booking_value AS __booking_payments
FROM ***************************.fct_bookings bookings_source_src_28000
