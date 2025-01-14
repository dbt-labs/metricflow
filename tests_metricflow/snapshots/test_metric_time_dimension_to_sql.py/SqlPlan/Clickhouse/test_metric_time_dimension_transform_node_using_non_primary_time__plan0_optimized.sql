test_name: test_metric_time_dimension_transform_node_using_non_primary_time
test_filename: test_metric_time_dimension_to_sql.py
docstring:
  Tests converting a PlotTimeDimensionTransform node using a non-primary time dimension to SQL.
sql_engine: Clickhouse
---
-- Read Elements From Semantic Model 'bookings_source'
-- Metric Time Dimension 'paid_at'
SELECT
  date_trunc('day', ds) AS ds__day
  , date_trunc('week', ds) AS ds__week
  , date_trunc('month', ds) AS ds__month
  , date_trunc('quarter', ds) AS ds__quarter
  , date_trunc('year', ds) AS ds__year
  , toYear(ds) AS ds__extract_year
  , toQuarter(ds) AS ds__extract_quarter
  , toMonth(ds) AS ds__extract_month
  , toDayOfMonth(ds) AS ds__extract_day
  , toDayOfWeek(ds) AS ds__extract_dow
  , toDayOfYear(ds) AS ds__extract_doy
  , date_trunc('day', ds_partitioned) AS ds_partitioned__day
  , date_trunc('week', ds_partitioned) AS ds_partitioned__week
  , date_trunc('month', ds_partitioned) AS ds_partitioned__month
  , date_trunc('quarter', ds_partitioned) AS ds_partitioned__quarter
  , date_trunc('year', ds_partitioned) AS ds_partitioned__year
  , toYear(ds_partitioned) AS ds_partitioned__extract_year
  , toQuarter(ds_partitioned) AS ds_partitioned__extract_quarter
  , toMonth(ds_partitioned) AS ds_partitioned__extract_month
  , toDayOfMonth(ds_partitioned) AS ds_partitioned__extract_day
  , toDayOfWeek(ds_partitioned) AS ds_partitioned__extract_dow
  , toDayOfYear(ds_partitioned) AS ds_partitioned__extract_doy
  , date_trunc('day', paid_at) AS paid_at__day
  , date_trunc('week', paid_at) AS paid_at__week
  , date_trunc('month', paid_at) AS paid_at__month
  , date_trunc('quarter', paid_at) AS paid_at__quarter
  , date_trunc('year', paid_at) AS paid_at__year
  , toYear(paid_at) AS paid_at__extract_year
  , toQuarter(paid_at) AS paid_at__extract_quarter
  , toMonth(paid_at) AS paid_at__extract_month
  , toDayOfMonth(paid_at) AS paid_at__extract_day
  , toDayOfWeek(paid_at) AS paid_at__extract_dow
  , toDayOfYear(paid_at) AS paid_at__extract_doy
  , date_trunc('day', ds) AS booking__ds__day
  , date_trunc('week', ds) AS booking__ds__week
  , date_trunc('month', ds) AS booking__ds__month
  , date_trunc('quarter', ds) AS booking__ds__quarter
  , date_trunc('year', ds) AS booking__ds__year
  , toYear(ds) AS booking__ds__extract_year
  , toQuarter(ds) AS booking__ds__extract_quarter
  , toMonth(ds) AS booking__ds__extract_month
  , toDayOfMonth(ds) AS booking__ds__extract_day
  , toDayOfWeek(ds) AS booking__ds__extract_dow
  , toDayOfYear(ds) AS booking__ds__extract_doy
  , date_trunc('day', ds_partitioned) AS booking__ds_partitioned__day
  , date_trunc('week', ds_partitioned) AS booking__ds_partitioned__week
  , date_trunc('month', ds_partitioned) AS booking__ds_partitioned__month
  , date_trunc('quarter', ds_partitioned) AS booking__ds_partitioned__quarter
  , date_trunc('year', ds_partitioned) AS booking__ds_partitioned__year
  , toYear(ds_partitioned) AS booking__ds_partitioned__extract_year
  , toQuarter(ds_partitioned) AS booking__ds_partitioned__extract_quarter
  , toMonth(ds_partitioned) AS booking__ds_partitioned__extract_month
  , toDayOfMonth(ds_partitioned) AS booking__ds_partitioned__extract_day
  , toDayOfWeek(ds_partitioned) AS booking__ds_partitioned__extract_dow
  , toDayOfYear(ds_partitioned) AS booking__ds_partitioned__extract_doy
  , date_trunc('day', paid_at) AS booking__paid_at__day
  , date_trunc('week', paid_at) AS booking__paid_at__week
  , date_trunc('month', paid_at) AS booking__paid_at__month
  , date_trunc('quarter', paid_at) AS booking__paid_at__quarter
  , date_trunc('year', paid_at) AS booking__paid_at__year
  , toYear(paid_at) AS booking__paid_at__extract_year
  , toQuarter(paid_at) AS booking__paid_at__extract_quarter
  , toMonth(paid_at) AS booking__paid_at__extract_month
  , toDayOfMonth(paid_at) AS booking__paid_at__extract_day
  , toDayOfWeek(paid_at) AS booking__paid_at__extract_dow
  , toDayOfYear(paid_at) AS booking__paid_at__extract_doy
  , date_trunc('day', paid_at) AS metric_time__day
  , date_trunc('week', paid_at) AS metric_time__week
  , date_trunc('month', paid_at) AS metric_time__month
  , date_trunc('quarter', paid_at) AS metric_time__quarter
  , date_trunc('year', paid_at) AS metric_time__year
  , toYear(paid_at) AS metric_time__extract_year
  , toQuarter(paid_at) AS metric_time__extract_quarter
  , toMonth(paid_at) AS metric_time__extract_month
  , toDayOfMonth(paid_at) AS metric_time__extract_day
  , toDayOfWeek(paid_at) AS metric_time__extract_dow
  , toDayOfYear(paid_at) AS metric_time__extract_doy
  , listing_id AS listing
  , guest_id AS guest
  , host_id AS host
  , listing_id AS booking__listing
  , guest_id AS booking__guest
  , host_id AS booking__host
  , is_instant
  , is_instant AS booking__is_instant
  , booking_value AS booking_payments
FROM ***************************.fct_bookings bookings_source_src_28000
