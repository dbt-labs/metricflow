test_name: test_metric_time_dimension_transform_node_using_primary_time
test_filename: test_metric_time_dimension_to_sql.py
docstring:
  Tests converting a PlotTimeDimensionTransform node using the primary time dimension to SQL.
sql_engine: ClickHouse
---
SELECT
  toStartOfDay(ds) AS ds__day
  , toStartOfWeek(ds, 1) AS ds__week
  , toStartOfMonth(ds) AS ds__month
  , toStartOfQuarter(ds) AS ds__quarter
  , toStartOfYear(ds) AS ds__year
  , toYear(ds) AS ds__extract_year
  , toQuarter(ds) AS ds__extract_quarter
  , toMonth(ds) AS ds__extract_month
  , toDayOfMonth(ds) AS ds__extract_day
  , toDayOfWeek(ds) AS ds__extract_dow
  , toDayOfYear(ds) AS ds__extract_doy
  , toStartOfDay(ds_partitioned) AS ds_partitioned__day
  , toStartOfWeek(ds_partitioned, 1) AS ds_partitioned__week
  , toStartOfMonth(ds_partitioned) AS ds_partitioned__month
  , toStartOfQuarter(ds_partitioned) AS ds_partitioned__quarter
  , toStartOfYear(ds_partitioned) AS ds_partitioned__year
  , toYear(ds_partitioned) AS ds_partitioned__extract_year
  , toQuarter(ds_partitioned) AS ds_partitioned__extract_quarter
  , toMonth(ds_partitioned) AS ds_partitioned__extract_month
  , toDayOfMonth(ds_partitioned) AS ds_partitioned__extract_day
  , toDayOfWeek(ds_partitioned) AS ds_partitioned__extract_dow
  , toDayOfYear(ds_partitioned) AS ds_partitioned__extract_doy
  , toStartOfDay(paid_at) AS paid_at__day
  , toStartOfWeek(paid_at, 1) AS paid_at__week
  , toStartOfMonth(paid_at) AS paid_at__month
  , toStartOfQuarter(paid_at) AS paid_at__quarter
  , toStartOfYear(paid_at) AS paid_at__year
  , toYear(paid_at) AS paid_at__extract_year
  , toQuarter(paid_at) AS paid_at__extract_quarter
  , toMonth(paid_at) AS paid_at__extract_month
  , toDayOfMonth(paid_at) AS paid_at__extract_day
  , toDayOfWeek(paid_at) AS paid_at__extract_dow
  , toDayOfYear(paid_at) AS paid_at__extract_doy
  , toStartOfDay(ds) AS booking__ds__day
  , toStartOfWeek(ds, 1) AS booking__ds__week
  , toStartOfMonth(ds) AS booking__ds__month
  , toStartOfQuarter(ds) AS booking__ds__quarter
  , toStartOfYear(ds) AS booking__ds__year
  , toYear(ds) AS booking__ds__extract_year
  , toQuarter(ds) AS booking__ds__extract_quarter
  , toMonth(ds) AS booking__ds__extract_month
  , toDayOfMonth(ds) AS booking__ds__extract_day
  , toDayOfWeek(ds) AS booking__ds__extract_dow
  , toDayOfYear(ds) AS booking__ds__extract_doy
  , toStartOfDay(ds_partitioned) AS booking__ds_partitioned__day
  , toStartOfWeek(ds_partitioned, 1) AS booking__ds_partitioned__week
  , toStartOfMonth(ds_partitioned) AS booking__ds_partitioned__month
  , toStartOfQuarter(ds_partitioned) AS booking__ds_partitioned__quarter
  , toStartOfYear(ds_partitioned) AS booking__ds_partitioned__year
  , toYear(ds_partitioned) AS booking__ds_partitioned__extract_year
  , toQuarter(ds_partitioned) AS booking__ds_partitioned__extract_quarter
  , toMonth(ds_partitioned) AS booking__ds_partitioned__extract_month
  , toDayOfMonth(ds_partitioned) AS booking__ds_partitioned__extract_day
  , toDayOfWeek(ds_partitioned) AS booking__ds_partitioned__extract_dow
  , toDayOfYear(ds_partitioned) AS booking__ds_partitioned__extract_doy
  , toStartOfDay(paid_at) AS booking__paid_at__day
  , toStartOfWeek(paid_at, 1) AS booking__paid_at__week
  , toStartOfMonth(paid_at) AS booking__paid_at__month
  , toStartOfQuarter(paid_at) AS booking__paid_at__quarter
  , toStartOfYear(paid_at) AS booking__paid_at__year
  , toYear(paid_at) AS booking__paid_at__extract_year
  , toQuarter(paid_at) AS booking__paid_at__extract_quarter
  , toMonth(paid_at) AS booking__paid_at__extract_month
  , toDayOfMonth(paid_at) AS booking__paid_at__extract_day
  , toDayOfWeek(paid_at) AS booking__paid_at__extract_dow
  , toDayOfYear(paid_at) AS booking__paid_at__extract_doy
  , toStartOfDay(ds) AS metric_time__day
  , toStartOfWeek(ds, 1) AS metric_time__week
  , toStartOfMonth(ds) AS metric_time__month
  , toStartOfQuarter(ds) AS metric_time__quarter
  , toStartOfYear(ds) AS metric_time__year
  , toYear(ds) AS metric_time__extract_year
  , toQuarter(ds) AS metric_time__extract_quarter
  , toMonth(ds) AS metric_time__extract_month
  , toDayOfMonth(ds) AS metric_time__extract_day
  , toDayOfWeek(ds) AS metric_time__extract_dow
  , toDayOfYear(ds) AS metric_time__extract_doy
  , listing_id AS listing
  , guest_id AS guest
  , host_id AS host
  , listing_id AS booking__listing
  , guest_id AS booking__guest
  , host_id AS booking__host
  , is_instant
  , is_instant AS booking__is_instant
  , 1 AS __bookings
  , booking_value AS __average_booking_value
  , CASE WHEN is_instant THEN 1 ELSE 0 END AS __instant_bookings
  , booking_value AS __booking_value
  , booking_value AS __max_booking_value
  , booking_value AS __min_booking_value
  , booking_value AS __instant_booking_value
  , booking_value AS __average_instant_booking_value
  , booking_value AS __booking_value_for_non_null_listing_id
  , guest_id AS __bookers
  , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS __referred_bookings
  , booking_value AS __median_booking_value
  , booking_value AS __booking_value_p99
  , booking_value AS __discrete_booking_value_p99
  , booking_value AS __approximate_continuous_booking_value_p99
  , booking_value AS __approximate_discrete_booking_value_p99
  , 1 AS __bookings_join_to_time_spine
  , 1 AS __bookings_fill_nulls_with_0_without_time_spine
  , 1 AS __bookings_fill_nulls_with_0
  , 1 AS __instant_bookings_with_measure_filter
  , 1 AS __bookings_join_to_time_spine_with_tiered_filters
  , guest_id AS __bookers_fill_nulls_with_0_join_to_timespine
FROM ***************************.fct_bookings bookings_source_src_28000
