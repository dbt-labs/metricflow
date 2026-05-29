test_name: test_metric_time_dimension_transform_node_using_non_primary_time
test_filename: test_metric_time_dimension_to_sql.py
docstring:
  Tests converting a PlotTimeDimensionTransform node using a non-primary time dimension to SQL.
sql_engine: Databricks
---
-- Metric Time Dimension 'paid_at'
SELECT
  subq_0.ds__day
  , subq_0.ds__week
  , subq_0.ds__month
  , subq_0.ds__quarter
  , subq_0.ds__year
  , subq_0.ds__extract_year
  , subq_0.ds__extract_quarter
  , subq_0.ds__extract_month
  , subq_0.ds__extract_day
  , subq_0.ds__extract_dow
  , subq_0.ds__extract_doy
  , subq_0.ds_partitioned__day
  , subq_0.ds_partitioned__week
  , subq_0.ds_partitioned__month
  , subq_0.ds_partitioned__quarter
  , subq_0.ds_partitioned__year
  , subq_0.ds_partitioned__extract_year
  , subq_0.ds_partitioned__extract_quarter
  , subq_0.ds_partitioned__extract_month
  , subq_0.ds_partitioned__extract_day
  , subq_0.ds_partitioned__extract_dow
  , subq_0.ds_partitioned__extract_doy
  , subq_0.paid_at__day
  , subq_0.paid_at__week
  , subq_0.paid_at__month
  , subq_0.paid_at__quarter
  , subq_0.paid_at__year
  , subq_0.paid_at__extract_year
  , subq_0.paid_at__extract_quarter
  , subq_0.paid_at__extract_month
  , subq_0.paid_at__extract_day
  , subq_0.paid_at__extract_dow
  , subq_0.paid_at__extract_doy
  , subq_0.booking__ds__day
  , subq_0.booking__ds__week
  , subq_0.booking__ds__month
  , subq_0.booking__ds__quarter
  , subq_0.booking__ds__year
  , subq_0.booking__ds__extract_year
  , subq_0.booking__ds__extract_quarter
  , subq_0.booking__ds__extract_month
  , subq_0.booking__ds__extract_day
  , subq_0.booking__ds__extract_dow
  , subq_0.booking__ds__extract_doy
  , subq_0.booking__ds_partitioned__day
  , subq_0.booking__ds_partitioned__week
  , subq_0.booking__ds_partitioned__month
  , subq_0.booking__ds_partitioned__quarter
  , subq_0.booking__ds_partitioned__year
  , subq_0.booking__ds_partitioned__extract_year
  , subq_0.booking__ds_partitioned__extract_quarter
  , subq_0.booking__ds_partitioned__extract_month
  , subq_0.booking__ds_partitioned__extract_day
  , subq_0.booking__ds_partitioned__extract_dow
  , subq_0.booking__ds_partitioned__extract_doy
  , subq_0.booking__paid_at__day
  , subq_0.booking__paid_at__week
  , subq_0.booking__paid_at__month
  , subq_0.booking__paid_at__quarter
  , subq_0.booking__paid_at__year
  , subq_0.booking__paid_at__extract_year
  , subq_0.booking__paid_at__extract_quarter
  , subq_0.booking__paid_at__extract_month
  , subq_0.booking__paid_at__extract_day
  , subq_0.booking__paid_at__extract_dow
  , subq_0.booking__paid_at__extract_doy
  , subq_0.paid_at__day AS metric_time__day
  , subq_0.paid_at__week AS metric_time__week
  , subq_0.paid_at__month AS metric_time__month
  , subq_0.paid_at__quarter AS metric_time__quarter
  , subq_0.paid_at__year AS metric_time__year
  , subq_0.paid_at__extract_year AS metric_time__extract_year
  , subq_0.paid_at__extract_quarter AS metric_time__extract_quarter
  , subq_0.paid_at__extract_month AS metric_time__extract_month
  , subq_0.paid_at__extract_day AS metric_time__extract_day
  , subq_0.paid_at__extract_dow AS metric_time__extract_dow
  , subq_0.paid_at__extract_doy AS metric_time__extract_doy
  , subq_0.listing
  , subq_0.guest
  , subq_0.host
  , subq_0.booking__listing
  , subq_0.booking__guest
  , subq_0.booking__host
  , subq_0.is_instant
  , subq_0.booking__is_instant
  , subq_0.__booking_payments
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  SELECT
    1 AS __bookings
    , bookings_source_src_28000.booking_value AS __average_booking_value
    , CASE WHEN is_instant THEN 1 ELSE 0 END AS __instant_bookings
    , bookings_source_src_28000.booking_value AS __booking_value
    , bookings_source_src_28000.booking_value AS __max_booking_value
    , bookings_source_src_28000.booking_value AS __min_booking_value
    , bookings_source_src_28000.booking_value AS __instant_booking_value
    , bookings_source_src_28000.booking_value AS __average_instant_booking_value
    , bookings_source_src_28000.booking_value AS __booking_value_for_non_null_listing_id
    , bookings_source_src_28000.guest_id AS __bookers
    , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS __referred_bookings
    , bookings_source_src_28000.booking_value AS __median_booking_value
    , bookings_source_src_28000.booking_value AS __booking_value_p99
    , bookings_source_src_28000.booking_value AS __discrete_booking_value_p99
    , bookings_source_src_28000.booking_value AS __approximate_continuous_booking_value_p99
    , bookings_source_src_28000.booking_value AS __approximate_discrete_booking_value_p99
    , 1 AS __bookings_join_to_time_spine
    , 1 AS __bookings_fill_nulls_with_0_without_time_spine
    , 1 AS __bookings_fill_nulls_with_0
    , 1 AS __instant_bookings_with_measure_filter
    , 1 AS __bookings_join_to_time_spine_with_tiered_filters
    , bookings_source_src_28000.guest_id AS __bookers_fill_nulls_with_0_join_to_timespine
    , bookings_source_src_28000.booking_value AS __booking_payments
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
    , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
    , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
    , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
    , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
    , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
    , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
    , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
    , bookings_source_src_28000.listing_id AS listing
    , bookings_source_src_28000.guest_id AS guest
    , bookings_source_src_28000.host_id AS host
    , bookings_source_src_28000.listing_id AS booking__listing
    , bookings_source_src_28000.guest_id AS booking__guest
    , bookings_source_src_28000.host_id AS booking__host
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_0
