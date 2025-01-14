test_name: test_filter_with_where_constraint_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting a dataflow plan to a SQL query plan where there is a leaf pass filter node.
sql_engine: Clickhouse
---
-- Constrain Output with WHERE
SELECT
  subq_1.ds__day
  , subq_1.bookings
FROM (
  -- Pass Only Elements: ['bookings', 'ds__day']
  SELECT
    subq_0.ds__day
    , subq_0.bookings
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
      , date_trunc('day', bookings_source_src_28000.ds) AS ds__day
      , date_trunc('week', bookings_source_src_28000.ds) AS ds__week
      , date_trunc('month', bookings_source_src_28000.ds) AS ds__month
      , date_trunc('quarter', bookings_source_src_28000.ds) AS ds__quarter
      , date_trunc('year', bookings_source_src_28000.ds) AS ds__year
      , toYear(bookings_source_src_28000.ds) AS ds__extract_year
      , toQuarter(bookings_source_src_28000.ds) AS ds__extract_quarter
      , toMonth(bookings_source_src_28000.ds) AS ds__extract_month
      , toDayOfMonth(bookings_source_src_28000.ds) AS ds__extract_day
      , toDayOfWeek(bookings_source_src_28000.ds) AS ds__extract_dow
      , toDayOfYear(bookings_source_src_28000.ds) AS ds__extract_doy
      , date_trunc('day', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__day
      , date_trunc('week', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__week
      , date_trunc('month', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__month
      , date_trunc('quarter', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
      , date_trunc('year', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__year
      , toYear(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
      , toQuarter(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
      , toMonth(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
      , toDayOfMonth(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
      , toDayOfWeek(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
      , toDayOfYear(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
      , date_trunc('day', bookings_source_src_28000.paid_at) AS paid_at__day
      , date_trunc('week', bookings_source_src_28000.paid_at) AS paid_at__week
      , date_trunc('month', bookings_source_src_28000.paid_at) AS paid_at__month
      , date_trunc('quarter', bookings_source_src_28000.paid_at) AS paid_at__quarter
      , date_trunc('year', bookings_source_src_28000.paid_at) AS paid_at__year
      , toYear(bookings_source_src_28000.paid_at) AS paid_at__extract_year
      , toQuarter(bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
      , toMonth(bookings_source_src_28000.paid_at) AS paid_at__extract_month
      , toDayOfMonth(bookings_source_src_28000.paid_at) AS paid_at__extract_day
      , toDayOfWeek(bookings_source_src_28000.paid_at) AS paid_at__extract_dow
      , toDayOfYear(bookings_source_src_28000.paid_at) AS paid_at__extract_doy
      , bookings_source_src_28000.is_instant AS booking__is_instant
      , date_trunc('day', bookings_source_src_28000.ds) AS booking__ds__day
      , date_trunc('week', bookings_source_src_28000.ds) AS booking__ds__week
      , date_trunc('month', bookings_source_src_28000.ds) AS booking__ds__month
      , date_trunc('quarter', bookings_source_src_28000.ds) AS booking__ds__quarter
      , date_trunc('year', bookings_source_src_28000.ds) AS booking__ds__year
      , toYear(bookings_source_src_28000.ds) AS booking__ds__extract_year
      , toQuarter(bookings_source_src_28000.ds) AS booking__ds__extract_quarter
      , toMonth(bookings_source_src_28000.ds) AS booking__ds__extract_month
      , toDayOfMonth(bookings_source_src_28000.ds) AS booking__ds__extract_day
      , toDayOfWeek(bookings_source_src_28000.ds) AS booking__ds__extract_dow
      , toDayOfYear(bookings_source_src_28000.ds) AS booking__ds__extract_doy
      , date_trunc('day', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__day
      , date_trunc('week', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__week
      , date_trunc('month', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__month
      , date_trunc('quarter', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__quarter
      , date_trunc('year', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__year
      , toYear(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
      , toQuarter(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
      , toMonth(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
      , toDayOfMonth(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
      , toDayOfWeek(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
      , toDayOfYear(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
      , date_trunc('day', bookings_source_src_28000.paid_at) AS booking__paid_at__day
      , date_trunc('week', bookings_source_src_28000.paid_at) AS booking__paid_at__week
      , date_trunc('month', bookings_source_src_28000.paid_at) AS booking__paid_at__month
      , date_trunc('quarter', bookings_source_src_28000.paid_at) AS booking__paid_at__quarter
      , date_trunc('year', bookings_source_src_28000.paid_at) AS booking__paid_at__year
      , toYear(bookings_source_src_28000.paid_at) AS booking__paid_at__extract_year
      , toQuarter(bookings_source_src_28000.paid_at) AS booking__paid_at__extract_quarter
      , toMonth(bookings_source_src_28000.paid_at) AS booking__paid_at__extract_month
      , toDayOfMonth(bookings_source_src_28000.paid_at) AS booking__paid_at__extract_day
      , toDayOfWeek(bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
      , toDayOfYear(bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
      , bookings_source_src_28000.listing_id AS listing
      , bookings_source_src_28000.guest_id AS guest
      , bookings_source_src_28000.host_id AS host
      , bookings_source_src_28000.listing_id AS booking__listing
      , bookings_source_src_28000.guest_id AS booking__guest
      , bookings_source_src_28000.host_id AS booking__host
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_0
) subq_1
WHERE booking__ds__day = '2020-01-01'
