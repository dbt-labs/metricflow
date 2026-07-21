test_name: test_min_max_only_time
test_filename: test_query_rendering.py
docstring:
  Tests a min max only query with a time dimension.
sql_engine: ClickHouse
---
SELECT
  subq_3.booking__paid_at__day__min
  , subq_3.booking__paid_at__day__max
FROM (
  SELECT
    MIN(subq_2.booking__paid_at__day) AS booking__paid_at__day__min
    , MAX(subq_2.booking__paid_at__day) AS booking__paid_at__day__max
  FROM (
    SELECT
      subq_1.booking__paid_at__day
    FROM (
      SELECT
        subq_0.booking__paid_at__day
      FROM (
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
          , toStartOfDay(bookings_source_src_28000.ds) AS ds__day
          , toStartOfWeek(bookings_source_src_28000.ds, 1) AS ds__week
          , toStartOfMonth(bookings_source_src_28000.ds) AS ds__month
          , toStartOfQuarter(bookings_source_src_28000.ds) AS ds__quarter
          , toStartOfYear(bookings_source_src_28000.ds) AS ds__year
          , toYear(bookings_source_src_28000.ds) AS ds__extract_year
          , toQuarter(bookings_source_src_28000.ds) AS ds__extract_quarter
          , toMonth(bookings_source_src_28000.ds) AS ds__extract_month
          , toDayOfMonth(bookings_source_src_28000.ds) AS ds__extract_day
          , toDayOfWeek(bookings_source_src_28000.ds) AS ds__extract_dow
          , toDayOfYear(bookings_source_src_28000.ds) AS ds__extract_doy
          , toStartOfDay(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__day
          , toStartOfWeek(bookings_source_src_28000.ds_partitioned, 1) AS ds_partitioned__week
          , toStartOfMonth(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__month
          , toStartOfQuarter(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
          , toStartOfYear(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__year
          , toYear(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
          , toQuarter(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
          , toMonth(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
          , toDayOfMonth(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
          , toDayOfWeek(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
          , toDayOfYear(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
          , toStartOfDay(bookings_source_src_28000.paid_at) AS paid_at__day
          , toStartOfWeek(bookings_source_src_28000.paid_at, 1) AS paid_at__week
          , toStartOfMonth(bookings_source_src_28000.paid_at) AS paid_at__month
          , toStartOfQuarter(bookings_source_src_28000.paid_at) AS paid_at__quarter
          , toStartOfYear(bookings_source_src_28000.paid_at) AS paid_at__year
          , toYear(bookings_source_src_28000.paid_at) AS paid_at__extract_year
          , toQuarter(bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
          , toMonth(bookings_source_src_28000.paid_at) AS paid_at__extract_month
          , toDayOfMonth(bookings_source_src_28000.paid_at) AS paid_at__extract_day
          , toDayOfWeek(bookings_source_src_28000.paid_at) AS paid_at__extract_dow
          , toDayOfYear(bookings_source_src_28000.paid_at) AS paid_at__extract_doy
          , bookings_source_src_28000.is_instant AS booking__is_instant
          , toStartOfDay(bookings_source_src_28000.ds) AS booking__ds__day
          , toStartOfWeek(bookings_source_src_28000.ds, 1) AS booking__ds__week
          , toStartOfMonth(bookings_source_src_28000.ds) AS booking__ds__month
          , toStartOfQuarter(bookings_source_src_28000.ds) AS booking__ds__quarter
          , toStartOfYear(bookings_source_src_28000.ds) AS booking__ds__year
          , toYear(bookings_source_src_28000.ds) AS booking__ds__extract_year
          , toQuarter(bookings_source_src_28000.ds) AS booking__ds__extract_quarter
          , toMonth(bookings_source_src_28000.ds) AS booking__ds__extract_month
          , toDayOfMonth(bookings_source_src_28000.ds) AS booking__ds__extract_day
          , toDayOfWeek(bookings_source_src_28000.ds) AS booking__ds__extract_dow
          , toDayOfYear(bookings_source_src_28000.ds) AS booking__ds__extract_doy
          , toStartOfDay(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__day
          , toStartOfWeek(bookings_source_src_28000.ds_partitioned, 1) AS booking__ds_partitioned__week
          , toStartOfMonth(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__month
          , toStartOfQuarter(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__quarter
          , toStartOfYear(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__year
          , toYear(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
          , toQuarter(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
          , toMonth(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
          , toDayOfMonth(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
          , toDayOfWeek(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
          , toDayOfYear(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
          , toStartOfDay(bookings_source_src_28000.paid_at) AS booking__paid_at__day
          , toStartOfWeek(bookings_source_src_28000.paid_at, 1) AS booking__paid_at__week
          , toStartOfMonth(bookings_source_src_28000.paid_at) AS booking__paid_at__month
          , toStartOfQuarter(bookings_source_src_28000.paid_at) AS booking__paid_at__quarter
          , toStartOfYear(bookings_source_src_28000.paid_at) AS booking__paid_at__year
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
    GROUP BY
      subq_1.booking__paid_at__day
  ) subq_2
) subq_3
