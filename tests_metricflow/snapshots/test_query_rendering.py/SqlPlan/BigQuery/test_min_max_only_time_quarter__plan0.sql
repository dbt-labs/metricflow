test_name: test_min_max_only_time_quarter
test_filename: test_query_rendering.py
docstring:
  Tests a min max only query with a time dimension and non-default granularity.
sql_engine: BigQuery
---
-- Write to DataTable
SELECT
  subq_3.booking__paid_at__quarter__min
  , subq_3.booking__paid_at__quarter__max
FROM (
  -- Calculate min and max
  SELECT
    MIN(subq_2.booking__paid_at__quarter) AS booking__paid_at__quarter__min
    , MAX(subq_2.booking__paid_at__quarter) AS booking__paid_at__quarter__max
  FROM (
    -- Pass Only Elements: ['booking__paid_at__quarter']
    SELECT
      subq_1.booking__paid_at__quarter
    FROM (
      -- Pass Only Elements: ['booking__paid_at__quarter']
      SELECT
        subq_0.booking__paid_at__quarter
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
          , DATETIME_TRUNC(bookings_source_src_28000.ds, day) AS ds__day
          , DATETIME_TRUNC(bookings_source_src_28000.ds, isoweek) AS ds__week
          , DATETIME_TRUNC(bookings_source_src_28000.ds, month) AS ds__month
          , DATETIME_TRUNC(bookings_source_src_28000.ds, quarter) AS ds__quarter
          , DATETIME_TRUNC(bookings_source_src_28000.ds, year) AS ds__year
          , EXTRACT(year FROM bookings_source_src_28000.ds) AS ds__extract_year
          , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS ds__extract_quarter
          , EXTRACT(month FROM bookings_source_src_28000.ds) AS ds__extract_month
          , EXTRACT(day FROM bookings_source_src_28000.ds) AS ds__extract_day
          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds) - 1) AS ds__extract_dow
          , EXTRACT(dayofyear FROM bookings_source_src_28000.ds) AS ds__extract_doy
          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, day) AS ds_partitioned__day
          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, isoweek) AS ds_partitioned__week
          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, month) AS ds_partitioned__month
          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, quarter) AS ds_partitioned__quarter
          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, year) AS ds_partitioned__year
          , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
          , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
          , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
          , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) - 1) AS ds_partitioned__extract_dow
          , EXTRACT(dayofyear FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, day) AS paid_at__day
          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, isoweek) AS paid_at__week
          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, month) AS paid_at__month
          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, quarter) AS paid_at__quarter
          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, year) AS paid_at__year
          , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS paid_at__extract_year
          , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
          , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS paid_at__extract_month
          , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS paid_at__extract_day
          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) - 1) AS paid_at__extract_dow
          , EXTRACT(dayofyear FROM bookings_source_src_28000.paid_at) AS paid_at__extract_doy
          , bookings_source_src_28000.is_instant AS booking__is_instant
          , DATETIME_TRUNC(bookings_source_src_28000.ds, day) AS booking__ds__day
          , DATETIME_TRUNC(bookings_source_src_28000.ds, isoweek) AS booking__ds__week
          , DATETIME_TRUNC(bookings_source_src_28000.ds, month) AS booking__ds__month
          , DATETIME_TRUNC(bookings_source_src_28000.ds, quarter) AS booking__ds__quarter
          , DATETIME_TRUNC(bookings_source_src_28000.ds, year) AS booking__ds__year
          , EXTRACT(year FROM bookings_source_src_28000.ds) AS booking__ds__extract_year
          , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS booking__ds__extract_quarter
          , EXTRACT(month FROM bookings_source_src_28000.ds) AS booking__ds__extract_month
          , EXTRACT(day FROM bookings_source_src_28000.ds) AS booking__ds__extract_day
          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds) - 1) AS booking__ds__extract_dow
          , EXTRACT(dayofyear FROM bookings_source_src_28000.ds) AS booking__ds__extract_doy
          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, day) AS booking__ds_partitioned__day
          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, isoweek) AS booking__ds_partitioned__week
          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, month) AS booking__ds_partitioned__month
          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, quarter) AS booking__ds_partitioned__quarter
          , DATETIME_TRUNC(bookings_source_src_28000.ds_partitioned, year) AS booking__ds_partitioned__year
          , EXTRACT(year FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
          , EXTRACT(quarter FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
          , EXTRACT(month FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
          , EXTRACT(day FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.ds_partitioned) - 1) AS booking__ds_partitioned__extract_dow
          , EXTRACT(dayofyear FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, day) AS booking__paid_at__day
          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, isoweek) AS booking__paid_at__week
          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, month) AS booking__paid_at__month
          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, quarter) AS booking__paid_at__quarter
          , DATETIME_TRUNC(bookings_source_src_28000.paid_at, year) AS booking__paid_at__year
          , EXTRACT(year FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_year
          , EXTRACT(quarter FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_quarter
          , EXTRACT(month FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_month
          , EXTRACT(day FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_day
          , IF(EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_28000.paid_at) - 1) AS booking__paid_at__extract_dow
          , EXTRACT(dayofyear FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
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
      booking__paid_at__quarter
  ) subq_2
) subq_3
