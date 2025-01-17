test_name: test_simple_fill_nulls_with_0_with_non_metric_time
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Clickhouse
---
-- Compute Metrics via Expressions
SELECT
  subq_3.booking__paid_at__day
  , COALESCE(subq_3.bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Aggregate Measures
  SELECT
    subq_2.booking__paid_at__day
    , SUM(subq_2.bookings) AS bookings
  FROM (
    -- Pass Only Elements: ['bookings', 'booking__paid_at__day']
    SELECT
      subq_1.booking__paid_at__day
      , subq_1.bookings
    FROM (
      -- Metric Time Dimension 'ds'
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
        , subq_0.ds__day AS metric_time__day
        , subq_0.ds__week AS metric_time__week
        , subq_0.ds__month AS metric_time__month
        , subq_0.ds__quarter AS metric_time__quarter
        , subq_0.ds__year AS metric_time__year
        , subq_0.ds__extract_year AS metric_time__extract_year
        , subq_0.ds__extract_quarter AS metric_time__extract_quarter
        , subq_0.ds__extract_month AS metric_time__extract_month
        , subq_0.ds__extract_day AS metric_time__extract_day
        , subq_0.ds__extract_dow AS metric_time__extract_dow
        , subq_0.ds__extract_doy AS metric_time__extract_doy
        , subq_0.listing
        , subq_0.guest
        , subq_0.host
        , subq_0.booking__listing
        , subq_0.booking__guest
        , subq_0.booking__host
        , subq_0.is_instant
        , subq_0.booking__is_instant
        , subq_0.bookings
        , subq_0.instant_bookings
        , subq_0.booking_value
        , subq_0.max_booking_value
        , subq_0.min_booking_value
        , subq_0.bookers
        , subq_0.average_booking_value
        , subq_0.referred_bookings
        , subq_0.median_booking_value
        , subq_0.booking_value_p99
        , subq_0.discrete_booking_value_p99
        , subq_0.approximate_continuous_booking_value_p99
        , subq_0.approximate_discrete_booking_value_p99
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
  ) subq_2
  GROUP BY
    booking__paid_at__day
) subq_3
