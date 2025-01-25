test_name: test_simple_fill_nulls_without_time_spine
test_filename: test_fill_nulls_with_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_2.metric_time__day
  , COALESCE(nr_subq_2.bookings, 0) AS bookings_fill_nulls_with_0_without_time_spine
FROM (
  -- Aggregate Measures
  SELECT
    nr_subq_1.metric_time__day
    , SUM(nr_subq_1.bookings) AS bookings
  FROM (
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    SELECT
      nr_subq_0.metric_time__day
      , nr_subq_0.bookings
    FROM (
      -- Metric Time Dimension 'ds'
      SELECT
        nr_subq_28002.ds__day
        , nr_subq_28002.ds__week
        , nr_subq_28002.ds__month
        , nr_subq_28002.ds__quarter
        , nr_subq_28002.ds__year
        , nr_subq_28002.ds__extract_year
        , nr_subq_28002.ds__extract_quarter
        , nr_subq_28002.ds__extract_month
        , nr_subq_28002.ds__extract_day
        , nr_subq_28002.ds__extract_dow
        , nr_subq_28002.ds__extract_doy
        , nr_subq_28002.ds_partitioned__day
        , nr_subq_28002.ds_partitioned__week
        , nr_subq_28002.ds_partitioned__month
        , nr_subq_28002.ds_partitioned__quarter
        , nr_subq_28002.ds_partitioned__year
        , nr_subq_28002.ds_partitioned__extract_year
        , nr_subq_28002.ds_partitioned__extract_quarter
        , nr_subq_28002.ds_partitioned__extract_month
        , nr_subq_28002.ds_partitioned__extract_day
        , nr_subq_28002.ds_partitioned__extract_dow
        , nr_subq_28002.ds_partitioned__extract_doy
        , nr_subq_28002.paid_at__day
        , nr_subq_28002.paid_at__week
        , nr_subq_28002.paid_at__month
        , nr_subq_28002.paid_at__quarter
        , nr_subq_28002.paid_at__year
        , nr_subq_28002.paid_at__extract_year
        , nr_subq_28002.paid_at__extract_quarter
        , nr_subq_28002.paid_at__extract_month
        , nr_subq_28002.paid_at__extract_day
        , nr_subq_28002.paid_at__extract_dow
        , nr_subq_28002.paid_at__extract_doy
        , nr_subq_28002.booking__ds__day
        , nr_subq_28002.booking__ds__week
        , nr_subq_28002.booking__ds__month
        , nr_subq_28002.booking__ds__quarter
        , nr_subq_28002.booking__ds__year
        , nr_subq_28002.booking__ds__extract_year
        , nr_subq_28002.booking__ds__extract_quarter
        , nr_subq_28002.booking__ds__extract_month
        , nr_subq_28002.booking__ds__extract_day
        , nr_subq_28002.booking__ds__extract_dow
        , nr_subq_28002.booking__ds__extract_doy
        , nr_subq_28002.booking__ds_partitioned__day
        , nr_subq_28002.booking__ds_partitioned__week
        , nr_subq_28002.booking__ds_partitioned__month
        , nr_subq_28002.booking__ds_partitioned__quarter
        , nr_subq_28002.booking__ds_partitioned__year
        , nr_subq_28002.booking__ds_partitioned__extract_year
        , nr_subq_28002.booking__ds_partitioned__extract_quarter
        , nr_subq_28002.booking__ds_partitioned__extract_month
        , nr_subq_28002.booking__ds_partitioned__extract_day
        , nr_subq_28002.booking__ds_partitioned__extract_dow
        , nr_subq_28002.booking__ds_partitioned__extract_doy
        , nr_subq_28002.booking__paid_at__day
        , nr_subq_28002.booking__paid_at__week
        , nr_subq_28002.booking__paid_at__month
        , nr_subq_28002.booking__paid_at__quarter
        , nr_subq_28002.booking__paid_at__year
        , nr_subq_28002.booking__paid_at__extract_year
        , nr_subq_28002.booking__paid_at__extract_quarter
        , nr_subq_28002.booking__paid_at__extract_month
        , nr_subq_28002.booking__paid_at__extract_day
        , nr_subq_28002.booking__paid_at__extract_dow
        , nr_subq_28002.booking__paid_at__extract_doy
        , nr_subq_28002.ds__day AS metric_time__day
        , nr_subq_28002.ds__week AS metric_time__week
        , nr_subq_28002.ds__month AS metric_time__month
        , nr_subq_28002.ds__quarter AS metric_time__quarter
        , nr_subq_28002.ds__year AS metric_time__year
        , nr_subq_28002.ds__extract_year AS metric_time__extract_year
        , nr_subq_28002.ds__extract_quarter AS metric_time__extract_quarter
        , nr_subq_28002.ds__extract_month AS metric_time__extract_month
        , nr_subq_28002.ds__extract_day AS metric_time__extract_day
        , nr_subq_28002.ds__extract_dow AS metric_time__extract_dow
        , nr_subq_28002.ds__extract_doy AS metric_time__extract_doy
        , nr_subq_28002.listing
        , nr_subq_28002.guest
        , nr_subq_28002.host
        , nr_subq_28002.booking__listing
        , nr_subq_28002.booking__guest
        , nr_subq_28002.booking__host
        , nr_subq_28002.is_instant
        , nr_subq_28002.booking__is_instant
        , nr_subq_28002.bookings
        , nr_subq_28002.instant_bookings
        , nr_subq_28002.booking_value
        , nr_subq_28002.max_booking_value
        , nr_subq_28002.min_booking_value
        , nr_subq_28002.bookers
        , nr_subq_28002.average_booking_value
        , nr_subq_28002.referred_bookings
        , nr_subq_28002.median_booking_value
        , nr_subq_28002.booking_value_p99
        , nr_subq_28002.discrete_booking_value_p99
        , nr_subq_28002.approximate_continuous_booking_value_p99
        , nr_subq_28002.approximate_discrete_booking_value_p99
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
      ) nr_subq_28002
    ) nr_subq_0
  ) nr_subq_1
  GROUP BY
    metric_time__day
) nr_subq_2
