test_name: test_simple_metric_with_custom_granularity_filter
test_filename: test_custom_granularity.py
docstring:
  Simple metric queried with a filter on a custom grain, where that grain is not used in the group by.
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_4.bookings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(nr_subq_3.bookings) AS bookings
  FROM (
    -- Pass Only Elements: ['bookings',]
    SELECT
      nr_subq_2.bookings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        nr_subq_1.metric_time__martian_day
        , nr_subq_1.ds__day
        , nr_subq_1.ds__week
        , nr_subq_1.ds__month
        , nr_subq_1.ds__quarter
        , nr_subq_1.ds__year
        , nr_subq_1.ds__extract_year
        , nr_subq_1.ds__extract_quarter
        , nr_subq_1.ds__extract_month
        , nr_subq_1.ds__extract_day
        , nr_subq_1.ds__extract_dow
        , nr_subq_1.ds__extract_doy
        , nr_subq_1.ds_partitioned__day
        , nr_subq_1.ds_partitioned__week
        , nr_subq_1.ds_partitioned__month
        , nr_subq_1.ds_partitioned__quarter
        , nr_subq_1.ds_partitioned__year
        , nr_subq_1.ds_partitioned__extract_year
        , nr_subq_1.ds_partitioned__extract_quarter
        , nr_subq_1.ds_partitioned__extract_month
        , nr_subq_1.ds_partitioned__extract_day
        , nr_subq_1.ds_partitioned__extract_dow
        , nr_subq_1.ds_partitioned__extract_doy
        , nr_subq_1.paid_at__day
        , nr_subq_1.paid_at__week
        , nr_subq_1.paid_at__month
        , nr_subq_1.paid_at__quarter
        , nr_subq_1.paid_at__year
        , nr_subq_1.paid_at__extract_year
        , nr_subq_1.paid_at__extract_quarter
        , nr_subq_1.paid_at__extract_month
        , nr_subq_1.paid_at__extract_day
        , nr_subq_1.paid_at__extract_dow
        , nr_subq_1.paid_at__extract_doy
        , nr_subq_1.booking__ds__day
        , nr_subq_1.booking__ds__week
        , nr_subq_1.booking__ds__month
        , nr_subq_1.booking__ds__quarter
        , nr_subq_1.booking__ds__year
        , nr_subq_1.booking__ds__extract_year
        , nr_subq_1.booking__ds__extract_quarter
        , nr_subq_1.booking__ds__extract_month
        , nr_subq_1.booking__ds__extract_day
        , nr_subq_1.booking__ds__extract_dow
        , nr_subq_1.booking__ds__extract_doy
        , nr_subq_1.booking__ds_partitioned__day
        , nr_subq_1.booking__ds_partitioned__week
        , nr_subq_1.booking__ds_partitioned__month
        , nr_subq_1.booking__ds_partitioned__quarter
        , nr_subq_1.booking__ds_partitioned__year
        , nr_subq_1.booking__ds_partitioned__extract_year
        , nr_subq_1.booking__ds_partitioned__extract_quarter
        , nr_subq_1.booking__ds_partitioned__extract_month
        , nr_subq_1.booking__ds_partitioned__extract_day
        , nr_subq_1.booking__ds_partitioned__extract_dow
        , nr_subq_1.booking__ds_partitioned__extract_doy
        , nr_subq_1.booking__paid_at__day
        , nr_subq_1.booking__paid_at__week
        , nr_subq_1.booking__paid_at__month
        , nr_subq_1.booking__paid_at__quarter
        , nr_subq_1.booking__paid_at__year
        , nr_subq_1.booking__paid_at__extract_year
        , nr_subq_1.booking__paid_at__extract_quarter
        , nr_subq_1.booking__paid_at__extract_month
        , nr_subq_1.booking__paid_at__extract_day
        , nr_subq_1.booking__paid_at__extract_dow
        , nr_subq_1.booking__paid_at__extract_doy
        , nr_subq_1.metric_time__day
        , nr_subq_1.metric_time__week
        , nr_subq_1.metric_time__month
        , nr_subq_1.metric_time__quarter
        , nr_subq_1.metric_time__year
        , nr_subq_1.metric_time__extract_year
        , nr_subq_1.metric_time__extract_quarter
        , nr_subq_1.metric_time__extract_month
        , nr_subq_1.metric_time__extract_day
        , nr_subq_1.metric_time__extract_dow
        , nr_subq_1.metric_time__extract_doy
        , nr_subq_1.listing
        , nr_subq_1.guest
        , nr_subq_1.host
        , nr_subq_1.booking__listing
        , nr_subq_1.booking__guest
        , nr_subq_1.booking__host
        , nr_subq_1.is_instant
        , nr_subq_1.booking__is_instant
        , nr_subq_1.bookings
        , nr_subq_1.instant_bookings
        , nr_subq_1.booking_value
        , nr_subq_1.max_booking_value
        , nr_subq_1.min_booking_value
        , nr_subq_1.bookers
        , nr_subq_1.average_booking_value
        , nr_subq_1.referred_bookings
        , nr_subq_1.median_booking_value
        , nr_subq_1.booking_value_p99
        , nr_subq_1.discrete_booking_value_p99
        , nr_subq_1.approximate_continuous_booking_value_p99
        , nr_subq_1.approximate_discrete_booking_value_p99
      FROM (
        -- Metric Time Dimension 'ds'
        -- Join to Custom Granularity Dataset
        SELECT
          nr_subq_28002.ds__day AS ds__day
          , nr_subq_28002.ds__week AS ds__week
          , nr_subq_28002.ds__month AS ds__month
          , nr_subq_28002.ds__quarter AS ds__quarter
          , nr_subq_28002.ds__year AS ds__year
          , nr_subq_28002.ds__extract_year AS ds__extract_year
          , nr_subq_28002.ds__extract_quarter AS ds__extract_quarter
          , nr_subq_28002.ds__extract_month AS ds__extract_month
          , nr_subq_28002.ds__extract_day AS ds__extract_day
          , nr_subq_28002.ds__extract_dow AS ds__extract_dow
          , nr_subq_28002.ds__extract_doy AS ds__extract_doy
          , nr_subq_28002.ds_partitioned__day AS ds_partitioned__day
          , nr_subq_28002.ds_partitioned__week AS ds_partitioned__week
          , nr_subq_28002.ds_partitioned__month AS ds_partitioned__month
          , nr_subq_28002.ds_partitioned__quarter AS ds_partitioned__quarter
          , nr_subq_28002.ds_partitioned__year AS ds_partitioned__year
          , nr_subq_28002.ds_partitioned__extract_year AS ds_partitioned__extract_year
          , nr_subq_28002.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
          , nr_subq_28002.ds_partitioned__extract_month AS ds_partitioned__extract_month
          , nr_subq_28002.ds_partitioned__extract_day AS ds_partitioned__extract_day
          , nr_subq_28002.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
          , nr_subq_28002.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
          , nr_subq_28002.paid_at__day AS paid_at__day
          , nr_subq_28002.paid_at__week AS paid_at__week
          , nr_subq_28002.paid_at__month AS paid_at__month
          , nr_subq_28002.paid_at__quarter AS paid_at__quarter
          , nr_subq_28002.paid_at__year AS paid_at__year
          , nr_subq_28002.paid_at__extract_year AS paid_at__extract_year
          , nr_subq_28002.paid_at__extract_quarter AS paid_at__extract_quarter
          , nr_subq_28002.paid_at__extract_month AS paid_at__extract_month
          , nr_subq_28002.paid_at__extract_day AS paid_at__extract_day
          , nr_subq_28002.paid_at__extract_dow AS paid_at__extract_dow
          , nr_subq_28002.paid_at__extract_doy AS paid_at__extract_doy
          , nr_subq_28002.booking__ds__day AS booking__ds__day
          , nr_subq_28002.booking__ds__week AS booking__ds__week
          , nr_subq_28002.booking__ds__month AS booking__ds__month
          , nr_subq_28002.booking__ds__quarter AS booking__ds__quarter
          , nr_subq_28002.booking__ds__year AS booking__ds__year
          , nr_subq_28002.booking__ds__extract_year AS booking__ds__extract_year
          , nr_subq_28002.booking__ds__extract_quarter AS booking__ds__extract_quarter
          , nr_subq_28002.booking__ds__extract_month AS booking__ds__extract_month
          , nr_subq_28002.booking__ds__extract_day AS booking__ds__extract_day
          , nr_subq_28002.booking__ds__extract_dow AS booking__ds__extract_dow
          , nr_subq_28002.booking__ds__extract_doy AS booking__ds__extract_doy
          , nr_subq_28002.booking__ds_partitioned__day AS booking__ds_partitioned__day
          , nr_subq_28002.booking__ds_partitioned__week AS booking__ds_partitioned__week
          , nr_subq_28002.booking__ds_partitioned__month AS booking__ds_partitioned__month
          , nr_subq_28002.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
          , nr_subq_28002.booking__ds_partitioned__year AS booking__ds_partitioned__year
          , nr_subq_28002.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
          , nr_subq_28002.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
          , nr_subq_28002.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
          , nr_subq_28002.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
          , nr_subq_28002.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
          , nr_subq_28002.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
          , nr_subq_28002.booking__paid_at__day AS booking__paid_at__day
          , nr_subq_28002.booking__paid_at__week AS booking__paid_at__week
          , nr_subq_28002.booking__paid_at__month AS booking__paid_at__month
          , nr_subq_28002.booking__paid_at__quarter AS booking__paid_at__quarter
          , nr_subq_28002.booking__paid_at__year AS booking__paid_at__year
          , nr_subq_28002.booking__paid_at__extract_year AS booking__paid_at__extract_year
          , nr_subq_28002.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
          , nr_subq_28002.booking__paid_at__extract_month AS booking__paid_at__extract_month
          , nr_subq_28002.booking__paid_at__extract_day AS booking__paid_at__extract_day
          , nr_subq_28002.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
          , nr_subq_28002.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
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
          , nr_subq_28002.listing AS listing
          , nr_subq_28002.guest AS guest
          , nr_subq_28002.host AS host
          , nr_subq_28002.booking__listing AS booking__listing
          , nr_subq_28002.booking__guest AS booking__guest
          , nr_subq_28002.booking__host AS booking__host
          , nr_subq_28002.is_instant AS is_instant
          , nr_subq_28002.booking__is_instant AS booking__is_instant
          , nr_subq_28002.bookings AS bookings
          , nr_subq_28002.instant_bookings AS instant_bookings
          , nr_subq_28002.booking_value AS booking_value
          , nr_subq_28002.max_booking_value AS max_booking_value
          , nr_subq_28002.min_booking_value AS min_booking_value
          , nr_subq_28002.bookers AS bookers
          , nr_subq_28002.average_booking_value AS average_booking_value
          , nr_subq_28002.referred_bookings AS referred_bookings
          , nr_subq_28002.median_booking_value AS median_booking_value
          , nr_subq_28002.booking_value_p99 AS booking_value_p99
          , nr_subq_28002.discrete_booking_value_p99 AS discrete_booking_value_p99
          , nr_subq_28002.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
          , nr_subq_28002.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
          , nr_subq_0.martian_day AS metric_time__martian_day
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
        LEFT OUTER JOIN
          ***************************.mf_time_spine nr_subq_0
        ON
          nr_subq_28002.ds__day = nr_subq_0.ds
      ) nr_subq_1
      WHERE metric_time__martian_day = '2020-01-01'
    ) nr_subq_2
  ) nr_subq_3
) nr_subq_4
