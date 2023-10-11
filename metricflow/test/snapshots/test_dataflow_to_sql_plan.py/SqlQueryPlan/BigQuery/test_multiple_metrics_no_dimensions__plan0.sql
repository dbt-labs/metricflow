-- Combine Metrics
SELECT
  MAX(subq_5.bookings) AS bookings
  , MAX(subq_11.listings) AS listings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_4.bookings
  FROM (
    -- Aggregate Measures
    SELECT
      SUM(subq_3.bookings) AS bookings
    FROM (
      -- Pass Only Elements:
      --   ['bookings']
      SELECT
        subq_2.bookings
      FROM (
        -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
        SELECT
          subq_1.ds__day
          , subq_1.ds__week
          , subq_1.ds__month
          , subq_1.ds__quarter
          , subq_1.ds__year
          , subq_1.ds__extract_year
          , subq_1.ds__extract_quarter
          , subq_1.ds__extract_month
          , subq_1.ds__extract_week
          , subq_1.ds__extract_day
          , subq_1.ds__extract_dow
          , subq_1.ds__extract_doy
          , subq_1.ds_partitioned__day
          , subq_1.ds_partitioned__week
          , subq_1.ds_partitioned__month
          , subq_1.ds_partitioned__quarter
          , subq_1.ds_partitioned__year
          , subq_1.ds_partitioned__extract_year
          , subq_1.ds_partitioned__extract_quarter
          , subq_1.ds_partitioned__extract_month
          , subq_1.ds_partitioned__extract_week
          , subq_1.ds_partitioned__extract_day
          , subq_1.ds_partitioned__extract_dow
          , subq_1.ds_partitioned__extract_doy
          , subq_1.paid_at__day
          , subq_1.paid_at__week
          , subq_1.paid_at__month
          , subq_1.paid_at__quarter
          , subq_1.paid_at__year
          , subq_1.paid_at__extract_year
          , subq_1.paid_at__extract_quarter
          , subq_1.paid_at__extract_month
          , subq_1.paid_at__extract_week
          , subq_1.paid_at__extract_day
          , subq_1.paid_at__extract_dow
          , subq_1.paid_at__extract_doy
          , subq_1.booking__ds__day
          , subq_1.booking__ds__week
          , subq_1.booking__ds__month
          , subq_1.booking__ds__quarter
          , subq_1.booking__ds__year
          , subq_1.booking__ds__extract_year
          , subq_1.booking__ds__extract_quarter
          , subq_1.booking__ds__extract_month
          , subq_1.booking__ds__extract_week
          , subq_1.booking__ds__extract_day
          , subq_1.booking__ds__extract_dow
          , subq_1.booking__ds__extract_doy
          , subq_1.booking__ds_partitioned__day
          , subq_1.booking__ds_partitioned__week
          , subq_1.booking__ds_partitioned__month
          , subq_1.booking__ds_partitioned__quarter
          , subq_1.booking__ds_partitioned__year
          , subq_1.booking__ds_partitioned__extract_year
          , subq_1.booking__ds_partitioned__extract_quarter
          , subq_1.booking__ds_partitioned__extract_month
          , subq_1.booking__ds_partitioned__extract_week
          , subq_1.booking__ds_partitioned__extract_day
          , subq_1.booking__ds_partitioned__extract_dow
          , subq_1.booking__ds_partitioned__extract_doy
          , subq_1.booking__paid_at__day
          , subq_1.booking__paid_at__week
          , subq_1.booking__paid_at__month
          , subq_1.booking__paid_at__quarter
          , subq_1.booking__paid_at__year
          , subq_1.booking__paid_at__extract_year
          , subq_1.booking__paid_at__extract_quarter
          , subq_1.booking__paid_at__extract_month
          , subq_1.booking__paid_at__extract_week
          , subq_1.booking__paid_at__extract_day
          , subq_1.booking__paid_at__extract_dow
          , subq_1.booking__paid_at__extract_doy
          , subq_1.metric_time__day
          , subq_1.metric_time__week
          , subq_1.metric_time__month
          , subq_1.metric_time__quarter
          , subq_1.metric_time__year
          , subq_1.metric_time__extract_year
          , subq_1.metric_time__extract_quarter
          , subq_1.metric_time__extract_month
          , subq_1.metric_time__extract_week
          , subq_1.metric_time__extract_day
          , subq_1.metric_time__extract_dow
          , subq_1.metric_time__extract_doy
          , subq_1.listing
          , subq_1.guest
          , subq_1.host
          , subq_1.booking__listing
          , subq_1.booking__guest
          , subq_1.booking__host
          , subq_1.is_instant
          , subq_1.booking__is_instant
          , subq_1.bookings
          , subq_1.instant_bookings
          , subq_1.booking_value
          , subq_1.max_booking_value
          , subq_1.min_booking_value
          , subq_1.bookers
          , subq_1.average_booking_value
          , subq_1.referred_bookings
          , subq_1.median_booking_value
          , subq_1.booking_value_p99
          , subq_1.discrete_booking_value_p99
          , subq_1.approximate_continuous_booking_value_p99
          , subq_1.approximate_discrete_booking_value_p99
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
            , subq_0.ds__extract_week
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
            , subq_0.ds_partitioned__extract_week
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
            , subq_0.paid_at__extract_week
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
            , subq_0.booking__ds__extract_week
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
            , subq_0.booking__ds_partitioned__extract_week
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
            , subq_0.booking__paid_at__extract_week
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
            , subq_0.ds__extract_week AS metric_time__extract_week
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
              , bookings_source_src_10001.booking_value
              , bookings_source_src_10001.booking_value AS max_booking_value
              , bookings_source_src_10001.booking_value AS min_booking_value
              , bookings_source_src_10001.guest_id AS bookers
              , bookings_source_src_10001.booking_value AS average_booking_value
              , bookings_source_src_10001.booking_value AS booking_payments
              , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
              , bookings_source_src_10001.booking_value AS median_booking_value
              , bookings_source_src_10001.booking_value AS booking_value_p99
              , bookings_source_src_10001.booking_value AS discrete_booking_value_p99
              , bookings_source_src_10001.booking_value AS approximate_continuous_booking_value_p99
              , bookings_source_src_10001.booking_value AS approximate_discrete_booking_value_p99
              , bookings_source_src_10001.is_instant
              , DATE_TRUNC(bookings_source_src_10001.ds, day) AS ds__day
              , DATE_TRUNC(bookings_source_src_10001.ds, isoweek) AS ds__week
              , DATE_TRUNC(bookings_source_src_10001.ds, month) AS ds__month
              , DATE_TRUNC(bookings_source_src_10001.ds, quarter) AS ds__quarter
              , DATE_TRUNC(bookings_source_src_10001.ds, year) AS ds__year
              , EXTRACT(year FROM bookings_source_src_10001.ds) AS ds__extract_year
              , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS ds__extract_quarter
              , EXTRACT(month FROM bookings_source_src_10001.ds) AS ds__extract_month
              , EXTRACT(isoweek FROM bookings_source_src_10001.ds) AS ds__extract_week
              , EXTRACT(day FROM bookings_source_src_10001.ds) AS ds__extract_day
              , EXTRACT(dayofweek FROM bookings_source_src_10001.ds) AS ds__extract_dow
              , EXTRACT(dayofyear FROM bookings_source_src_10001.ds) AS ds__extract_doy
              , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, day) AS ds_partitioned__day
              , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, isoweek) AS ds_partitioned__week
              , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, month) AS ds_partitioned__month
              , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, quarter) AS ds_partitioned__quarter
              , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, year) AS ds_partitioned__year
              , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_year
              , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_quarter
              , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_month
              , EXTRACT(isoweek FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_week
              , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_day
              , EXTRACT(dayofweek FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_dow
              , EXTRACT(dayofyear FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_doy
              , DATE_TRUNC(bookings_source_src_10001.paid_at, day) AS paid_at__day
              , DATE_TRUNC(bookings_source_src_10001.paid_at, isoweek) AS paid_at__week
              , DATE_TRUNC(bookings_source_src_10001.paid_at, month) AS paid_at__month
              , DATE_TRUNC(bookings_source_src_10001.paid_at, quarter) AS paid_at__quarter
              , DATE_TRUNC(bookings_source_src_10001.paid_at, year) AS paid_at__year
              , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS paid_at__extract_year
              , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS paid_at__extract_quarter
              , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS paid_at__extract_month
              , EXTRACT(isoweek FROM bookings_source_src_10001.paid_at) AS paid_at__extract_week
              , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS paid_at__extract_day
              , EXTRACT(dayofweek FROM bookings_source_src_10001.paid_at) AS paid_at__extract_dow
              , EXTRACT(dayofyear FROM bookings_source_src_10001.paid_at) AS paid_at__extract_doy
              , bookings_source_src_10001.is_instant AS booking__is_instant
              , DATE_TRUNC(bookings_source_src_10001.ds, day) AS booking__ds__day
              , DATE_TRUNC(bookings_source_src_10001.ds, isoweek) AS booking__ds__week
              , DATE_TRUNC(bookings_source_src_10001.ds, month) AS booking__ds__month
              , DATE_TRUNC(bookings_source_src_10001.ds, quarter) AS booking__ds__quarter
              , DATE_TRUNC(bookings_source_src_10001.ds, year) AS booking__ds__year
              , EXTRACT(year FROM bookings_source_src_10001.ds) AS booking__ds__extract_year
              , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS booking__ds__extract_quarter
              , EXTRACT(month FROM bookings_source_src_10001.ds) AS booking__ds__extract_month
              , EXTRACT(isoweek FROM bookings_source_src_10001.ds) AS booking__ds__extract_week
              , EXTRACT(day FROM bookings_source_src_10001.ds) AS booking__ds__extract_day
              , EXTRACT(dayofweek FROM bookings_source_src_10001.ds) AS booking__ds__extract_dow
              , EXTRACT(dayofyear FROM bookings_source_src_10001.ds) AS booking__ds__extract_doy
              , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, day) AS booking__ds_partitioned__day
              , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, isoweek) AS booking__ds_partitioned__week
              , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, month) AS booking__ds_partitioned__month
              , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, quarter) AS booking__ds_partitioned__quarter
              , DATE_TRUNC(bookings_source_src_10001.ds_partitioned, year) AS booking__ds_partitioned__year
              , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_year
              , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_quarter
              , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_month
              , EXTRACT(isoweek FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_week
              , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_day
              , EXTRACT(dayofweek FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_dow
              , EXTRACT(dayofyear FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_doy
              , DATE_TRUNC(bookings_source_src_10001.paid_at, day) AS booking__paid_at__day
              , DATE_TRUNC(bookings_source_src_10001.paid_at, isoweek) AS booking__paid_at__week
              , DATE_TRUNC(bookings_source_src_10001.paid_at, month) AS booking__paid_at__month
              , DATE_TRUNC(bookings_source_src_10001.paid_at, quarter) AS booking__paid_at__quarter
              , DATE_TRUNC(bookings_source_src_10001.paid_at, year) AS booking__paid_at__year
              , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_year
              , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_quarter
              , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_month
              , EXTRACT(isoweek FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_week
              , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_day
              , EXTRACT(dayofweek FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_dow
              , EXTRACT(dayofyear FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_doy
              , bookings_source_src_10001.listing_id AS listing
              , bookings_source_src_10001.guest_id AS guest
              , bookings_source_src_10001.host_id AS host
              , bookings_source_src_10001.listing_id AS booking__listing
              , bookings_source_src_10001.guest_id AS booking__guest
              , bookings_source_src_10001.host_id AS booking__host
            FROM ***************************.fct_bookings bookings_source_src_10001
          ) subq_0
        ) subq_1
        WHERE subq_1.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
      ) subq_2
    ) subq_3
  ) subq_4
) subq_5
CROSS JOIN (
  -- Compute Metrics via Expressions
  SELECT
    subq_10.listings
  FROM (
    -- Aggregate Measures
    SELECT
      SUM(subq_9.listings) AS listings
    FROM (
      -- Pass Only Elements:
      --   ['listings']
      SELECT
        subq_8.listings
      FROM (
        -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
        SELECT
          subq_7.ds__day
          , subq_7.ds__week
          , subq_7.ds__month
          , subq_7.ds__quarter
          , subq_7.ds__year
          , subq_7.ds__extract_year
          , subq_7.ds__extract_quarter
          , subq_7.ds__extract_month
          , subq_7.ds__extract_week
          , subq_7.ds__extract_day
          , subq_7.ds__extract_dow
          , subq_7.ds__extract_doy
          , subq_7.created_at__day
          , subq_7.created_at__week
          , subq_7.created_at__month
          , subq_7.created_at__quarter
          , subq_7.created_at__year
          , subq_7.created_at__extract_year
          , subq_7.created_at__extract_quarter
          , subq_7.created_at__extract_month
          , subq_7.created_at__extract_week
          , subq_7.created_at__extract_day
          , subq_7.created_at__extract_dow
          , subq_7.created_at__extract_doy
          , subq_7.listing__ds__day
          , subq_7.listing__ds__week
          , subq_7.listing__ds__month
          , subq_7.listing__ds__quarter
          , subq_7.listing__ds__year
          , subq_7.listing__ds__extract_year
          , subq_7.listing__ds__extract_quarter
          , subq_7.listing__ds__extract_month
          , subq_7.listing__ds__extract_week
          , subq_7.listing__ds__extract_day
          , subq_7.listing__ds__extract_dow
          , subq_7.listing__ds__extract_doy
          , subq_7.listing__created_at__day
          , subq_7.listing__created_at__week
          , subq_7.listing__created_at__month
          , subq_7.listing__created_at__quarter
          , subq_7.listing__created_at__year
          , subq_7.listing__created_at__extract_year
          , subq_7.listing__created_at__extract_quarter
          , subq_7.listing__created_at__extract_month
          , subq_7.listing__created_at__extract_week
          , subq_7.listing__created_at__extract_day
          , subq_7.listing__created_at__extract_dow
          , subq_7.listing__created_at__extract_doy
          , subq_7.metric_time__day
          , subq_7.metric_time__week
          , subq_7.metric_time__month
          , subq_7.metric_time__quarter
          , subq_7.metric_time__year
          , subq_7.metric_time__extract_year
          , subq_7.metric_time__extract_quarter
          , subq_7.metric_time__extract_month
          , subq_7.metric_time__extract_week
          , subq_7.metric_time__extract_day
          , subq_7.metric_time__extract_dow
          , subq_7.metric_time__extract_doy
          , subq_7.listing
          , subq_7.user
          , subq_7.listing__user
          , subq_7.country_latest
          , subq_7.is_lux_latest
          , subq_7.capacity_latest
          , subq_7.listing__country_latest
          , subq_7.listing__is_lux_latest
          , subq_7.listing__capacity_latest
          , subq_7.listings
          , subq_7.largest_listing
          , subq_7.smallest_listing
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_6.ds__day
            , subq_6.ds__week
            , subq_6.ds__month
            , subq_6.ds__quarter
            , subq_6.ds__year
            , subq_6.ds__extract_year
            , subq_6.ds__extract_quarter
            , subq_6.ds__extract_month
            , subq_6.ds__extract_week
            , subq_6.ds__extract_day
            , subq_6.ds__extract_dow
            , subq_6.ds__extract_doy
            , subq_6.created_at__day
            , subq_6.created_at__week
            , subq_6.created_at__month
            , subq_6.created_at__quarter
            , subq_6.created_at__year
            , subq_6.created_at__extract_year
            , subq_6.created_at__extract_quarter
            , subq_6.created_at__extract_month
            , subq_6.created_at__extract_week
            , subq_6.created_at__extract_day
            , subq_6.created_at__extract_dow
            , subq_6.created_at__extract_doy
            , subq_6.listing__ds__day
            , subq_6.listing__ds__week
            , subq_6.listing__ds__month
            , subq_6.listing__ds__quarter
            , subq_6.listing__ds__year
            , subq_6.listing__ds__extract_year
            , subq_6.listing__ds__extract_quarter
            , subq_6.listing__ds__extract_month
            , subq_6.listing__ds__extract_week
            , subq_6.listing__ds__extract_day
            , subq_6.listing__ds__extract_dow
            , subq_6.listing__ds__extract_doy
            , subq_6.listing__created_at__day
            , subq_6.listing__created_at__week
            , subq_6.listing__created_at__month
            , subq_6.listing__created_at__quarter
            , subq_6.listing__created_at__year
            , subq_6.listing__created_at__extract_year
            , subq_6.listing__created_at__extract_quarter
            , subq_6.listing__created_at__extract_month
            , subq_6.listing__created_at__extract_week
            , subq_6.listing__created_at__extract_day
            , subq_6.listing__created_at__extract_dow
            , subq_6.listing__created_at__extract_doy
            , subq_6.ds__day AS metric_time__day
            , subq_6.ds__week AS metric_time__week
            , subq_6.ds__month AS metric_time__month
            , subq_6.ds__quarter AS metric_time__quarter
            , subq_6.ds__year AS metric_time__year
            , subq_6.ds__extract_year AS metric_time__extract_year
            , subq_6.ds__extract_quarter AS metric_time__extract_quarter
            , subq_6.ds__extract_month AS metric_time__extract_month
            , subq_6.ds__extract_week AS metric_time__extract_week
            , subq_6.ds__extract_day AS metric_time__extract_day
            , subq_6.ds__extract_dow AS metric_time__extract_dow
            , subq_6.ds__extract_doy AS metric_time__extract_doy
            , subq_6.listing
            , subq_6.user
            , subq_6.listing__user
            , subq_6.country_latest
            , subq_6.is_lux_latest
            , subq_6.capacity_latest
            , subq_6.listing__country_latest
            , subq_6.listing__is_lux_latest
            , subq_6.listing__capacity_latest
            , subq_6.listings
            , subq_6.largest_listing
            , subq_6.smallest_listing
          FROM (
            -- Read Elements From Semantic Model 'listings_latest'
            SELECT
              1 AS listings
              , listings_latest_src_10004.capacity AS largest_listing
              , listings_latest_src_10004.capacity AS smallest_listing
              , DATE_TRUNC(listings_latest_src_10004.created_at, day) AS ds__day
              , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS ds__week
              , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS ds__month
              , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS ds__quarter
              , DATE_TRUNC(listings_latest_src_10004.created_at, year) AS ds__year
              , EXTRACT(year FROM listings_latest_src_10004.created_at) AS ds__extract_year
              , EXTRACT(quarter FROM listings_latest_src_10004.created_at) AS ds__extract_quarter
              , EXTRACT(month FROM listings_latest_src_10004.created_at) AS ds__extract_month
              , EXTRACT(isoweek FROM listings_latest_src_10004.created_at) AS ds__extract_week
              , EXTRACT(day FROM listings_latest_src_10004.created_at) AS ds__extract_day
              , EXTRACT(dayofweek FROM listings_latest_src_10004.created_at) AS ds__extract_dow
              , EXTRACT(dayofyear FROM listings_latest_src_10004.created_at) AS ds__extract_doy
              , DATE_TRUNC(listings_latest_src_10004.created_at, day) AS created_at__day
              , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS created_at__week
              , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS created_at__month
              , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS created_at__quarter
              , DATE_TRUNC(listings_latest_src_10004.created_at, year) AS created_at__year
              , EXTRACT(year FROM listings_latest_src_10004.created_at) AS created_at__extract_year
              , EXTRACT(quarter FROM listings_latest_src_10004.created_at) AS created_at__extract_quarter
              , EXTRACT(month FROM listings_latest_src_10004.created_at) AS created_at__extract_month
              , EXTRACT(isoweek FROM listings_latest_src_10004.created_at) AS created_at__extract_week
              , EXTRACT(day FROM listings_latest_src_10004.created_at) AS created_at__extract_day
              , EXTRACT(dayofweek FROM listings_latest_src_10004.created_at) AS created_at__extract_dow
              , EXTRACT(dayofyear FROM listings_latest_src_10004.created_at) AS created_at__extract_doy
              , listings_latest_src_10004.country AS country_latest
              , listings_latest_src_10004.is_lux AS is_lux_latest
              , listings_latest_src_10004.capacity AS capacity_latest
              , DATE_TRUNC(listings_latest_src_10004.created_at, day) AS listing__ds__day
              , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS listing__ds__week
              , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS listing__ds__month
              , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS listing__ds__quarter
              , DATE_TRUNC(listings_latest_src_10004.created_at, year) AS listing__ds__year
              , EXTRACT(year FROM listings_latest_src_10004.created_at) AS listing__ds__extract_year
              , EXTRACT(quarter FROM listings_latest_src_10004.created_at) AS listing__ds__extract_quarter
              , EXTRACT(month FROM listings_latest_src_10004.created_at) AS listing__ds__extract_month
              , EXTRACT(isoweek FROM listings_latest_src_10004.created_at) AS listing__ds__extract_week
              , EXTRACT(day FROM listings_latest_src_10004.created_at) AS listing__ds__extract_day
              , EXTRACT(dayofweek FROM listings_latest_src_10004.created_at) AS listing__ds__extract_dow
              , EXTRACT(dayofyear FROM listings_latest_src_10004.created_at) AS listing__ds__extract_doy
              , DATE_TRUNC(listings_latest_src_10004.created_at, day) AS listing__created_at__day
              , DATE_TRUNC(listings_latest_src_10004.created_at, isoweek) AS listing__created_at__week
              , DATE_TRUNC(listings_latest_src_10004.created_at, month) AS listing__created_at__month
              , DATE_TRUNC(listings_latest_src_10004.created_at, quarter) AS listing__created_at__quarter
              , DATE_TRUNC(listings_latest_src_10004.created_at, year) AS listing__created_at__year
              , EXTRACT(year FROM listings_latest_src_10004.created_at) AS listing__created_at__extract_year
              , EXTRACT(quarter FROM listings_latest_src_10004.created_at) AS listing__created_at__extract_quarter
              , EXTRACT(month FROM listings_latest_src_10004.created_at) AS listing__created_at__extract_month
              , EXTRACT(isoweek FROM listings_latest_src_10004.created_at) AS listing__created_at__extract_week
              , EXTRACT(day FROM listings_latest_src_10004.created_at) AS listing__created_at__extract_day
              , EXTRACT(dayofweek FROM listings_latest_src_10004.created_at) AS listing__created_at__extract_dow
              , EXTRACT(dayofyear FROM listings_latest_src_10004.created_at) AS listing__created_at__extract_doy
              , listings_latest_src_10004.country AS listing__country_latest
              , listings_latest_src_10004.is_lux AS listing__is_lux_latest
              , listings_latest_src_10004.capacity AS listing__capacity_latest
              , listings_latest_src_10004.listing_id AS listing
              , listings_latest_src_10004.user_id AS user
              , listings_latest_src_10004.user_id AS listing__user
            FROM ***************************.dim_listings_latest listings_latest_src_10004
          ) subq_6
        ) subq_7
        WHERE subq_7.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
      ) subq_8
    ) subq_9
  ) subq_10
) subq_11
