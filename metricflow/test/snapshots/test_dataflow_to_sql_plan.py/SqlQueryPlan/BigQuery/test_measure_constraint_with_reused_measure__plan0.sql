-- Compute Metrics via Expressions
SELECT
  subq_12.metric_time__day
  , CAST(subq_12.booking_value_with_is_instant_constraint AS FLOAT64) / CAST(NULLIF(subq_12.booking_value, 0) AS FLOAT64) AS instant_booking_value_ratio
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_6.metric_time__day, subq_11.metric_time__day) AS metric_time__day
    , subq_6.booking_value_with_is_instant_constraint AS booking_value_with_is_instant_constraint
    , subq_11.booking_value AS booking_value
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_5.metric_time__day
      , subq_5.booking_value AS booking_value_with_is_instant_constraint
    FROM (
      -- Aggregate Measures
      SELECT
        subq_4.metric_time__day
        , SUM(subq_4.booking_value) AS booking_value
      FROM (
        -- Pass Only Elements:
        --   ['booking_value', 'metric_time__day']
        SELECT
          subq_3.metric_time__day
          , subq_3.booking_value
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_2.metric_time__day
            , subq_2.booking__is_instant
            , subq_2.booking_value
          FROM (
            -- Pass Only Elements:
            --   ['booking_value', 'booking__is_instant', 'metric_time__day']
            SELECT
              subq_1.metric_time__day
              , subq_1.booking__is_instant
              , subq_1.booking_value
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
          ) subq_2
          WHERE booking__is_instant
        ) subq_3
      ) subq_4
      GROUP BY
        metric_time__day
    ) subq_5
  ) subq_6
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_10.metric_time__day
      , subq_10.booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        subq_9.metric_time__day
        , SUM(subq_9.booking_value) AS booking_value
      FROM (
        -- Pass Only Elements:
        --   ['booking_value', 'metric_time__day']
        SELECT
          subq_8.metric_time__day
          , subq_8.booking_value
        FROM (
          -- Metric Time Dimension 'ds'
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
            , subq_7.ds_partitioned__day
            , subq_7.ds_partitioned__week
            , subq_7.ds_partitioned__month
            , subq_7.ds_partitioned__quarter
            , subq_7.ds_partitioned__year
            , subq_7.ds_partitioned__extract_year
            , subq_7.ds_partitioned__extract_quarter
            , subq_7.ds_partitioned__extract_month
            , subq_7.ds_partitioned__extract_week
            , subq_7.ds_partitioned__extract_day
            , subq_7.ds_partitioned__extract_dow
            , subq_7.ds_partitioned__extract_doy
            , subq_7.paid_at__day
            , subq_7.paid_at__week
            , subq_7.paid_at__month
            , subq_7.paid_at__quarter
            , subq_7.paid_at__year
            , subq_7.paid_at__extract_year
            , subq_7.paid_at__extract_quarter
            , subq_7.paid_at__extract_month
            , subq_7.paid_at__extract_week
            , subq_7.paid_at__extract_day
            , subq_7.paid_at__extract_dow
            , subq_7.paid_at__extract_doy
            , subq_7.booking__ds__day
            , subq_7.booking__ds__week
            , subq_7.booking__ds__month
            , subq_7.booking__ds__quarter
            , subq_7.booking__ds__year
            , subq_7.booking__ds__extract_year
            , subq_7.booking__ds__extract_quarter
            , subq_7.booking__ds__extract_month
            , subq_7.booking__ds__extract_week
            , subq_7.booking__ds__extract_day
            , subq_7.booking__ds__extract_dow
            , subq_7.booking__ds__extract_doy
            , subq_7.booking__ds_partitioned__day
            , subq_7.booking__ds_partitioned__week
            , subq_7.booking__ds_partitioned__month
            , subq_7.booking__ds_partitioned__quarter
            , subq_7.booking__ds_partitioned__year
            , subq_7.booking__ds_partitioned__extract_year
            , subq_7.booking__ds_partitioned__extract_quarter
            , subq_7.booking__ds_partitioned__extract_month
            , subq_7.booking__ds_partitioned__extract_week
            , subq_7.booking__ds_partitioned__extract_day
            , subq_7.booking__ds_partitioned__extract_dow
            , subq_7.booking__ds_partitioned__extract_doy
            , subq_7.booking__paid_at__day
            , subq_7.booking__paid_at__week
            , subq_7.booking__paid_at__month
            , subq_7.booking__paid_at__quarter
            , subq_7.booking__paid_at__year
            , subq_7.booking__paid_at__extract_year
            , subq_7.booking__paid_at__extract_quarter
            , subq_7.booking__paid_at__extract_month
            , subq_7.booking__paid_at__extract_week
            , subq_7.booking__paid_at__extract_day
            , subq_7.booking__paid_at__extract_dow
            , subq_7.booking__paid_at__extract_doy
            , subq_7.ds__day AS metric_time__day
            , subq_7.ds__week AS metric_time__week
            , subq_7.ds__month AS metric_time__month
            , subq_7.ds__quarter AS metric_time__quarter
            , subq_7.ds__year AS metric_time__year
            , subq_7.ds__extract_year AS metric_time__extract_year
            , subq_7.ds__extract_quarter AS metric_time__extract_quarter
            , subq_7.ds__extract_month AS metric_time__extract_month
            , subq_7.ds__extract_week AS metric_time__extract_week
            , subq_7.ds__extract_day AS metric_time__extract_day
            , subq_7.ds__extract_dow AS metric_time__extract_dow
            , subq_7.ds__extract_doy AS metric_time__extract_doy
            , subq_7.listing
            , subq_7.guest
            , subq_7.host
            , subq_7.booking__listing
            , subq_7.booking__guest
            , subq_7.booking__host
            , subq_7.is_instant
            , subq_7.booking__is_instant
            , subq_7.bookings
            , subq_7.instant_bookings
            , subq_7.booking_value
            , subq_7.max_booking_value
            , subq_7.min_booking_value
            , subq_7.bookers
            , subq_7.average_booking_value
            , subq_7.referred_bookings
            , subq_7.median_booking_value
            , subq_7.booking_value_p99
            , subq_7.discrete_booking_value_p99
            , subq_7.approximate_continuous_booking_value_p99
            , subq_7.approximate_discrete_booking_value_p99
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
          ) subq_7
        ) subq_8
      ) subq_9
      GROUP BY
        metric_time__day
    ) subq_10
  ) subq_11
  ON
    (
      subq_6.metric_time__day = subq_11.metric_time__day
    ) OR (
      (
        subq_6.metric_time__day IS NULL
      ) AND (
        subq_11.metric_time__day IS NULL
      )
    )
) subq_12
