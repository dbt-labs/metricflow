test_name: test_custom_offset_window_with_granularity_and_date_part
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  subq_16.metric_time__martian_day
  , subq_16.booking__ds__month
  , subq_16.metric_time__extract_year
  , bookings AS bookings_offset_one_martian_day
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_15.metric_time__martian_day
    , subq_15.booking__ds__month
    , subq_15.metric_time__extract_year
    , subq_15.bookings
  FROM (
    -- Aggregate Measures
    SELECT
      subq_14.metric_time__martian_day
      , subq_14.booking__ds__month
      , subq_14.metric_time__extract_year
      , SUM(subq_14.bookings) AS bookings
    FROM (
      -- Pass Only Elements: ['bookings', 'booking__ds__month', 'metric_time__extract_year', 'metric_time__martian_day']
      SELECT
        subq_13.metric_time__martian_day
        , subq_13.booking__ds__month
        , subq_13.metric_time__extract_year
        , subq_13.bookings
      FROM (
        -- Join to Time Spine Dataset
        -- Join to Custom Granularity Dataset
        SELECT
          subq_11.booking__ds__month AS booking__ds__month
          , subq_11.metric_time__extract_year AS metric_time__extract_year
          , subq_11.metric_time__day AS metric_time__day
          , subq_1.ds__day AS ds__day
          , subq_1.ds__week AS ds__week
          , subq_1.ds__month AS ds__month
          , subq_1.ds__quarter AS ds__quarter
          , subq_1.ds__year AS ds__year
          , subq_1.ds__extract_year AS ds__extract_year
          , subq_1.ds__extract_quarter AS ds__extract_quarter
          , subq_1.ds__extract_month AS ds__extract_month
          , subq_1.ds__extract_day AS ds__extract_day
          , subq_1.ds__extract_dow AS ds__extract_dow
          , subq_1.ds__extract_doy AS ds__extract_doy
          , subq_1.ds_partitioned__day AS ds_partitioned__day
          , subq_1.ds_partitioned__week AS ds_partitioned__week
          , subq_1.ds_partitioned__month AS ds_partitioned__month
          , subq_1.ds_partitioned__quarter AS ds_partitioned__quarter
          , subq_1.ds_partitioned__year AS ds_partitioned__year
          , subq_1.ds_partitioned__extract_year AS ds_partitioned__extract_year
          , subq_1.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
          , subq_1.ds_partitioned__extract_month AS ds_partitioned__extract_month
          , subq_1.ds_partitioned__extract_day AS ds_partitioned__extract_day
          , subq_1.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
          , subq_1.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
          , subq_1.paid_at__day AS paid_at__day
          , subq_1.paid_at__week AS paid_at__week
          , subq_1.paid_at__month AS paid_at__month
          , subq_1.paid_at__quarter AS paid_at__quarter
          , subq_1.paid_at__year AS paid_at__year
          , subq_1.paid_at__extract_year AS paid_at__extract_year
          , subq_1.paid_at__extract_quarter AS paid_at__extract_quarter
          , subq_1.paid_at__extract_month AS paid_at__extract_month
          , subq_1.paid_at__extract_day AS paid_at__extract_day
          , subq_1.paid_at__extract_dow AS paid_at__extract_dow
          , subq_1.paid_at__extract_doy AS paid_at__extract_doy
          , subq_1.booking__ds__day AS booking__ds__day
          , subq_1.booking__ds__week AS booking__ds__week
          , subq_1.booking__ds__quarter AS booking__ds__quarter
          , subq_1.booking__ds__year AS booking__ds__year
          , subq_1.booking__ds__extract_year AS booking__ds__extract_year
          , subq_1.booking__ds__extract_quarter AS booking__ds__extract_quarter
          , subq_1.booking__ds__extract_month AS booking__ds__extract_month
          , subq_1.booking__ds__extract_day AS booking__ds__extract_day
          , subq_1.booking__ds__extract_dow AS booking__ds__extract_dow
          , subq_1.booking__ds__extract_doy AS booking__ds__extract_doy
          , subq_1.booking__ds_partitioned__day AS booking__ds_partitioned__day
          , subq_1.booking__ds_partitioned__week AS booking__ds_partitioned__week
          , subq_1.booking__ds_partitioned__month AS booking__ds_partitioned__month
          , subq_1.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
          , subq_1.booking__ds_partitioned__year AS booking__ds_partitioned__year
          , subq_1.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
          , subq_1.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
          , subq_1.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
          , subq_1.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
          , subq_1.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
          , subq_1.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
          , subq_1.booking__paid_at__day AS booking__paid_at__day
          , subq_1.booking__paid_at__week AS booking__paid_at__week
          , subq_1.booking__paid_at__month AS booking__paid_at__month
          , subq_1.booking__paid_at__quarter AS booking__paid_at__quarter
          , subq_1.booking__paid_at__year AS booking__paid_at__year
          , subq_1.booking__paid_at__extract_year AS booking__paid_at__extract_year
          , subq_1.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
          , subq_1.booking__paid_at__extract_month AS booking__paid_at__extract_month
          , subq_1.booking__paid_at__extract_day AS booking__paid_at__extract_day
          , subq_1.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
          , subq_1.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
          , subq_1.metric_time__week AS metric_time__week
          , subq_1.metric_time__month AS metric_time__month
          , subq_1.metric_time__quarter AS metric_time__quarter
          , subq_1.metric_time__year AS metric_time__year
          , subq_1.metric_time__extract_quarter AS metric_time__extract_quarter
          , subq_1.metric_time__extract_month AS metric_time__extract_month
          , subq_1.metric_time__extract_day AS metric_time__extract_day
          , subq_1.metric_time__extract_dow AS metric_time__extract_dow
          , subq_1.metric_time__extract_doy AS metric_time__extract_doy
          , subq_1.listing AS listing
          , subq_1.guest AS guest
          , subq_1.host AS host
          , subq_1.booking__listing AS booking__listing
          , subq_1.booking__guest AS booking__guest
          , subq_1.booking__host AS booking__host
          , subq_1.is_instant AS is_instant
          , subq_1.booking__is_instant AS booking__is_instant
          , subq_1.bookings AS bookings
          , subq_1.instant_bookings AS instant_bookings
          , subq_1.booking_value AS booking_value
          , subq_1.max_booking_value AS max_booking_value
          , subq_1.min_booking_value AS min_booking_value
          , subq_1.bookers AS bookers
          , subq_1.average_booking_value AS average_booking_value
          , subq_1.referred_bookings AS referred_bookings
          , subq_1.median_booking_value AS median_booking_value
          , subq_1.booking_value_p99 AS booking_value_p99
          , subq_1.discrete_booking_value_p99 AS discrete_booking_value_p99
          , subq_1.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
          , subq_1.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
          , subq_12.martian_day AS metric_time__martian_day
        FROM (
          -- Pass Only Elements: ['ds__day', 'booking__ds__month', 'metric_time__extract_year', 'metric_time__day']
          SELECT
            subq_10.ds__day
            , subq_10.booking__ds__month
            , subq_10.metric_time__extract_year
            , subq_10.metric_time__day
          FROM (
            -- Apply Requested Granularities
            SELECT
              subq_9.ds__day
              , DATE_TRUNC('month', subq_9.ds__day__lead) AS booking__ds__month
              , EXTRACT(year FROM subq_9.ds__day__lead) AS metric_time__extract_year
              , subq_9.ds__day__lead AS metric_time__day
            FROM (
              -- Offset Base Granularity By Custom Granularity Period(s)
              SELECT
                subq_3.ds__day AS ds__day
                , CASE
                  WHEN subq_8.ds__martian_day__first_value__offset + INTERVAL (subq_3.ds__day__row_number - 1) day <= subq_8.ds__martian_day__last_value__offset
                    THEN subq_8.ds__martian_day__first_value__offset + INTERVAL (subq_3.ds__day__row_number - 1) day
                  ELSE subq_8.ds__martian_day__last_value__offset
                END AS ds__day__lead
              FROM (
                -- Calculate Custom Granularity Bounds
                SELECT
                  time_spine_src_28006.ds AS ds__day
                  , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
                  , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
                  , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
                  , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
                  , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
                  , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
                  , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
                  , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
                  , EXTRACT(isodow FROM time_spine_src_28006.ds) AS ds__extract_dow
                  , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
                  , time_spine_src_28006.martian_day AS ds__martian_day
                  , FIRST_VALUE(subq_2.ds__day) OVER (
                    PARTITION BY subq_2.ds__martian_day
                    ORDER BY subq_2.ds__day
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                  ) AS ds__martian_day__first_value
                  , LAST_VALUE(subq_2.ds__day) OVER (
                    PARTITION BY subq_2.ds__martian_day
                    ORDER BY subq_2.ds__day
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                  ) AS ds__martian_day__last_value
                  , ROW_NUMBER() OVER (
                    PARTITION BY subq_2.ds__martian_day
                    ORDER BY subq_2.ds__day
                  ) AS ds__day__row_number
                FROM (
                  -- Read From Time Spine 'mf_time_spine'
                  SELECT
                    time_spine_src_28006.ds AS ds__day
                    , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
                    , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
                    , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
                    , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
                    , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
                    , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
                    , EXTRACT(isodow FROM time_spine_src_28006.ds) AS ds__extract_dow
                    , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
                    , time_spine_src_28006.martian_day AS ds__martian_day
                  FROM ***************************.mf_time_spine time_spine_src_28006
                ) subq_2
              ) subq_3
              INNER JOIN (
                -- Offset Custom Granularity Bounds
                SELECT
                  subq_6.ds__martian_day
                  , LEAD(subq_6.ds__martian_day__first_value, 1) OVER (ORDER BY subq_6.ds__martian_day) AS ds__martian_day__first_value__offset
                  , LEAD(subq_6.ds__martian_day__last_value, 1) OVER (ORDER BY subq_6.ds__martian_day) AS ds__martian_day__last_value__offset
                FROM (
                  -- Pass Only Elements: ['ds__martian_day', 'ds__martian_day__first_value', 'ds__martian_day__last_value']
                  SELECT
                    subq_5.ds__martian_day__first_value
                    , subq_5.ds__martian_day__last_value
                    , subq_5.ds__martian_day
                  FROM (
                    -- Calculate Custom Granularity Bounds
                    SELECT
                      time_spine_src_28006.ds AS ds__day
                      , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
                      , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
                      , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
                      , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
                      , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
                      , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
                      , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
                      , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
                      , EXTRACT(isodow FROM time_spine_src_28006.ds) AS ds__extract_dow
                      , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
                      , time_spine_src_28006.martian_day AS ds__martian_day
                      , FIRST_VALUE(subq_4.ds__day) OVER (
                        PARTITION BY subq_4.ds__martian_day
                        ORDER BY subq_4.ds__day
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                      ) AS ds__martian_day__first_value
                      , LAST_VALUE(subq_4.ds__day) OVER (
                        PARTITION BY subq_4.ds__martian_day
                        ORDER BY subq_4.ds__day
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                      ) AS ds__martian_day__last_value
                      , ROW_NUMBER() OVER (
                        PARTITION BY subq_4.ds__martian_day
                        ORDER BY subq_4.ds__day
                      ) AS ds__day__row_number
                    FROM (
                      -- Read From Time Spine 'mf_time_spine'
                      SELECT
                        time_spine_src_28006.ds AS ds__day
                        , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
                        , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
                        , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
                        , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
                        , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
                        , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
                        , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
                        , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
                        , EXTRACT(isodow FROM time_spine_src_28006.ds) AS ds__extract_dow
                        , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
                        , time_spine_src_28006.martian_day AS ds__martian_day
                      FROM ***************************.mf_time_spine time_spine_src_28006
                    ) subq_4
                  ) subq_5
                  GROUP BY
                    subq_5.ds__martian_day__first_value
                    , subq_5.ds__martian_day__last_value
                    , subq_5.ds__martian_day
                ) subq_6
              ) subq_8
              ON
                subq_3.ds__martian_day = subq_8.ds__martian_day
            ) subq_9
          ) subq_10
        ) subq_11
        INNER JOIN (
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
              , DATE_TRUNC('day', bookings_source_src_28000.ds) AS ds__day
              , DATE_TRUNC('week', bookings_source_src_28000.ds) AS ds__week
              , DATE_TRUNC('month', bookings_source_src_28000.ds) AS ds__month
              , DATE_TRUNC('quarter', bookings_source_src_28000.ds) AS ds__quarter
              , DATE_TRUNC('year', bookings_source_src_28000.ds) AS ds__year
              , EXTRACT(year FROM bookings_source_src_28000.ds) AS ds__extract_year
              , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS ds__extract_quarter
              , EXTRACT(month FROM bookings_source_src_28000.ds) AS ds__extract_month
              , EXTRACT(day FROM bookings_source_src_28000.ds) AS ds__extract_day
              , EXTRACT(isodow FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
              , EXTRACT(isodow FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
              , EXTRACT(isodow FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
              , EXTRACT(isodow FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
              , EXTRACT(isodow FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
              , EXTRACT(isodow FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
              , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
              , bookings_source_src_28000.listing_id AS listing
              , bookings_source_src_28000.guest_id AS guest
              , bookings_source_src_28000.host_id AS host
              , bookings_source_src_28000.listing_id AS booking__listing
              , bookings_source_src_28000.guest_id AS booking__guest
              , bookings_source_src_28000.host_id AS booking__host
            FROM ***************************.fct_bookings bookings_source_src_28000
          ) subq_0
        ) subq_1
        ON
          subq_11.ds__day = subq_1.metric_time__day
        LEFT OUTER JOIN
          ***************************.mf_time_spine subq_12
        ON
          subq_11.metric_time__day = subq_12.ds
      ) subq_13
    ) subq_14
    GROUP BY
      subq_14.metric_time__martian_day
      , subq_14.booking__ds__month
      , subq_14.metric_time__extract_year
  ) subq_15
) subq_16
