test_name: test_custom_offset_window_with_filter_not_in_group_by
test_filename: test_custom_granularity.py
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
SELECT
  subq_16.metric_time__day
  , bookings AS bookings_offset_one_alien_day
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_15.metric_time__day
    , subq_15.bookings
  FROM (
    -- Aggregate Measures
    SELECT
      subq_14.metric_time__day
      , SUM(subq_14.bookings) AS bookings
    FROM (
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      SELECT
        subq_13.metric_time__day
        , subq_13.bookings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_12.ds__day
          , subq_12.ds__week
          , subq_12.ds__month
          , subq_12.ds__quarter
          , subq_12.ds__year
          , subq_12.ds__extract_year
          , subq_12.ds__extract_quarter
          , subq_12.ds__extract_month
          , subq_12.ds__extract_day
          , subq_12.ds__extract_dow
          , subq_12.ds__extract_doy
          , subq_12.ds_partitioned__day
          , subq_12.ds_partitioned__week
          , subq_12.ds_partitioned__month
          , subq_12.ds_partitioned__quarter
          , subq_12.ds_partitioned__year
          , subq_12.ds_partitioned__extract_year
          , subq_12.ds_partitioned__extract_quarter
          , subq_12.ds_partitioned__extract_month
          , subq_12.ds_partitioned__extract_day
          , subq_12.ds_partitioned__extract_dow
          , subq_12.ds_partitioned__extract_doy
          , subq_12.paid_at__day
          , subq_12.paid_at__week
          , subq_12.paid_at__month
          , subq_12.paid_at__quarter
          , subq_12.paid_at__year
          , subq_12.paid_at__extract_year
          , subq_12.paid_at__extract_quarter
          , subq_12.paid_at__extract_month
          , subq_12.paid_at__extract_day
          , subq_12.paid_at__extract_dow
          , subq_12.paid_at__extract_doy
          , subq_12.booking__ds__day
          , subq_12.booking__ds__week
          , subq_12.booking__ds__month
          , subq_12.booking__ds__quarter
          , subq_12.booking__ds__year
          , subq_12.booking__ds__extract_year
          , subq_12.booking__ds__extract_quarter
          , subq_12.booking__ds__extract_month
          , subq_12.booking__ds__extract_day
          , subq_12.booking__ds__extract_dow
          , subq_12.booking__ds__extract_doy
          , subq_12.booking__ds_partitioned__day
          , subq_12.booking__ds_partitioned__week
          , subq_12.booking__ds_partitioned__month
          , subq_12.booking__ds_partitioned__quarter
          , subq_12.booking__ds_partitioned__year
          , subq_12.booking__ds_partitioned__extract_year
          , subq_12.booking__ds_partitioned__extract_quarter
          , subq_12.booking__ds_partitioned__extract_month
          , subq_12.booking__ds_partitioned__extract_day
          , subq_12.booking__ds_partitioned__extract_dow
          , subq_12.booking__ds_partitioned__extract_doy
          , subq_12.booking__paid_at__day
          , subq_12.booking__paid_at__week
          , subq_12.booking__paid_at__month
          , subq_12.booking__paid_at__quarter
          , subq_12.booking__paid_at__year
          , subq_12.booking__paid_at__extract_year
          , subq_12.booking__paid_at__extract_quarter
          , subq_12.booking__paid_at__extract_month
          , subq_12.booking__paid_at__extract_day
          , subq_12.booking__paid_at__extract_dow
          , subq_12.booking__paid_at__extract_doy
          , subq_12.metric_time__week
          , subq_12.metric_time__month
          , subq_12.metric_time__quarter
          , subq_12.metric_time__year
          , subq_12.metric_time__extract_year
          , subq_12.metric_time__extract_quarter
          , subq_12.metric_time__extract_month
          , subq_12.metric_time__extract_day
          , subq_12.metric_time__extract_dow
          , subq_12.metric_time__extract_doy
          , subq_12.metric_time__day
          , subq_12.listing
          , subq_12.guest
          , subq_12.host
          , subq_12.booking__listing
          , subq_12.booking__guest
          , subq_12.booking__host
          , subq_12.is_instant
          , subq_12.booking__is_instant
          , subq_12.bookings
          , subq_12.instant_bookings
          , subq_12.booking_value
          , subq_12.max_booking_value
          , subq_12.min_booking_value
          , subq_12.bookers
          , subq_12.average_booking_value
          , subq_12.referred_bookings
          , subq_12.median_booking_value
          , subq_12.booking_value_p99
          , subq_12.discrete_booking_value_p99
          , subq_12.approximate_continuous_booking_value_p99
          , subq_12.approximate_discrete_booking_value_p99
        FROM (
          -- Join to Time Spine Dataset
          SELECT
            subq_11.metric_time__day AS metric_time__day
            , subq_5.ds__day AS ds__day
            , subq_5.ds__week AS ds__week
            , subq_5.ds__month AS ds__month
            , subq_5.ds__quarter AS ds__quarter
            , subq_5.ds__year AS ds__year
            , subq_5.ds__extract_year AS ds__extract_year
            , subq_5.ds__extract_quarter AS ds__extract_quarter
            , subq_5.ds__extract_month AS ds__extract_month
            , subq_5.ds__extract_day AS ds__extract_day
            , subq_5.ds__extract_dow AS ds__extract_dow
            , subq_5.ds__extract_doy AS ds__extract_doy
            , subq_5.ds_partitioned__day AS ds_partitioned__day
            , subq_5.ds_partitioned__week AS ds_partitioned__week
            , subq_5.ds_partitioned__month AS ds_partitioned__month
            , subq_5.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_5.ds_partitioned__year AS ds_partitioned__year
            , subq_5.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , subq_5.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , subq_5.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , subq_5.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , subq_5.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , subq_5.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , subq_5.paid_at__day AS paid_at__day
            , subq_5.paid_at__week AS paid_at__week
            , subq_5.paid_at__month AS paid_at__month
            , subq_5.paid_at__quarter AS paid_at__quarter
            , subq_5.paid_at__year AS paid_at__year
            , subq_5.paid_at__extract_year AS paid_at__extract_year
            , subq_5.paid_at__extract_quarter AS paid_at__extract_quarter
            , subq_5.paid_at__extract_month AS paid_at__extract_month
            , subq_5.paid_at__extract_day AS paid_at__extract_day
            , subq_5.paid_at__extract_dow AS paid_at__extract_dow
            , subq_5.paid_at__extract_doy AS paid_at__extract_doy
            , subq_5.booking__ds__day AS booking__ds__day
            , subq_5.booking__ds__week AS booking__ds__week
            , subq_5.booking__ds__month AS booking__ds__month
            , subq_5.booking__ds__quarter AS booking__ds__quarter
            , subq_5.booking__ds__year AS booking__ds__year
            , subq_5.booking__ds__extract_year AS booking__ds__extract_year
            , subq_5.booking__ds__extract_quarter AS booking__ds__extract_quarter
            , subq_5.booking__ds__extract_month AS booking__ds__extract_month
            , subq_5.booking__ds__extract_day AS booking__ds__extract_day
            , subq_5.booking__ds__extract_dow AS booking__ds__extract_dow
            , subq_5.booking__ds__extract_doy AS booking__ds__extract_doy
            , subq_5.booking__ds_partitioned__day AS booking__ds_partitioned__day
            , subq_5.booking__ds_partitioned__week AS booking__ds_partitioned__week
            , subq_5.booking__ds_partitioned__month AS booking__ds_partitioned__month
            , subq_5.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
            , subq_5.booking__ds_partitioned__year AS booking__ds_partitioned__year
            , subq_5.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
            , subq_5.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
            , subq_5.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
            , subq_5.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
            , subq_5.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
            , subq_5.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
            , subq_5.booking__paid_at__day AS booking__paid_at__day
            , subq_5.booking__paid_at__week AS booking__paid_at__week
            , subq_5.booking__paid_at__month AS booking__paid_at__month
            , subq_5.booking__paid_at__quarter AS booking__paid_at__quarter
            , subq_5.booking__paid_at__year AS booking__paid_at__year
            , subq_5.booking__paid_at__extract_year AS booking__paid_at__extract_year
            , subq_5.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
            , subq_5.booking__paid_at__extract_month AS booking__paid_at__extract_month
            , subq_5.booking__paid_at__extract_day AS booking__paid_at__extract_day
            , subq_5.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
            , subq_5.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
            , subq_5.metric_time__week AS metric_time__week
            , subq_5.metric_time__month AS metric_time__month
            , subq_5.metric_time__quarter AS metric_time__quarter
            , subq_5.metric_time__year AS metric_time__year
            , subq_5.metric_time__extract_year AS metric_time__extract_year
            , subq_5.metric_time__extract_quarter AS metric_time__extract_quarter
            , subq_5.metric_time__extract_month AS metric_time__extract_month
            , subq_5.metric_time__extract_day AS metric_time__extract_day
            , subq_5.metric_time__extract_dow AS metric_time__extract_dow
            , subq_5.metric_time__extract_doy AS metric_time__extract_doy
            , subq_5.listing AS listing
            , subq_5.guest AS guest
            , subq_5.host AS host
            , subq_5.booking__listing AS booking__listing
            , subq_5.booking__guest AS booking__guest
            , subq_5.booking__host AS booking__host
            , subq_5.is_instant AS is_instant
            , subq_5.booking__is_instant AS booking__is_instant
            , subq_5.bookings AS bookings
            , subq_5.instant_bookings AS instant_bookings
            , subq_5.booking_value AS booking_value
            , subq_5.max_booking_value AS max_booking_value
            , subq_5.min_booking_value AS min_booking_value
            , subq_5.bookers AS bookers
            , subq_5.average_booking_value AS average_booking_value
            , subq_5.referred_bookings AS referred_bookings
            , subq_5.median_booking_value AS median_booking_value
            , subq_5.booking_value_p99 AS booking_value_p99
            , subq_5.discrete_booking_value_p99 AS discrete_booking_value_p99
            , subq_5.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
            , subq_5.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
          FROM (
            -- Pass Only Elements: ['ds__day', 'metric_time__day']
            SELECT
              subq_10.ds__day
              , subq_10.metric_time__day
            FROM (
              -- Apply Requested Granularities
              SELECT
                subq_9.ds__day
                , subq_9.ds__day__lead AS metric_time__day
              FROM (
                -- Offset Base Granularity By Custom Granularity Period(s)
                WITH cte_2 AS (
                  -- Get Custom Granularity Bounds
                  SELECT
                    subq_6.ds__day
                    , subq_6.ds__alien_day
                    , FIRST_VALUE(subq_6.ds__day) OVER (
                      PARTITION BY subq_6.ds__alien_day
                      ORDER BY subq_6.ds__day
                      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) AS ds__day__first_value
                    , LAST_VALUE(subq_6.ds__day) OVER (
                      PARTITION BY subq_6.ds__alien_day
                      ORDER BY subq_6.ds__day
                      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) AS ds__day__last_value
                    , ROW_NUMBER() OVER (
                      PARTITION BY subq_6.ds__alien_day
                      ORDER BY subq_6.ds__day
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
                      , EXTRACT(dayofweekiso FROM time_spine_src_28006.ds) AS ds__extract_dow
                      , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
                      , time_spine_src_28006.alien_day AS ds__alien_day
                    FROM ***************************.mf_time_spine time_spine_src_28006
                  ) subq_6
                )

                SELECT
                  cte_2.ds__day AS ds__day
                  , CASE
                    WHEN DATEADD(day, (cte_2.ds__day__row_number - 1), subq_8.ds__day__first_value__lead) <= subq_8.ds__day__last_value__lead
                      THEN DATEADD(day, (cte_2.ds__day__row_number - 1), subq_8.ds__day__first_value__lead)
                    ELSE NULL
                  END AS ds__day__lead
                FROM cte_2 cte_2
                INNER JOIN (
                  -- Offset Custom Granularity Bounds
                  SELECT
                    subq_7.ds__alien_day
                    , LEAD(subq_7.ds__day__first_value, 1) OVER (ORDER BY subq_7.ds__alien_day) AS ds__day__first_value__lead
                    , LEAD(subq_7.ds__day__last_value, 1) OVER (ORDER BY subq_7.ds__alien_day) AS ds__day__last_value__lead
                  FROM (
                    -- Get Unique Rows for Custom Granularity Bounds
                    SELECT
                      cte_2.ds__alien_day
                      , cte_2.ds__day__first_value
                      , cte_2.ds__day__last_value
                    FROM cte_2 cte_2
                    GROUP BY
                      cte_2.ds__alien_day
                      , cte_2.ds__day__first_value
                      , cte_2.ds__day__last_value
                  ) subq_7
                ) subq_8
                ON
                  cte_2.ds__alien_day = subq_8.ds__alien_day
              ) subq_9
            ) subq_10
          ) subq_11
          INNER JOIN (
            -- Metric Time Dimension 'ds'
            SELECT
              subq_4.ds__day
              , subq_4.ds__week
              , subq_4.ds__month
              , subq_4.ds__quarter
              , subq_4.ds__year
              , subq_4.ds__extract_year
              , subq_4.ds__extract_quarter
              , subq_4.ds__extract_month
              , subq_4.ds__extract_day
              , subq_4.ds__extract_dow
              , subq_4.ds__extract_doy
              , subq_4.ds_partitioned__day
              , subq_4.ds_partitioned__week
              , subq_4.ds_partitioned__month
              , subq_4.ds_partitioned__quarter
              , subq_4.ds_partitioned__year
              , subq_4.ds_partitioned__extract_year
              , subq_4.ds_partitioned__extract_quarter
              , subq_4.ds_partitioned__extract_month
              , subq_4.ds_partitioned__extract_day
              , subq_4.ds_partitioned__extract_dow
              , subq_4.ds_partitioned__extract_doy
              , subq_4.paid_at__day
              , subq_4.paid_at__week
              , subq_4.paid_at__month
              , subq_4.paid_at__quarter
              , subq_4.paid_at__year
              , subq_4.paid_at__extract_year
              , subq_4.paid_at__extract_quarter
              , subq_4.paid_at__extract_month
              , subq_4.paid_at__extract_day
              , subq_4.paid_at__extract_dow
              , subq_4.paid_at__extract_doy
              , subq_4.booking__ds__day
              , subq_4.booking__ds__week
              , subq_4.booking__ds__month
              , subq_4.booking__ds__quarter
              , subq_4.booking__ds__year
              , subq_4.booking__ds__extract_year
              , subq_4.booking__ds__extract_quarter
              , subq_4.booking__ds__extract_month
              , subq_4.booking__ds__extract_day
              , subq_4.booking__ds__extract_dow
              , subq_4.booking__ds__extract_doy
              , subq_4.booking__ds_partitioned__day
              , subq_4.booking__ds_partitioned__week
              , subq_4.booking__ds_partitioned__month
              , subq_4.booking__ds_partitioned__quarter
              , subq_4.booking__ds_partitioned__year
              , subq_4.booking__ds_partitioned__extract_year
              , subq_4.booking__ds_partitioned__extract_quarter
              , subq_4.booking__ds_partitioned__extract_month
              , subq_4.booking__ds_partitioned__extract_day
              , subq_4.booking__ds_partitioned__extract_dow
              , subq_4.booking__ds_partitioned__extract_doy
              , subq_4.booking__paid_at__day
              , subq_4.booking__paid_at__week
              , subq_4.booking__paid_at__month
              , subq_4.booking__paid_at__quarter
              , subq_4.booking__paid_at__year
              , subq_4.booking__paid_at__extract_year
              , subq_4.booking__paid_at__extract_quarter
              , subq_4.booking__paid_at__extract_month
              , subq_4.booking__paid_at__extract_day
              , subq_4.booking__paid_at__extract_dow
              , subq_4.booking__paid_at__extract_doy
              , subq_4.ds__day AS metric_time__day
              , subq_4.ds__week AS metric_time__week
              , subq_4.ds__month AS metric_time__month
              , subq_4.ds__quarter AS metric_time__quarter
              , subq_4.ds__year AS metric_time__year
              , subq_4.ds__extract_year AS metric_time__extract_year
              , subq_4.ds__extract_quarter AS metric_time__extract_quarter
              , subq_4.ds__extract_month AS metric_time__extract_month
              , subq_4.ds__extract_day AS metric_time__extract_day
              , subq_4.ds__extract_dow AS metric_time__extract_dow
              , subq_4.ds__extract_doy AS metric_time__extract_doy
              , subq_4.listing
              , subq_4.guest
              , subq_4.host
              , subq_4.booking__listing
              , subq_4.booking__guest
              , subq_4.booking__host
              , subq_4.is_instant
              , subq_4.booking__is_instant
              , subq_4.bookings
              , subq_4.instant_bookings
              , subq_4.booking_value
              , subq_4.max_booking_value
              , subq_4.min_booking_value
              , subq_4.bookers
              , subq_4.average_booking_value
              , subq_4.referred_bookings
              , subq_4.median_booking_value
              , subq_4.booking_value_p99
              , subq_4.discrete_booking_value_p99
              , subq_4.approximate_continuous_booking_value_p99
              , subq_4.approximate_discrete_booking_value_p99
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
                , EXTRACT(dayofweekiso FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
                , EXTRACT(dayofweekiso FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
                , EXTRACT(dayofweekiso FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
                , EXTRACT(dayofweekiso FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
                , EXTRACT(dayofweekiso FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
                , EXTRACT(dayofweekiso FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
                , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
                , bookings_source_src_28000.listing_id AS listing
                , bookings_source_src_28000.guest_id AS guest
                , bookings_source_src_28000.host_id AS host
                , bookings_source_src_28000.listing_id AS booking__listing
                , bookings_source_src_28000.guest_id AS booking__guest
                , bookings_source_src_28000.host_id AS booking__host
              FROM ***************************.fct_bookings bookings_source_src_28000
            ) subq_4
          ) subq_5
          ON
            subq_11.ds__day = subq_5.metric_time__day
        ) subq_12
        WHERE metric_time__month = '2020-01-01'
      ) subq_13
    ) subq_14
    GROUP BY
      subq_14.metric_time__day
  ) subq_15
) subq_16
