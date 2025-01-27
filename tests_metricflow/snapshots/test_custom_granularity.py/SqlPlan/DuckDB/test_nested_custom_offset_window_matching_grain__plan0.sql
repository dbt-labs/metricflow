test_name: test_nested_custom_offset_window_matching_grain
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  subq_28.metric_time__day
  , subq_28.metric_time__martian_day
  , bookings_offset_one_martian_day AS bookings_offset_one_martian_day_then_2_martian_days
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_27.metric_time__martian_day AS metric_time__martian_day
    , subq_21.metric_time__day AS metric_time__day
    , subq_21.bookings_offset_one_martian_day AS bookings_offset_one_martian_day
  FROM (
    -- Pass Only Elements: ['ds__day', 'metric_time__martian_day']
    SELECT
      subq_26.ds__day
      , subq_26.metric_time__martian_day
    FROM (
      -- Apply Requested Granularities
      SELECT
        subq_25.ds__day
        , subq_25.ds__day__lead AS metric_time__martian_day
      FROM (
        -- Offset Base Granularity By Custom Granularity Period(s)
        WITH cte_6 AS (
          -- Get Custom Granularity Bounds
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
            , FIRST_VALUE(subq_22.ds__day) OVER (
              PARTITION BY subq_22.ds__martian_day
              ORDER BY subq_22.ds__day
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS ds__martian_day__first_value
            , LAST_VALUE(subq_22.ds__day) OVER (
              PARTITION BY subq_22.ds__martian_day
              ORDER BY subq_22.ds__day
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS ds__martian_day__last_value
            , ROW_NUMBER() OVER (
              PARTITION BY subq_22.ds__martian_day
              ORDER BY subq_22.ds__day
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
          ) subq_22
        )

        SELECT
          cte_6.ds__day AS ds__day
          , CASE
            WHEN subq_24.ds__martian_day__first_value__lead + INTERVAL (cte_6.ds__day__row_number - 1) day <= subq_24.ds__martian_day__last_value__lead
              THEN subq_24.ds__martian_day__first_value__lead + INTERVAL (cte_6.ds__day__row_number - 1) day
            ELSE NULL
          END AS ds__day__lead
        FROM cte_6 cte_6
        INNER JOIN (
          -- Offset Custom Granularity Bounds
          SELECT
            subq_23.ds__martian_day
            , LEAD(subq_23.ds__martian_day__first_value, 2) OVER (ORDER BY subq_23.ds__martian_day) AS ds__martian_day__first_value__lead
            , LEAD(subq_23.ds__martian_day__last_value, 2) OVER (ORDER BY subq_23.ds__martian_day) AS ds__martian_day__last_value__lead
          FROM (
            -- Get Unique Rows for Custom Granularity Bounds
            SELECT
              cte_6.ds__martian_day
              , cte_6.ds__martian_day__first_value
              , cte_6.ds__martian_day__last_value
            FROM cte_6 cte_6
            GROUP BY
              cte_6.ds__martian_day
              , cte_6.ds__martian_day__first_value
              , cte_6.ds__martian_day__last_value
          ) subq_23
        ) subq_24
        ON
          cte_6.ds__martian_day = subq_24.ds__martian_day
      ) subq_25
    ) subq_26
  ) subq_27
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_20.metric_time__martian_day
      , subq_20.metric_time__day
      , bookings AS bookings_offset_one_martian_day
    FROM (
      -- Compute Metrics via Expressions
      SELECT
        subq_19.metric_time__martian_day
        , subq_19.metric_time__day
        , subq_19.bookings
      FROM (
        -- Aggregate Measures
        SELECT
          subq_18.metric_time__martian_day
          , subq_18.metric_time__day
          , SUM(subq_18.bookings) AS bookings
        FROM (
          -- Pass Only Elements: ['bookings', 'metric_time__martian_day', 'metric_time__day']
          SELECT
            subq_17.metric_time__martian_day
            , subq_17.metric_time__day
            , subq_17.bookings
          FROM (
            -- Join to Time Spine Dataset
            -- Join to Custom Granularity Dataset
            SELECT
              subq_15.metric_time__day AS metric_time__day
              , subq_9.ds__day AS ds__day
              , subq_9.ds__week AS ds__week
              , subq_9.ds__month AS ds__month
              , subq_9.ds__quarter AS ds__quarter
              , subq_9.ds__year AS ds__year
              , subq_9.ds__extract_year AS ds__extract_year
              , subq_9.ds__extract_quarter AS ds__extract_quarter
              , subq_9.ds__extract_month AS ds__extract_month
              , subq_9.ds__extract_day AS ds__extract_day
              , subq_9.ds__extract_dow AS ds__extract_dow
              , subq_9.ds__extract_doy AS ds__extract_doy
              , subq_9.ds_partitioned__day AS ds_partitioned__day
              , subq_9.ds_partitioned__week AS ds_partitioned__week
              , subq_9.ds_partitioned__month AS ds_partitioned__month
              , subq_9.ds_partitioned__quarter AS ds_partitioned__quarter
              , subq_9.ds_partitioned__year AS ds_partitioned__year
              , subq_9.ds_partitioned__extract_year AS ds_partitioned__extract_year
              , subq_9.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
              , subq_9.ds_partitioned__extract_month AS ds_partitioned__extract_month
              , subq_9.ds_partitioned__extract_day AS ds_partitioned__extract_day
              , subq_9.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
              , subq_9.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
              , subq_9.paid_at__day AS paid_at__day
              , subq_9.paid_at__week AS paid_at__week
              , subq_9.paid_at__month AS paid_at__month
              , subq_9.paid_at__quarter AS paid_at__quarter
              , subq_9.paid_at__year AS paid_at__year
              , subq_9.paid_at__extract_year AS paid_at__extract_year
              , subq_9.paid_at__extract_quarter AS paid_at__extract_quarter
              , subq_9.paid_at__extract_month AS paid_at__extract_month
              , subq_9.paid_at__extract_day AS paid_at__extract_day
              , subq_9.paid_at__extract_dow AS paid_at__extract_dow
              , subq_9.paid_at__extract_doy AS paid_at__extract_doy
              , subq_9.booking__ds__day AS booking__ds__day
              , subq_9.booking__ds__week AS booking__ds__week
              , subq_9.booking__ds__month AS booking__ds__month
              , subq_9.booking__ds__quarter AS booking__ds__quarter
              , subq_9.booking__ds__year AS booking__ds__year
              , subq_9.booking__ds__extract_year AS booking__ds__extract_year
              , subq_9.booking__ds__extract_quarter AS booking__ds__extract_quarter
              , subq_9.booking__ds__extract_month AS booking__ds__extract_month
              , subq_9.booking__ds__extract_day AS booking__ds__extract_day
              , subq_9.booking__ds__extract_dow AS booking__ds__extract_dow
              , subq_9.booking__ds__extract_doy AS booking__ds__extract_doy
              , subq_9.booking__ds_partitioned__day AS booking__ds_partitioned__day
              , subq_9.booking__ds_partitioned__week AS booking__ds_partitioned__week
              , subq_9.booking__ds_partitioned__month AS booking__ds_partitioned__month
              , subq_9.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
              , subq_9.booking__ds_partitioned__year AS booking__ds_partitioned__year
              , subq_9.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
              , subq_9.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
              , subq_9.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
              , subq_9.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
              , subq_9.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
              , subq_9.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
              , subq_9.booking__paid_at__day AS booking__paid_at__day
              , subq_9.booking__paid_at__week AS booking__paid_at__week
              , subq_9.booking__paid_at__month AS booking__paid_at__month
              , subq_9.booking__paid_at__quarter AS booking__paid_at__quarter
              , subq_9.booking__paid_at__year AS booking__paid_at__year
              , subq_9.booking__paid_at__extract_year AS booking__paid_at__extract_year
              , subq_9.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
              , subq_9.booking__paid_at__extract_month AS booking__paid_at__extract_month
              , subq_9.booking__paid_at__extract_day AS booking__paid_at__extract_day
              , subq_9.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
              , subq_9.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
              , subq_9.metric_time__week AS metric_time__week
              , subq_9.metric_time__month AS metric_time__month
              , subq_9.metric_time__quarter AS metric_time__quarter
              , subq_9.metric_time__year AS metric_time__year
              , subq_9.metric_time__extract_year AS metric_time__extract_year
              , subq_9.metric_time__extract_quarter AS metric_time__extract_quarter
              , subq_9.metric_time__extract_month AS metric_time__extract_month
              , subq_9.metric_time__extract_day AS metric_time__extract_day
              , subq_9.metric_time__extract_dow AS metric_time__extract_dow
              , subq_9.metric_time__extract_doy AS metric_time__extract_doy
              , subq_9.listing AS listing
              , subq_9.guest AS guest
              , subq_9.host AS host
              , subq_9.booking__listing AS booking__listing
              , subq_9.booking__guest AS booking__guest
              , subq_9.booking__host AS booking__host
              , subq_9.is_instant AS is_instant
              , subq_9.booking__is_instant AS booking__is_instant
              , subq_9.bookings AS bookings
              , subq_9.instant_bookings AS instant_bookings
              , subq_9.booking_value AS booking_value
              , subq_9.max_booking_value AS max_booking_value
              , subq_9.min_booking_value AS min_booking_value
              , subq_9.bookers AS bookers
              , subq_9.average_booking_value AS average_booking_value
              , subq_9.referred_bookings AS referred_bookings
              , subq_9.median_booking_value AS median_booking_value
              , subq_9.booking_value_p99 AS booking_value_p99
              , subq_9.discrete_booking_value_p99 AS discrete_booking_value_p99
              , subq_9.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
              , subq_9.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
              , subq_16.martian_day AS metric_time__martian_day
            FROM (
              -- Pass Only Elements: ['ds__day', 'metric_time__day']
              SELECT
                subq_14.ds__day
                , subq_14.metric_time__day
              FROM (
                -- Apply Requested Granularities
                SELECT
                  subq_13.ds__day
                  , subq_13.ds__day__lead AS metric_time__day
                FROM (
                  -- Offset Base Granularity By Custom Granularity Period(s)
                  WITH cte_4 AS (
                    -- Get Custom Granularity Bounds
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
                      , FIRST_VALUE(subq_10.ds__day) OVER (
                        PARTITION BY subq_10.ds__martian_day
                        ORDER BY subq_10.ds__day
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                      ) AS ds__martian_day__first_value
                      , LAST_VALUE(subq_10.ds__day) OVER (
                        PARTITION BY subq_10.ds__martian_day
                        ORDER BY subq_10.ds__day
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                      ) AS ds__martian_day__last_value
                      , ROW_NUMBER() OVER (
                        PARTITION BY subq_10.ds__martian_day
                        ORDER BY subq_10.ds__day
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
                    ) subq_10
                  )

                  SELECT
                    cte_4.ds__day AS ds__day
                    , CASE
                      WHEN subq_12.ds__martian_day__first_value__lead + INTERVAL (cte_4.ds__day__row_number - 1) day <= subq_12.ds__martian_day__last_value__lead
                        THEN subq_12.ds__martian_day__first_value__lead + INTERVAL (cte_4.ds__day__row_number - 1) day
                      ELSE NULL
                    END AS ds__day__lead
                  FROM cte_4 cte_4
                  INNER JOIN (
                    -- Offset Custom Granularity Bounds
                    SELECT
                      subq_11.ds__martian_day
                      , LEAD(subq_11.ds__martian_day__first_value, 1) OVER (ORDER BY subq_11.ds__martian_day) AS ds__martian_day__first_value__lead
                      , LEAD(subq_11.ds__martian_day__last_value, 1) OVER (ORDER BY subq_11.ds__martian_day) AS ds__martian_day__last_value__lead
                    FROM (
                      -- Get Unique Rows for Custom Granularity Bounds
                      SELECT
                        cte_4.ds__martian_day
                        , cte_4.ds__martian_day__first_value
                        , cte_4.ds__martian_day__last_value
                      FROM cte_4 cte_4
                      GROUP BY
                        cte_4.ds__martian_day
                        , cte_4.ds__martian_day__first_value
                        , cte_4.ds__martian_day__last_value
                    ) subq_11
                  ) subq_12
                  ON
                    cte_4.ds__martian_day = subq_12.ds__martian_day
                ) subq_13
              ) subq_14
            ) subq_15
            INNER JOIN (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_8.ds__day
                , subq_8.ds__week
                , subq_8.ds__month
                , subq_8.ds__quarter
                , subq_8.ds__year
                , subq_8.ds__extract_year
                , subq_8.ds__extract_quarter
                , subq_8.ds__extract_month
                , subq_8.ds__extract_day
                , subq_8.ds__extract_dow
                , subq_8.ds__extract_doy
                , subq_8.ds_partitioned__day
                , subq_8.ds_partitioned__week
                , subq_8.ds_partitioned__month
                , subq_8.ds_partitioned__quarter
                , subq_8.ds_partitioned__year
                , subq_8.ds_partitioned__extract_year
                , subq_8.ds_partitioned__extract_quarter
                , subq_8.ds_partitioned__extract_month
                , subq_8.ds_partitioned__extract_day
                , subq_8.ds_partitioned__extract_dow
                , subq_8.ds_partitioned__extract_doy
                , subq_8.paid_at__day
                , subq_8.paid_at__week
                , subq_8.paid_at__month
                , subq_8.paid_at__quarter
                , subq_8.paid_at__year
                , subq_8.paid_at__extract_year
                , subq_8.paid_at__extract_quarter
                , subq_8.paid_at__extract_month
                , subq_8.paid_at__extract_day
                , subq_8.paid_at__extract_dow
                , subq_8.paid_at__extract_doy
                , subq_8.booking__ds__day
                , subq_8.booking__ds__week
                , subq_8.booking__ds__month
                , subq_8.booking__ds__quarter
                , subq_8.booking__ds__year
                , subq_8.booking__ds__extract_year
                , subq_8.booking__ds__extract_quarter
                , subq_8.booking__ds__extract_month
                , subq_8.booking__ds__extract_day
                , subq_8.booking__ds__extract_dow
                , subq_8.booking__ds__extract_doy
                , subq_8.booking__ds_partitioned__day
                , subq_8.booking__ds_partitioned__week
                , subq_8.booking__ds_partitioned__month
                , subq_8.booking__ds_partitioned__quarter
                , subq_8.booking__ds_partitioned__year
                , subq_8.booking__ds_partitioned__extract_year
                , subq_8.booking__ds_partitioned__extract_quarter
                , subq_8.booking__ds_partitioned__extract_month
                , subq_8.booking__ds_partitioned__extract_day
                , subq_8.booking__ds_partitioned__extract_dow
                , subq_8.booking__ds_partitioned__extract_doy
                , subq_8.booking__paid_at__day
                , subq_8.booking__paid_at__week
                , subq_8.booking__paid_at__month
                , subq_8.booking__paid_at__quarter
                , subq_8.booking__paid_at__year
                , subq_8.booking__paid_at__extract_year
                , subq_8.booking__paid_at__extract_quarter
                , subq_8.booking__paid_at__extract_month
                , subq_8.booking__paid_at__extract_day
                , subq_8.booking__paid_at__extract_dow
                , subq_8.booking__paid_at__extract_doy
                , subq_8.ds__day AS metric_time__day
                , subq_8.ds__week AS metric_time__week
                , subq_8.ds__month AS metric_time__month
                , subq_8.ds__quarter AS metric_time__quarter
                , subq_8.ds__year AS metric_time__year
                , subq_8.ds__extract_year AS metric_time__extract_year
                , subq_8.ds__extract_quarter AS metric_time__extract_quarter
                , subq_8.ds__extract_month AS metric_time__extract_month
                , subq_8.ds__extract_day AS metric_time__extract_day
                , subq_8.ds__extract_dow AS metric_time__extract_dow
                , subq_8.ds__extract_doy AS metric_time__extract_doy
                , subq_8.listing
                , subq_8.guest
                , subq_8.host
                , subq_8.booking__listing
                , subq_8.booking__guest
                , subq_8.booking__host
                , subq_8.is_instant
                , subq_8.booking__is_instant
                , subq_8.bookings
                , subq_8.instant_bookings
                , subq_8.booking_value
                , subq_8.max_booking_value
                , subq_8.min_booking_value
                , subq_8.bookers
                , subq_8.average_booking_value
                , subq_8.referred_bookings
                , subq_8.median_booking_value
                , subq_8.booking_value_p99
                , subq_8.discrete_booking_value_p99
                , subq_8.approximate_continuous_booking_value_p99
                , subq_8.approximate_discrete_booking_value_p99
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
              ) subq_8
            ) subq_9
            ON
              subq_15.ds__day = subq_9.metric_time__day
            LEFT OUTER JOIN
              ***************************.mf_time_spine subq_16
            ON
              subq_15.metric_time__day = subq_16.ds
          ) subq_17
        ) subq_18
        GROUP BY
          subq_18.metric_time__martian_day
          , subq_18.metric_time__day
      ) subq_19
    ) subq_20
  ) subq_21
  ON
    subq_27.metric_time__martian_day = subq_21.metric_time__martian_day
) subq_28
