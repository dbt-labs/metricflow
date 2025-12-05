test_name: test_custom_offset_window_with_only_window_grain
test_filename: test_custom_granularity.py
sql_engine: Redshift
---
-- Write to DataTable
SELECT
  subq_12.metric_time__alien_day
  , subq_12.booking__ds__alien_day
  , subq_12.bookings_offset_one_alien_day
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_11.metric_time__alien_day
    , subq_11.booking__ds__alien_day
    , bookings AS bookings_offset_one_alien_day
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_10.metric_time__alien_day
      , subq_10.booking__ds__alien_day
      , subq_10.__bookings AS bookings
    FROM (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_9.metric_time__alien_day
        , subq_9.booking__ds__alien_day
        , SUM(subq_9.__bookings) AS __bookings
      FROM (
        -- Pass Only Elements: ['__bookings', 'metric_time__alien_day', 'booking__ds__alien_day']
        SELECT
          subq_8.metric_time__alien_day
          , subq_8.booking__ds__alien_day
          , subq_8.__bookings
        FROM (
          -- Pass Only Elements: ['__bookings', 'metric_time__alien_day', 'booking__ds__alien_day']
          SELECT
            subq_7.metric_time__alien_day
            , subq_7.booking__ds__alien_day
            , subq_7.__bookings
          FROM (
            -- Join to Time Spine Dataset
            SELECT
              subq_6.metric_time__day AS metric_time__day
              , subq_6.metric_time__alien_day AS metric_time__alien_day
              , subq_6.booking__ds__alien_day AS booking__ds__alien_day
              , subq_2.ds__day AS ds__day
              , subq_2.ds__week AS ds__week
              , subq_2.ds__month AS ds__month
              , subq_2.ds__quarter AS ds__quarter
              , subq_2.ds__year AS ds__year
              , subq_2.ds__extract_year AS ds__extract_year
              , subq_2.ds__extract_quarter AS ds__extract_quarter
              , subq_2.ds__extract_month AS ds__extract_month
              , subq_2.ds__extract_day AS ds__extract_day
              , subq_2.ds__extract_dow AS ds__extract_dow
              , subq_2.ds__extract_doy AS ds__extract_doy
              , subq_2.ds_partitioned__day AS ds_partitioned__day
              , subq_2.ds_partitioned__week AS ds_partitioned__week
              , subq_2.ds_partitioned__month AS ds_partitioned__month
              , subq_2.ds_partitioned__quarter AS ds_partitioned__quarter
              , subq_2.ds_partitioned__year AS ds_partitioned__year
              , subq_2.ds_partitioned__extract_year AS ds_partitioned__extract_year
              , subq_2.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
              , subq_2.ds_partitioned__extract_month AS ds_partitioned__extract_month
              , subq_2.ds_partitioned__extract_day AS ds_partitioned__extract_day
              , subq_2.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
              , subq_2.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
              , subq_2.paid_at__day AS paid_at__day
              , subq_2.paid_at__week AS paid_at__week
              , subq_2.paid_at__month AS paid_at__month
              , subq_2.paid_at__quarter AS paid_at__quarter
              , subq_2.paid_at__year AS paid_at__year
              , subq_2.paid_at__extract_year AS paid_at__extract_year
              , subq_2.paid_at__extract_quarter AS paid_at__extract_quarter
              , subq_2.paid_at__extract_month AS paid_at__extract_month
              , subq_2.paid_at__extract_day AS paid_at__extract_day
              , subq_2.paid_at__extract_dow AS paid_at__extract_dow
              , subq_2.paid_at__extract_doy AS paid_at__extract_doy
              , subq_2.booking__ds__day AS booking__ds__day
              , subq_2.booking__ds__week AS booking__ds__week
              , subq_2.booking__ds__month AS booking__ds__month
              , subq_2.booking__ds__quarter AS booking__ds__quarter
              , subq_2.booking__ds__year AS booking__ds__year
              , subq_2.booking__ds__extract_year AS booking__ds__extract_year
              , subq_2.booking__ds__extract_quarter AS booking__ds__extract_quarter
              , subq_2.booking__ds__extract_month AS booking__ds__extract_month
              , subq_2.booking__ds__extract_day AS booking__ds__extract_day
              , subq_2.booking__ds__extract_dow AS booking__ds__extract_dow
              , subq_2.booking__ds__extract_doy AS booking__ds__extract_doy
              , subq_2.booking__ds_partitioned__day AS booking__ds_partitioned__day
              , subq_2.booking__ds_partitioned__week AS booking__ds_partitioned__week
              , subq_2.booking__ds_partitioned__month AS booking__ds_partitioned__month
              , subq_2.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
              , subq_2.booking__ds_partitioned__year AS booking__ds_partitioned__year
              , subq_2.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
              , subq_2.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
              , subq_2.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
              , subq_2.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
              , subq_2.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
              , subq_2.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
              , subq_2.booking__paid_at__day AS booking__paid_at__day
              , subq_2.booking__paid_at__week AS booking__paid_at__week
              , subq_2.booking__paid_at__month AS booking__paid_at__month
              , subq_2.booking__paid_at__quarter AS booking__paid_at__quarter
              , subq_2.booking__paid_at__year AS booking__paid_at__year
              , subq_2.booking__paid_at__extract_year AS booking__paid_at__extract_year
              , subq_2.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
              , subq_2.booking__paid_at__extract_month AS booking__paid_at__extract_month
              , subq_2.booking__paid_at__extract_day AS booking__paid_at__extract_day
              , subq_2.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
              , subq_2.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
              , subq_2.metric_time__week AS metric_time__week
              , subq_2.metric_time__month AS metric_time__month
              , subq_2.metric_time__quarter AS metric_time__quarter
              , subq_2.metric_time__year AS metric_time__year
              , subq_2.metric_time__extract_year AS metric_time__extract_year
              , subq_2.metric_time__extract_quarter AS metric_time__extract_quarter
              , subq_2.metric_time__extract_month AS metric_time__extract_month
              , subq_2.metric_time__extract_day AS metric_time__extract_day
              , subq_2.metric_time__extract_dow AS metric_time__extract_dow
              , subq_2.metric_time__extract_doy AS metric_time__extract_doy
              , subq_2.listing AS listing
              , subq_2.guest AS guest
              , subq_2.host AS host
              , subq_2.booking__listing AS booking__listing
              , subq_2.booking__guest AS booking__guest
              , subq_2.booking__host AS booking__host
              , subq_2.is_instant AS is_instant
              , subq_2.booking__is_instant AS booking__is_instant
              , subq_2.__bookings AS __bookings
              , subq_2.__average_booking_value AS __average_booking_value
              , subq_2.__instant_bookings AS __instant_bookings
              , subq_2.__booking_value AS __booking_value
              , subq_2.__max_booking_value AS __max_booking_value
              , subq_2.__min_booking_value AS __min_booking_value
              , subq_2.__instant_booking_value AS __instant_booking_value
              , subq_2.__average_instant_booking_value AS __average_instant_booking_value
              , subq_2.__booking_value_for_non_null_listing_id AS __booking_value_for_non_null_listing_id
              , subq_2.__bookers AS __bookers
              , subq_2.__referred_bookings AS __referred_bookings
              , subq_2.__median_booking_value AS __median_booking_value
              , subq_2.__booking_value_p99 AS __booking_value_p99
              , subq_2.__discrete_booking_value_p99 AS __discrete_booking_value_p99
              , subq_2.__approximate_continuous_booking_value_p99 AS __approximate_continuous_booking_value_p99
              , subq_2.__approximate_discrete_booking_value_p99 AS __approximate_discrete_booking_value_p99
              , subq_2.__bookings_join_to_time_spine AS __bookings_join_to_time_spine
              , subq_2.__bookings_fill_nulls_with_0_without_time_spine AS __bookings_fill_nulls_with_0_without_time_spine
              , subq_2.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
              , subq_2.__instant_bookings_with_measure_filter AS __instant_bookings_with_measure_filter
              , subq_2.__bookings_join_to_time_spine_with_tiered_filters AS __bookings_join_to_time_spine_with_tiered_filters
              , subq_2.__bookers_fill_nulls_with_0_join_to_timespine AS __bookers_fill_nulls_with_0_join_to_timespine
            FROM (
              -- Pass Only Elements: ['ds__day', 'metric_time__day', 'metric_time__alien_day', 'booking__ds__alien_day']
              SELECT
                subq_5.ds__day
                , subq_5.metric_time__day
                , subq_5.metric_time__alien_day
                , subq_5.booking__ds__alien_day
              FROM (
                -- Pass Only Elements: ['ds__day', 'metric_time__day', 'metric_time__alien_day', 'booking__ds__alien_day']
                SELECT
                  subq_4.ds__day
                  , subq_4.metric_time__day
                  , subq_4.metric_time__alien_day
                  , subq_4.booking__ds__alien_day
                FROM (
                  -- Join Offset Custom Granularity to Base Granularity
                  WITH cte_2 AS (
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
                      , CASE WHEN EXTRACT(dow FROM time_spine_src_28006.ds) = 0 THEN EXTRACT(dow FROM time_spine_src_28006.ds) + 7 ELSE EXTRACT(dow FROM time_spine_src_28006.ds) END AS ds__extract_dow
                      , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
                      , time_spine_src_28006.alien_day AS ds__alien_day
                    FROM ***************************.mf_time_spine time_spine_src_28006
                  )

                  SELECT
                    cte_2.ds__day AS ds__day
                    , subq_3.ds__alien_day__lead AS metric_time__day
                    , subq_3.ds__alien_day__lead AS metric_time__alien_day
                    , subq_3.ds__alien_day__lead AS booking__ds__alien_day
                  FROM cte_2
                  INNER JOIN (
                    -- Offset Custom Granularity
                    SELECT
                      cte_2.ds__alien_day
                      , LEAD(cte_2.ds__alien_day, 1) OVER (ORDER BY cte_2.ds__alien_day) AS ds__alien_day__lead
                    FROM cte_2
                    GROUP BY
                      cte_2.ds__alien_day
                  ) subq_3
                  ON
                    cte_2.ds__alien_day = subq_3.ds__alien_day
                ) subq_4
              ) subq_5
            ) subq_6
            INNER JOIN (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_1.ds__day
                , subq_1.ds__week
                , subq_1.ds__month
                , subq_1.ds__quarter
                , subq_1.ds__year
                , subq_1.ds__extract_year
                , subq_1.ds__extract_quarter
                , subq_1.ds__extract_month
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
                , subq_1.booking__paid_at__extract_day
                , subq_1.booking__paid_at__extract_dow
                , subq_1.booking__paid_at__extract_doy
                , subq_1.ds__day AS metric_time__day
                , subq_1.ds__week AS metric_time__week
                , subq_1.ds__month AS metric_time__month
                , subq_1.ds__quarter AS metric_time__quarter
                , subq_1.ds__year AS metric_time__year
                , subq_1.ds__extract_year AS metric_time__extract_year
                , subq_1.ds__extract_quarter AS metric_time__extract_quarter
                , subq_1.ds__extract_month AS metric_time__extract_month
                , subq_1.ds__extract_day AS metric_time__extract_day
                , subq_1.ds__extract_dow AS metric_time__extract_dow
                , subq_1.ds__extract_doy AS metric_time__extract_doy
                , subq_1.listing
                , subq_1.guest
                , subq_1.host
                , subq_1.booking__listing
                , subq_1.booking__guest
                , subq_1.booking__host
                , subq_1.is_instant
                , subq_1.booking__is_instant
                , subq_1.__bookings
                , subq_1.__average_booking_value
                , subq_1.__instant_bookings
                , subq_1.__booking_value
                , subq_1.__max_booking_value
                , subq_1.__min_booking_value
                , subq_1.__instant_booking_value
                , subq_1.__average_instant_booking_value
                , subq_1.__booking_value_for_non_null_listing_id
                , subq_1.__bookers
                , subq_1.__referred_bookings
                , subq_1.__median_booking_value
                , subq_1.__booking_value_p99
                , subq_1.__discrete_booking_value_p99
                , subq_1.__approximate_continuous_booking_value_p99
                , subq_1.__approximate_discrete_booking_value_p99
                , subq_1.__bookings_join_to_time_spine
                , subq_1.__bookings_fill_nulls_with_0_without_time_spine
                , subq_1.__bookings_fill_nulls_with_0
                , subq_1.__instant_bookings_with_measure_filter
                , subq_1.__bookings_join_to_time_spine_with_tiered_filters
                , subq_1.__bookers_fill_nulls_with_0_join_to_timespine
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
                  , DATE_TRUNC('day', bookings_source_src_28000.ds) AS ds__day
                  , DATE_TRUNC('week', bookings_source_src_28000.ds) AS ds__week
                  , DATE_TRUNC('month', bookings_source_src_28000.ds) AS ds__month
                  , DATE_TRUNC('quarter', bookings_source_src_28000.ds) AS ds__quarter
                  , DATE_TRUNC('year', bookings_source_src_28000.ds) AS ds__year
                  , EXTRACT(year FROM bookings_source_src_28000.ds) AS ds__extract_year
                  , EXTRACT(quarter FROM bookings_source_src_28000.ds) AS ds__extract_quarter
                  , EXTRACT(month FROM bookings_source_src_28000.ds) AS ds__extract_month
                  , EXTRACT(day FROM bookings_source_src_28000.ds) AS ds__extract_day
                  , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds) END AS ds__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) END AS ds_partitioned__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.paid_at) END AS paid_at__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds) END AS booking__ds__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.ds_partitioned) END AS booking__ds_partitioned__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) = 0 THEN EXTRACT(dow FROM bookings_source_src_28000.paid_at) + 7 ELSE EXTRACT(dow FROM bookings_source_src_28000.paid_at) END AS booking__paid_at__extract_dow
                  , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
                  , bookings_source_src_28000.listing_id AS listing
                  , bookings_source_src_28000.guest_id AS guest
                  , bookings_source_src_28000.host_id AS host
                  , bookings_source_src_28000.listing_id AS booking__listing
                  , bookings_source_src_28000.guest_id AS booking__guest
                  , bookings_source_src_28000.host_id AS booking__host
                FROM ***************************.fct_bookings bookings_source_src_28000
              ) subq_1
            ) subq_2
            ON
              subq_6.ds__day = subq_2.metric_time__day
          ) subq_7
        ) subq_8
      ) subq_9
      GROUP BY
        subq_9.metric_time__alien_day
        , subq_9.booking__ds__alien_day
    ) subq_10
  ) subq_11
) subq_12
