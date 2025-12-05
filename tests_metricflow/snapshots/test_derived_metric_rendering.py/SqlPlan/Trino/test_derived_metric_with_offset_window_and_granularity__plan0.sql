test_name: test_derived_metric_with_offset_window_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: Trino
---
-- Write to DataTable
SELECT
  subq_17.metric_time__quarter
  , subq_17.bookings_growth_2_weeks
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_16.metric_time__quarter
    , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_5.metric_time__quarter, subq_15.metric_time__quarter) AS metric_time__quarter
      , MAX(subq_5.bookings) AS bookings
      , MAX(subq_15.bookings_2_weeks_ago) AS bookings_2_weeks_ago
    FROM (
      -- Compute Metrics via Expressions
      SELECT
        subq_4.metric_time__quarter
        , subq_4.__bookings AS bookings
      FROM (
        -- Aggregate Inputs for Simple Metrics
        SELECT
          subq_3.metric_time__quarter
          , SUM(subq_3.__bookings) AS __bookings
        FROM (
          -- Pass Only Elements: ['__bookings', 'metric_time__quarter']
          SELECT
            subq_2.metric_time__quarter
            , subq_2.__bookings
          FROM (
            -- Pass Only Elements: ['__bookings', 'metric_time__quarter']
            SELECT
              subq_1.metric_time__quarter
              , subq_1.__bookings
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
                , subq_0.__bookings
                , subq_0.__average_booking_value
                , subq_0.__instant_bookings
                , subq_0.__booking_value
                , subq_0.__max_booking_value
                , subq_0.__min_booking_value
                , subq_0.__instant_booking_value
                , subq_0.__average_instant_booking_value
                , subq_0.__booking_value_for_non_null_listing_id
                , subq_0.__bookers
                , subq_0.__referred_bookings
                , subq_0.__median_booking_value
                , subq_0.__booking_value_p99
                , subq_0.__discrete_booking_value_p99
                , subq_0.__approximate_continuous_booking_value_p99
                , subq_0.__approximate_discrete_booking_value_p99
                , subq_0.__bookings_join_to_time_spine
                , subq_0.__bookings_fill_nulls_with_0_without_time_spine
                , subq_0.__bookings_fill_nulls_with_0
                , subq_0.__instant_bookings_with_measure_filter
                , subq_0.__bookings_join_to_time_spine_with_tiered_filters
                , subq_0.__bookers_fill_nulls_with_0_join_to_timespine
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
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
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
          ) subq_2
        ) subq_3
        GROUP BY
          subq_3.metric_time__quarter
      ) subq_4
    ) subq_5
    FULL OUTER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        subq_14.metric_time__quarter
        , subq_14.__bookings AS bookings_2_weeks_ago
      FROM (
        -- Aggregate Inputs for Simple Metrics
        SELECT
          subq_13.metric_time__quarter
          , SUM(subq_13.__bookings) AS __bookings
        FROM (
          -- Pass Only Elements: ['__bookings', 'metric_time__quarter']
          SELECT
            subq_12.metric_time__quarter
            , subq_12.__bookings
          FROM (
            -- Pass Only Elements: ['__bookings', 'metric_time__quarter']
            SELECT
              subq_11.metric_time__quarter
              , subq_11.__bookings
            FROM (
              -- Join to Time Spine Dataset
              SELECT
                subq_10.metric_time__day AS metric_time__day
                , subq_10.metric_time__quarter AS metric_time__quarter
                , subq_6.ds__day AS ds__day
                , subq_6.ds__week AS ds__week
                , subq_6.ds__month AS ds__month
                , subq_6.ds__quarter AS ds__quarter
                , subq_6.ds__year AS ds__year
                , subq_6.ds__extract_year AS ds__extract_year
                , subq_6.ds__extract_quarter AS ds__extract_quarter
                , subq_6.ds__extract_month AS ds__extract_month
                , subq_6.ds__extract_day AS ds__extract_day
                , subq_6.ds__extract_dow AS ds__extract_dow
                , subq_6.ds__extract_doy AS ds__extract_doy
                , subq_6.ds_partitioned__day AS ds_partitioned__day
                , subq_6.ds_partitioned__week AS ds_partitioned__week
                , subq_6.ds_partitioned__month AS ds_partitioned__month
                , subq_6.ds_partitioned__quarter AS ds_partitioned__quarter
                , subq_6.ds_partitioned__year AS ds_partitioned__year
                , subq_6.ds_partitioned__extract_year AS ds_partitioned__extract_year
                , subq_6.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                , subq_6.ds_partitioned__extract_month AS ds_partitioned__extract_month
                , subq_6.ds_partitioned__extract_day AS ds_partitioned__extract_day
                , subq_6.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                , subq_6.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                , subq_6.paid_at__day AS paid_at__day
                , subq_6.paid_at__week AS paid_at__week
                , subq_6.paid_at__month AS paid_at__month
                , subq_6.paid_at__quarter AS paid_at__quarter
                , subq_6.paid_at__year AS paid_at__year
                , subq_6.paid_at__extract_year AS paid_at__extract_year
                , subq_6.paid_at__extract_quarter AS paid_at__extract_quarter
                , subq_6.paid_at__extract_month AS paid_at__extract_month
                , subq_6.paid_at__extract_day AS paid_at__extract_day
                , subq_6.paid_at__extract_dow AS paid_at__extract_dow
                , subq_6.paid_at__extract_doy AS paid_at__extract_doy
                , subq_6.booking__ds__day AS booking__ds__day
                , subq_6.booking__ds__week AS booking__ds__week
                , subq_6.booking__ds__month AS booking__ds__month
                , subq_6.booking__ds__quarter AS booking__ds__quarter
                , subq_6.booking__ds__year AS booking__ds__year
                , subq_6.booking__ds__extract_year AS booking__ds__extract_year
                , subq_6.booking__ds__extract_quarter AS booking__ds__extract_quarter
                , subq_6.booking__ds__extract_month AS booking__ds__extract_month
                , subq_6.booking__ds__extract_day AS booking__ds__extract_day
                , subq_6.booking__ds__extract_dow AS booking__ds__extract_dow
                , subq_6.booking__ds__extract_doy AS booking__ds__extract_doy
                , subq_6.booking__ds_partitioned__day AS booking__ds_partitioned__day
                , subq_6.booking__ds_partitioned__week AS booking__ds_partitioned__week
                , subq_6.booking__ds_partitioned__month AS booking__ds_partitioned__month
                , subq_6.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                , subq_6.booking__ds_partitioned__year AS booking__ds_partitioned__year
                , subq_6.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                , subq_6.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                , subq_6.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                , subq_6.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                , subq_6.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                , subq_6.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                , subq_6.booking__paid_at__day AS booking__paid_at__day
                , subq_6.booking__paid_at__week AS booking__paid_at__week
                , subq_6.booking__paid_at__month AS booking__paid_at__month
                , subq_6.booking__paid_at__quarter AS booking__paid_at__quarter
                , subq_6.booking__paid_at__year AS booking__paid_at__year
                , subq_6.booking__paid_at__extract_year AS booking__paid_at__extract_year
                , subq_6.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                , subq_6.booking__paid_at__extract_month AS booking__paid_at__extract_month
                , subq_6.booking__paid_at__extract_day AS booking__paid_at__extract_day
                , subq_6.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                , subq_6.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
                , subq_6.metric_time__week AS metric_time__week
                , subq_6.metric_time__month AS metric_time__month
                , subq_6.metric_time__year AS metric_time__year
                , subq_6.metric_time__extract_year AS metric_time__extract_year
                , subq_6.metric_time__extract_quarter AS metric_time__extract_quarter
                , subq_6.metric_time__extract_month AS metric_time__extract_month
                , subq_6.metric_time__extract_day AS metric_time__extract_day
                , subq_6.metric_time__extract_dow AS metric_time__extract_dow
                , subq_6.metric_time__extract_doy AS metric_time__extract_doy
                , subq_6.listing AS listing
                , subq_6.guest AS guest
                , subq_6.host AS host
                , subq_6.booking__listing AS booking__listing
                , subq_6.booking__guest AS booking__guest
                , subq_6.booking__host AS booking__host
                , subq_6.is_instant AS is_instant
                , subq_6.booking__is_instant AS booking__is_instant
                , subq_6.__bookings AS __bookings
                , subq_6.__average_booking_value AS __average_booking_value
                , subq_6.__instant_bookings AS __instant_bookings
                , subq_6.__booking_value AS __booking_value
                , subq_6.__max_booking_value AS __max_booking_value
                , subq_6.__min_booking_value AS __min_booking_value
                , subq_6.__instant_booking_value AS __instant_booking_value
                , subq_6.__average_instant_booking_value AS __average_instant_booking_value
                , subq_6.__booking_value_for_non_null_listing_id AS __booking_value_for_non_null_listing_id
                , subq_6.__bookers AS __bookers
                , subq_6.__referred_bookings AS __referred_bookings
                , subq_6.__median_booking_value AS __median_booking_value
                , subq_6.__booking_value_p99 AS __booking_value_p99
                , subq_6.__discrete_booking_value_p99 AS __discrete_booking_value_p99
                , subq_6.__approximate_continuous_booking_value_p99 AS __approximate_continuous_booking_value_p99
                , subq_6.__approximate_discrete_booking_value_p99 AS __approximate_discrete_booking_value_p99
                , subq_6.__bookings_join_to_time_spine AS __bookings_join_to_time_spine
                , subq_6.__bookings_fill_nulls_with_0_without_time_spine AS __bookings_fill_nulls_with_0_without_time_spine
                , subq_6.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
                , subq_6.__instant_bookings_with_measure_filter AS __instant_bookings_with_measure_filter
                , subq_6.__bookings_join_to_time_spine_with_tiered_filters AS __bookings_join_to_time_spine_with_tiered_filters
                , subq_6.__bookers_fill_nulls_with_0_join_to_timespine AS __bookers_fill_nulls_with_0_join_to_timespine
              FROM (
                -- Pass Only Elements: ['metric_time__day', 'metric_time__quarter']
                SELECT
                  subq_9.metric_time__day
                  , subq_9.metric_time__quarter
                FROM (
                  -- Pass Only Elements: ['metric_time__day', 'metric_time__quarter']
                  SELECT
                    subq_8.metric_time__day
                    , subq_8.metric_time__quarter
                  FROM (
                    -- Change Column Aliases
                    SELECT
                      subq_7.ds__day AS metric_time__day
                      , subq_7.ds__week
                      , subq_7.ds__month
                      , subq_7.ds__quarter AS metric_time__quarter
                      , subq_7.ds__year
                      , subq_7.ds__extract_year
                      , subq_7.ds__extract_quarter
                      , subq_7.ds__extract_month
                      , subq_7.ds__extract_day
                      , subq_7.ds__extract_dow
                      , subq_7.ds__extract_doy
                      , subq_7.ds__alien_day
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
                        , EXTRACT(DAY_OF_WEEK FROM time_spine_src_28006.ds) AS ds__extract_dow
                        , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
                        , time_spine_src_28006.alien_day AS ds__alien_day
                      FROM ***************************.mf_time_spine time_spine_src_28006
                    ) subq_7
                  ) subq_8
                ) subq_9
              ) subq_10
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
                  , subq_0.__bookings
                  , subq_0.__average_booking_value
                  , subq_0.__instant_bookings
                  , subq_0.__booking_value
                  , subq_0.__max_booking_value
                  , subq_0.__min_booking_value
                  , subq_0.__instant_booking_value
                  , subq_0.__average_instant_booking_value
                  , subq_0.__booking_value_for_non_null_listing_id
                  , subq_0.__bookers
                  , subq_0.__referred_bookings
                  , subq_0.__median_booking_value
                  , subq_0.__booking_value_p99
                  , subq_0.__discrete_booking_value_p99
                  , subq_0.__approximate_continuous_booking_value_p99
                  , subq_0.__approximate_discrete_booking_value_p99
                  , subq_0.__bookings_join_to_time_spine
                  , subq_0.__bookings_fill_nulls_with_0_without_time_spine
                  , subq_0.__bookings_fill_nulls_with_0
                  , subq_0.__instant_bookings_with_measure_filter
                  , subq_0.__bookings_join_to_time_spine_with_tiered_filters
                  , subq_0.__bookers_fill_nulls_with_0_join_to_timespine
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
                    , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
                    , bookings_source_src_28000.listing_id AS listing
                    , bookings_source_src_28000.guest_id AS guest
                    , bookings_source_src_28000.host_id AS host
                    , bookings_source_src_28000.listing_id AS booking__listing
                    , bookings_source_src_28000.guest_id AS booking__guest
                    , bookings_source_src_28000.host_id AS booking__host
                  FROM ***************************.fct_bookings bookings_source_src_28000
                ) subq_0
              ) subq_6
              ON
                DATE_ADD('day', -14, subq_10.metric_time__day) = subq_6.metric_time__day
            ) subq_11
          ) subq_12
        ) subq_13
        GROUP BY
          subq_13.metric_time__quarter
      ) subq_14
    ) subq_15
    ON
      subq_5.metric_time__quarter = subq_15.metric_time__quarter
    GROUP BY
      COALESCE(subq_5.metric_time__quarter, subq_15.metric_time__quarter)
  ) subq_16
) subq_17
