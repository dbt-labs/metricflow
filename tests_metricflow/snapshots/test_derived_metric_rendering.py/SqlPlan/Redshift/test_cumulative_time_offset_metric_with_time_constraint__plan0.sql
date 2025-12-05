test_name: test_cumulative_time_offset_metric_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: Redshift
---
-- Write to DataTable
SELECT
  subq_22.metric_time__day
  , subq_22.every_2_days_bookers_2_days_ago
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_21.metric_time__day
    , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_20.metric_time__day
      , subq_20.bookers AS every_2_days_bookers_2_days_ago
    FROM (
      -- Compute Metrics via Expressions
      SELECT
        subq_19.metric_time__day
        , subq_19.__bookers AS bookers
      FROM (
        -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
        SELECT
          subq_18.metric_time__day
          , subq_18.__bookers
        FROM (
          -- Join to Time Spine Dataset
          SELECT
            subq_17.metric_time__day AS metric_time__day
            , subq_12.__bookers AS __bookers
          FROM (
            -- Pass Only Elements: ['metric_time__day']
            SELECT
              subq_16.metric_time__day
            FROM (
              -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
              SELECT
                subq_15.metric_time__day
              FROM (
                -- Pass Only Elements: ['metric_time__day']
                SELECT
                  subq_14.metric_time__day
                FROM (
                  -- Change Column Aliases
                  SELECT
                    subq_13.ds__day AS metric_time__day
                    , subq_13.ds__week
                    , subq_13.ds__month
                    , subq_13.ds__quarter
                    , subq_13.ds__year
                    , subq_13.ds__extract_year
                    , subq_13.ds__extract_quarter
                    , subq_13.ds__extract_month
                    , subq_13.ds__extract_day
                    , subq_13.ds__extract_dow
                    , subq_13.ds__extract_doy
                    , subq_13.ds__alien_day
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
                      , CASE WHEN EXTRACT(dow FROM time_spine_src_28006.ds) = 0 THEN EXTRACT(dow FROM time_spine_src_28006.ds) + 7 ELSE EXTRACT(dow FROM time_spine_src_28006.ds) END AS ds__extract_dow
                      , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
                      , time_spine_src_28006.alien_day AS ds__alien_day
                    FROM ***************************.mf_time_spine time_spine_src_28006
                  ) subq_13
                ) subq_14
              ) subq_15
              WHERE subq_15.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
            ) subq_16
          ) subq_17
          INNER JOIN (
            -- Aggregate Inputs for Simple Metrics
            SELECT
              subq_11.metric_time__day
              , COUNT(DISTINCT subq_11.__bookers) AS __bookers
            FROM (
              -- Pass Only Elements: ['__bookers', 'metric_time__day']
              SELECT
                subq_10.metric_time__day
                , subq_10.__bookers
              FROM (
                -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
                SELECT
                  subq_9.metric_time__day
                  , subq_9.__bookers
                FROM (
                  -- Pass Only Elements: ['__bookers', 'metric_time__day']
                  SELECT
                    subq_8.metric_time__day
                    , subq_8.__bookers
                  FROM (
                    -- Join Self Over Time Range
                    SELECT
                      subq_6.metric_time__day AS metric_time__day
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
                      , subq_5.__bookings AS __bookings
                      , subq_5.__average_booking_value AS __average_booking_value
                      , subq_5.__instant_bookings AS __instant_bookings
                      , subq_5.__booking_value AS __booking_value
                      , subq_5.__max_booking_value AS __max_booking_value
                      , subq_5.__min_booking_value AS __min_booking_value
                      , subq_5.__instant_booking_value AS __instant_booking_value
                      , subq_5.__average_instant_booking_value AS __average_instant_booking_value
                      , subq_5.__booking_value_for_non_null_listing_id AS __booking_value_for_non_null_listing_id
                      , subq_5.__bookers AS __bookers
                      , subq_5.__referred_bookings AS __referred_bookings
                      , subq_5.__median_booking_value AS __median_booking_value
                      , subq_5.__booking_value_p99 AS __booking_value_p99
                      , subq_5.__discrete_booking_value_p99 AS __discrete_booking_value_p99
                      , subq_5.__approximate_continuous_booking_value_p99 AS __approximate_continuous_booking_value_p99
                      , subq_5.__approximate_discrete_booking_value_p99 AS __approximate_discrete_booking_value_p99
                      , subq_5.__bookings_join_to_time_spine AS __bookings_join_to_time_spine
                      , subq_5.__bookings_fill_nulls_with_0_without_time_spine AS __bookings_fill_nulls_with_0_without_time_spine
                      , subq_5.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
                      , subq_5.__instant_bookings_with_measure_filter AS __instant_bookings_with_measure_filter
                      , subq_5.__bookings_join_to_time_spine_with_tiered_filters AS __bookings_join_to_time_spine_with_tiered_filters
                      , subq_5.__bookers_fill_nulls_with_0_join_to_timespine AS __bookers_fill_nulls_with_0_join_to_timespine
                    FROM (
                      -- Read From Time Spine 'mf_time_spine'
                      SELECT
                        subq_7.ds AS metric_time__day
                      FROM ***************************.mf_time_spine subq_7
                    ) subq_6
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
                        , subq_4.__bookings
                        , subq_4.__average_booking_value
                        , subq_4.__instant_bookings
                        , subq_4.__booking_value
                        , subq_4.__max_booking_value
                        , subq_4.__min_booking_value
                        , subq_4.__instant_booking_value
                        , subq_4.__average_instant_booking_value
                        , subq_4.__booking_value_for_non_null_listing_id
                        , subq_4.__bookers
                        , subq_4.__referred_bookings
                        , subq_4.__median_booking_value
                        , subq_4.__booking_value_p99
                        , subq_4.__discrete_booking_value_p99
                        , subq_4.__approximate_continuous_booking_value_p99
                        , subq_4.__approximate_discrete_booking_value_p99
                        , subq_4.__bookings_join_to_time_spine
                        , subq_4.__bookings_fill_nulls_with_0_without_time_spine
                        , subq_4.__bookings_fill_nulls_with_0
                        , subq_4.__instant_bookings_with_measure_filter
                        , subq_4.__bookings_join_to_time_spine_with_tiered_filters
                        , subq_4.__bookers_fill_nulls_with_0_join_to_timespine
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
                      ) subq_4
                    ) subq_5
                    ON
                      (
                        subq_5.metric_time__day <= subq_6.metric_time__day
                      ) AND (
                        subq_5.metric_time__day > DATEADD(day, -2, subq_6.metric_time__day)
                      )
                  ) subq_8
                ) subq_9
                WHERE subq_9.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
              ) subq_10
            ) subq_11
            GROUP BY
              subq_11.metric_time__day
          ) subq_12
          ON
            DATEADD(day, -2, subq_17.metric_time__day) = subq_12.metric_time__day
        ) subq_18
        WHERE subq_18.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
      ) subq_19
    ) subq_20
  ) subq_21
) subq_22
