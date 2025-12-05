test_name: test_time_offset_metric_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Write to DataTable
SELECT
  subq_14.metric_time__day
  , subq_14.bookings_5_day_lag
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_13.metric_time__day
    , bookings_5_days_ago AS bookings_5_day_lag
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_12.metric_time__day
      , subq_12.__bookings AS bookings_5_days_ago
    FROM (
      -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
      SELECT
        subq_11.metric_time__day
        , subq_11.__bookings
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          subq_10.metric_time__day AS metric_time__day
          , subq_5.__bookings AS __bookings
        FROM (
          -- Pass Only Elements: ['metric_time__day']
          SELECT
            subq_9.metric_time__day
          FROM (
            -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
            SELECT
              subq_8.metric_time__day
            FROM (
              -- Pass Only Elements: ['metric_time__day']
              SELECT
                subq_7.metric_time__day
              FROM (
                -- Change Column Aliases
                SELECT
                  subq_6.ds__day AS metric_time__day
                  , subq_6.ds__week
                  , subq_6.ds__month
                  , subq_6.ds__quarter
                  , subq_6.ds__year
                  , subq_6.ds__extract_year
                  , subq_6.ds__extract_quarter
                  , subq_6.ds__extract_month
                  , subq_6.ds__extract_day
                  , subq_6.ds__extract_dow
                  , subq_6.ds__extract_doy
                  , subq_6.ds__alien_day
                FROM (
                  -- Read From Time Spine 'mf_time_spine'
                  SELECT
                    time_spine_src_28006.ds AS ds__day
                    , DATETIME_TRUNC(time_spine_src_28006.ds, isoweek) AS ds__week
                    , DATETIME_TRUNC(time_spine_src_28006.ds, month) AS ds__month
                    , DATETIME_TRUNC(time_spine_src_28006.ds, quarter) AS ds__quarter
                    , DATETIME_TRUNC(time_spine_src_28006.ds, year) AS ds__year
                    , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
                    , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
                    , IF(EXTRACT(dayofweek FROM time_spine_src_28006.ds) = 1, 7, EXTRACT(dayofweek FROM time_spine_src_28006.ds) - 1) AS ds__extract_dow
                    , EXTRACT(dayofyear FROM time_spine_src_28006.ds) AS ds__extract_doy
                    , time_spine_src_28006.alien_day AS ds__alien_day
                  FROM ***************************.mf_time_spine time_spine_src_28006
                ) subq_6
              ) subq_7
            ) subq_8
            WHERE subq_8.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
          ) subq_9
        ) subq_10
        INNER JOIN (
          -- Aggregate Inputs for Simple Metrics
          SELECT
            subq_4.metric_time__day
            , SUM(subq_4.__bookings) AS __bookings
          FROM (
            -- Pass Only Elements: ['__bookings', 'metric_time__day']
            SELECT
              subq_3.metric_time__day
              , subq_3.__bookings
            FROM (
              -- Pass Only Elements: ['__bookings', 'metric_time__day']
              SELECT
                subq_2.metric_time__day
                , subq_2.__bookings
              FROM (
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
                ) subq_1
              ) subq_2
            ) subq_3
          ) subq_4
          GROUP BY
            metric_time__day
        ) subq_5
        ON
          DATE_SUB(CAST(subq_10.metric_time__day AS DATETIME), INTERVAL 5 day) = subq_5.metric_time__day
      ) subq_11
      WHERE subq_11.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
    ) subq_12
  ) subq_13
) subq_14
