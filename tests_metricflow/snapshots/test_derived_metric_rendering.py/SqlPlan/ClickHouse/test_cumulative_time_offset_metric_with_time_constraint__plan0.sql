test_name: test_cumulative_time_offset_metric_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
SELECT
  subq_22.metric_time__day
  , subq_22.every_2_days_bookers_2_days_ago
FROM (
  SELECT
    subq_21.metric_time__day
    , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
  FROM (
    SELECT
      subq_20.metric_time__day
      , subq_20.bookers AS every_2_days_bookers_2_days_ago
    FROM (
      SELECT
        subq_19.metric_time__day
        , subq_19.__bookers AS bookers
      FROM (
        SELECT
          subq_18.metric_time__day
          , subq_18.__bookers
        FROM (
          SELECT
            subq_17.metric_time__day AS metric_time__day
            , subq_12.__bookers AS __bookers
          FROM (
            SELECT
              subq_16.metric_time__day
            FROM (
              SELECT
                subq_15.metric_time__day
              FROM (
                SELECT
                  subq_14.metric_time__day
                FROM (
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
                    SELECT
                      time_spine_src_28006.ds AS ds__day
                      , toStartOfWeek(time_spine_src_28006.ds, 1) AS ds__week
                      , toStartOfMonth(time_spine_src_28006.ds) AS ds__month
                      , toStartOfQuarter(time_spine_src_28006.ds) AS ds__quarter
                      , toStartOfYear(time_spine_src_28006.ds) AS ds__year
                      , toYear(time_spine_src_28006.ds) AS ds__extract_year
                      , toQuarter(time_spine_src_28006.ds) AS ds__extract_quarter
                      , toMonth(time_spine_src_28006.ds) AS ds__extract_month
                      , toDayOfMonth(time_spine_src_28006.ds) AS ds__extract_day
                      , toDayOfWeek(time_spine_src_28006.ds) AS ds__extract_dow
                      , toDayOfYear(time_spine_src_28006.ds) AS ds__extract_doy
                      , time_spine_src_28006.alien_day AS ds__alien_day
                    FROM ***************************.mf_time_spine time_spine_src_28006
                  ) subq_13
                ) subq_14
              ) subq_15
              WHERE subq_15.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
            ) subq_16
          ) subq_17
          INNER JOIN (
            SELECT
              subq_11.metric_time__day
              , COUNT(DISTINCT subq_11.__bookers) AS __bookers
            FROM (
              SELECT
                subq_10.metric_time__day
                , subq_10.__bookers
              FROM (
                SELECT
                  subq_9.metric_time__day
                  , subq_9.__bookers
                FROM (
                  SELECT
                    subq_8.metric_time__day
                    , subq_8.__bookers
                  FROM (
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
                      SELECT
                        subq_7.ds AS metric_time__day
                      FROM ***************************.mf_time_spine subq_7
                    ) subq_6
                    INNER JOIN (
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
                          , toStartOfDay(bookings_source_src_28000.ds) AS ds__day
                          , toStartOfWeek(bookings_source_src_28000.ds, 1) AS ds__week
                          , toStartOfMonth(bookings_source_src_28000.ds) AS ds__month
                          , toStartOfQuarter(bookings_source_src_28000.ds) AS ds__quarter
                          , toStartOfYear(bookings_source_src_28000.ds) AS ds__year
                          , toYear(bookings_source_src_28000.ds) AS ds__extract_year
                          , toQuarter(bookings_source_src_28000.ds) AS ds__extract_quarter
                          , toMonth(bookings_source_src_28000.ds) AS ds__extract_month
                          , toDayOfMonth(bookings_source_src_28000.ds) AS ds__extract_day
                          , toDayOfWeek(bookings_source_src_28000.ds) AS ds__extract_dow
                          , toDayOfYear(bookings_source_src_28000.ds) AS ds__extract_doy
                          , toStartOfDay(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__day
                          , toStartOfWeek(bookings_source_src_28000.ds_partitioned, 1) AS ds_partitioned__week
                          , toStartOfMonth(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__month
                          , toStartOfQuarter(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                          , toStartOfYear(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__year
                          , toYear(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                          , toQuarter(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                          , toMonth(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                          , toDayOfMonth(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                          , toDayOfWeek(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
                          , toDayOfYear(bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                          , toStartOfDay(bookings_source_src_28000.paid_at) AS paid_at__day
                          , toStartOfWeek(bookings_source_src_28000.paid_at, 1) AS paid_at__week
                          , toStartOfMonth(bookings_source_src_28000.paid_at) AS paid_at__month
                          , toStartOfQuarter(bookings_source_src_28000.paid_at) AS paid_at__quarter
                          , toStartOfYear(bookings_source_src_28000.paid_at) AS paid_at__year
                          , toYear(bookings_source_src_28000.paid_at) AS paid_at__extract_year
                          , toQuarter(bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
                          , toMonth(bookings_source_src_28000.paid_at) AS paid_at__extract_month
                          , toDayOfMonth(bookings_source_src_28000.paid_at) AS paid_at__extract_day
                          , toDayOfWeek(bookings_source_src_28000.paid_at) AS paid_at__extract_dow
                          , toDayOfYear(bookings_source_src_28000.paid_at) AS paid_at__extract_doy
                          , bookings_source_src_28000.is_instant AS booking__is_instant
                          , toStartOfDay(bookings_source_src_28000.ds) AS booking__ds__day
                          , toStartOfWeek(bookings_source_src_28000.ds, 1) AS booking__ds__week
                          , toStartOfMonth(bookings_source_src_28000.ds) AS booking__ds__month
                          , toStartOfQuarter(bookings_source_src_28000.ds) AS booking__ds__quarter
                          , toStartOfYear(bookings_source_src_28000.ds) AS booking__ds__year
                          , toYear(bookings_source_src_28000.ds) AS booking__ds__extract_year
                          , toQuarter(bookings_source_src_28000.ds) AS booking__ds__extract_quarter
                          , toMonth(bookings_source_src_28000.ds) AS booking__ds__extract_month
                          , toDayOfMonth(bookings_source_src_28000.ds) AS booking__ds__extract_day
                          , toDayOfWeek(bookings_source_src_28000.ds) AS booking__ds__extract_dow
                          , toDayOfYear(bookings_source_src_28000.ds) AS booking__ds__extract_doy
                          , toStartOfDay(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__day
                          , toStartOfWeek(bookings_source_src_28000.ds_partitioned, 1) AS booking__ds_partitioned__week
                          , toStartOfMonth(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__month
                          , toStartOfQuarter(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__quarter
                          , toStartOfYear(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__year
                          , toYear(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
                          , toQuarter(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                          , toMonth(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
                          , toDayOfMonth(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
                          , toDayOfWeek(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
                          , toDayOfYear(bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
                          , toStartOfDay(bookings_source_src_28000.paid_at) AS booking__paid_at__day
                          , toStartOfWeek(bookings_source_src_28000.paid_at, 1) AS booking__paid_at__week
                          , toStartOfMonth(bookings_source_src_28000.paid_at) AS booking__paid_at__month
                          , toStartOfQuarter(bookings_source_src_28000.paid_at) AS booking__paid_at__quarter
                          , toStartOfYear(bookings_source_src_28000.paid_at) AS booking__paid_at__year
                          , toYear(bookings_source_src_28000.paid_at) AS booking__paid_at__extract_year
                          , toQuarter(bookings_source_src_28000.paid_at) AS booking__paid_at__extract_quarter
                          , toMonth(bookings_source_src_28000.paid_at) AS booking__paid_at__extract_month
                          , toDayOfMonth(bookings_source_src_28000.paid_at) AS booking__paid_at__extract_day
                          , toDayOfWeek(bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
                          , toDayOfYear(bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
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
                        subq_5.metric_time__day > addDays(subq_6.metric_time__day, -2)
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
            addDays(subq_17.metric_time__day, -2) = subq_12.metric_time__day
        ) subq_18
        WHERE subq_18.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
      ) subq_19
    ) subq_20
  ) subq_21
) subq_22
