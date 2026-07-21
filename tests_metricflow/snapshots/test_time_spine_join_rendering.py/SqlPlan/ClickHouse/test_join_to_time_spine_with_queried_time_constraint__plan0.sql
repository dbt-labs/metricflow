test_name: test_join_to_time_spine_with_queried_time_constraint
test_filename: test_time_spine_join_rendering.py
docstring:
  Test case where metric that fills nulls is queried with metric time and a time constraint. Should apply constraint twice.
sql_engine: ClickHouse
---
SELECT
  subq_15.metric_time__day
  , subq_15.bookings_fill_nulls_with_0
FROM (
  SELECT
    subq_14.metric_time__day
    , COALESCE(subq_14.__bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
  FROM (
    SELECT
      subq_13.metric_time__day
      , subq_13.__bookings_fill_nulls_with_0
    FROM (
      SELECT
        subq_12.metric_time__day AS metric_time__day
        , subq_7.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
      FROM (
        SELECT
          subq_11.metric_time__day
        FROM (
          SELECT
            subq_10.metric_time__day
          FROM (
            SELECT
              subq_9.metric_time__day
            FROM (
              SELECT
                subq_8.ds__day AS metric_time__day
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
                , subq_8.ds__alien_day
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
              ) subq_8
            ) subq_9
          ) subq_10
          WHERE subq_10.metric_time__day BETWEEN '2020-01-03' AND '2020-01-05'
        ) subq_11
      ) subq_12
      LEFT OUTER JOIN (
        SELECT
          subq_6.metric_time__day
          , SUM(subq_6.__bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
        FROM (
          SELECT
            subq_5.metric_time__day
            , subq_5.__bookings_fill_nulls_with_0
          FROM (
            SELECT
              subq_4.metric_time__day
              , subq_4.__bookings_fill_nulls_with_0
            FROM (
              SELECT
                subq_3.ds__day
                , subq_3.ds__week
                , subq_3.ds__month
                , subq_3.ds__quarter
                , subq_3.ds__year
                , subq_3.ds__extract_year
                , subq_3.ds__extract_quarter
                , subq_3.ds__extract_month
                , subq_3.ds__extract_day
                , subq_3.ds__extract_dow
                , subq_3.ds__extract_doy
                , subq_3.ds_partitioned__day
                , subq_3.ds_partitioned__week
                , subq_3.ds_partitioned__month
                , subq_3.ds_partitioned__quarter
                , subq_3.ds_partitioned__year
                , subq_3.ds_partitioned__extract_year
                , subq_3.ds_partitioned__extract_quarter
                , subq_3.ds_partitioned__extract_month
                , subq_3.ds_partitioned__extract_day
                , subq_3.ds_partitioned__extract_dow
                , subq_3.ds_partitioned__extract_doy
                , subq_3.paid_at__day
                , subq_3.paid_at__week
                , subq_3.paid_at__month
                , subq_3.paid_at__quarter
                , subq_3.paid_at__year
                , subq_3.paid_at__extract_year
                , subq_3.paid_at__extract_quarter
                , subq_3.paid_at__extract_month
                , subq_3.paid_at__extract_day
                , subq_3.paid_at__extract_dow
                , subq_3.paid_at__extract_doy
                , subq_3.booking__ds__day
                , subq_3.booking__ds__week
                , subq_3.booking__ds__month
                , subq_3.booking__ds__quarter
                , subq_3.booking__ds__year
                , subq_3.booking__ds__extract_year
                , subq_3.booking__ds__extract_quarter
                , subq_3.booking__ds__extract_month
                , subq_3.booking__ds__extract_day
                , subq_3.booking__ds__extract_dow
                , subq_3.booking__ds__extract_doy
                , subq_3.booking__ds_partitioned__day
                , subq_3.booking__ds_partitioned__week
                , subq_3.booking__ds_partitioned__month
                , subq_3.booking__ds_partitioned__quarter
                , subq_3.booking__ds_partitioned__year
                , subq_3.booking__ds_partitioned__extract_year
                , subq_3.booking__ds_partitioned__extract_quarter
                , subq_3.booking__ds_partitioned__extract_month
                , subq_3.booking__ds_partitioned__extract_day
                , subq_3.booking__ds_partitioned__extract_dow
                , subq_3.booking__ds_partitioned__extract_doy
                , subq_3.booking__paid_at__day
                , subq_3.booking__paid_at__week
                , subq_3.booking__paid_at__month
                , subq_3.booking__paid_at__quarter
                , subq_3.booking__paid_at__year
                , subq_3.booking__paid_at__extract_year
                , subq_3.booking__paid_at__extract_quarter
                , subq_3.booking__paid_at__extract_month
                , subq_3.booking__paid_at__extract_day
                , subq_3.booking__paid_at__extract_dow
                , subq_3.booking__paid_at__extract_doy
                , subq_3.metric_time__day
                , subq_3.metric_time__week
                , subq_3.metric_time__month
                , subq_3.metric_time__quarter
                , subq_3.metric_time__year
                , subq_3.metric_time__extract_year
                , subq_3.metric_time__extract_quarter
                , subq_3.metric_time__extract_month
                , subq_3.metric_time__extract_day
                , subq_3.metric_time__extract_dow
                , subq_3.metric_time__extract_doy
                , subq_3.listing
                , subq_3.guest
                , subq_3.host
                , subq_3.booking__listing
                , subq_3.booking__guest
                , subq_3.booking__host
                , subq_3.is_instant
                , subq_3.booking__is_instant
                , subq_3.__bookings
                , subq_3.__average_booking_value
                , subq_3.__instant_bookings
                , subq_3.__booking_value
                , subq_3.__max_booking_value
                , subq_3.__min_booking_value
                , subq_3.__instant_booking_value
                , subq_3.__average_instant_booking_value
                , subq_3.__booking_value_for_non_null_listing_id
                , subq_3.__bookers
                , subq_3.__referred_bookings
                , subq_3.__median_booking_value
                , subq_3.__booking_value_p99
                , subq_3.__discrete_booking_value_p99
                , subq_3.__approximate_continuous_booking_value_p99
                , subq_3.__approximate_discrete_booking_value_p99
                , subq_3.__bookings_join_to_time_spine
                , subq_3.__bookings_fill_nulls_with_0_without_time_spine
                , subq_3.__bookings_fill_nulls_with_0
                , subq_3.__instant_bookings_with_measure_filter
                , subq_3.__bookings_join_to_time_spine_with_tiered_filters
                , subq_3.__bookers_fill_nulls_with_0_join_to_timespine
              FROM (
                SELECT
                  subq_2.ds__day
                  , subq_2.ds__week
                  , subq_2.ds__month
                  , subq_2.ds__quarter
                  , subq_2.ds__year
                  , subq_2.ds__extract_year
                  , subq_2.ds__extract_quarter
                  , subq_2.ds__extract_month
                  , subq_2.ds__extract_day
                  , subq_2.ds__extract_dow
                  , subq_2.ds__extract_doy
                  , subq_2.ds_partitioned__day
                  , subq_2.ds_partitioned__week
                  , subq_2.ds_partitioned__month
                  , subq_2.ds_partitioned__quarter
                  , subq_2.ds_partitioned__year
                  , subq_2.ds_partitioned__extract_year
                  , subq_2.ds_partitioned__extract_quarter
                  , subq_2.ds_partitioned__extract_month
                  , subq_2.ds_partitioned__extract_day
                  , subq_2.ds_partitioned__extract_dow
                  , subq_2.ds_partitioned__extract_doy
                  , subq_2.paid_at__day
                  , subq_2.paid_at__week
                  , subq_2.paid_at__month
                  , subq_2.paid_at__quarter
                  , subq_2.paid_at__year
                  , subq_2.paid_at__extract_year
                  , subq_2.paid_at__extract_quarter
                  , subq_2.paid_at__extract_month
                  , subq_2.paid_at__extract_day
                  , subq_2.paid_at__extract_dow
                  , subq_2.paid_at__extract_doy
                  , subq_2.booking__ds__day
                  , subq_2.booking__ds__week
                  , subq_2.booking__ds__month
                  , subq_2.booking__ds__quarter
                  , subq_2.booking__ds__year
                  , subq_2.booking__ds__extract_year
                  , subq_2.booking__ds__extract_quarter
                  , subq_2.booking__ds__extract_month
                  , subq_2.booking__ds__extract_day
                  , subq_2.booking__ds__extract_dow
                  , subq_2.booking__ds__extract_doy
                  , subq_2.booking__ds_partitioned__day
                  , subq_2.booking__ds_partitioned__week
                  , subq_2.booking__ds_partitioned__month
                  , subq_2.booking__ds_partitioned__quarter
                  , subq_2.booking__ds_partitioned__year
                  , subq_2.booking__ds_partitioned__extract_year
                  , subq_2.booking__ds_partitioned__extract_quarter
                  , subq_2.booking__ds_partitioned__extract_month
                  , subq_2.booking__ds_partitioned__extract_day
                  , subq_2.booking__ds_partitioned__extract_dow
                  , subq_2.booking__ds_partitioned__extract_doy
                  , subq_2.booking__paid_at__day
                  , subq_2.booking__paid_at__week
                  , subq_2.booking__paid_at__month
                  , subq_2.booking__paid_at__quarter
                  , subq_2.booking__paid_at__year
                  , subq_2.booking__paid_at__extract_year
                  , subq_2.booking__paid_at__extract_quarter
                  , subq_2.booking__paid_at__extract_month
                  , subq_2.booking__paid_at__extract_day
                  , subq_2.booking__paid_at__extract_dow
                  , subq_2.booking__paid_at__extract_doy
                  , subq_2.ds__day AS metric_time__day
                  , subq_2.ds__week AS metric_time__week
                  , subq_2.ds__month AS metric_time__month
                  , subq_2.ds__quarter AS metric_time__quarter
                  , subq_2.ds__year AS metric_time__year
                  , subq_2.ds__extract_year AS metric_time__extract_year
                  , subq_2.ds__extract_quarter AS metric_time__extract_quarter
                  , subq_2.ds__extract_month AS metric_time__extract_month
                  , subq_2.ds__extract_day AS metric_time__extract_day
                  , subq_2.ds__extract_dow AS metric_time__extract_dow
                  , subq_2.ds__extract_doy AS metric_time__extract_doy
                  , subq_2.listing
                  , subq_2.guest
                  , subq_2.host
                  , subq_2.booking__listing
                  , subq_2.booking__guest
                  , subq_2.booking__host
                  , subq_2.is_instant
                  , subq_2.booking__is_instant
                  , subq_2.__bookings
                  , subq_2.__average_booking_value
                  , subq_2.__instant_bookings
                  , subq_2.__booking_value
                  , subq_2.__max_booking_value
                  , subq_2.__min_booking_value
                  , subq_2.__instant_booking_value
                  , subq_2.__average_instant_booking_value
                  , subq_2.__booking_value_for_non_null_listing_id
                  , subq_2.__bookers
                  , subq_2.__referred_bookings
                  , subq_2.__median_booking_value
                  , subq_2.__booking_value_p99
                  , subq_2.__discrete_booking_value_p99
                  , subq_2.__approximate_continuous_booking_value_p99
                  , subq_2.__approximate_discrete_booking_value_p99
                  , subq_2.__bookings_join_to_time_spine
                  , subq_2.__bookings_fill_nulls_with_0_without_time_spine
                  , subq_2.__bookings_fill_nulls_with_0
                  , subq_2.__instant_bookings_with_measure_filter
                  , subq_2.__bookings_join_to_time_spine_with_tiered_filters
                  , subq_2.__bookers_fill_nulls_with_0_join_to_timespine
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
                ) subq_2
              ) subq_3
              WHERE subq_3.metric_time__day BETWEEN '2020-01-03' AND '2020-01-05'
            ) subq_4
          ) subq_5
        ) subq_6
        GROUP BY
          subq_6.metric_time__day
      ) subq_7
      ON
        subq_12.metric_time__day = subq_7.metric_time__day
    ) subq_13
    WHERE subq_13.metric_time__day BETWEEN '2020-01-03' AND '2020-01-05'
  ) subq_14
) subq_15
