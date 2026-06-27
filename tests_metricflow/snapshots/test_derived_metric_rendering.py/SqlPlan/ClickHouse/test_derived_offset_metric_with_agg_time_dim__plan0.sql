test_name: test_derived_offset_metric_with_agg_time_dim
test_filename: test_derived_metric_rendering.py
sql_engine: ClickHouse
---
SELECT
  subq_17.booking__ds__day
  , subq_17.booking_fees_last_week_per_booker_this_week
FROM (
  SELECT
    subq_16.booking__ds__day
    , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
  FROM (
    SELECT
      COALESCE(subq_10.booking__ds__day, subq_15.booking__ds__day) AS booking__ds__day
      , MAX(subq_10.booking_value) AS booking_value
      , MAX(subq_15.bookers) AS bookers
    FROM (
      SELECT
        subq_9.booking__ds__day
        , subq_9.__booking_value AS booking_value
      FROM (
        SELECT
          subq_8.booking__ds__day AS booking__ds__day
          , subq_4.__booking_value AS __booking_value
        FROM (
          SELECT
            subq_7.booking__ds__day
          FROM (
            SELECT
              subq_6.booking__ds__day
            FROM (
              SELECT
                subq_5.ds__day AS booking__ds__day
                , subq_5.ds__week
                , subq_5.ds__month
                , subq_5.ds__quarter
                , subq_5.ds__year
                , subq_5.ds__extract_year
                , subq_5.ds__extract_quarter
                , subq_5.ds__extract_month
                , subq_5.ds__extract_day
                , subq_5.ds__extract_dow
                , subq_5.ds__extract_doy
                , subq_5.ds__alien_day
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
              ) subq_5
            ) subq_6
          ) subq_7
        ) subq_8
        INNER JOIN (
          SELECT
            subq_3.booking__ds__day
            , SUM(subq_3.__booking_value) AS __booking_value
          FROM (
            SELECT
              subq_2.booking__ds__day
              , subq_2.__booking_value
            FROM (
              SELECT
                subq_1.booking__ds__day
                , subq_1.__booking_value
              FROM (
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
                ) subq_0
              ) subq_1
            ) subq_2
          ) subq_3
          GROUP BY
            subq_3.booking__ds__day
        ) subq_4
        ON
          addDays(subq_8.booking__ds__day, -7) = subq_4.booking__ds__day
      ) subq_9
    ) subq_10
    FULL OUTER JOIN (
      SELECT
        subq_14.booking__ds__day
        , subq_14.__bookers AS bookers
      FROM (
        SELECT
          subq_13.booking__ds__day
          , COUNT(DISTINCT subq_13.__bookers) AS __bookers
        FROM (
          SELECT
            subq_12.booking__ds__day
            , subq_12.__bookers
          FROM (
            SELECT
              subq_11.booking__ds__day
              , subq_11.__bookers
            FROM (
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
              ) subq_0
            ) subq_11
          ) subq_12
        ) subq_13
        GROUP BY
          subq_13.booking__ds__day
      ) subq_14
    ) subq_15
    ON
      subq_10.booking__ds__day = subq_15.booking__ds__day
    GROUP BY
      COALESCE(subq_10.booking__ds__day, subq_15.booking__ds__day)
  ) subq_16
) subq_17
