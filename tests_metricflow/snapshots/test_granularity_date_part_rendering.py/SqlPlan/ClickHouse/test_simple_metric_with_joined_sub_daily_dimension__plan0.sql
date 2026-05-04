test_name: test_simple_metric_with_joined_sub_daily_dimension
test_filename: test_granularity_date_part_rendering.py
sql_engine: ClickHouse
---
SELECT
  subq_25.listing__user__bio_added_ts__minute
  , subq_25.bookings
FROM (
  SELECT
    subq_24.listing__user__bio_added_ts__minute
    , subq_24.__bookings AS bookings
  FROM (
    SELECT
      subq_23.listing__user__bio_added_ts__minute
      , SUM(subq_23.__bookings) AS __bookings
    FROM (
      SELECT
        subq_22.listing__user__bio_added_ts__minute
        , subq_22.__bookings
      FROM (
        SELECT
          subq_21.listing__user__bio_added_ts__minute
          , subq_21.__bookings
        FROM (
          SELECT
            subq_20.user__ds_partitioned__day AS listing__user__ds_partitioned__day
            , subq_20.user__bio_added_ts__minute AS listing__user__bio_added_ts__minute
            , subq_13.ds__day AS ds__day
            , subq_13.ds__week AS ds__week
            , subq_13.ds__month AS ds__month
            , subq_13.ds__quarter AS ds__quarter
            , subq_13.ds__year AS ds__year
            , subq_13.ds__extract_year AS ds__extract_year
            , subq_13.ds__extract_quarter AS ds__extract_quarter
            , subq_13.ds__extract_month AS ds__extract_month
            , subq_13.ds__extract_day AS ds__extract_day
            , subq_13.ds__extract_dow AS ds__extract_dow
            , subq_13.ds__extract_doy AS ds__extract_doy
            , subq_13.ds_partitioned__day AS ds_partitioned__day
            , subq_13.ds_partitioned__week AS ds_partitioned__week
            , subq_13.ds_partitioned__month AS ds_partitioned__month
            , subq_13.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_13.ds_partitioned__year AS ds_partitioned__year
            , subq_13.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , subq_13.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , subq_13.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , subq_13.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , subq_13.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , subq_13.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , subq_13.paid_at__day AS paid_at__day
            , subq_13.paid_at__week AS paid_at__week
            , subq_13.paid_at__month AS paid_at__month
            , subq_13.paid_at__quarter AS paid_at__quarter
            , subq_13.paid_at__year AS paid_at__year
            , subq_13.paid_at__extract_year AS paid_at__extract_year
            , subq_13.paid_at__extract_quarter AS paid_at__extract_quarter
            , subq_13.paid_at__extract_month AS paid_at__extract_month
            , subq_13.paid_at__extract_day AS paid_at__extract_day
            , subq_13.paid_at__extract_dow AS paid_at__extract_dow
            , subq_13.paid_at__extract_doy AS paid_at__extract_doy
            , subq_13.booking__ds__day AS booking__ds__day
            , subq_13.booking__ds__week AS booking__ds__week
            , subq_13.booking__ds__month AS booking__ds__month
            , subq_13.booking__ds__quarter AS booking__ds__quarter
            , subq_13.booking__ds__year AS booking__ds__year
            , subq_13.booking__ds__extract_year AS booking__ds__extract_year
            , subq_13.booking__ds__extract_quarter AS booking__ds__extract_quarter
            , subq_13.booking__ds__extract_month AS booking__ds__extract_month
            , subq_13.booking__ds__extract_day AS booking__ds__extract_day
            , subq_13.booking__ds__extract_dow AS booking__ds__extract_dow
            , subq_13.booking__ds__extract_doy AS booking__ds__extract_doy
            , subq_13.booking__ds_partitioned__day AS booking__ds_partitioned__day
            , subq_13.booking__ds_partitioned__week AS booking__ds_partitioned__week
            , subq_13.booking__ds_partitioned__month AS booking__ds_partitioned__month
            , subq_13.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
            , subq_13.booking__ds_partitioned__year AS booking__ds_partitioned__year
            , subq_13.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
            , subq_13.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
            , subq_13.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
            , subq_13.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
            , subq_13.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
            , subq_13.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
            , subq_13.booking__paid_at__day AS booking__paid_at__day
            , subq_13.booking__paid_at__week AS booking__paid_at__week
            , subq_13.booking__paid_at__month AS booking__paid_at__month
            , subq_13.booking__paid_at__quarter AS booking__paid_at__quarter
            , subq_13.booking__paid_at__year AS booking__paid_at__year
            , subq_13.booking__paid_at__extract_year AS booking__paid_at__extract_year
            , subq_13.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
            , subq_13.booking__paid_at__extract_month AS booking__paid_at__extract_month
            , subq_13.booking__paid_at__extract_day AS booking__paid_at__extract_day
            , subq_13.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
            , subq_13.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
            , subq_13.metric_time__day AS metric_time__day
            , subq_13.metric_time__week AS metric_time__week
            , subq_13.metric_time__month AS metric_time__month
            , subq_13.metric_time__quarter AS metric_time__quarter
            , subq_13.metric_time__year AS metric_time__year
            , subq_13.metric_time__extract_year AS metric_time__extract_year
            , subq_13.metric_time__extract_quarter AS metric_time__extract_quarter
            , subq_13.metric_time__extract_month AS metric_time__extract_month
            , subq_13.metric_time__extract_day AS metric_time__extract_day
            , subq_13.metric_time__extract_dow AS metric_time__extract_dow
            , subq_13.metric_time__extract_doy AS metric_time__extract_doy
            , subq_13.listing AS listing
            , subq_13.guest AS guest
            , subq_13.host AS host
            , subq_13.booking__listing AS booking__listing
            , subq_13.booking__guest AS booking__guest
            , subq_13.booking__host AS booking__host
            , subq_13.is_instant AS is_instant
            , subq_13.booking__is_instant AS booking__is_instant
            , subq_13.__bookings AS __bookings
            , subq_13.__average_booking_value AS __average_booking_value
            , subq_13.__instant_bookings AS __instant_bookings
            , subq_13.__booking_value AS __booking_value
            , subq_13.__max_booking_value AS __max_booking_value
            , subq_13.__min_booking_value AS __min_booking_value
            , subq_13.__instant_booking_value AS __instant_booking_value
            , subq_13.__average_instant_booking_value AS __average_instant_booking_value
            , subq_13.__booking_value_for_non_null_listing_id AS __booking_value_for_non_null_listing_id
            , subq_13.__bookers AS __bookers
            , subq_13.__referred_bookings AS __referred_bookings
            , subq_13.__median_booking_value AS __median_booking_value
            , subq_13.__booking_value_p99 AS __booking_value_p99
            , subq_13.__discrete_booking_value_p99 AS __discrete_booking_value_p99
            , subq_13.__approximate_continuous_booking_value_p99 AS __approximate_continuous_booking_value_p99
            , subq_13.__approximate_discrete_booking_value_p99 AS __approximate_discrete_booking_value_p99
            , subq_13.__bookings_join_to_time_spine AS __bookings_join_to_time_spine
            , subq_13.__bookings_fill_nulls_with_0_without_time_spine AS __bookings_fill_nulls_with_0_without_time_spine
            , subq_13.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
            , subq_13.__instant_bookings_with_measure_filter AS __instant_bookings_with_measure_filter
            , subq_13.__bookings_join_to_time_spine_with_tiered_filters AS __bookings_join_to_time_spine_with_tiered_filters
            , subq_13.__bookers_fill_nulls_with_0_join_to_timespine AS __bookers_fill_nulls_with_0_join_to_timespine
          FROM (
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
              , subq_12.ds__day AS metric_time__day
              , subq_12.ds__week AS metric_time__week
              , subq_12.ds__month AS metric_time__month
              , subq_12.ds__quarter AS metric_time__quarter
              , subq_12.ds__year AS metric_time__year
              , subq_12.ds__extract_year AS metric_time__extract_year
              , subq_12.ds__extract_quarter AS metric_time__extract_quarter
              , subq_12.ds__extract_month AS metric_time__extract_month
              , subq_12.ds__extract_day AS metric_time__extract_day
              , subq_12.ds__extract_dow AS metric_time__extract_dow
              , subq_12.ds__extract_doy AS metric_time__extract_doy
              , subq_12.listing
              , subq_12.guest
              , subq_12.host
              , subq_12.booking__listing
              , subq_12.booking__guest
              , subq_12.booking__host
              , subq_12.is_instant
              , subq_12.booking__is_instant
              , subq_12.__bookings
              , subq_12.__average_booking_value
              , subq_12.__instant_bookings
              , subq_12.__booking_value
              , subq_12.__max_booking_value
              , subq_12.__min_booking_value
              , subq_12.__instant_booking_value
              , subq_12.__average_instant_booking_value
              , subq_12.__booking_value_for_non_null_listing_id
              , subq_12.__bookers
              , subq_12.__referred_bookings
              , subq_12.__median_booking_value
              , subq_12.__booking_value_p99
              , subq_12.__discrete_booking_value_p99
              , subq_12.__approximate_continuous_booking_value_p99
              , subq_12.__approximate_discrete_booking_value_p99
              , subq_12.__bookings_join_to_time_spine
              , subq_12.__bookings_fill_nulls_with_0_without_time_spine
              , subq_12.__bookings_fill_nulls_with_0
              , subq_12.__instant_bookings_with_measure_filter
              , subq_12.__bookings_join_to_time_spine_with_tiered_filters
              , subq_12.__bookers_fill_nulls_with_0_join_to_timespine
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
            ) subq_12
          ) subq_13
          LEFT OUTER JOIN (
            SELECT
              subq_19.user__ds_partitioned__day
              , subq_19.user__bio_added_ts__minute
              , subq_19.listing
            FROM (
              SELECT
                subq_18.home_state AS user__home_state
                , subq_18.ds__day AS user__ds__day
                , subq_18.ds__week AS user__ds__week
                , subq_18.ds__month AS user__ds__month
                , subq_18.ds__quarter AS user__ds__quarter
                , subq_18.ds__year AS user__ds__year
                , subq_18.ds__extract_year AS user__ds__extract_year
                , subq_18.ds__extract_quarter AS user__ds__extract_quarter
                , subq_18.ds__extract_month AS user__ds__extract_month
                , subq_18.ds__extract_day AS user__ds__extract_day
                , subq_18.ds__extract_dow AS user__ds__extract_dow
                , subq_18.ds__extract_doy AS user__ds__extract_doy
                , subq_18.created_at__day AS user__created_at__day
                , subq_18.created_at__week AS user__created_at__week
                , subq_18.created_at__month AS user__created_at__month
                , subq_18.created_at__quarter AS user__created_at__quarter
                , subq_18.created_at__year AS user__created_at__year
                , subq_18.created_at__extract_year AS user__created_at__extract_year
                , subq_18.created_at__extract_quarter AS user__created_at__extract_quarter
                , subq_18.created_at__extract_month AS user__created_at__extract_month
                , subq_18.created_at__extract_day AS user__created_at__extract_day
                , subq_18.created_at__extract_dow AS user__created_at__extract_dow
                , subq_18.created_at__extract_doy AS user__created_at__extract_doy
                , subq_18.ds_partitioned__day AS user__ds_partitioned__day
                , subq_18.ds_partitioned__week AS user__ds_partitioned__week
                , subq_18.ds_partitioned__month AS user__ds_partitioned__month
                , subq_18.ds_partitioned__quarter AS user__ds_partitioned__quarter
                , subq_18.ds_partitioned__year AS user__ds_partitioned__year
                , subq_18.ds_partitioned__extract_year AS user__ds_partitioned__extract_year
                , subq_18.ds_partitioned__extract_quarter AS user__ds_partitioned__extract_quarter
                , subq_18.ds_partitioned__extract_month AS user__ds_partitioned__extract_month
                , subq_18.ds_partitioned__extract_day AS user__ds_partitioned__extract_day
                , subq_18.ds_partitioned__extract_dow AS user__ds_partitioned__extract_dow
                , subq_18.ds_partitioned__extract_doy AS user__ds_partitioned__extract_doy
                , subq_18.last_profile_edit_ts__millisecond AS user__last_profile_edit_ts__millisecond
                , subq_18.last_profile_edit_ts__second AS user__last_profile_edit_ts__second
                , subq_18.last_profile_edit_ts__minute AS user__last_profile_edit_ts__minute
                , subq_18.last_profile_edit_ts__hour AS user__last_profile_edit_ts__hour
                , subq_18.last_profile_edit_ts__day AS user__last_profile_edit_ts__day
                , subq_18.last_profile_edit_ts__week AS user__last_profile_edit_ts__week
                , subq_18.last_profile_edit_ts__month AS user__last_profile_edit_ts__month
                , subq_18.last_profile_edit_ts__quarter AS user__last_profile_edit_ts__quarter
                , subq_18.last_profile_edit_ts__year AS user__last_profile_edit_ts__year
                , subq_18.last_profile_edit_ts__extract_year AS user__last_profile_edit_ts__extract_year
                , subq_18.last_profile_edit_ts__extract_quarter AS user__last_profile_edit_ts__extract_quarter
                , subq_18.last_profile_edit_ts__extract_month AS user__last_profile_edit_ts__extract_month
                , subq_18.last_profile_edit_ts__extract_day AS user__last_profile_edit_ts__extract_day
                , subq_18.last_profile_edit_ts__extract_dow AS user__last_profile_edit_ts__extract_dow
                , subq_18.last_profile_edit_ts__extract_doy AS user__last_profile_edit_ts__extract_doy
                , subq_18.bio_added_ts__second AS user__bio_added_ts__second
                , subq_18.bio_added_ts__minute AS user__bio_added_ts__minute
                , subq_18.bio_added_ts__hour AS user__bio_added_ts__hour
                , subq_18.bio_added_ts__day AS user__bio_added_ts__day
                , subq_18.bio_added_ts__week AS user__bio_added_ts__week
                , subq_18.bio_added_ts__month AS user__bio_added_ts__month
                , subq_18.bio_added_ts__quarter AS user__bio_added_ts__quarter
                , subq_18.bio_added_ts__year AS user__bio_added_ts__year
                , subq_18.bio_added_ts__extract_year AS user__bio_added_ts__extract_year
                , subq_18.bio_added_ts__extract_quarter AS user__bio_added_ts__extract_quarter
                , subq_18.bio_added_ts__extract_month AS user__bio_added_ts__extract_month
                , subq_18.bio_added_ts__extract_day AS user__bio_added_ts__extract_day
                , subq_18.bio_added_ts__extract_dow AS user__bio_added_ts__extract_dow
                , subq_18.bio_added_ts__extract_doy AS user__bio_added_ts__extract_doy
                , subq_18.last_login_ts__minute AS user__last_login_ts__minute
                , subq_18.last_login_ts__hour AS user__last_login_ts__hour
                , subq_18.last_login_ts__day AS user__last_login_ts__day
                , subq_18.last_login_ts__week AS user__last_login_ts__week
                , subq_18.last_login_ts__month AS user__last_login_ts__month
                , subq_18.last_login_ts__quarter AS user__last_login_ts__quarter
                , subq_18.last_login_ts__year AS user__last_login_ts__year
                , subq_18.last_login_ts__extract_year AS user__last_login_ts__extract_year
                , subq_18.last_login_ts__extract_quarter AS user__last_login_ts__extract_quarter
                , subq_18.last_login_ts__extract_month AS user__last_login_ts__extract_month
                , subq_18.last_login_ts__extract_day AS user__last_login_ts__extract_day
                , subq_18.last_login_ts__extract_dow AS user__last_login_ts__extract_dow
                , subq_18.last_login_ts__extract_doy AS user__last_login_ts__extract_doy
                , subq_18.archived_at__hour AS user__archived_at__hour
                , subq_18.archived_at__day AS user__archived_at__day
                , subq_18.archived_at__week AS user__archived_at__week
                , subq_18.archived_at__month AS user__archived_at__month
                , subq_18.archived_at__quarter AS user__archived_at__quarter
                , subq_18.archived_at__year AS user__archived_at__year
                , subq_18.archived_at__extract_year AS user__archived_at__extract_year
                , subq_18.archived_at__extract_quarter AS user__archived_at__extract_quarter
                , subq_18.archived_at__extract_month AS user__archived_at__extract_month
                , subq_18.archived_at__extract_day AS user__archived_at__extract_day
                , subq_18.archived_at__extract_dow AS user__archived_at__extract_dow
                , subq_18.archived_at__extract_doy AS user__archived_at__extract_doy
                , subq_18.metric_time__day AS user__metric_time__day
                , subq_18.metric_time__week AS user__metric_time__week
                , subq_18.metric_time__month AS user__metric_time__month
                , subq_18.metric_time__quarter AS user__metric_time__quarter
                , subq_18.metric_time__year AS user__metric_time__year
                , subq_18.metric_time__extract_year AS user__metric_time__extract_year
                , subq_18.metric_time__extract_quarter AS user__metric_time__extract_quarter
                , subq_18.metric_time__extract_month AS user__metric_time__extract_month
                , subq_18.metric_time__extract_day AS user__metric_time__extract_day
                , subq_18.metric_time__extract_dow AS user__metric_time__extract_dow
                , subq_18.metric_time__extract_doy AS user__metric_time__extract_doy
                , subq_15.ds__day AS ds__day
                , subq_15.ds__week AS ds__week
                , subq_15.ds__month AS ds__month
                , subq_15.ds__quarter AS ds__quarter
                , subq_15.ds__year AS ds__year
                , subq_15.ds__extract_year AS ds__extract_year
                , subq_15.ds__extract_quarter AS ds__extract_quarter
                , subq_15.ds__extract_month AS ds__extract_month
                , subq_15.ds__extract_day AS ds__extract_day
                , subq_15.ds__extract_dow AS ds__extract_dow
                , subq_15.ds__extract_doy AS ds__extract_doy
                , subq_15.created_at__day AS created_at__day
                , subq_15.created_at__week AS created_at__week
                , subq_15.created_at__month AS created_at__month
                , subq_15.created_at__quarter AS created_at__quarter
                , subq_15.created_at__year AS created_at__year
                , subq_15.created_at__extract_year AS created_at__extract_year
                , subq_15.created_at__extract_quarter AS created_at__extract_quarter
                , subq_15.created_at__extract_month AS created_at__extract_month
                , subq_15.created_at__extract_day AS created_at__extract_day
                , subq_15.created_at__extract_dow AS created_at__extract_dow
                , subq_15.created_at__extract_doy AS created_at__extract_doy
                , subq_15.listing__ds__day AS listing__ds__day
                , subq_15.listing__ds__week AS listing__ds__week
                , subq_15.listing__ds__month AS listing__ds__month
                , subq_15.listing__ds__quarter AS listing__ds__quarter
                , subq_15.listing__ds__year AS listing__ds__year
                , subq_15.listing__ds__extract_year AS listing__ds__extract_year
                , subq_15.listing__ds__extract_quarter AS listing__ds__extract_quarter
                , subq_15.listing__ds__extract_month AS listing__ds__extract_month
                , subq_15.listing__ds__extract_day AS listing__ds__extract_day
                , subq_15.listing__ds__extract_dow AS listing__ds__extract_dow
                , subq_15.listing__ds__extract_doy AS listing__ds__extract_doy
                , subq_15.listing__created_at__day AS listing__created_at__day
                , subq_15.listing__created_at__week AS listing__created_at__week
                , subq_15.listing__created_at__month AS listing__created_at__month
                , subq_15.listing__created_at__quarter AS listing__created_at__quarter
                , subq_15.listing__created_at__year AS listing__created_at__year
                , subq_15.listing__created_at__extract_year AS listing__created_at__extract_year
                , subq_15.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
                , subq_15.listing__created_at__extract_month AS listing__created_at__extract_month
                , subq_15.listing__created_at__extract_day AS listing__created_at__extract_day
                , subq_15.listing__created_at__extract_dow AS listing__created_at__extract_dow
                , subq_15.listing__created_at__extract_doy AS listing__created_at__extract_doy
                , subq_15.metric_time__day AS metric_time__day
                , subq_15.metric_time__week AS metric_time__week
                , subq_15.metric_time__month AS metric_time__month
                , subq_15.metric_time__quarter AS metric_time__quarter
                , subq_15.metric_time__year AS metric_time__year
                , subq_15.metric_time__extract_year AS metric_time__extract_year
                , subq_15.metric_time__extract_quarter AS metric_time__extract_quarter
                , subq_15.metric_time__extract_month AS metric_time__extract_month
                , subq_15.metric_time__extract_day AS metric_time__extract_day
                , subq_15.metric_time__extract_dow AS metric_time__extract_dow
                , subq_15.metric_time__extract_doy AS metric_time__extract_doy
                , subq_15.listing AS listing
                , subq_15.user AS user
                , subq_15.listing__user AS listing__user
                , subq_15.country_latest AS country_latest
                , subq_15.is_lux_latest AS is_lux_latest
                , subq_15.capacity_latest AS capacity_latest
                , subq_15.listing__country_latest AS listing__country_latest
                , subq_15.listing__is_lux_latest AS listing__is_lux_latest
                , subq_15.listing__capacity_latest AS listing__capacity_latest
                , subq_15.__listings AS __listings
                , subq_15.__lux_listings AS __lux_listings
                , subq_15.__smallest_listing AS __smallest_listing
                , subq_15.__largest_listing AS __largest_listing
                , subq_15.__active_listings AS __active_listings
              FROM (
                SELECT
                  subq_14.ds__day
                  , subq_14.ds__week
                  , subq_14.ds__month
                  , subq_14.ds__quarter
                  , subq_14.ds__year
                  , subq_14.ds__extract_year
                  , subq_14.ds__extract_quarter
                  , subq_14.ds__extract_month
                  , subq_14.ds__extract_day
                  , subq_14.ds__extract_dow
                  , subq_14.ds__extract_doy
                  , subq_14.created_at__day
                  , subq_14.created_at__week
                  , subq_14.created_at__month
                  , subq_14.created_at__quarter
                  , subq_14.created_at__year
                  , subq_14.created_at__extract_year
                  , subq_14.created_at__extract_quarter
                  , subq_14.created_at__extract_month
                  , subq_14.created_at__extract_day
                  , subq_14.created_at__extract_dow
                  , subq_14.created_at__extract_doy
                  , subq_14.listing__ds__day
                  , subq_14.listing__ds__week
                  , subq_14.listing__ds__month
                  , subq_14.listing__ds__quarter
                  , subq_14.listing__ds__year
                  , subq_14.listing__ds__extract_year
                  , subq_14.listing__ds__extract_quarter
                  , subq_14.listing__ds__extract_month
                  , subq_14.listing__ds__extract_day
                  , subq_14.listing__ds__extract_dow
                  , subq_14.listing__ds__extract_doy
                  , subq_14.listing__created_at__day
                  , subq_14.listing__created_at__week
                  , subq_14.listing__created_at__month
                  , subq_14.listing__created_at__quarter
                  , subq_14.listing__created_at__year
                  , subq_14.listing__created_at__extract_year
                  , subq_14.listing__created_at__extract_quarter
                  , subq_14.listing__created_at__extract_month
                  , subq_14.listing__created_at__extract_day
                  , subq_14.listing__created_at__extract_dow
                  , subq_14.listing__created_at__extract_doy
                  , subq_14.ds__day AS metric_time__day
                  , subq_14.ds__week AS metric_time__week
                  , subq_14.ds__month AS metric_time__month
                  , subq_14.ds__quarter AS metric_time__quarter
                  , subq_14.ds__year AS metric_time__year
                  , subq_14.ds__extract_year AS metric_time__extract_year
                  , subq_14.ds__extract_quarter AS metric_time__extract_quarter
                  , subq_14.ds__extract_month AS metric_time__extract_month
                  , subq_14.ds__extract_day AS metric_time__extract_day
                  , subq_14.ds__extract_dow AS metric_time__extract_dow
                  , subq_14.ds__extract_doy AS metric_time__extract_doy
                  , subq_14.listing
                  , subq_14.user
                  , subq_14.listing__user
                  , subq_14.country_latest
                  , subq_14.is_lux_latest
                  , subq_14.capacity_latest
                  , subq_14.listing__country_latest
                  , subq_14.listing__is_lux_latest
                  , subq_14.listing__capacity_latest
                  , subq_14.__listings
                  , subq_14.__lux_listings
                  , subq_14.__smallest_listing
                  , subq_14.__largest_listing
                  , subq_14.__active_listings
                FROM (
                  SELECT
                    1 AS __listings
                    , 1 AS __lux_listings
                    , listings_latest_src_28000.capacity AS __smallest_listing
                    , listings_latest_src_28000.capacity AS __largest_listing
                    , 1 AS __active_listings
                    , toStartOfDay(listings_latest_src_28000.created_at) AS ds__day
                    , toStartOfWeek(listings_latest_src_28000.created_at, 1) AS ds__week
                    , toStartOfMonth(listings_latest_src_28000.created_at) AS ds__month
                    , toStartOfQuarter(listings_latest_src_28000.created_at) AS ds__quarter
                    , toStartOfYear(listings_latest_src_28000.created_at) AS ds__year
                    , toYear(listings_latest_src_28000.created_at) AS ds__extract_year
                    , toQuarter(listings_latest_src_28000.created_at) AS ds__extract_quarter
                    , toMonth(listings_latest_src_28000.created_at) AS ds__extract_month
                    , toDayOfMonth(listings_latest_src_28000.created_at) AS ds__extract_day
                    , toDayOfWeek(listings_latest_src_28000.created_at) AS ds__extract_dow
                    , toDayOfYear(listings_latest_src_28000.created_at) AS ds__extract_doy
                    , toStartOfDay(listings_latest_src_28000.created_at) AS created_at__day
                    , toStartOfWeek(listings_latest_src_28000.created_at, 1) AS created_at__week
                    , toStartOfMonth(listings_latest_src_28000.created_at) AS created_at__month
                    , toStartOfQuarter(listings_latest_src_28000.created_at) AS created_at__quarter
                    , toStartOfYear(listings_latest_src_28000.created_at) AS created_at__year
                    , toYear(listings_latest_src_28000.created_at) AS created_at__extract_year
                    , toQuarter(listings_latest_src_28000.created_at) AS created_at__extract_quarter
                    , toMonth(listings_latest_src_28000.created_at) AS created_at__extract_month
                    , toDayOfMonth(listings_latest_src_28000.created_at) AS created_at__extract_day
                    , toDayOfWeek(listings_latest_src_28000.created_at) AS created_at__extract_dow
                    , toDayOfYear(listings_latest_src_28000.created_at) AS created_at__extract_doy
                    , listings_latest_src_28000.country AS country_latest
                    , listings_latest_src_28000.is_lux AS is_lux_latest
                    , listings_latest_src_28000.capacity AS capacity_latest
                    , toStartOfDay(listings_latest_src_28000.created_at) AS listing__ds__day
                    , toStartOfWeek(listings_latest_src_28000.created_at, 1) AS listing__ds__week
                    , toStartOfMonth(listings_latest_src_28000.created_at) AS listing__ds__month
                    , toStartOfQuarter(listings_latest_src_28000.created_at) AS listing__ds__quarter
                    , toStartOfYear(listings_latest_src_28000.created_at) AS listing__ds__year
                    , toYear(listings_latest_src_28000.created_at) AS listing__ds__extract_year
                    , toQuarter(listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                    , toMonth(listings_latest_src_28000.created_at) AS listing__ds__extract_month
                    , toDayOfMonth(listings_latest_src_28000.created_at) AS listing__ds__extract_day
                    , toDayOfWeek(listings_latest_src_28000.created_at) AS listing__ds__extract_dow
                    , toDayOfYear(listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                    , toStartOfDay(listings_latest_src_28000.created_at) AS listing__created_at__day
                    , toStartOfWeek(listings_latest_src_28000.created_at, 1) AS listing__created_at__week
                    , toStartOfMonth(listings_latest_src_28000.created_at) AS listing__created_at__month
                    , toStartOfQuarter(listings_latest_src_28000.created_at) AS listing__created_at__quarter
                    , toStartOfYear(listings_latest_src_28000.created_at) AS listing__created_at__year
                    , toYear(listings_latest_src_28000.created_at) AS listing__created_at__extract_year
                    , toQuarter(listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
                    , toMonth(listings_latest_src_28000.created_at) AS listing__created_at__extract_month
                    , toDayOfMonth(listings_latest_src_28000.created_at) AS listing__created_at__extract_day
                    , toDayOfWeek(listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
                    , toDayOfYear(listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                    , listings_latest_src_28000.country AS listing__country_latest
                    , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                    , listings_latest_src_28000.capacity AS listing__capacity_latest
                    , listings_latest_src_28000.listing_id AS listing
                    , listings_latest_src_28000.user_id AS user
                    , listings_latest_src_28000.user_id AS listing__user
                  FROM ***************************.dim_listings_latest listings_latest_src_28000
                ) subq_14
              ) subq_15
              LEFT OUTER JOIN (
                SELECT
                  subq_17.ds__day
                  , subq_17.ds__week
                  , subq_17.ds__month
                  , subq_17.ds__quarter
                  , subq_17.ds__year
                  , subq_17.ds__extract_year
                  , subq_17.ds__extract_quarter
                  , subq_17.ds__extract_month
                  , subq_17.ds__extract_day
                  , subq_17.ds__extract_dow
                  , subq_17.ds__extract_doy
                  , subq_17.created_at__day
                  , subq_17.created_at__week
                  , subq_17.created_at__month
                  , subq_17.created_at__quarter
                  , subq_17.created_at__year
                  , subq_17.created_at__extract_year
                  , subq_17.created_at__extract_quarter
                  , subq_17.created_at__extract_month
                  , subq_17.created_at__extract_day
                  , subq_17.created_at__extract_dow
                  , subq_17.created_at__extract_doy
                  , subq_17.ds_partitioned__day
                  , subq_17.ds_partitioned__week
                  , subq_17.ds_partitioned__month
                  , subq_17.ds_partitioned__quarter
                  , subq_17.ds_partitioned__year
                  , subq_17.ds_partitioned__extract_year
                  , subq_17.ds_partitioned__extract_quarter
                  , subq_17.ds_partitioned__extract_month
                  , subq_17.ds_partitioned__extract_day
                  , subq_17.ds_partitioned__extract_dow
                  , subq_17.ds_partitioned__extract_doy
                  , subq_17.last_profile_edit_ts__millisecond
                  , subq_17.last_profile_edit_ts__second
                  , subq_17.last_profile_edit_ts__minute
                  , subq_17.last_profile_edit_ts__hour
                  , subq_17.last_profile_edit_ts__day
                  , subq_17.last_profile_edit_ts__week
                  , subq_17.last_profile_edit_ts__month
                  , subq_17.last_profile_edit_ts__quarter
                  , subq_17.last_profile_edit_ts__year
                  , subq_17.last_profile_edit_ts__extract_year
                  , subq_17.last_profile_edit_ts__extract_quarter
                  , subq_17.last_profile_edit_ts__extract_month
                  , subq_17.last_profile_edit_ts__extract_day
                  , subq_17.last_profile_edit_ts__extract_dow
                  , subq_17.last_profile_edit_ts__extract_doy
                  , subq_17.bio_added_ts__second
                  , subq_17.bio_added_ts__minute
                  , subq_17.bio_added_ts__hour
                  , subq_17.bio_added_ts__day
                  , subq_17.bio_added_ts__week
                  , subq_17.bio_added_ts__month
                  , subq_17.bio_added_ts__quarter
                  , subq_17.bio_added_ts__year
                  , subq_17.bio_added_ts__extract_year
                  , subq_17.bio_added_ts__extract_quarter
                  , subq_17.bio_added_ts__extract_month
                  , subq_17.bio_added_ts__extract_day
                  , subq_17.bio_added_ts__extract_dow
                  , subq_17.bio_added_ts__extract_doy
                  , subq_17.last_login_ts__minute
                  , subq_17.last_login_ts__hour
                  , subq_17.last_login_ts__day
                  , subq_17.last_login_ts__week
                  , subq_17.last_login_ts__month
                  , subq_17.last_login_ts__quarter
                  , subq_17.last_login_ts__year
                  , subq_17.last_login_ts__extract_year
                  , subq_17.last_login_ts__extract_quarter
                  , subq_17.last_login_ts__extract_month
                  , subq_17.last_login_ts__extract_day
                  , subq_17.last_login_ts__extract_dow
                  , subq_17.last_login_ts__extract_doy
                  , subq_17.archived_at__hour
                  , subq_17.archived_at__day
                  , subq_17.archived_at__week
                  , subq_17.archived_at__month
                  , subq_17.archived_at__quarter
                  , subq_17.archived_at__year
                  , subq_17.archived_at__extract_year
                  , subq_17.archived_at__extract_quarter
                  , subq_17.archived_at__extract_month
                  , subq_17.archived_at__extract_day
                  , subq_17.archived_at__extract_dow
                  , subq_17.archived_at__extract_doy
                  , subq_17.user__ds__day
                  , subq_17.user__ds__week
                  , subq_17.user__ds__month
                  , subq_17.user__ds__quarter
                  , subq_17.user__ds__year
                  , subq_17.user__ds__extract_year
                  , subq_17.user__ds__extract_quarter
                  , subq_17.user__ds__extract_month
                  , subq_17.user__ds__extract_day
                  , subq_17.user__ds__extract_dow
                  , subq_17.user__ds__extract_doy
                  , subq_17.user__created_at__day
                  , subq_17.user__created_at__week
                  , subq_17.user__created_at__month
                  , subq_17.user__created_at__quarter
                  , subq_17.user__created_at__year
                  , subq_17.user__created_at__extract_year
                  , subq_17.user__created_at__extract_quarter
                  , subq_17.user__created_at__extract_month
                  , subq_17.user__created_at__extract_day
                  , subq_17.user__created_at__extract_dow
                  , subq_17.user__created_at__extract_doy
                  , subq_17.user__ds_partitioned__day
                  , subq_17.user__ds_partitioned__week
                  , subq_17.user__ds_partitioned__month
                  , subq_17.user__ds_partitioned__quarter
                  , subq_17.user__ds_partitioned__year
                  , subq_17.user__ds_partitioned__extract_year
                  , subq_17.user__ds_partitioned__extract_quarter
                  , subq_17.user__ds_partitioned__extract_month
                  , subq_17.user__ds_partitioned__extract_day
                  , subq_17.user__ds_partitioned__extract_dow
                  , subq_17.user__ds_partitioned__extract_doy
                  , subq_17.user__last_profile_edit_ts__millisecond
                  , subq_17.user__last_profile_edit_ts__second
                  , subq_17.user__last_profile_edit_ts__minute
                  , subq_17.user__last_profile_edit_ts__hour
                  , subq_17.user__last_profile_edit_ts__day
                  , subq_17.user__last_profile_edit_ts__week
                  , subq_17.user__last_profile_edit_ts__month
                  , subq_17.user__last_profile_edit_ts__quarter
                  , subq_17.user__last_profile_edit_ts__year
                  , subq_17.user__last_profile_edit_ts__extract_year
                  , subq_17.user__last_profile_edit_ts__extract_quarter
                  , subq_17.user__last_profile_edit_ts__extract_month
                  , subq_17.user__last_profile_edit_ts__extract_day
                  , subq_17.user__last_profile_edit_ts__extract_dow
                  , subq_17.user__last_profile_edit_ts__extract_doy
                  , subq_17.user__bio_added_ts__second
                  , subq_17.user__bio_added_ts__minute
                  , subq_17.user__bio_added_ts__hour
                  , subq_17.user__bio_added_ts__day
                  , subq_17.user__bio_added_ts__week
                  , subq_17.user__bio_added_ts__month
                  , subq_17.user__bio_added_ts__quarter
                  , subq_17.user__bio_added_ts__year
                  , subq_17.user__bio_added_ts__extract_year
                  , subq_17.user__bio_added_ts__extract_quarter
                  , subq_17.user__bio_added_ts__extract_month
                  , subq_17.user__bio_added_ts__extract_day
                  , subq_17.user__bio_added_ts__extract_dow
                  , subq_17.user__bio_added_ts__extract_doy
                  , subq_17.user__last_login_ts__minute
                  , subq_17.user__last_login_ts__hour
                  , subq_17.user__last_login_ts__day
                  , subq_17.user__last_login_ts__week
                  , subq_17.user__last_login_ts__month
                  , subq_17.user__last_login_ts__quarter
                  , subq_17.user__last_login_ts__year
                  , subq_17.user__last_login_ts__extract_year
                  , subq_17.user__last_login_ts__extract_quarter
                  , subq_17.user__last_login_ts__extract_month
                  , subq_17.user__last_login_ts__extract_day
                  , subq_17.user__last_login_ts__extract_dow
                  , subq_17.user__last_login_ts__extract_doy
                  , subq_17.user__archived_at__hour
                  , subq_17.user__archived_at__day
                  , subq_17.user__archived_at__week
                  , subq_17.user__archived_at__month
                  , subq_17.user__archived_at__quarter
                  , subq_17.user__archived_at__year
                  , subq_17.user__archived_at__extract_year
                  , subq_17.user__archived_at__extract_quarter
                  , subq_17.user__archived_at__extract_month
                  , subq_17.user__archived_at__extract_day
                  , subq_17.user__archived_at__extract_dow
                  , subq_17.user__archived_at__extract_doy
                  , subq_17.metric_time__day
                  , subq_17.metric_time__week
                  , subq_17.metric_time__month
                  , subq_17.metric_time__quarter
                  , subq_17.metric_time__year
                  , subq_17.metric_time__extract_year
                  , subq_17.metric_time__extract_quarter
                  , subq_17.metric_time__extract_month
                  , subq_17.metric_time__extract_day
                  , subq_17.metric_time__extract_dow
                  , subq_17.metric_time__extract_doy
                  , subq_17.user
                  , subq_17.home_state
                  , subq_17.user__home_state
                FROM (
                  SELECT
                    subq_16.ds__day
                    , subq_16.ds__week
                    , subq_16.ds__month
                    , subq_16.ds__quarter
                    , subq_16.ds__year
                    , subq_16.ds__extract_year
                    , subq_16.ds__extract_quarter
                    , subq_16.ds__extract_month
                    , subq_16.ds__extract_day
                    , subq_16.ds__extract_dow
                    , subq_16.ds__extract_doy
                    , subq_16.created_at__day
                    , subq_16.created_at__week
                    , subq_16.created_at__month
                    , subq_16.created_at__quarter
                    , subq_16.created_at__year
                    , subq_16.created_at__extract_year
                    , subq_16.created_at__extract_quarter
                    , subq_16.created_at__extract_month
                    , subq_16.created_at__extract_day
                    , subq_16.created_at__extract_dow
                    , subq_16.created_at__extract_doy
                    , subq_16.ds_partitioned__day
                    , subq_16.ds_partitioned__week
                    , subq_16.ds_partitioned__month
                    , subq_16.ds_partitioned__quarter
                    , subq_16.ds_partitioned__year
                    , subq_16.ds_partitioned__extract_year
                    , subq_16.ds_partitioned__extract_quarter
                    , subq_16.ds_partitioned__extract_month
                    , subq_16.ds_partitioned__extract_day
                    , subq_16.ds_partitioned__extract_dow
                    , subq_16.ds_partitioned__extract_doy
                    , subq_16.last_profile_edit_ts__millisecond
                    , subq_16.last_profile_edit_ts__second
                    , subq_16.last_profile_edit_ts__minute
                    , subq_16.last_profile_edit_ts__hour
                    , subq_16.last_profile_edit_ts__day
                    , subq_16.last_profile_edit_ts__week
                    , subq_16.last_profile_edit_ts__month
                    , subq_16.last_profile_edit_ts__quarter
                    , subq_16.last_profile_edit_ts__year
                    , subq_16.last_profile_edit_ts__extract_year
                    , subq_16.last_profile_edit_ts__extract_quarter
                    , subq_16.last_profile_edit_ts__extract_month
                    , subq_16.last_profile_edit_ts__extract_day
                    , subq_16.last_profile_edit_ts__extract_dow
                    , subq_16.last_profile_edit_ts__extract_doy
                    , subq_16.bio_added_ts__second
                    , subq_16.bio_added_ts__minute
                    , subq_16.bio_added_ts__hour
                    , subq_16.bio_added_ts__day
                    , subq_16.bio_added_ts__week
                    , subq_16.bio_added_ts__month
                    , subq_16.bio_added_ts__quarter
                    , subq_16.bio_added_ts__year
                    , subq_16.bio_added_ts__extract_year
                    , subq_16.bio_added_ts__extract_quarter
                    , subq_16.bio_added_ts__extract_month
                    , subq_16.bio_added_ts__extract_day
                    , subq_16.bio_added_ts__extract_dow
                    , subq_16.bio_added_ts__extract_doy
                    , subq_16.last_login_ts__minute
                    , subq_16.last_login_ts__hour
                    , subq_16.last_login_ts__day
                    , subq_16.last_login_ts__week
                    , subq_16.last_login_ts__month
                    , subq_16.last_login_ts__quarter
                    , subq_16.last_login_ts__year
                    , subq_16.last_login_ts__extract_year
                    , subq_16.last_login_ts__extract_quarter
                    , subq_16.last_login_ts__extract_month
                    , subq_16.last_login_ts__extract_day
                    , subq_16.last_login_ts__extract_dow
                    , subq_16.last_login_ts__extract_doy
                    , subq_16.archived_at__hour
                    , subq_16.archived_at__day
                    , subq_16.archived_at__week
                    , subq_16.archived_at__month
                    , subq_16.archived_at__quarter
                    , subq_16.archived_at__year
                    , subq_16.archived_at__extract_year
                    , subq_16.archived_at__extract_quarter
                    , subq_16.archived_at__extract_month
                    , subq_16.archived_at__extract_day
                    , subq_16.archived_at__extract_dow
                    , subq_16.archived_at__extract_doy
                    , subq_16.user__ds__day
                    , subq_16.user__ds__week
                    , subq_16.user__ds__month
                    , subq_16.user__ds__quarter
                    , subq_16.user__ds__year
                    , subq_16.user__ds__extract_year
                    , subq_16.user__ds__extract_quarter
                    , subq_16.user__ds__extract_month
                    , subq_16.user__ds__extract_day
                    , subq_16.user__ds__extract_dow
                    , subq_16.user__ds__extract_doy
                    , subq_16.user__created_at__day
                    , subq_16.user__created_at__week
                    , subq_16.user__created_at__month
                    , subq_16.user__created_at__quarter
                    , subq_16.user__created_at__year
                    , subq_16.user__created_at__extract_year
                    , subq_16.user__created_at__extract_quarter
                    , subq_16.user__created_at__extract_month
                    , subq_16.user__created_at__extract_day
                    , subq_16.user__created_at__extract_dow
                    , subq_16.user__created_at__extract_doy
                    , subq_16.user__ds_partitioned__day
                    , subq_16.user__ds_partitioned__week
                    , subq_16.user__ds_partitioned__month
                    , subq_16.user__ds_partitioned__quarter
                    , subq_16.user__ds_partitioned__year
                    , subq_16.user__ds_partitioned__extract_year
                    , subq_16.user__ds_partitioned__extract_quarter
                    , subq_16.user__ds_partitioned__extract_month
                    , subq_16.user__ds_partitioned__extract_day
                    , subq_16.user__ds_partitioned__extract_dow
                    , subq_16.user__ds_partitioned__extract_doy
                    , subq_16.user__last_profile_edit_ts__millisecond
                    , subq_16.user__last_profile_edit_ts__second
                    , subq_16.user__last_profile_edit_ts__minute
                    , subq_16.user__last_profile_edit_ts__hour
                    , subq_16.user__last_profile_edit_ts__day
                    , subq_16.user__last_profile_edit_ts__week
                    , subq_16.user__last_profile_edit_ts__month
                    , subq_16.user__last_profile_edit_ts__quarter
                    , subq_16.user__last_profile_edit_ts__year
                    , subq_16.user__last_profile_edit_ts__extract_year
                    , subq_16.user__last_profile_edit_ts__extract_quarter
                    , subq_16.user__last_profile_edit_ts__extract_month
                    , subq_16.user__last_profile_edit_ts__extract_day
                    , subq_16.user__last_profile_edit_ts__extract_dow
                    , subq_16.user__last_profile_edit_ts__extract_doy
                    , subq_16.user__bio_added_ts__second
                    , subq_16.user__bio_added_ts__minute
                    , subq_16.user__bio_added_ts__hour
                    , subq_16.user__bio_added_ts__day
                    , subq_16.user__bio_added_ts__week
                    , subq_16.user__bio_added_ts__month
                    , subq_16.user__bio_added_ts__quarter
                    , subq_16.user__bio_added_ts__year
                    , subq_16.user__bio_added_ts__extract_year
                    , subq_16.user__bio_added_ts__extract_quarter
                    , subq_16.user__bio_added_ts__extract_month
                    , subq_16.user__bio_added_ts__extract_day
                    , subq_16.user__bio_added_ts__extract_dow
                    , subq_16.user__bio_added_ts__extract_doy
                    , subq_16.user__last_login_ts__minute
                    , subq_16.user__last_login_ts__hour
                    , subq_16.user__last_login_ts__day
                    , subq_16.user__last_login_ts__week
                    , subq_16.user__last_login_ts__month
                    , subq_16.user__last_login_ts__quarter
                    , subq_16.user__last_login_ts__year
                    , subq_16.user__last_login_ts__extract_year
                    , subq_16.user__last_login_ts__extract_quarter
                    , subq_16.user__last_login_ts__extract_month
                    , subq_16.user__last_login_ts__extract_day
                    , subq_16.user__last_login_ts__extract_dow
                    , subq_16.user__last_login_ts__extract_doy
                    , subq_16.user__archived_at__hour
                    , subq_16.user__archived_at__day
                    , subq_16.user__archived_at__week
                    , subq_16.user__archived_at__month
                    , subq_16.user__archived_at__quarter
                    , subq_16.user__archived_at__year
                    , subq_16.user__archived_at__extract_year
                    , subq_16.user__archived_at__extract_quarter
                    , subq_16.user__archived_at__extract_month
                    , subq_16.user__archived_at__extract_day
                    , subq_16.user__archived_at__extract_dow
                    , subq_16.user__archived_at__extract_doy
                    , subq_16.created_at__day AS metric_time__day
                    , subq_16.created_at__week AS metric_time__week
                    , subq_16.created_at__month AS metric_time__month
                    , subq_16.created_at__quarter AS metric_time__quarter
                    , subq_16.created_at__year AS metric_time__year
                    , subq_16.created_at__extract_year AS metric_time__extract_year
                    , subq_16.created_at__extract_quarter AS metric_time__extract_quarter
                    , subq_16.created_at__extract_month AS metric_time__extract_month
                    , subq_16.created_at__extract_day AS metric_time__extract_day
                    , subq_16.created_at__extract_dow AS metric_time__extract_dow
                    , subq_16.created_at__extract_doy AS metric_time__extract_doy
                    , subq_16.user
                    , subq_16.home_state
                    , subq_16.user__home_state
                    , subq_16.__new_users
                  FROM (
                    SELECT
                      1 AS __subdaily_join_to_time_spine_metric
                      , 1 AS __simple_subdaily_metric_default_day
                      , 1 AS __simple_subdaily_metric_default_hour
                      , 1 AS __archived_users_join_to_time_spine
                      , 1 AS __archived_users
                      , 1 AS __new_users
                      , toStartOfDay(users_ds_source_src_28000.ds) AS ds__day
                      , toStartOfWeek(users_ds_source_src_28000.ds, 1) AS ds__week
                      , toStartOfMonth(users_ds_source_src_28000.ds) AS ds__month
                      , toStartOfQuarter(users_ds_source_src_28000.ds) AS ds__quarter
                      , toStartOfYear(users_ds_source_src_28000.ds) AS ds__year
                      , toYear(users_ds_source_src_28000.ds) AS ds__extract_year
                      , toQuarter(users_ds_source_src_28000.ds) AS ds__extract_quarter
                      , toMonth(users_ds_source_src_28000.ds) AS ds__extract_month
                      , toDayOfMonth(users_ds_source_src_28000.ds) AS ds__extract_day
                      , toDayOfWeek(users_ds_source_src_28000.ds) AS ds__extract_dow
                      , toDayOfYear(users_ds_source_src_28000.ds) AS ds__extract_doy
                      , toStartOfDay(users_ds_source_src_28000.created_at) AS created_at__day
                      , toStartOfWeek(users_ds_source_src_28000.created_at, 1) AS created_at__week
                      , toStartOfMonth(users_ds_source_src_28000.created_at) AS created_at__month
                      , toStartOfQuarter(users_ds_source_src_28000.created_at) AS created_at__quarter
                      , toStartOfYear(users_ds_source_src_28000.created_at) AS created_at__year
                      , toYear(users_ds_source_src_28000.created_at) AS created_at__extract_year
                      , toQuarter(users_ds_source_src_28000.created_at) AS created_at__extract_quarter
                      , toMonth(users_ds_source_src_28000.created_at) AS created_at__extract_month
                      , toDayOfMonth(users_ds_source_src_28000.created_at) AS created_at__extract_day
                      , toDayOfWeek(users_ds_source_src_28000.created_at) AS created_at__extract_dow
                      , toDayOfYear(users_ds_source_src_28000.created_at) AS created_at__extract_doy
                      , toStartOfDay(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__day
                      , toStartOfWeek(users_ds_source_src_28000.ds_partitioned, 1) AS ds_partitioned__week
                      , toStartOfMonth(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__month
                      , toStartOfQuarter(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                      , toStartOfYear(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__year
                      , toYear(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                      , toQuarter(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                      , toMonth(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                      , toDayOfMonth(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                      , toDayOfWeek(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
                      , toDayOfYear(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                      , users_ds_source_src_28000.home_state
                      , toStartOfMillisecond(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__millisecond
                      , toStartOfSecond(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__second
                      , toStartOfMinute(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__minute
                      , toStartOfHour(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__hour
                      , toStartOfDay(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__day
                      , toStartOfWeek(users_ds_source_src_28000.last_profile_edit_ts, 1) AS last_profile_edit_ts__week
                      , toStartOfMonth(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__month
                      , toStartOfQuarter(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__quarter
                      , toStartOfYear(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__year
                      , toYear(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_year
                      , toQuarter(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_quarter
                      , toMonth(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_month
                      , toDayOfMonth(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_day
                      , toDayOfWeek(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_dow
                      , toDayOfYear(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_doy
                      , toStartOfSecond(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__second
                      , toStartOfMinute(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__minute
                      , toStartOfHour(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__hour
                      , toStartOfDay(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__day
                      , toStartOfWeek(users_ds_source_src_28000.bio_added_ts, 1) AS bio_added_ts__week
                      , toStartOfMonth(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__month
                      , toStartOfQuarter(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__quarter
                      , toStartOfYear(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__year
                      , toYear(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_year
                      , toQuarter(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_quarter
                      , toMonth(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_month
                      , toDayOfMonth(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_day
                      , toDayOfWeek(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_dow
                      , toDayOfYear(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_doy
                      , toStartOfMinute(users_ds_source_src_28000.last_login_ts) AS last_login_ts__minute
                      , toStartOfHour(users_ds_source_src_28000.last_login_ts) AS last_login_ts__hour
                      , toStartOfDay(users_ds_source_src_28000.last_login_ts) AS last_login_ts__day
                      , toStartOfWeek(users_ds_source_src_28000.last_login_ts, 1) AS last_login_ts__week
                      , toStartOfMonth(users_ds_source_src_28000.last_login_ts) AS last_login_ts__month
                      , toStartOfQuarter(users_ds_source_src_28000.last_login_ts) AS last_login_ts__quarter
                      , toStartOfYear(users_ds_source_src_28000.last_login_ts) AS last_login_ts__year
                      , toYear(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_year
                      , toQuarter(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_quarter
                      , toMonth(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_month
                      , toDayOfMonth(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_day
                      , toDayOfWeek(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_dow
                      , toDayOfYear(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_doy
                      , toStartOfHour(users_ds_source_src_28000.archived_at) AS archived_at__hour
                      , toStartOfDay(users_ds_source_src_28000.archived_at) AS archived_at__day
                      , toStartOfWeek(users_ds_source_src_28000.archived_at, 1) AS archived_at__week
                      , toStartOfMonth(users_ds_source_src_28000.archived_at) AS archived_at__month
                      , toStartOfQuarter(users_ds_source_src_28000.archived_at) AS archived_at__quarter
                      , toStartOfYear(users_ds_source_src_28000.archived_at) AS archived_at__year
                      , toYear(users_ds_source_src_28000.archived_at) AS archived_at__extract_year
                      , toQuarter(users_ds_source_src_28000.archived_at) AS archived_at__extract_quarter
                      , toMonth(users_ds_source_src_28000.archived_at) AS archived_at__extract_month
                      , toDayOfMonth(users_ds_source_src_28000.archived_at) AS archived_at__extract_day
                      , toDayOfWeek(users_ds_source_src_28000.archived_at) AS archived_at__extract_dow
                      , toDayOfYear(users_ds_source_src_28000.archived_at) AS archived_at__extract_doy
                      , toStartOfDay(users_ds_source_src_28000.ds) AS user__ds__day
                      , toStartOfWeek(users_ds_source_src_28000.ds, 1) AS user__ds__week
                      , toStartOfMonth(users_ds_source_src_28000.ds) AS user__ds__month
                      , toStartOfQuarter(users_ds_source_src_28000.ds) AS user__ds__quarter
                      , toStartOfYear(users_ds_source_src_28000.ds) AS user__ds__year
                      , toYear(users_ds_source_src_28000.ds) AS user__ds__extract_year
                      , toQuarter(users_ds_source_src_28000.ds) AS user__ds__extract_quarter
                      , toMonth(users_ds_source_src_28000.ds) AS user__ds__extract_month
                      , toDayOfMonth(users_ds_source_src_28000.ds) AS user__ds__extract_day
                      , toDayOfWeek(users_ds_source_src_28000.ds) AS user__ds__extract_dow
                      , toDayOfYear(users_ds_source_src_28000.ds) AS user__ds__extract_doy
                      , toStartOfDay(users_ds_source_src_28000.created_at) AS user__created_at__day
                      , toStartOfWeek(users_ds_source_src_28000.created_at, 1) AS user__created_at__week
                      , toStartOfMonth(users_ds_source_src_28000.created_at) AS user__created_at__month
                      , toStartOfQuarter(users_ds_source_src_28000.created_at) AS user__created_at__quarter
                      , toStartOfYear(users_ds_source_src_28000.created_at) AS user__created_at__year
                      , toYear(users_ds_source_src_28000.created_at) AS user__created_at__extract_year
                      , toQuarter(users_ds_source_src_28000.created_at) AS user__created_at__extract_quarter
                      , toMonth(users_ds_source_src_28000.created_at) AS user__created_at__extract_month
                      , toDayOfMonth(users_ds_source_src_28000.created_at) AS user__created_at__extract_day
                      , toDayOfWeek(users_ds_source_src_28000.created_at) AS user__created_at__extract_dow
                      , toDayOfYear(users_ds_source_src_28000.created_at) AS user__created_at__extract_doy
                      , toStartOfDay(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__day
                      , toStartOfWeek(users_ds_source_src_28000.ds_partitioned, 1) AS user__ds_partitioned__week
                      , toStartOfMonth(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__month
                      , toStartOfQuarter(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__quarter
                      , toStartOfYear(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__year
                      , toYear(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_year
                      , toQuarter(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_quarter
                      , toMonth(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_month
                      , toDayOfMonth(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_day
                      , toDayOfWeek(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_dow
                      , toDayOfYear(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_doy
                      , users_ds_source_src_28000.home_state AS user__home_state
                      , toStartOfMillisecond(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__millisecond
                      , toStartOfSecond(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__second
                      , toStartOfMinute(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__minute
                      , toStartOfHour(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__hour
                      , toStartOfDay(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__day
                      , toStartOfWeek(users_ds_source_src_28000.last_profile_edit_ts, 1) AS user__last_profile_edit_ts__week
                      , toStartOfMonth(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__month
                      , toStartOfQuarter(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__quarter
                      , toStartOfYear(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__year
                      , toYear(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_year
                      , toQuarter(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_quarter
                      , toMonth(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_month
                      , toDayOfMonth(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_day
                      , toDayOfWeek(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_dow
                      , toDayOfYear(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_doy
                      , toStartOfSecond(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__second
                      , toStartOfMinute(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__minute
                      , toStartOfHour(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__hour
                      , toStartOfDay(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__day
                      , toStartOfWeek(users_ds_source_src_28000.bio_added_ts, 1) AS user__bio_added_ts__week
                      , toStartOfMonth(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__month
                      , toStartOfQuarter(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__quarter
                      , toStartOfYear(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__year
                      , toYear(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_year
                      , toQuarter(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_quarter
                      , toMonth(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_month
                      , toDayOfMonth(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_day
                      , toDayOfWeek(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_dow
                      , toDayOfYear(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_doy
                      , toStartOfMinute(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__minute
                      , toStartOfHour(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__hour
                      , toStartOfDay(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__day
                      , toStartOfWeek(users_ds_source_src_28000.last_login_ts, 1) AS user__last_login_ts__week
                      , toStartOfMonth(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__month
                      , toStartOfQuarter(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__quarter
                      , toStartOfYear(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__year
                      , toYear(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_year
                      , toQuarter(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_quarter
                      , toMonth(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_month
                      , toDayOfMonth(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_day
                      , toDayOfWeek(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_dow
                      , toDayOfYear(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_doy
                      , toStartOfHour(users_ds_source_src_28000.archived_at) AS user__archived_at__hour
                      , toStartOfDay(users_ds_source_src_28000.archived_at) AS user__archived_at__day
                      , toStartOfWeek(users_ds_source_src_28000.archived_at, 1) AS user__archived_at__week
                      , toStartOfMonth(users_ds_source_src_28000.archived_at) AS user__archived_at__month
                      , toStartOfQuarter(users_ds_source_src_28000.archived_at) AS user__archived_at__quarter
                      , toStartOfYear(users_ds_source_src_28000.archived_at) AS user__archived_at__year
                      , toYear(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_year
                      , toQuarter(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_quarter
                      , toMonth(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_month
                      , toDayOfMonth(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_day
                      , toDayOfWeek(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_dow
                      , toDayOfYear(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_doy
                      , users_ds_source_src_28000.user_id AS user
                    FROM ***************************.dim_users users_ds_source_src_28000
                  ) subq_16
                ) subq_17
              ) subq_18
              ON
                subq_15.user = subq_18.user
            ) subq_19
          ) subq_20
          ON
            (
              subq_13.listing = subq_20.listing
            ) AND (
              subq_13.ds_partitioned__day = subq_20.user__ds_partitioned__day
            )
        ) subq_21
      ) subq_22
    ) subq_23
    GROUP BY
      subq_23.listing__user__bio_added_ts__minute
  ) subq_24
) subq_25
