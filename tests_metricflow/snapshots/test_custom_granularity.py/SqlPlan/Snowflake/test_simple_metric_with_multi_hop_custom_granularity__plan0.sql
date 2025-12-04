test_name: test_simple_metric_with_multi_hop_custom_granularity
test_filename: test_custom_granularity.py
docstring:
  Test simple metric with a multi hop dimension and custom grain.
sql_engine: Snowflake
---
-- Write to DataTable
SELECT
  subq_38.listing__user__ds__alien_day
  , subq_38.bookings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_37.listing__user__ds__alien_day
    , subq_37.__bookings AS bookings
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_36.listing__user__ds__alien_day
      , SUM(subq_36.__bookings) AS __bookings
    FROM (
      -- Pass Only Elements: ['__bookings', 'listing__user__ds__alien_day']
      SELECT
        subq_35.listing__user__ds__alien_day
        , subq_35.__bookings
      FROM (
        -- Pass Only Elements: ['__bookings', 'listing__user__ds__alien_day']
        SELECT
          subq_34.listing__user__ds__alien_day
          , subq_34.__bookings
        FROM (
          -- Join Standard Outputs
          -- Join to Custom Granularity Dataset
          SELECT
            subq_32.user__ds__day AS listing__user__ds__day
            , subq_32.user__ds_partitioned__day AS listing__user__ds_partitioned__day
            , subq_25.ds__day AS ds__day
            , subq_25.ds__week AS ds__week
            , subq_25.ds__month AS ds__month
            , subq_25.ds__quarter AS ds__quarter
            , subq_25.ds__year AS ds__year
            , subq_25.ds__extract_year AS ds__extract_year
            , subq_25.ds__extract_quarter AS ds__extract_quarter
            , subq_25.ds__extract_month AS ds__extract_month
            , subq_25.ds__extract_day AS ds__extract_day
            , subq_25.ds__extract_dow AS ds__extract_dow
            , subq_25.ds__extract_doy AS ds__extract_doy
            , subq_25.ds_partitioned__day AS ds_partitioned__day
            , subq_25.ds_partitioned__week AS ds_partitioned__week
            , subq_25.ds_partitioned__month AS ds_partitioned__month
            , subq_25.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_25.ds_partitioned__year AS ds_partitioned__year
            , subq_25.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , subq_25.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , subq_25.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , subq_25.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , subq_25.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , subq_25.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , subq_25.paid_at__day AS paid_at__day
            , subq_25.paid_at__week AS paid_at__week
            , subq_25.paid_at__month AS paid_at__month
            , subq_25.paid_at__quarter AS paid_at__quarter
            , subq_25.paid_at__year AS paid_at__year
            , subq_25.paid_at__extract_year AS paid_at__extract_year
            , subq_25.paid_at__extract_quarter AS paid_at__extract_quarter
            , subq_25.paid_at__extract_month AS paid_at__extract_month
            , subq_25.paid_at__extract_day AS paid_at__extract_day
            , subq_25.paid_at__extract_dow AS paid_at__extract_dow
            , subq_25.paid_at__extract_doy AS paid_at__extract_doy
            , subq_25.booking__ds__day AS booking__ds__day
            , subq_25.booking__ds__week AS booking__ds__week
            , subq_25.booking__ds__month AS booking__ds__month
            , subq_25.booking__ds__quarter AS booking__ds__quarter
            , subq_25.booking__ds__year AS booking__ds__year
            , subq_25.booking__ds__extract_year AS booking__ds__extract_year
            , subq_25.booking__ds__extract_quarter AS booking__ds__extract_quarter
            , subq_25.booking__ds__extract_month AS booking__ds__extract_month
            , subq_25.booking__ds__extract_day AS booking__ds__extract_day
            , subq_25.booking__ds__extract_dow AS booking__ds__extract_dow
            , subq_25.booking__ds__extract_doy AS booking__ds__extract_doy
            , subq_25.booking__ds_partitioned__day AS booking__ds_partitioned__day
            , subq_25.booking__ds_partitioned__week AS booking__ds_partitioned__week
            , subq_25.booking__ds_partitioned__month AS booking__ds_partitioned__month
            , subq_25.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
            , subq_25.booking__ds_partitioned__year AS booking__ds_partitioned__year
            , subq_25.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
            , subq_25.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
            , subq_25.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
            , subq_25.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
            , subq_25.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
            , subq_25.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
            , subq_25.booking__paid_at__day AS booking__paid_at__day
            , subq_25.booking__paid_at__week AS booking__paid_at__week
            , subq_25.booking__paid_at__month AS booking__paid_at__month
            , subq_25.booking__paid_at__quarter AS booking__paid_at__quarter
            , subq_25.booking__paid_at__year AS booking__paid_at__year
            , subq_25.booking__paid_at__extract_year AS booking__paid_at__extract_year
            , subq_25.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
            , subq_25.booking__paid_at__extract_month AS booking__paid_at__extract_month
            , subq_25.booking__paid_at__extract_day AS booking__paid_at__extract_day
            , subq_25.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
            , subq_25.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
            , subq_25.metric_time__day AS metric_time__day
            , subq_25.metric_time__week AS metric_time__week
            , subq_25.metric_time__month AS metric_time__month
            , subq_25.metric_time__quarter AS metric_time__quarter
            , subq_25.metric_time__year AS metric_time__year
            , subq_25.metric_time__extract_year AS metric_time__extract_year
            , subq_25.metric_time__extract_quarter AS metric_time__extract_quarter
            , subq_25.metric_time__extract_month AS metric_time__extract_month
            , subq_25.metric_time__extract_day AS metric_time__extract_day
            , subq_25.metric_time__extract_dow AS metric_time__extract_dow
            , subq_25.metric_time__extract_doy AS metric_time__extract_doy
            , subq_25.listing AS listing
            , subq_25.guest AS guest
            , subq_25.host AS host
            , subq_25.booking__listing AS booking__listing
            , subq_25.booking__guest AS booking__guest
            , subq_25.booking__host AS booking__host
            , subq_25.is_instant AS is_instant
            , subq_25.booking__is_instant AS booking__is_instant
            , subq_25.__bookings AS __bookings
            , subq_25.__average_booking_value AS __average_booking_value
            , subq_25.__instant_bookings AS __instant_bookings
            , subq_25.__booking_value AS __booking_value
            , subq_25.__max_booking_value AS __max_booking_value
            , subq_25.__min_booking_value AS __min_booking_value
            , subq_25.__instant_booking_value AS __instant_booking_value
            , subq_25.__average_instant_booking_value AS __average_instant_booking_value
            , subq_25.__booking_value_for_non_null_listing_id AS __booking_value_for_non_null_listing_id
            , subq_25.__bookers AS __bookers
            , subq_25.__referred_bookings AS __referred_bookings
            , subq_25.__median_booking_value AS __median_booking_value
            , subq_25.__booking_value_p99 AS __booking_value_p99
            , subq_25.__discrete_booking_value_p99 AS __discrete_booking_value_p99
            , subq_25.__approximate_continuous_booking_value_p99 AS __approximate_continuous_booking_value_p99
            , subq_25.__approximate_discrete_booking_value_p99 AS __approximate_discrete_booking_value_p99
            , subq_25.__bookings_join_to_time_spine AS __bookings_join_to_time_spine
            , subq_25.__bookings_fill_nulls_with_0_without_time_spine AS __bookings_fill_nulls_with_0_without_time_spine
            , subq_25.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
            , subq_25.__instant_bookings_with_measure_filter AS __instant_bookings_with_measure_filter
            , subq_25.__bookings_join_to_time_spine_with_tiered_filters AS __bookings_join_to_time_spine_with_tiered_filters
            , subq_25.__bookers_fill_nulls_with_0_join_to_timespine AS __bookers_fill_nulls_with_0_join_to_timespine
            , subq_33.alien_day AS listing__user__ds__alien_day
          FROM (
            -- Metric Time Dimension 'ds'
            SELECT
              subq_24.ds__day
              , subq_24.ds__week
              , subq_24.ds__month
              , subq_24.ds__quarter
              , subq_24.ds__year
              , subq_24.ds__extract_year
              , subq_24.ds__extract_quarter
              , subq_24.ds__extract_month
              , subq_24.ds__extract_day
              , subq_24.ds__extract_dow
              , subq_24.ds__extract_doy
              , subq_24.ds_partitioned__day
              , subq_24.ds_partitioned__week
              , subq_24.ds_partitioned__month
              , subq_24.ds_partitioned__quarter
              , subq_24.ds_partitioned__year
              , subq_24.ds_partitioned__extract_year
              , subq_24.ds_partitioned__extract_quarter
              , subq_24.ds_partitioned__extract_month
              , subq_24.ds_partitioned__extract_day
              , subq_24.ds_partitioned__extract_dow
              , subq_24.ds_partitioned__extract_doy
              , subq_24.paid_at__day
              , subq_24.paid_at__week
              , subq_24.paid_at__month
              , subq_24.paid_at__quarter
              , subq_24.paid_at__year
              , subq_24.paid_at__extract_year
              , subq_24.paid_at__extract_quarter
              , subq_24.paid_at__extract_month
              , subq_24.paid_at__extract_day
              , subq_24.paid_at__extract_dow
              , subq_24.paid_at__extract_doy
              , subq_24.booking__ds__day
              , subq_24.booking__ds__week
              , subq_24.booking__ds__month
              , subq_24.booking__ds__quarter
              , subq_24.booking__ds__year
              , subq_24.booking__ds__extract_year
              , subq_24.booking__ds__extract_quarter
              , subq_24.booking__ds__extract_month
              , subq_24.booking__ds__extract_day
              , subq_24.booking__ds__extract_dow
              , subq_24.booking__ds__extract_doy
              , subq_24.booking__ds_partitioned__day
              , subq_24.booking__ds_partitioned__week
              , subq_24.booking__ds_partitioned__month
              , subq_24.booking__ds_partitioned__quarter
              , subq_24.booking__ds_partitioned__year
              , subq_24.booking__ds_partitioned__extract_year
              , subq_24.booking__ds_partitioned__extract_quarter
              , subq_24.booking__ds_partitioned__extract_month
              , subq_24.booking__ds_partitioned__extract_day
              , subq_24.booking__ds_partitioned__extract_dow
              , subq_24.booking__ds_partitioned__extract_doy
              , subq_24.booking__paid_at__day
              , subq_24.booking__paid_at__week
              , subq_24.booking__paid_at__month
              , subq_24.booking__paid_at__quarter
              , subq_24.booking__paid_at__year
              , subq_24.booking__paid_at__extract_year
              , subq_24.booking__paid_at__extract_quarter
              , subq_24.booking__paid_at__extract_month
              , subq_24.booking__paid_at__extract_day
              , subq_24.booking__paid_at__extract_dow
              , subq_24.booking__paid_at__extract_doy
              , subq_24.ds__day AS metric_time__day
              , subq_24.ds__week AS metric_time__week
              , subq_24.ds__month AS metric_time__month
              , subq_24.ds__quarter AS metric_time__quarter
              , subq_24.ds__year AS metric_time__year
              , subq_24.ds__extract_year AS metric_time__extract_year
              , subq_24.ds__extract_quarter AS metric_time__extract_quarter
              , subq_24.ds__extract_month AS metric_time__extract_month
              , subq_24.ds__extract_day AS metric_time__extract_day
              , subq_24.ds__extract_dow AS metric_time__extract_dow
              , subq_24.ds__extract_doy AS metric_time__extract_doy
              , subq_24.listing
              , subq_24.guest
              , subq_24.host
              , subq_24.booking__listing
              , subq_24.booking__guest
              , subq_24.booking__host
              , subq_24.is_instant
              , subq_24.booking__is_instant
              , subq_24.__bookings
              , subq_24.__average_booking_value
              , subq_24.__instant_bookings
              , subq_24.__booking_value
              , subq_24.__max_booking_value
              , subq_24.__min_booking_value
              , subq_24.__instant_booking_value
              , subq_24.__average_instant_booking_value
              , subq_24.__booking_value_for_non_null_listing_id
              , subq_24.__bookers
              , subq_24.__referred_bookings
              , subq_24.__median_booking_value
              , subq_24.__booking_value_p99
              , subq_24.__discrete_booking_value_p99
              , subq_24.__approximate_continuous_booking_value_p99
              , subq_24.__approximate_discrete_booking_value_p99
              , subq_24.__bookings_join_to_time_spine
              , subq_24.__bookings_fill_nulls_with_0_without_time_spine
              , subq_24.__bookings_fill_nulls_with_0
              , subq_24.__instant_bookings_with_measure_filter
              , subq_24.__bookings_join_to_time_spine_with_tiered_filters
              , subq_24.__bookers_fill_nulls_with_0_join_to_timespine
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
            ) subq_24
          ) subq_25
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['user__ds_partitioned__day', 'user__ds__day', 'listing']
            SELECT
              subq_31.user__ds__day
              , subq_31.user__ds_partitioned__day
              , subq_31.listing
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_30.home_state AS user__home_state
                , subq_30.ds__day AS user__ds__day
                , subq_30.ds__week AS user__ds__week
                , subq_30.ds__month AS user__ds__month
                , subq_30.ds__quarter AS user__ds__quarter
                , subq_30.ds__year AS user__ds__year
                , subq_30.ds__extract_year AS user__ds__extract_year
                , subq_30.ds__extract_quarter AS user__ds__extract_quarter
                , subq_30.ds__extract_month AS user__ds__extract_month
                , subq_30.ds__extract_day AS user__ds__extract_day
                , subq_30.ds__extract_dow AS user__ds__extract_dow
                , subq_30.ds__extract_doy AS user__ds__extract_doy
                , subq_30.created_at__day AS user__created_at__day
                , subq_30.created_at__week AS user__created_at__week
                , subq_30.created_at__month AS user__created_at__month
                , subq_30.created_at__quarter AS user__created_at__quarter
                , subq_30.created_at__year AS user__created_at__year
                , subq_30.created_at__extract_year AS user__created_at__extract_year
                , subq_30.created_at__extract_quarter AS user__created_at__extract_quarter
                , subq_30.created_at__extract_month AS user__created_at__extract_month
                , subq_30.created_at__extract_day AS user__created_at__extract_day
                , subq_30.created_at__extract_dow AS user__created_at__extract_dow
                , subq_30.created_at__extract_doy AS user__created_at__extract_doy
                , subq_30.ds_partitioned__day AS user__ds_partitioned__day
                , subq_30.ds_partitioned__week AS user__ds_partitioned__week
                , subq_30.ds_partitioned__month AS user__ds_partitioned__month
                , subq_30.ds_partitioned__quarter AS user__ds_partitioned__quarter
                , subq_30.ds_partitioned__year AS user__ds_partitioned__year
                , subq_30.ds_partitioned__extract_year AS user__ds_partitioned__extract_year
                , subq_30.ds_partitioned__extract_quarter AS user__ds_partitioned__extract_quarter
                , subq_30.ds_partitioned__extract_month AS user__ds_partitioned__extract_month
                , subq_30.ds_partitioned__extract_day AS user__ds_partitioned__extract_day
                , subq_30.ds_partitioned__extract_dow AS user__ds_partitioned__extract_dow
                , subq_30.ds_partitioned__extract_doy AS user__ds_partitioned__extract_doy
                , subq_30.last_profile_edit_ts__millisecond AS user__last_profile_edit_ts__millisecond
                , subq_30.last_profile_edit_ts__second AS user__last_profile_edit_ts__second
                , subq_30.last_profile_edit_ts__minute AS user__last_profile_edit_ts__minute
                , subq_30.last_profile_edit_ts__hour AS user__last_profile_edit_ts__hour
                , subq_30.last_profile_edit_ts__day AS user__last_profile_edit_ts__day
                , subq_30.last_profile_edit_ts__week AS user__last_profile_edit_ts__week
                , subq_30.last_profile_edit_ts__month AS user__last_profile_edit_ts__month
                , subq_30.last_profile_edit_ts__quarter AS user__last_profile_edit_ts__quarter
                , subq_30.last_profile_edit_ts__year AS user__last_profile_edit_ts__year
                , subq_30.last_profile_edit_ts__extract_year AS user__last_profile_edit_ts__extract_year
                , subq_30.last_profile_edit_ts__extract_quarter AS user__last_profile_edit_ts__extract_quarter
                , subq_30.last_profile_edit_ts__extract_month AS user__last_profile_edit_ts__extract_month
                , subq_30.last_profile_edit_ts__extract_day AS user__last_profile_edit_ts__extract_day
                , subq_30.last_profile_edit_ts__extract_dow AS user__last_profile_edit_ts__extract_dow
                , subq_30.last_profile_edit_ts__extract_doy AS user__last_profile_edit_ts__extract_doy
                , subq_30.bio_added_ts__second AS user__bio_added_ts__second
                , subq_30.bio_added_ts__minute AS user__bio_added_ts__minute
                , subq_30.bio_added_ts__hour AS user__bio_added_ts__hour
                , subq_30.bio_added_ts__day AS user__bio_added_ts__day
                , subq_30.bio_added_ts__week AS user__bio_added_ts__week
                , subq_30.bio_added_ts__month AS user__bio_added_ts__month
                , subq_30.bio_added_ts__quarter AS user__bio_added_ts__quarter
                , subq_30.bio_added_ts__year AS user__bio_added_ts__year
                , subq_30.bio_added_ts__extract_year AS user__bio_added_ts__extract_year
                , subq_30.bio_added_ts__extract_quarter AS user__bio_added_ts__extract_quarter
                , subq_30.bio_added_ts__extract_month AS user__bio_added_ts__extract_month
                , subq_30.bio_added_ts__extract_day AS user__bio_added_ts__extract_day
                , subq_30.bio_added_ts__extract_dow AS user__bio_added_ts__extract_dow
                , subq_30.bio_added_ts__extract_doy AS user__bio_added_ts__extract_doy
                , subq_30.last_login_ts__minute AS user__last_login_ts__minute
                , subq_30.last_login_ts__hour AS user__last_login_ts__hour
                , subq_30.last_login_ts__day AS user__last_login_ts__day
                , subq_30.last_login_ts__week AS user__last_login_ts__week
                , subq_30.last_login_ts__month AS user__last_login_ts__month
                , subq_30.last_login_ts__quarter AS user__last_login_ts__quarter
                , subq_30.last_login_ts__year AS user__last_login_ts__year
                , subq_30.last_login_ts__extract_year AS user__last_login_ts__extract_year
                , subq_30.last_login_ts__extract_quarter AS user__last_login_ts__extract_quarter
                , subq_30.last_login_ts__extract_month AS user__last_login_ts__extract_month
                , subq_30.last_login_ts__extract_day AS user__last_login_ts__extract_day
                , subq_30.last_login_ts__extract_dow AS user__last_login_ts__extract_dow
                , subq_30.last_login_ts__extract_doy AS user__last_login_ts__extract_doy
                , subq_30.archived_at__hour AS user__archived_at__hour
                , subq_30.archived_at__day AS user__archived_at__day
                , subq_30.archived_at__week AS user__archived_at__week
                , subq_30.archived_at__month AS user__archived_at__month
                , subq_30.archived_at__quarter AS user__archived_at__quarter
                , subq_30.archived_at__year AS user__archived_at__year
                , subq_30.archived_at__extract_year AS user__archived_at__extract_year
                , subq_30.archived_at__extract_quarter AS user__archived_at__extract_quarter
                , subq_30.archived_at__extract_month AS user__archived_at__extract_month
                , subq_30.archived_at__extract_day AS user__archived_at__extract_day
                , subq_30.archived_at__extract_dow AS user__archived_at__extract_dow
                , subq_30.archived_at__extract_doy AS user__archived_at__extract_doy
                , subq_30.metric_time__day AS user__metric_time__day
                , subq_30.metric_time__week AS user__metric_time__week
                , subq_30.metric_time__month AS user__metric_time__month
                , subq_30.metric_time__quarter AS user__metric_time__quarter
                , subq_30.metric_time__year AS user__metric_time__year
                , subq_30.metric_time__extract_year AS user__metric_time__extract_year
                , subq_30.metric_time__extract_quarter AS user__metric_time__extract_quarter
                , subq_30.metric_time__extract_month AS user__metric_time__extract_month
                , subq_30.metric_time__extract_day AS user__metric_time__extract_day
                , subq_30.metric_time__extract_dow AS user__metric_time__extract_dow
                , subq_30.metric_time__extract_doy AS user__metric_time__extract_doy
                , subq_27.ds__day AS ds__day
                , subq_27.ds__week AS ds__week
                , subq_27.ds__month AS ds__month
                , subq_27.ds__quarter AS ds__quarter
                , subq_27.ds__year AS ds__year
                , subq_27.ds__extract_year AS ds__extract_year
                , subq_27.ds__extract_quarter AS ds__extract_quarter
                , subq_27.ds__extract_month AS ds__extract_month
                , subq_27.ds__extract_day AS ds__extract_day
                , subq_27.ds__extract_dow AS ds__extract_dow
                , subq_27.ds__extract_doy AS ds__extract_doy
                , subq_27.created_at__day AS created_at__day
                , subq_27.created_at__week AS created_at__week
                , subq_27.created_at__month AS created_at__month
                , subq_27.created_at__quarter AS created_at__quarter
                , subq_27.created_at__year AS created_at__year
                , subq_27.created_at__extract_year AS created_at__extract_year
                , subq_27.created_at__extract_quarter AS created_at__extract_quarter
                , subq_27.created_at__extract_month AS created_at__extract_month
                , subq_27.created_at__extract_day AS created_at__extract_day
                , subq_27.created_at__extract_dow AS created_at__extract_dow
                , subq_27.created_at__extract_doy AS created_at__extract_doy
                , subq_27.listing__ds__day AS listing__ds__day
                , subq_27.listing__ds__week AS listing__ds__week
                , subq_27.listing__ds__month AS listing__ds__month
                , subq_27.listing__ds__quarter AS listing__ds__quarter
                , subq_27.listing__ds__year AS listing__ds__year
                , subq_27.listing__ds__extract_year AS listing__ds__extract_year
                , subq_27.listing__ds__extract_quarter AS listing__ds__extract_quarter
                , subq_27.listing__ds__extract_month AS listing__ds__extract_month
                , subq_27.listing__ds__extract_day AS listing__ds__extract_day
                , subq_27.listing__ds__extract_dow AS listing__ds__extract_dow
                , subq_27.listing__ds__extract_doy AS listing__ds__extract_doy
                , subq_27.listing__created_at__day AS listing__created_at__day
                , subq_27.listing__created_at__week AS listing__created_at__week
                , subq_27.listing__created_at__month AS listing__created_at__month
                , subq_27.listing__created_at__quarter AS listing__created_at__quarter
                , subq_27.listing__created_at__year AS listing__created_at__year
                , subq_27.listing__created_at__extract_year AS listing__created_at__extract_year
                , subq_27.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
                , subq_27.listing__created_at__extract_month AS listing__created_at__extract_month
                , subq_27.listing__created_at__extract_day AS listing__created_at__extract_day
                , subq_27.listing__created_at__extract_dow AS listing__created_at__extract_dow
                , subq_27.listing__created_at__extract_doy AS listing__created_at__extract_doy
                , subq_27.metric_time__day AS metric_time__day
                , subq_27.metric_time__week AS metric_time__week
                , subq_27.metric_time__month AS metric_time__month
                , subq_27.metric_time__quarter AS metric_time__quarter
                , subq_27.metric_time__year AS metric_time__year
                , subq_27.metric_time__extract_year AS metric_time__extract_year
                , subq_27.metric_time__extract_quarter AS metric_time__extract_quarter
                , subq_27.metric_time__extract_month AS metric_time__extract_month
                , subq_27.metric_time__extract_day AS metric_time__extract_day
                , subq_27.metric_time__extract_dow AS metric_time__extract_dow
                , subq_27.metric_time__extract_doy AS metric_time__extract_doy
                , subq_27.listing AS listing
                , subq_27.user AS user
                , subq_27.listing__user AS listing__user
                , subq_27.country_latest AS country_latest
                , subq_27.is_lux_latest AS is_lux_latest
                , subq_27.capacity_latest AS capacity_latest
                , subq_27.listing__country_latest AS listing__country_latest
                , subq_27.listing__is_lux_latest AS listing__is_lux_latest
                , subq_27.listing__capacity_latest AS listing__capacity_latest
                , subq_27.__listings AS __listings
                , subq_27.__lux_listings AS __lux_listings
                , subq_27.__smallest_listing AS __smallest_listing
                , subq_27.__largest_listing AS __largest_listing
                , subq_27.__active_listings AS __active_listings
              FROM (
                -- Metric Time Dimension 'ds'
                SELECT
                  subq_26.ds__day
                  , subq_26.ds__week
                  , subq_26.ds__month
                  , subq_26.ds__quarter
                  , subq_26.ds__year
                  , subq_26.ds__extract_year
                  , subq_26.ds__extract_quarter
                  , subq_26.ds__extract_month
                  , subq_26.ds__extract_day
                  , subq_26.ds__extract_dow
                  , subq_26.ds__extract_doy
                  , subq_26.created_at__day
                  , subq_26.created_at__week
                  , subq_26.created_at__month
                  , subq_26.created_at__quarter
                  , subq_26.created_at__year
                  , subq_26.created_at__extract_year
                  , subq_26.created_at__extract_quarter
                  , subq_26.created_at__extract_month
                  , subq_26.created_at__extract_day
                  , subq_26.created_at__extract_dow
                  , subq_26.created_at__extract_doy
                  , subq_26.listing__ds__day
                  , subq_26.listing__ds__week
                  , subq_26.listing__ds__month
                  , subq_26.listing__ds__quarter
                  , subq_26.listing__ds__year
                  , subq_26.listing__ds__extract_year
                  , subq_26.listing__ds__extract_quarter
                  , subq_26.listing__ds__extract_month
                  , subq_26.listing__ds__extract_day
                  , subq_26.listing__ds__extract_dow
                  , subq_26.listing__ds__extract_doy
                  , subq_26.listing__created_at__day
                  , subq_26.listing__created_at__week
                  , subq_26.listing__created_at__month
                  , subq_26.listing__created_at__quarter
                  , subq_26.listing__created_at__year
                  , subq_26.listing__created_at__extract_year
                  , subq_26.listing__created_at__extract_quarter
                  , subq_26.listing__created_at__extract_month
                  , subq_26.listing__created_at__extract_day
                  , subq_26.listing__created_at__extract_dow
                  , subq_26.listing__created_at__extract_doy
                  , subq_26.ds__day AS metric_time__day
                  , subq_26.ds__week AS metric_time__week
                  , subq_26.ds__month AS metric_time__month
                  , subq_26.ds__quarter AS metric_time__quarter
                  , subq_26.ds__year AS metric_time__year
                  , subq_26.ds__extract_year AS metric_time__extract_year
                  , subq_26.ds__extract_quarter AS metric_time__extract_quarter
                  , subq_26.ds__extract_month AS metric_time__extract_month
                  , subq_26.ds__extract_day AS metric_time__extract_day
                  , subq_26.ds__extract_dow AS metric_time__extract_dow
                  , subq_26.ds__extract_doy AS metric_time__extract_doy
                  , subq_26.listing
                  , subq_26.user
                  , subq_26.listing__user
                  , subq_26.country_latest
                  , subq_26.is_lux_latest
                  , subq_26.capacity_latest
                  , subq_26.listing__country_latest
                  , subq_26.listing__is_lux_latest
                  , subq_26.listing__capacity_latest
                  , subq_26.__listings
                  , subq_26.__lux_listings
                  , subq_26.__smallest_listing
                  , subq_26.__largest_listing
                  , subq_26.__active_listings
                FROM (
                  -- Read Elements From Semantic Model 'listings_latest'
                  SELECT
                    1 AS __listings
                    , 1 AS __lux_listings
                    , listings_latest_src_28000.capacity AS __smallest_listing
                    , listings_latest_src_28000.capacity AS __largest_listing
                    , 1 AS __active_listings
                    , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS ds__day
                    , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS ds__week
                    , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS ds__month
                    , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS ds__quarter
                    , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS ds__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
                    , EXTRACT(dayofweekiso FROM listings_latest_src_28000.created_at) AS ds__extract_dow
                    , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS ds__extract_doy
                    , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS created_at__day
                    , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS created_at__week
                    , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS created_at__month
                    , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS created_at__quarter
                    , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS created_at__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
                    , EXTRACT(dayofweekiso FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
                    , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
                    , listings_latest_src_28000.country AS country_latest
                    , listings_latest_src_28000.is_lux AS is_lux_latest
                    , listings_latest_src_28000.capacity AS capacity_latest
                    , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__ds__day
                    , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__ds__week
                    , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__ds__month
                    , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__ds__quarter
                    , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__ds__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
                    , EXTRACT(dayofweekiso FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
                    , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                    , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__created_at__day
                    , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__created_at__week
                    , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__created_at__month
                    , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__created_at__quarter
                    , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__created_at__year
                    , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_year
                    , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
                    , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_month
                    , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_day
                    , EXTRACT(dayofweekiso FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
                    , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                    , listings_latest_src_28000.country AS listing__country_latest
                    , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                    , listings_latest_src_28000.capacity AS listing__capacity_latest
                    , listings_latest_src_28000.listing_id AS listing
                    , listings_latest_src_28000.user_id AS user
                    , listings_latest_src_28000.user_id AS listing__user
                  FROM ***************************.dim_listings_latest listings_latest_src_28000
                ) subq_26
              ) subq_27
              LEFT OUTER JOIN (
                -- Pass Only Elements: [
                --   'home_state',
                --   'user__home_state',
                --   'ds__day',
                --   'ds__week',
                --   'ds__month',
                --   'ds__quarter',
                --   'ds__year',
                --   'ds__extract_year',
                --   'ds__extract_quarter',
                --   'ds__extract_month',
                --   'ds__extract_day',
                --   'ds__extract_dow',
                --   'ds__extract_doy',
                --   'created_at__day',
                --   'created_at__week',
                --   'created_at__month',
                --   'created_at__quarter',
                --   'created_at__year',
                --   'created_at__extract_year',
                --   'created_at__extract_quarter',
                --   'created_at__extract_month',
                --   'created_at__extract_day',
                --   'created_at__extract_dow',
                --   'created_at__extract_doy',
                --   'ds_partitioned__day',
                --   'ds_partitioned__week',
                --   'ds_partitioned__month',
                --   'ds_partitioned__quarter',
                --   'ds_partitioned__year',
                --   'ds_partitioned__extract_year',
                --   'ds_partitioned__extract_quarter',
                --   'ds_partitioned__extract_month',
                --   'ds_partitioned__extract_day',
                --   'ds_partitioned__extract_dow',
                --   'ds_partitioned__extract_doy',
                --   'last_profile_edit_ts__millisecond',
                --   'last_profile_edit_ts__second',
                --   'last_profile_edit_ts__minute',
                --   'last_profile_edit_ts__hour',
                --   'last_profile_edit_ts__day',
                --   'last_profile_edit_ts__week',
                --   'last_profile_edit_ts__month',
                --   'last_profile_edit_ts__quarter',
                --   'last_profile_edit_ts__year',
                --   'last_profile_edit_ts__extract_year',
                --   'last_profile_edit_ts__extract_quarter',
                --   'last_profile_edit_ts__extract_month',
                --   'last_profile_edit_ts__extract_day',
                --   'last_profile_edit_ts__extract_dow',
                --   'last_profile_edit_ts__extract_doy',
                --   'bio_added_ts__second',
                --   'bio_added_ts__minute',
                --   'bio_added_ts__hour',
                --   'bio_added_ts__day',
                --   'bio_added_ts__week',
                --   'bio_added_ts__month',
                --   'bio_added_ts__quarter',
                --   'bio_added_ts__year',
                --   'bio_added_ts__extract_year',
                --   'bio_added_ts__extract_quarter',
                --   'bio_added_ts__extract_month',
                --   'bio_added_ts__extract_day',
                --   'bio_added_ts__extract_dow',
                --   'bio_added_ts__extract_doy',
                --   'last_login_ts__minute',
                --   'last_login_ts__hour',
                --   'last_login_ts__day',
                --   'last_login_ts__week',
                --   'last_login_ts__month',
                --   'last_login_ts__quarter',
                --   'last_login_ts__year',
                --   'last_login_ts__extract_year',
                --   'last_login_ts__extract_quarter',
                --   'last_login_ts__extract_month',
                --   'last_login_ts__extract_day',
                --   'last_login_ts__extract_dow',
                --   'last_login_ts__extract_doy',
                --   'archived_at__hour',
                --   'archived_at__day',
                --   'archived_at__week',
                --   'archived_at__month',
                --   'archived_at__quarter',
                --   'archived_at__year',
                --   'archived_at__extract_year',
                --   'archived_at__extract_quarter',
                --   'archived_at__extract_month',
                --   'archived_at__extract_day',
                --   'archived_at__extract_dow',
                --   'archived_at__extract_doy',
                --   'user__ds__day',
                --   'user__ds__week',
                --   'user__ds__month',
                --   'user__ds__quarter',
                --   'user__ds__year',
                --   'user__ds__extract_year',
                --   'user__ds__extract_quarter',
                --   'user__ds__extract_month',
                --   'user__ds__extract_day',
                --   'user__ds__extract_dow',
                --   'user__ds__extract_doy',
                --   'user__created_at__day',
                --   'user__created_at__week',
                --   'user__created_at__month',
                --   'user__created_at__quarter',
                --   'user__created_at__year',
                --   'user__created_at__extract_year',
                --   'user__created_at__extract_quarter',
                --   'user__created_at__extract_month',
                --   'user__created_at__extract_day',
                --   'user__created_at__extract_dow',
                --   'user__created_at__extract_doy',
                --   'user__ds_partitioned__day',
                --   'user__ds_partitioned__week',
                --   'user__ds_partitioned__month',
                --   'user__ds_partitioned__quarter',
                --   'user__ds_partitioned__year',
                --   'user__ds_partitioned__extract_year',
                --   'user__ds_partitioned__extract_quarter',
                --   'user__ds_partitioned__extract_month',
                --   'user__ds_partitioned__extract_day',
                --   'user__ds_partitioned__extract_dow',
                --   'user__ds_partitioned__extract_doy',
                --   'user__last_profile_edit_ts__millisecond',
                --   'user__last_profile_edit_ts__second',
                --   'user__last_profile_edit_ts__minute',
                --   'user__last_profile_edit_ts__hour',
                --   'user__last_profile_edit_ts__day',
                --   'user__last_profile_edit_ts__week',
                --   'user__last_profile_edit_ts__month',
                --   'user__last_profile_edit_ts__quarter',
                --   'user__last_profile_edit_ts__year',
                --   'user__last_profile_edit_ts__extract_year',
                --   'user__last_profile_edit_ts__extract_quarter',
                --   'user__last_profile_edit_ts__extract_month',
                --   'user__last_profile_edit_ts__extract_day',
                --   'user__last_profile_edit_ts__extract_dow',
                --   'user__last_profile_edit_ts__extract_doy',
                --   'user__bio_added_ts__second',
                --   'user__bio_added_ts__minute',
                --   'user__bio_added_ts__hour',
                --   'user__bio_added_ts__day',
                --   'user__bio_added_ts__week',
                --   'user__bio_added_ts__month',
                --   'user__bio_added_ts__quarter',
                --   'user__bio_added_ts__year',
                --   'user__bio_added_ts__extract_year',
                --   'user__bio_added_ts__extract_quarter',
                --   'user__bio_added_ts__extract_month',
                --   'user__bio_added_ts__extract_day',
                --   'user__bio_added_ts__extract_dow',
                --   'user__bio_added_ts__extract_doy',
                --   'user__last_login_ts__minute',
                --   'user__last_login_ts__hour',
                --   'user__last_login_ts__day',
                --   'user__last_login_ts__week',
                --   'user__last_login_ts__month',
                --   'user__last_login_ts__quarter',
                --   'user__last_login_ts__year',
                --   'user__last_login_ts__extract_year',
                --   'user__last_login_ts__extract_quarter',
                --   'user__last_login_ts__extract_month',
                --   'user__last_login_ts__extract_day',
                --   'user__last_login_ts__extract_dow',
                --   'user__last_login_ts__extract_doy',
                --   'user__archived_at__hour',
                --   'user__archived_at__day',
                --   'user__archived_at__week',
                --   'user__archived_at__month',
                --   'user__archived_at__quarter',
                --   'user__archived_at__year',
                --   'user__archived_at__extract_year',
                --   'user__archived_at__extract_quarter',
                --   'user__archived_at__extract_month',
                --   'user__archived_at__extract_day',
                --   'user__archived_at__extract_dow',
                --   'user__archived_at__extract_doy',
                --   'metric_time__day',
                --   'metric_time__week',
                --   'metric_time__month',
                --   'metric_time__quarter',
                --   'metric_time__year',
                --   'metric_time__extract_year',
                --   'metric_time__extract_quarter',
                --   'metric_time__extract_month',
                --   'metric_time__extract_day',
                --   'metric_time__extract_dow',
                --   'metric_time__extract_doy',
                --   'user',
                -- ]
                SELECT
                  subq_29.ds__day
                  , subq_29.ds__week
                  , subq_29.ds__month
                  , subq_29.ds__quarter
                  , subq_29.ds__year
                  , subq_29.ds__extract_year
                  , subq_29.ds__extract_quarter
                  , subq_29.ds__extract_month
                  , subq_29.ds__extract_day
                  , subq_29.ds__extract_dow
                  , subq_29.ds__extract_doy
                  , subq_29.created_at__day
                  , subq_29.created_at__week
                  , subq_29.created_at__month
                  , subq_29.created_at__quarter
                  , subq_29.created_at__year
                  , subq_29.created_at__extract_year
                  , subq_29.created_at__extract_quarter
                  , subq_29.created_at__extract_month
                  , subq_29.created_at__extract_day
                  , subq_29.created_at__extract_dow
                  , subq_29.created_at__extract_doy
                  , subq_29.ds_partitioned__day
                  , subq_29.ds_partitioned__week
                  , subq_29.ds_partitioned__month
                  , subq_29.ds_partitioned__quarter
                  , subq_29.ds_partitioned__year
                  , subq_29.ds_partitioned__extract_year
                  , subq_29.ds_partitioned__extract_quarter
                  , subq_29.ds_partitioned__extract_month
                  , subq_29.ds_partitioned__extract_day
                  , subq_29.ds_partitioned__extract_dow
                  , subq_29.ds_partitioned__extract_doy
                  , subq_29.last_profile_edit_ts__millisecond
                  , subq_29.last_profile_edit_ts__second
                  , subq_29.last_profile_edit_ts__minute
                  , subq_29.last_profile_edit_ts__hour
                  , subq_29.last_profile_edit_ts__day
                  , subq_29.last_profile_edit_ts__week
                  , subq_29.last_profile_edit_ts__month
                  , subq_29.last_profile_edit_ts__quarter
                  , subq_29.last_profile_edit_ts__year
                  , subq_29.last_profile_edit_ts__extract_year
                  , subq_29.last_profile_edit_ts__extract_quarter
                  , subq_29.last_profile_edit_ts__extract_month
                  , subq_29.last_profile_edit_ts__extract_day
                  , subq_29.last_profile_edit_ts__extract_dow
                  , subq_29.last_profile_edit_ts__extract_doy
                  , subq_29.bio_added_ts__second
                  , subq_29.bio_added_ts__minute
                  , subq_29.bio_added_ts__hour
                  , subq_29.bio_added_ts__day
                  , subq_29.bio_added_ts__week
                  , subq_29.bio_added_ts__month
                  , subq_29.bio_added_ts__quarter
                  , subq_29.bio_added_ts__year
                  , subq_29.bio_added_ts__extract_year
                  , subq_29.bio_added_ts__extract_quarter
                  , subq_29.bio_added_ts__extract_month
                  , subq_29.bio_added_ts__extract_day
                  , subq_29.bio_added_ts__extract_dow
                  , subq_29.bio_added_ts__extract_doy
                  , subq_29.last_login_ts__minute
                  , subq_29.last_login_ts__hour
                  , subq_29.last_login_ts__day
                  , subq_29.last_login_ts__week
                  , subq_29.last_login_ts__month
                  , subq_29.last_login_ts__quarter
                  , subq_29.last_login_ts__year
                  , subq_29.last_login_ts__extract_year
                  , subq_29.last_login_ts__extract_quarter
                  , subq_29.last_login_ts__extract_month
                  , subq_29.last_login_ts__extract_day
                  , subq_29.last_login_ts__extract_dow
                  , subq_29.last_login_ts__extract_doy
                  , subq_29.archived_at__hour
                  , subq_29.archived_at__day
                  , subq_29.archived_at__week
                  , subq_29.archived_at__month
                  , subq_29.archived_at__quarter
                  , subq_29.archived_at__year
                  , subq_29.archived_at__extract_year
                  , subq_29.archived_at__extract_quarter
                  , subq_29.archived_at__extract_month
                  , subq_29.archived_at__extract_day
                  , subq_29.archived_at__extract_dow
                  , subq_29.archived_at__extract_doy
                  , subq_29.user__ds__day
                  , subq_29.user__ds__week
                  , subq_29.user__ds__month
                  , subq_29.user__ds__quarter
                  , subq_29.user__ds__year
                  , subq_29.user__ds__extract_year
                  , subq_29.user__ds__extract_quarter
                  , subq_29.user__ds__extract_month
                  , subq_29.user__ds__extract_day
                  , subq_29.user__ds__extract_dow
                  , subq_29.user__ds__extract_doy
                  , subq_29.user__created_at__day
                  , subq_29.user__created_at__week
                  , subq_29.user__created_at__month
                  , subq_29.user__created_at__quarter
                  , subq_29.user__created_at__year
                  , subq_29.user__created_at__extract_year
                  , subq_29.user__created_at__extract_quarter
                  , subq_29.user__created_at__extract_month
                  , subq_29.user__created_at__extract_day
                  , subq_29.user__created_at__extract_dow
                  , subq_29.user__created_at__extract_doy
                  , subq_29.user__ds_partitioned__day
                  , subq_29.user__ds_partitioned__week
                  , subq_29.user__ds_partitioned__month
                  , subq_29.user__ds_partitioned__quarter
                  , subq_29.user__ds_partitioned__year
                  , subq_29.user__ds_partitioned__extract_year
                  , subq_29.user__ds_partitioned__extract_quarter
                  , subq_29.user__ds_partitioned__extract_month
                  , subq_29.user__ds_partitioned__extract_day
                  , subq_29.user__ds_partitioned__extract_dow
                  , subq_29.user__ds_partitioned__extract_doy
                  , subq_29.user__last_profile_edit_ts__millisecond
                  , subq_29.user__last_profile_edit_ts__second
                  , subq_29.user__last_profile_edit_ts__minute
                  , subq_29.user__last_profile_edit_ts__hour
                  , subq_29.user__last_profile_edit_ts__day
                  , subq_29.user__last_profile_edit_ts__week
                  , subq_29.user__last_profile_edit_ts__month
                  , subq_29.user__last_profile_edit_ts__quarter
                  , subq_29.user__last_profile_edit_ts__year
                  , subq_29.user__last_profile_edit_ts__extract_year
                  , subq_29.user__last_profile_edit_ts__extract_quarter
                  , subq_29.user__last_profile_edit_ts__extract_month
                  , subq_29.user__last_profile_edit_ts__extract_day
                  , subq_29.user__last_profile_edit_ts__extract_dow
                  , subq_29.user__last_profile_edit_ts__extract_doy
                  , subq_29.user__bio_added_ts__second
                  , subq_29.user__bio_added_ts__minute
                  , subq_29.user__bio_added_ts__hour
                  , subq_29.user__bio_added_ts__day
                  , subq_29.user__bio_added_ts__week
                  , subq_29.user__bio_added_ts__month
                  , subq_29.user__bio_added_ts__quarter
                  , subq_29.user__bio_added_ts__year
                  , subq_29.user__bio_added_ts__extract_year
                  , subq_29.user__bio_added_ts__extract_quarter
                  , subq_29.user__bio_added_ts__extract_month
                  , subq_29.user__bio_added_ts__extract_day
                  , subq_29.user__bio_added_ts__extract_dow
                  , subq_29.user__bio_added_ts__extract_doy
                  , subq_29.user__last_login_ts__minute
                  , subq_29.user__last_login_ts__hour
                  , subq_29.user__last_login_ts__day
                  , subq_29.user__last_login_ts__week
                  , subq_29.user__last_login_ts__month
                  , subq_29.user__last_login_ts__quarter
                  , subq_29.user__last_login_ts__year
                  , subq_29.user__last_login_ts__extract_year
                  , subq_29.user__last_login_ts__extract_quarter
                  , subq_29.user__last_login_ts__extract_month
                  , subq_29.user__last_login_ts__extract_day
                  , subq_29.user__last_login_ts__extract_dow
                  , subq_29.user__last_login_ts__extract_doy
                  , subq_29.user__archived_at__hour
                  , subq_29.user__archived_at__day
                  , subq_29.user__archived_at__week
                  , subq_29.user__archived_at__month
                  , subq_29.user__archived_at__quarter
                  , subq_29.user__archived_at__year
                  , subq_29.user__archived_at__extract_year
                  , subq_29.user__archived_at__extract_quarter
                  , subq_29.user__archived_at__extract_month
                  , subq_29.user__archived_at__extract_day
                  , subq_29.user__archived_at__extract_dow
                  , subq_29.user__archived_at__extract_doy
                  , subq_29.metric_time__day
                  , subq_29.metric_time__week
                  , subq_29.metric_time__month
                  , subq_29.metric_time__quarter
                  , subq_29.metric_time__year
                  , subq_29.metric_time__extract_year
                  , subq_29.metric_time__extract_quarter
                  , subq_29.metric_time__extract_month
                  , subq_29.metric_time__extract_day
                  , subq_29.metric_time__extract_dow
                  , subq_29.metric_time__extract_doy
                  , subq_29.user
                  , subq_29.home_state
                  , subq_29.user__home_state
                FROM (
                  -- Metric Time Dimension 'created_at'
                  SELECT
                    subq_28.ds__day
                    , subq_28.ds__week
                    , subq_28.ds__month
                    , subq_28.ds__quarter
                    , subq_28.ds__year
                    , subq_28.ds__extract_year
                    , subq_28.ds__extract_quarter
                    , subq_28.ds__extract_month
                    , subq_28.ds__extract_day
                    , subq_28.ds__extract_dow
                    , subq_28.ds__extract_doy
                    , subq_28.created_at__day
                    , subq_28.created_at__week
                    , subq_28.created_at__month
                    , subq_28.created_at__quarter
                    , subq_28.created_at__year
                    , subq_28.created_at__extract_year
                    , subq_28.created_at__extract_quarter
                    , subq_28.created_at__extract_month
                    , subq_28.created_at__extract_day
                    , subq_28.created_at__extract_dow
                    , subq_28.created_at__extract_doy
                    , subq_28.ds_partitioned__day
                    , subq_28.ds_partitioned__week
                    , subq_28.ds_partitioned__month
                    , subq_28.ds_partitioned__quarter
                    , subq_28.ds_partitioned__year
                    , subq_28.ds_partitioned__extract_year
                    , subq_28.ds_partitioned__extract_quarter
                    , subq_28.ds_partitioned__extract_month
                    , subq_28.ds_partitioned__extract_day
                    , subq_28.ds_partitioned__extract_dow
                    , subq_28.ds_partitioned__extract_doy
                    , subq_28.last_profile_edit_ts__millisecond
                    , subq_28.last_profile_edit_ts__second
                    , subq_28.last_profile_edit_ts__minute
                    , subq_28.last_profile_edit_ts__hour
                    , subq_28.last_profile_edit_ts__day
                    , subq_28.last_profile_edit_ts__week
                    , subq_28.last_profile_edit_ts__month
                    , subq_28.last_profile_edit_ts__quarter
                    , subq_28.last_profile_edit_ts__year
                    , subq_28.last_profile_edit_ts__extract_year
                    , subq_28.last_profile_edit_ts__extract_quarter
                    , subq_28.last_profile_edit_ts__extract_month
                    , subq_28.last_profile_edit_ts__extract_day
                    , subq_28.last_profile_edit_ts__extract_dow
                    , subq_28.last_profile_edit_ts__extract_doy
                    , subq_28.bio_added_ts__second
                    , subq_28.bio_added_ts__minute
                    , subq_28.bio_added_ts__hour
                    , subq_28.bio_added_ts__day
                    , subq_28.bio_added_ts__week
                    , subq_28.bio_added_ts__month
                    , subq_28.bio_added_ts__quarter
                    , subq_28.bio_added_ts__year
                    , subq_28.bio_added_ts__extract_year
                    , subq_28.bio_added_ts__extract_quarter
                    , subq_28.bio_added_ts__extract_month
                    , subq_28.bio_added_ts__extract_day
                    , subq_28.bio_added_ts__extract_dow
                    , subq_28.bio_added_ts__extract_doy
                    , subq_28.last_login_ts__minute
                    , subq_28.last_login_ts__hour
                    , subq_28.last_login_ts__day
                    , subq_28.last_login_ts__week
                    , subq_28.last_login_ts__month
                    , subq_28.last_login_ts__quarter
                    , subq_28.last_login_ts__year
                    , subq_28.last_login_ts__extract_year
                    , subq_28.last_login_ts__extract_quarter
                    , subq_28.last_login_ts__extract_month
                    , subq_28.last_login_ts__extract_day
                    , subq_28.last_login_ts__extract_dow
                    , subq_28.last_login_ts__extract_doy
                    , subq_28.archived_at__hour
                    , subq_28.archived_at__day
                    , subq_28.archived_at__week
                    , subq_28.archived_at__month
                    , subq_28.archived_at__quarter
                    , subq_28.archived_at__year
                    , subq_28.archived_at__extract_year
                    , subq_28.archived_at__extract_quarter
                    , subq_28.archived_at__extract_month
                    , subq_28.archived_at__extract_day
                    , subq_28.archived_at__extract_dow
                    , subq_28.archived_at__extract_doy
                    , subq_28.user__ds__day
                    , subq_28.user__ds__week
                    , subq_28.user__ds__month
                    , subq_28.user__ds__quarter
                    , subq_28.user__ds__year
                    , subq_28.user__ds__extract_year
                    , subq_28.user__ds__extract_quarter
                    , subq_28.user__ds__extract_month
                    , subq_28.user__ds__extract_day
                    , subq_28.user__ds__extract_dow
                    , subq_28.user__ds__extract_doy
                    , subq_28.user__created_at__day
                    , subq_28.user__created_at__week
                    , subq_28.user__created_at__month
                    , subq_28.user__created_at__quarter
                    , subq_28.user__created_at__year
                    , subq_28.user__created_at__extract_year
                    , subq_28.user__created_at__extract_quarter
                    , subq_28.user__created_at__extract_month
                    , subq_28.user__created_at__extract_day
                    , subq_28.user__created_at__extract_dow
                    , subq_28.user__created_at__extract_doy
                    , subq_28.user__ds_partitioned__day
                    , subq_28.user__ds_partitioned__week
                    , subq_28.user__ds_partitioned__month
                    , subq_28.user__ds_partitioned__quarter
                    , subq_28.user__ds_partitioned__year
                    , subq_28.user__ds_partitioned__extract_year
                    , subq_28.user__ds_partitioned__extract_quarter
                    , subq_28.user__ds_partitioned__extract_month
                    , subq_28.user__ds_partitioned__extract_day
                    , subq_28.user__ds_partitioned__extract_dow
                    , subq_28.user__ds_partitioned__extract_doy
                    , subq_28.user__last_profile_edit_ts__millisecond
                    , subq_28.user__last_profile_edit_ts__second
                    , subq_28.user__last_profile_edit_ts__minute
                    , subq_28.user__last_profile_edit_ts__hour
                    , subq_28.user__last_profile_edit_ts__day
                    , subq_28.user__last_profile_edit_ts__week
                    , subq_28.user__last_profile_edit_ts__month
                    , subq_28.user__last_profile_edit_ts__quarter
                    , subq_28.user__last_profile_edit_ts__year
                    , subq_28.user__last_profile_edit_ts__extract_year
                    , subq_28.user__last_profile_edit_ts__extract_quarter
                    , subq_28.user__last_profile_edit_ts__extract_month
                    , subq_28.user__last_profile_edit_ts__extract_day
                    , subq_28.user__last_profile_edit_ts__extract_dow
                    , subq_28.user__last_profile_edit_ts__extract_doy
                    , subq_28.user__bio_added_ts__second
                    , subq_28.user__bio_added_ts__minute
                    , subq_28.user__bio_added_ts__hour
                    , subq_28.user__bio_added_ts__day
                    , subq_28.user__bio_added_ts__week
                    , subq_28.user__bio_added_ts__month
                    , subq_28.user__bio_added_ts__quarter
                    , subq_28.user__bio_added_ts__year
                    , subq_28.user__bio_added_ts__extract_year
                    , subq_28.user__bio_added_ts__extract_quarter
                    , subq_28.user__bio_added_ts__extract_month
                    , subq_28.user__bio_added_ts__extract_day
                    , subq_28.user__bio_added_ts__extract_dow
                    , subq_28.user__bio_added_ts__extract_doy
                    , subq_28.user__last_login_ts__minute
                    , subq_28.user__last_login_ts__hour
                    , subq_28.user__last_login_ts__day
                    , subq_28.user__last_login_ts__week
                    , subq_28.user__last_login_ts__month
                    , subq_28.user__last_login_ts__quarter
                    , subq_28.user__last_login_ts__year
                    , subq_28.user__last_login_ts__extract_year
                    , subq_28.user__last_login_ts__extract_quarter
                    , subq_28.user__last_login_ts__extract_month
                    , subq_28.user__last_login_ts__extract_day
                    , subq_28.user__last_login_ts__extract_dow
                    , subq_28.user__last_login_ts__extract_doy
                    , subq_28.user__archived_at__hour
                    , subq_28.user__archived_at__day
                    , subq_28.user__archived_at__week
                    , subq_28.user__archived_at__month
                    , subq_28.user__archived_at__quarter
                    , subq_28.user__archived_at__year
                    , subq_28.user__archived_at__extract_year
                    , subq_28.user__archived_at__extract_quarter
                    , subq_28.user__archived_at__extract_month
                    , subq_28.user__archived_at__extract_day
                    , subq_28.user__archived_at__extract_dow
                    , subq_28.user__archived_at__extract_doy
                    , subq_28.created_at__day AS metric_time__day
                    , subq_28.created_at__week AS metric_time__week
                    , subq_28.created_at__month AS metric_time__month
                    , subq_28.created_at__quarter AS metric_time__quarter
                    , subq_28.created_at__year AS metric_time__year
                    , subq_28.created_at__extract_year AS metric_time__extract_year
                    , subq_28.created_at__extract_quarter AS metric_time__extract_quarter
                    , subq_28.created_at__extract_month AS metric_time__extract_month
                    , subq_28.created_at__extract_day AS metric_time__extract_day
                    , subq_28.created_at__extract_dow AS metric_time__extract_dow
                    , subq_28.created_at__extract_doy AS metric_time__extract_doy
                    , subq_28.user
                    , subq_28.home_state
                    , subq_28.user__home_state
                    , subq_28.__new_users
                  FROM (
                    -- Read Elements From Semantic Model 'users_ds_source'
                    SELECT
                      1 AS __subdaily_join_to_time_spine_metric
                      , 1 AS __simple_subdaily_metric_default_day
                      , 1 AS __simple_subdaily_metric_default_hour
                      , 1 AS __archived_users_join_to_time_spine
                      , 1 AS __archived_users
                      , 1 AS __new_users
                      , DATE_TRUNC('day', users_ds_source_src_28000.ds) AS ds__day
                      , DATE_TRUNC('week', users_ds_source_src_28000.ds) AS ds__week
                      , DATE_TRUNC('month', users_ds_source_src_28000.ds) AS ds__month
                      , DATE_TRUNC('quarter', users_ds_source_src_28000.ds) AS ds__quarter
                      , DATE_TRUNC('year', users_ds_source_src_28000.ds) AS ds__year
                      , EXTRACT(year FROM users_ds_source_src_28000.ds) AS ds__extract_year
                      , EXTRACT(quarter FROM users_ds_source_src_28000.ds) AS ds__extract_quarter
                      , EXTRACT(month FROM users_ds_source_src_28000.ds) AS ds__extract_month
                      , EXTRACT(day FROM users_ds_source_src_28000.ds) AS ds__extract_day
                      , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.ds) AS ds__extract_dow
                      , EXTRACT(doy FROM users_ds_source_src_28000.ds) AS ds__extract_doy
                      , DATE_TRUNC('day', users_ds_source_src_28000.created_at) AS created_at__day
                      , DATE_TRUNC('week', users_ds_source_src_28000.created_at) AS created_at__week
                      , DATE_TRUNC('month', users_ds_source_src_28000.created_at) AS created_at__month
                      , DATE_TRUNC('quarter', users_ds_source_src_28000.created_at) AS created_at__quarter
                      , DATE_TRUNC('year', users_ds_source_src_28000.created_at) AS created_at__year
                      , EXTRACT(year FROM users_ds_source_src_28000.created_at) AS created_at__extract_year
                      , EXTRACT(quarter FROM users_ds_source_src_28000.created_at) AS created_at__extract_quarter
                      , EXTRACT(month FROM users_ds_source_src_28000.created_at) AS created_at__extract_month
                      , EXTRACT(day FROM users_ds_source_src_28000.created_at) AS created_at__extract_day
                      , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.created_at) AS created_at__extract_dow
                      , EXTRACT(doy FROM users_ds_source_src_28000.created_at) AS created_at__extract_doy
                      , DATE_TRUNC('day', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__day
                      , DATE_TRUNC('week', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__week
                      , DATE_TRUNC('month', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__month
                      , DATE_TRUNC('quarter', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                      , DATE_TRUNC('year', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__year
                      , EXTRACT(year FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                      , EXTRACT(quarter FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                      , EXTRACT(month FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                      , EXTRACT(day FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                      , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
                      , EXTRACT(doy FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                      , users_ds_source_src_28000.home_state
                      , DATE_TRUNC('millisecond', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__millisecond
                      , DATE_TRUNC('second', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__second
                      , DATE_TRUNC('minute', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__minute
                      , DATE_TRUNC('hour', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__hour
                      , DATE_TRUNC('day', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__day
                      , DATE_TRUNC('week', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__week
                      , DATE_TRUNC('month', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__month
                      , DATE_TRUNC('quarter', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__quarter
                      , DATE_TRUNC('year', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__year
                      , EXTRACT(year FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_year
                      , EXTRACT(quarter FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_quarter
                      , EXTRACT(month FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_month
                      , EXTRACT(day FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_day
                      , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_dow
                      , EXTRACT(doy FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_doy
                      , DATE_TRUNC('second', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__second
                      , DATE_TRUNC('minute', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__minute
                      , DATE_TRUNC('hour', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__hour
                      , DATE_TRUNC('day', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__day
                      , DATE_TRUNC('week', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__week
                      , DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__month
                      , DATE_TRUNC('quarter', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__quarter
                      , DATE_TRUNC('year', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__year
                      , EXTRACT(year FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_year
                      , EXTRACT(quarter FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_quarter
                      , EXTRACT(month FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_month
                      , EXTRACT(day FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_day
                      , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_dow
                      , EXTRACT(doy FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_doy
                      , DATE_TRUNC('minute', users_ds_source_src_28000.last_login_ts) AS last_login_ts__minute
                      , DATE_TRUNC('hour', users_ds_source_src_28000.last_login_ts) AS last_login_ts__hour
                      , DATE_TRUNC('day', users_ds_source_src_28000.last_login_ts) AS last_login_ts__day
                      , DATE_TRUNC('week', users_ds_source_src_28000.last_login_ts) AS last_login_ts__week
                      , DATE_TRUNC('month', users_ds_source_src_28000.last_login_ts) AS last_login_ts__month
                      , DATE_TRUNC('quarter', users_ds_source_src_28000.last_login_ts) AS last_login_ts__quarter
                      , DATE_TRUNC('year', users_ds_source_src_28000.last_login_ts) AS last_login_ts__year
                      , EXTRACT(year FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_year
                      , EXTRACT(quarter FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_quarter
                      , EXTRACT(month FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_month
                      , EXTRACT(day FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_day
                      , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_dow
                      , EXTRACT(doy FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_doy
                      , DATE_TRUNC('hour', users_ds_source_src_28000.archived_at) AS archived_at__hour
                      , DATE_TRUNC('day', users_ds_source_src_28000.archived_at) AS archived_at__day
                      , DATE_TRUNC('week', users_ds_source_src_28000.archived_at) AS archived_at__week
                      , DATE_TRUNC('month', users_ds_source_src_28000.archived_at) AS archived_at__month
                      , DATE_TRUNC('quarter', users_ds_source_src_28000.archived_at) AS archived_at__quarter
                      , DATE_TRUNC('year', users_ds_source_src_28000.archived_at) AS archived_at__year
                      , EXTRACT(year FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_year
                      , EXTRACT(quarter FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_quarter
                      , EXTRACT(month FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_month
                      , EXTRACT(day FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_day
                      , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_dow
                      , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_doy
                      , DATE_TRUNC('day', users_ds_source_src_28000.ds) AS user__ds__day
                      , DATE_TRUNC('week', users_ds_source_src_28000.ds) AS user__ds__week
                      , DATE_TRUNC('month', users_ds_source_src_28000.ds) AS user__ds__month
                      , DATE_TRUNC('quarter', users_ds_source_src_28000.ds) AS user__ds__quarter
                      , DATE_TRUNC('year', users_ds_source_src_28000.ds) AS user__ds__year
                      , EXTRACT(year FROM users_ds_source_src_28000.ds) AS user__ds__extract_year
                      , EXTRACT(quarter FROM users_ds_source_src_28000.ds) AS user__ds__extract_quarter
                      , EXTRACT(month FROM users_ds_source_src_28000.ds) AS user__ds__extract_month
                      , EXTRACT(day FROM users_ds_source_src_28000.ds) AS user__ds__extract_day
                      , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.ds) AS user__ds__extract_dow
                      , EXTRACT(doy FROM users_ds_source_src_28000.ds) AS user__ds__extract_doy
                      , DATE_TRUNC('day', users_ds_source_src_28000.created_at) AS user__created_at__day
                      , DATE_TRUNC('week', users_ds_source_src_28000.created_at) AS user__created_at__week
                      , DATE_TRUNC('month', users_ds_source_src_28000.created_at) AS user__created_at__month
                      , DATE_TRUNC('quarter', users_ds_source_src_28000.created_at) AS user__created_at__quarter
                      , DATE_TRUNC('year', users_ds_source_src_28000.created_at) AS user__created_at__year
                      , EXTRACT(year FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_year
                      , EXTRACT(quarter FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_quarter
                      , EXTRACT(month FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_month
                      , EXTRACT(day FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_day
                      , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_dow
                      , EXTRACT(doy FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_doy
                      , DATE_TRUNC('day', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__day
                      , DATE_TRUNC('week', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__week
                      , DATE_TRUNC('month', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__month
                      , DATE_TRUNC('quarter', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__quarter
                      , DATE_TRUNC('year', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__year
                      , EXTRACT(year FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_year
                      , EXTRACT(quarter FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_quarter
                      , EXTRACT(month FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_month
                      , EXTRACT(day FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_day
                      , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_dow
                      , EXTRACT(doy FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_doy
                      , users_ds_source_src_28000.home_state AS user__home_state
                      , DATE_TRUNC('millisecond', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__millisecond
                      , DATE_TRUNC('second', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__second
                      , DATE_TRUNC('minute', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__minute
                      , DATE_TRUNC('hour', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__hour
                      , DATE_TRUNC('day', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__day
                      , DATE_TRUNC('week', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__week
                      , DATE_TRUNC('month', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__month
                      , DATE_TRUNC('quarter', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__quarter
                      , DATE_TRUNC('year', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__year
                      , EXTRACT(year FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_year
                      , EXTRACT(quarter FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_quarter
                      , EXTRACT(month FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_month
                      , EXTRACT(day FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_day
                      , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_dow
                      , EXTRACT(doy FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_doy
                      , DATE_TRUNC('second', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__second
                      , DATE_TRUNC('minute', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__minute
                      , DATE_TRUNC('hour', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__hour
                      , DATE_TRUNC('day', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__day
                      , DATE_TRUNC('week', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__week
                      , DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__month
                      , DATE_TRUNC('quarter', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__quarter
                      , DATE_TRUNC('year', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__year
                      , EXTRACT(year FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_year
                      , EXTRACT(quarter FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_quarter
                      , EXTRACT(month FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_month
                      , EXTRACT(day FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_day
                      , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_dow
                      , EXTRACT(doy FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_doy
                      , DATE_TRUNC('minute', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__minute
                      , DATE_TRUNC('hour', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__hour
                      , DATE_TRUNC('day', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__day
                      , DATE_TRUNC('week', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__week
                      , DATE_TRUNC('month', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__month
                      , DATE_TRUNC('quarter', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__quarter
                      , DATE_TRUNC('year', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__year
                      , EXTRACT(year FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_year
                      , EXTRACT(quarter FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_quarter
                      , EXTRACT(month FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_month
                      , EXTRACT(day FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_day
                      , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_dow
                      , EXTRACT(doy FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_doy
                      , DATE_TRUNC('hour', users_ds_source_src_28000.archived_at) AS user__archived_at__hour
                      , DATE_TRUNC('day', users_ds_source_src_28000.archived_at) AS user__archived_at__day
                      , DATE_TRUNC('week', users_ds_source_src_28000.archived_at) AS user__archived_at__week
                      , DATE_TRUNC('month', users_ds_source_src_28000.archived_at) AS user__archived_at__month
                      , DATE_TRUNC('quarter', users_ds_source_src_28000.archived_at) AS user__archived_at__quarter
                      , DATE_TRUNC('year', users_ds_source_src_28000.archived_at) AS user__archived_at__year
                      , EXTRACT(year FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_year
                      , EXTRACT(quarter FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_quarter
                      , EXTRACT(month FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_month
                      , EXTRACT(day FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_day
                      , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_dow
                      , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_doy
                      , users_ds_source_src_28000.user_id AS user
                    FROM ***************************.dim_users users_ds_source_src_28000
                  ) subq_28
                ) subq_29
              ) subq_30
              ON
                subq_27.user = subq_30.user
            ) subq_31
          ) subq_32
          ON
            (
              subq_25.listing = subq_32.listing
            ) AND (
              subq_25.ds_partitioned__day = subq_32.user__ds_partitioned__day
            )
          LEFT OUTER JOIN
            ***************************.mf_time_spine subq_33
          ON
            subq_32.user__ds__day = subq_33.ds
        ) subq_34
      ) subq_35
    ) subq_36
    GROUP BY
      subq_36.listing__user__ds__alien_day
  ) subq_37
) subq_38
