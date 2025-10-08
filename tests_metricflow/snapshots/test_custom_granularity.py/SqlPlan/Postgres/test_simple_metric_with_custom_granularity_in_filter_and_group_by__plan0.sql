test_name: test_simple_metric_with_custom_granularity_in_filter_and_group_by
test_filename: test_custom_granularity.py
docstring:
  Simple metric queried with a filter on a custom grain, where that grain is also used in the group by.
sql_engine: Postgres
---
-- Write to DataTable
SELECT
  subq_6.metric_time__alien_day
  , subq_6.bookings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_5.metric_time__alien_day
    , subq_5.bookings
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_4.metric_time__alien_day
      , SUM(subq_4.bookings) AS bookings
    FROM (
      -- Pass Only Elements: ['bookings', 'metric_time__alien_day']
      SELECT
        subq_3.metric_time__alien_day
        , subq_3.bookings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_2.metric_time__alien_day
          , subq_2.ds__day
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
          , subq_2.metric_time__day
          , subq_2.metric_time__week
          , subq_2.metric_time__month
          , subq_2.metric_time__quarter
          , subq_2.metric_time__year
          , subq_2.metric_time__extract_year
          , subq_2.metric_time__extract_quarter
          , subq_2.metric_time__extract_month
          , subq_2.metric_time__extract_day
          , subq_2.metric_time__extract_dow
          , subq_2.metric_time__extract_doy
          , subq_2.listing
          , subq_2.guest
          , subq_2.host
          , subq_2.booking__listing
          , subq_2.booking__guest
          , subq_2.booking__host
          , subq_2.is_instant
          , subq_2.booking__is_instant
          , subq_2.bookings
          , subq_2.average_booking_value
          , subq_2.instant_bookings
          , subq_2.booking_value
          , subq_2.max_booking_value
          , subq_2.min_booking_value
          , subq_2.instant_booking_value
          , subq_2.average_instant_booking_value
          , subq_2.booking_value_for_non_null_listing_id
          , subq_2.bookers
          , subq_2.referred_bookings
          , subq_2.median_booking_value
          , subq_2.booking_value_p99
          , subq_2.discrete_booking_value_p99
          , subq_2.approximate_continuous_booking_value_p99
          , subq_2.approximate_discrete_booking_value_p99
          , subq_2.bookings_join_to_time_spine
          , subq_2.bookings_fill_nulls_with_0_without_time_spine
          , subq_2.bookings_fill_nulls_with_0
          , subq_2.instant_bookings_with_measure_filter
          , subq_2.bookings_join_to_time_spine_with_tiered_filters
          , subq_2.bookers_fill_nulls_with_0_join_to_timespine
        FROM (
          -- Metric Time Dimension 'ds'
          -- Join to Custom Granularity Dataset
          SELECT
            subq_0.ds__day AS ds__day
            , subq_0.ds__week AS ds__week
            , subq_0.ds__month AS ds__month
            , subq_0.ds__quarter AS ds__quarter
            , subq_0.ds__year AS ds__year
            , subq_0.ds__extract_year AS ds__extract_year
            , subq_0.ds__extract_quarter AS ds__extract_quarter
            , subq_0.ds__extract_month AS ds__extract_month
            , subq_0.ds__extract_day AS ds__extract_day
            , subq_0.ds__extract_dow AS ds__extract_dow
            , subq_0.ds__extract_doy AS ds__extract_doy
            , subq_0.ds_partitioned__day AS ds_partitioned__day
            , subq_0.ds_partitioned__week AS ds_partitioned__week
            , subq_0.ds_partitioned__month AS ds_partitioned__month
            , subq_0.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_0.ds_partitioned__year AS ds_partitioned__year
            , subq_0.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , subq_0.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , subq_0.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , subq_0.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , subq_0.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , subq_0.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , subq_0.paid_at__day AS paid_at__day
            , subq_0.paid_at__week AS paid_at__week
            , subq_0.paid_at__month AS paid_at__month
            , subq_0.paid_at__quarter AS paid_at__quarter
            , subq_0.paid_at__year AS paid_at__year
            , subq_0.paid_at__extract_year AS paid_at__extract_year
            , subq_0.paid_at__extract_quarter AS paid_at__extract_quarter
            , subq_0.paid_at__extract_month AS paid_at__extract_month
            , subq_0.paid_at__extract_day AS paid_at__extract_day
            , subq_0.paid_at__extract_dow AS paid_at__extract_dow
            , subq_0.paid_at__extract_doy AS paid_at__extract_doy
            , subq_0.booking__ds__day AS booking__ds__day
            , subq_0.booking__ds__week AS booking__ds__week
            , subq_0.booking__ds__month AS booking__ds__month
            , subq_0.booking__ds__quarter AS booking__ds__quarter
            , subq_0.booking__ds__year AS booking__ds__year
            , subq_0.booking__ds__extract_year AS booking__ds__extract_year
            , subq_0.booking__ds__extract_quarter AS booking__ds__extract_quarter
            , subq_0.booking__ds__extract_month AS booking__ds__extract_month
            , subq_0.booking__ds__extract_day AS booking__ds__extract_day
            , subq_0.booking__ds__extract_dow AS booking__ds__extract_dow
            , subq_0.booking__ds__extract_doy AS booking__ds__extract_doy
            , subq_0.booking__ds_partitioned__day AS booking__ds_partitioned__day
            , subq_0.booking__ds_partitioned__week AS booking__ds_partitioned__week
            , subq_0.booking__ds_partitioned__month AS booking__ds_partitioned__month
            , subq_0.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
            , subq_0.booking__ds_partitioned__year AS booking__ds_partitioned__year
            , subq_0.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
            , subq_0.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
            , subq_0.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
            , subq_0.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
            , subq_0.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
            , subq_0.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
            , subq_0.booking__paid_at__day AS booking__paid_at__day
            , subq_0.booking__paid_at__week AS booking__paid_at__week
            , subq_0.booking__paid_at__month AS booking__paid_at__month
            , subq_0.booking__paid_at__quarter AS booking__paid_at__quarter
            , subq_0.booking__paid_at__year AS booking__paid_at__year
            , subq_0.booking__paid_at__extract_year AS booking__paid_at__extract_year
            , subq_0.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
            , subq_0.booking__paid_at__extract_month AS booking__paid_at__extract_month
            , subq_0.booking__paid_at__extract_day AS booking__paid_at__extract_day
            , subq_0.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
            , subq_0.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
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
            , subq_0.listing AS listing
            , subq_0.guest AS guest
            , subq_0.host AS host
            , subq_0.booking__listing AS booking__listing
            , subq_0.booking__guest AS booking__guest
            , subq_0.booking__host AS booking__host
            , subq_0.is_instant AS is_instant
            , subq_0.booking__is_instant AS booking__is_instant
            , subq_0.bookings AS bookings
            , subq_0.average_booking_value AS average_booking_value
            , subq_0.instant_bookings AS instant_bookings
            , subq_0.booking_value AS booking_value
            , subq_0.max_booking_value AS max_booking_value
            , subq_0.min_booking_value AS min_booking_value
            , subq_0.instant_booking_value AS instant_booking_value
            , subq_0.average_instant_booking_value AS average_instant_booking_value
            , subq_0.booking_value_for_non_null_listing_id AS booking_value_for_non_null_listing_id
            , subq_0.bookers AS bookers
            , subq_0.referred_bookings AS referred_bookings
            , subq_0.median_booking_value AS median_booking_value
            , subq_0.booking_value_p99 AS booking_value_p99
            , subq_0.discrete_booking_value_p99 AS discrete_booking_value_p99
            , subq_0.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
            , subq_0.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
            , subq_0.bookings_join_to_time_spine AS bookings_join_to_time_spine
            , subq_0.bookings_fill_nulls_with_0_without_time_spine AS bookings_fill_nulls_with_0_without_time_spine
            , subq_0.bookings_fill_nulls_with_0 AS bookings_fill_nulls_with_0
            , subq_0.instant_bookings_with_measure_filter AS instant_bookings_with_measure_filter
            , subq_0.bookings_join_to_time_spine_with_tiered_filters AS bookings_join_to_time_spine_with_tiered_filters
            , subq_0.bookers_fill_nulls_with_0_join_to_timespine AS bookers_fill_nulls_with_0_join_to_timespine
            , subq_1.alien_day AS metric_time__alien_day
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
            SELECT
              1 AS bookings
              , bookings_source_src_28000.booking_value AS average_booking_value
              , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
              , bookings_source_src_28000.booking_value
              , bookings_source_src_28000.booking_value AS max_booking_value
              , bookings_source_src_28000.booking_value AS min_booking_value
              , bookings_source_src_28000.booking_value AS instant_booking_value
              , bookings_source_src_28000.booking_value AS average_instant_booking_value
              , bookings_source_src_28000.booking_value AS booking_value_for_non_null_listing_id
              , bookings_source_src_28000.guest_id AS bookers
              , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
              , bookings_source_src_28000.booking_value AS median_booking_value
              , bookings_source_src_28000.booking_value AS booking_value_p99
              , bookings_source_src_28000.booking_value AS discrete_booking_value_p99
              , bookings_source_src_28000.booking_value AS approximate_continuous_booking_value_p99
              , bookings_source_src_28000.booking_value AS approximate_discrete_booking_value_p99
              , 1 AS bookings_join_to_time_spine
              , 1 AS bookings_fill_nulls_with_0_without_time_spine
              , 1 AS bookings_fill_nulls_with_0
              , 1 AS instant_bookings_with_measure_filter
              , 1 AS bookings_join_to_time_spine_with_tiered_filters
              , bookings_source_src_28000.guest_id AS bookers_fill_nulls_with_0_join_to_timespine
              , bookings_source_src_28000.booking_value AS booking_payments
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
          ) subq_0
          LEFT OUTER JOIN
            ***************************.mf_time_spine subq_1
          ON
            subq_0.ds__day = subq_1.ds
        ) subq_2
        WHERE metric_time__alien_day = '2020-01-01'
      ) subq_3
    ) subq_4
    GROUP BY
      subq_4.metric_time__alien_day
  ) subq_5
) subq_6
