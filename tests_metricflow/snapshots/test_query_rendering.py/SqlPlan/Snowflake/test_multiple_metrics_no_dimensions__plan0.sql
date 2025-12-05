test_name: test_multiple_metrics_no_dimensions
test_filename: test_query_rendering.py
sql_engine: Snowflake
---
-- Write to DataTable
SELECT
  subq_16.bookings
  , subq_16.listings
FROM (
  -- Combine Aggregated Outputs
  SELECT
    MAX(subq_8.bookings) AS bookings
    , MAX(subq_15.listings) AS listings
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_7.__bookings AS bookings
    FROM (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        SUM(subq_6.__bookings) AS __bookings
      FROM (
        -- Pass Only Elements: ['__bookings']
        SELECT
          subq_5.__bookings
        FROM (
          -- Pass Only Elements: ['__bookings']
          SELECT
            subq_4.__bookings
          FROM (
            -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
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
              -- Metric Time Dimension 'ds'
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
              ) subq_2
            ) subq_3
            WHERE subq_3.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
          ) subq_4
        ) subq_5
      ) subq_6
    ) subq_7
  ) subq_8
  CROSS JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_14.__listings AS listings
    FROM (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        SUM(subq_13.__listings) AS __listings
      FROM (
        -- Pass Only Elements: ['__listings']
        SELECT
          subq_12.__listings
        FROM (
          -- Pass Only Elements: ['__listings']
          SELECT
            subq_11.__listings
          FROM (
            -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
            SELECT
              subq_10.ds__day
              , subq_10.ds__week
              , subq_10.ds__month
              , subq_10.ds__quarter
              , subq_10.ds__year
              , subq_10.ds__extract_year
              , subq_10.ds__extract_quarter
              , subq_10.ds__extract_month
              , subq_10.ds__extract_day
              , subq_10.ds__extract_dow
              , subq_10.ds__extract_doy
              , subq_10.created_at__day
              , subq_10.created_at__week
              , subq_10.created_at__month
              , subq_10.created_at__quarter
              , subq_10.created_at__year
              , subq_10.created_at__extract_year
              , subq_10.created_at__extract_quarter
              , subq_10.created_at__extract_month
              , subq_10.created_at__extract_day
              , subq_10.created_at__extract_dow
              , subq_10.created_at__extract_doy
              , subq_10.listing__ds__day
              , subq_10.listing__ds__week
              , subq_10.listing__ds__month
              , subq_10.listing__ds__quarter
              , subq_10.listing__ds__year
              , subq_10.listing__ds__extract_year
              , subq_10.listing__ds__extract_quarter
              , subq_10.listing__ds__extract_month
              , subq_10.listing__ds__extract_day
              , subq_10.listing__ds__extract_dow
              , subq_10.listing__ds__extract_doy
              , subq_10.listing__created_at__day
              , subq_10.listing__created_at__week
              , subq_10.listing__created_at__month
              , subq_10.listing__created_at__quarter
              , subq_10.listing__created_at__year
              , subq_10.listing__created_at__extract_year
              , subq_10.listing__created_at__extract_quarter
              , subq_10.listing__created_at__extract_month
              , subq_10.listing__created_at__extract_day
              , subq_10.listing__created_at__extract_dow
              , subq_10.listing__created_at__extract_doy
              , subq_10.metric_time__day
              , subq_10.metric_time__week
              , subq_10.metric_time__month
              , subq_10.metric_time__quarter
              , subq_10.metric_time__year
              , subq_10.metric_time__extract_year
              , subq_10.metric_time__extract_quarter
              , subq_10.metric_time__extract_month
              , subq_10.metric_time__extract_day
              , subq_10.metric_time__extract_dow
              , subq_10.metric_time__extract_doy
              , subq_10.listing
              , subq_10.user
              , subq_10.listing__user
              , subq_10.country_latest
              , subq_10.is_lux_latest
              , subq_10.capacity_latest
              , subq_10.listing__country_latest
              , subq_10.listing__is_lux_latest
              , subq_10.listing__capacity_latest
              , subq_10.__listings
              , subq_10.__lux_listings
              , subq_10.__smallest_listing
              , subq_10.__largest_listing
              , subq_10.__active_listings
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_9.ds__day
                , subq_9.ds__week
                , subq_9.ds__month
                , subq_9.ds__quarter
                , subq_9.ds__year
                , subq_9.ds__extract_year
                , subq_9.ds__extract_quarter
                , subq_9.ds__extract_month
                , subq_9.ds__extract_day
                , subq_9.ds__extract_dow
                , subq_9.ds__extract_doy
                , subq_9.created_at__day
                , subq_9.created_at__week
                , subq_9.created_at__month
                , subq_9.created_at__quarter
                , subq_9.created_at__year
                , subq_9.created_at__extract_year
                , subq_9.created_at__extract_quarter
                , subq_9.created_at__extract_month
                , subq_9.created_at__extract_day
                , subq_9.created_at__extract_dow
                , subq_9.created_at__extract_doy
                , subq_9.listing__ds__day
                , subq_9.listing__ds__week
                , subq_9.listing__ds__month
                , subq_9.listing__ds__quarter
                , subq_9.listing__ds__year
                , subq_9.listing__ds__extract_year
                , subq_9.listing__ds__extract_quarter
                , subq_9.listing__ds__extract_month
                , subq_9.listing__ds__extract_day
                , subq_9.listing__ds__extract_dow
                , subq_9.listing__ds__extract_doy
                , subq_9.listing__created_at__day
                , subq_9.listing__created_at__week
                , subq_9.listing__created_at__month
                , subq_9.listing__created_at__quarter
                , subq_9.listing__created_at__year
                , subq_9.listing__created_at__extract_year
                , subq_9.listing__created_at__extract_quarter
                , subq_9.listing__created_at__extract_month
                , subq_9.listing__created_at__extract_day
                , subq_9.listing__created_at__extract_dow
                , subq_9.listing__created_at__extract_doy
                , subq_9.ds__day AS metric_time__day
                , subq_9.ds__week AS metric_time__week
                , subq_9.ds__month AS metric_time__month
                , subq_9.ds__quarter AS metric_time__quarter
                , subq_9.ds__year AS metric_time__year
                , subq_9.ds__extract_year AS metric_time__extract_year
                , subq_9.ds__extract_quarter AS metric_time__extract_quarter
                , subq_9.ds__extract_month AS metric_time__extract_month
                , subq_9.ds__extract_day AS metric_time__extract_day
                , subq_9.ds__extract_dow AS metric_time__extract_dow
                , subq_9.ds__extract_doy AS metric_time__extract_doy
                , subq_9.listing
                , subq_9.user
                , subq_9.listing__user
                , subq_9.country_latest
                , subq_9.is_lux_latest
                , subq_9.capacity_latest
                , subq_9.listing__country_latest
                , subq_9.listing__is_lux_latest
                , subq_9.listing__capacity_latest
                , subq_9.__listings
                , subq_9.__lux_listings
                , subq_9.__smallest_listing
                , subq_9.__largest_listing
                , subq_9.__active_listings
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
              ) subq_9
            ) subq_10
            WHERE subq_10.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
          ) subq_11
        ) subq_12
      ) subq_13
    ) subq_14
  ) subq_15
) subq_16
