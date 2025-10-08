test_name: test_nested_filters
test_filename: test_derived_metric_rendering.py
docstring:
  Tests derived metric rendering for a nested derived metric with filters on the outer metric spec.
sql_engine: Snowflake
---
-- Write to DataTable
SELECT
  subq_25.instant_lux_booking_value_rate
FROM (
  -- Compute Metrics via Expressions
  SELECT
    instant_lux_booking_value_rate AS instant_lux_booking_value_rate
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      average_booking_value * bookings / NULLIF(booking_value, 0) AS instant_lux_booking_value_rate
    FROM (
      -- Combine Aggregated Outputs
      SELECT
        MAX(subq_9.average_booking_value) AS average_booking_value
        , MAX(subq_17.bookings) AS bookings
        , MAX(subq_22.booking_value) AS booking_value
      FROM (
        -- Compute Metrics via Expressions
        SELECT
          subq_8.average_booking_value
        FROM (
          -- Aggregate Inputs for Simple Metrics
          SELECT
            AVG(subq_7.average_booking_value) AS average_booking_value
          FROM (
            -- Pass Only Elements: ['average_booking_value']
            SELECT
              subq_6.average_booking_value
            FROM (
              -- Constrain Output with WHERE
              SELECT
                subq_5.ds__day
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
                , subq_5.ds_partitioned__day
                , subq_5.ds_partitioned__week
                , subq_5.ds_partitioned__month
                , subq_5.ds_partitioned__quarter
                , subq_5.ds_partitioned__year
                , subq_5.ds_partitioned__extract_year
                , subq_5.ds_partitioned__extract_quarter
                , subq_5.ds_partitioned__extract_month
                , subq_5.ds_partitioned__extract_day
                , subq_5.ds_partitioned__extract_dow
                , subq_5.ds_partitioned__extract_doy
                , subq_5.paid_at__day
                , subq_5.paid_at__week
                , subq_5.paid_at__month
                , subq_5.paid_at__quarter
                , subq_5.paid_at__year
                , subq_5.paid_at__extract_year
                , subq_5.paid_at__extract_quarter
                , subq_5.paid_at__extract_month
                , subq_5.paid_at__extract_day
                , subq_5.paid_at__extract_dow
                , subq_5.paid_at__extract_doy
                , subq_5.booking__ds__day
                , subq_5.booking__ds__week
                , subq_5.booking__ds__month
                , subq_5.booking__ds__quarter
                , subq_5.booking__ds__year
                , subq_5.booking__ds__extract_year
                , subq_5.booking__ds__extract_quarter
                , subq_5.booking__ds__extract_month
                , subq_5.booking__ds__extract_day
                , subq_5.booking__ds__extract_dow
                , subq_5.booking__ds__extract_doy
                , subq_5.booking__ds_partitioned__day
                , subq_5.booking__ds_partitioned__week
                , subq_5.booking__ds_partitioned__month
                , subq_5.booking__ds_partitioned__quarter
                , subq_5.booking__ds_partitioned__year
                , subq_5.booking__ds_partitioned__extract_year
                , subq_5.booking__ds_partitioned__extract_quarter
                , subq_5.booking__ds_partitioned__extract_month
                , subq_5.booking__ds_partitioned__extract_day
                , subq_5.booking__ds_partitioned__extract_dow
                , subq_5.booking__ds_partitioned__extract_doy
                , subq_5.booking__paid_at__day
                , subq_5.booking__paid_at__week
                , subq_5.booking__paid_at__month
                , subq_5.booking__paid_at__quarter
                , subq_5.booking__paid_at__year
                , subq_5.booking__paid_at__extract_year
                , subq_5.booking__paid_at__extract_quarter
                , subq_5.booking__paid_at__extract_month
                , subq_5.booking__paid_at__extract_day
                , subq_5.booking__paid_at__extract_dow
                , subq_5.booking__paid_at__extract_doy
                , subq_5.metric_time__day
                , subq_5.metric_time__week
                , subq_5.metric_time__month
                , subq_5.metric_time__quarter
                , subq_5.metric_time__year
                , subq_5.metric_time__extract_year
                , subq_5.metric_time__extract_quarter
                , subq_5.metric_time__extract_month
                , subq_5.metric_time__extract_day
                , subq_5.metric_time__extract_dow
                , subq_5.metric_time__extract_doy
                , subq_5.listing
                , subq_5.guest
                , subq_5.host
                , subq_5.booking__listing
                , subq_5.booking__guest
                , subq_5.booking__host
                , subq_5.is_instant
                , subq_5.booking__is_instant
                , subq_5.listing__is_lux_latest
                , subq_5.bookings
                , subq_5.average_booking_value
                , subq_5.instant_bookings
                , subq_5.booking_value
                , subq_5.max_booking_value
                , subq_5.min_booking_value
                , subq_5.instant_booking_value
                , subq_5.average_instant_booking_value
                , subq_5.booking_value_for_non_null_listing_id
                , subq_5.bookers
                , subq_5.referred_bookings
                , subq_5.median_booking_value
                , subq_5.booking_value_p99
                , subq_5.discrete_booking_value_p99
                , subq_5.approximate_continuous_booking_value_p99
                , subq_5.approximate_discrete_booking_value_p99
                , subq_5.bookings_join_to_time_spine
                , subq_5.bookings_fill_nulls_with_0_without_time_spine
                , subq_5.bookings_fill_nulls_with_0
                , subq_5.instant_bookings_with_measure_filter
                , subq_5.bookings_join_to_time_spine_with_tiered_filters
                , subq_5.bookers_fill_nulls_with_0_join_to_timespine
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_4.is_lux_latest AS listing__is_lux_latest
                  , subq_1.ds__day AS ds__day
                  , subq_1.ds__week AS ds__week
                  , subq_1.ds__month AS ds__month
                  , subq_1.ds__quarter AS ds__quarter
                  , subq_1.ds__year AS ds__year
                  , subq_1.ds__extract_year AS ds__extract_year
                  , subq_1.ds__extract_quarter AS ds__extract_quarter
                  , subq_1.ds__extract_month AS ds__extract_month
                  , subq_1.ds__extract_day AS ds__extract_day
                  , subq_1.ds__extract_dow AS ds__extract_dow
                  , subq_1.ds__extract_doy AS ds__extract_doy
                  , subq_1.ds_partitioned__day AS ds_partitioned__day
                  , subq_1.ds_partitioned__week AS ds_partitioned__week
                  , subq_1.ds_partitioned__month AS ds_partitioned__month
                  , subq_1.ds_partitioned__quarter AS ds_partitioned__quarter
                  , subq_1.ds_partitioned__year AS ds_partitioned__year
                  , subq_1.ds_partitioned__extract_year AS ds_partitioned__extract_year
                  , subq_1.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                  , subq_1.ds_partitioned__extract_month AS ds_partitioned__extract_month
                  , subq_1.ds_partitioned__extract_day AS ds_partitioned__extract_day
                  , subq_1.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                  , subq_1.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                  , subq_1.paid_at__day AS paid_at__day
                  , subq_1.paid_at__week AS paid_at__week
                  , subq_1.paid_at__month AS paid_at__month
                  , subq_1.paid_at__quarter AS paid_at__quarter
                  , subq_1.paid_at__year AS paid_at__year
                  , subq_1.paid_at__extract_year AS paid_at__extract_year
                  , subq_1.paid_at__extract_quarter AS paid_at__extract_quarter
                  , subq_1.paid_at__extract_month AS paid_at__extract_month
                  , subq_1.paid_at__extract_day AS paid_at__extract_day
                  , subq_1.paid_at__extract_dow AS paid_at__extract_dow
                  , subq_1.paid_at__extract_doy AS paid_at__extract_doy
                  , subq_1.booking__ds__day AS booking__ds__day
                  , subq_1.booking__ds__week AS booking__ds__week
                  , subq_1.booking__ds__month AS booking__ds__month
                  , subq_1.booking__ds__quarter AS booking__ds__quarter
                  , subq_1.booking__ds__year AS booking__ds__year
                  , subq_1.booking__ds__extract_year AS booking__ds__extract_year
                  , subq_1.booking__ds__extract_quarter AS booking__ds__extract_quarter
                  , subq_1.booking__ds__extract_month AS booking__ds__extract_month
                  , subq_1.booking__ds__extract_day AS booking__ds__extract_day
                  , subq_1.booking__ds__extract_dow AS booking__ds__extract_dow
                  , subq_1.booking__ds__extract_doy AS booking__ds__extract_doy
                  , subq_1.booking__ds_partitioned__day AS booking__ds_partitioned__day
                  , subq_1.booking__ds_partitioned__week AS booking__ds_partitioned__week
                  , subq_1.booking__ds_partitioned__month AS booking__ds_partitioned__month
                  , subq_1.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                  , subq_1.booking__ds_partitioned__year AS booking__ds_partitioned__year
                  , subq_1.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                  , subq_1.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                  , subq_1.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                  , subq_1.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                  , subq_1.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                  , subq_1.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                  , subq_1.booking__paid_at__day AS booking__paid_at__day
                  , subq_1.booking__paid_at__week AS booking__paid_at__week
                  , subq_1.booking__paid_at__month AS booking__paid_at__month
                  , subq_1.booking__paid_at__quarter AS booking__paid_at__quarter
                  , subq_1.booking__paid_at__year AS booking__paid_at__year
                  , subq_1.booking__paid_at__extract_year AS booking__paid_at__extract_year
                  , subq_1.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                  , subq_1.booking__paid_at__extract_month AS booking__paid_at__extract_month
                  , subq_1.booking__paid_at__extract_day AS booking__paid_at__extract_day
                  , subq_1.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                  , subq_1.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
                  , subq_1.metric_time__day AS metric_time__day
                  , subq_1.metric_time__week AS metric_time__week
                  , subq_1.metric_time__month AS metric_time__month
                  , subq_1.metric_time__quarter AS metric_time__quarter
                  , subq_1.metric_time__year AS metric_time__year
                  , subq_1.metric_time__extract_year AS metric_time__extract_year
                  , subq_1.metric_time__extract_quarter AS metric_time__extract_quarter
                  , subq_1.metric_time__extract_month AS metric_time__extract_month
                  , subq_1.metric_time__extract_day AS metric_time__extract_day
                  , subq_1.metric_time__extract_dow AS metric_time__extract_dow
                  , subq_1.metric_time__extract_doy AS metric_time__extract_doy
                  , subq_1.listing AS listing
                  , subq_1.guest AS guest
                  , subq_1.host AS host
                  , subq_1.booking__listing AS booking__listing
                  , subq_1.booking__guest AS booking__guest
                  , subq_1.booking__host AS booking__host
                  , subq_1.is_instant AS is_instant
                  , subq_1.booking__is_instant AS booking__is_instant
                  , subq_1.bookings AS bookings
                  , subq_1.average_booking_value AS average_booking_value
                  , subq_1.instant_bookings AS instant_bookings
                  , subq_1.booking_value AS booking_value
                  , subq_1.max_booking_value AS max_booking_value
                  , subq_1.min_booking_value AS min_booking_value
                  , subq_1.instant_booking_value AS instant_booking_value
                  , subq_1.average_instant_booking_value AS average_instant_booking_value
                  , subq_1.booking_value_for_non_null_listing_id AS booking_value_for_non_null_listing_id
                  , subq_1.bookers AS bookers
                  , subq_1.referred_bookings AS referred_bookings
                  , subq_1.median_booking_value AS median_booking_value
                  , subq_1.booking_value_p99 AS booking_value_p99
                  , subq_1.discrete_booking_value_p99 AS discrete_booking_value_p99
                  , subq_1.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
                  , subq_1.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
                  , subq_1.bookings_join_to_time_spine AS bookings_join_to_time_spine
                  , subq_1.bookings_fill_nulls_with_0_without_time_spine AS bookings_fill_nulls_with_0_without_time_spine
                  , subq_1.bookings_fill_nulls_with_0 AS bookings_fill_nulls_with_0
                  , subq_1.instant_bookings_with_measure_filter AS instant_bookings_with_measure_filter
                  , subq_1.bookings_join_to_time_spine_with_tiered_filters AS bookings_join_to_time_spine_with_tiered_filters
                  , subq_1.bookers_fill_nulls_with_0_join_to_timespine AS bookers_fill_nulls_with_0_join_to_timespine
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
                    , subq_0.bookings
                    , subq_0.average_booking_value
                    , subq_0.instant_bookings
                    , subq_0.booking_value
                    , subq_0.max_booking_value
                    , subq_0.min_booking_value
                    , subq_0.instant_booking_value
                    , subq_0.average_instant_booking_value
                    , subq_0.booking_value_for_non_null_listing_id
                    , subq_0.bookers
                    , subq_0.referred_bookings
                    , subq_0.median_booking_value
                    , subq_0.booking_value_p99
                    , subq_0.discrete_booking_value_p99
                    , subq_0.approximate_continuous_booking_value_p99
                    , subq_0.approximate_discrete_booking_value_p99
                    , subq_0.bookings_join_to_time_spine
                    , subq_0.bookings_fill_nulls_with_0_without_time_spine
                    , subq_0.bookings_fill_nulls_with_0
                    , subq_0.instant_bookings_with_measure_filter
                    , subq_0.bookings_join_to_time_spine_with_tiered_filters
                    , subq_0.bookers_fill_nulls_with_0_join_to_timespine
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
                  ) subq_0
                ) subq_1
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['is_lux_latest', 'listing']
                  SELECT
                    subq_3.listing
                    , subq_3.is_lux_latest
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
                      , subq_2.created_at__day
                      , subq_2.created_at__week
                      , subq_2.created_at__month
                      , subq_2.created_at__quarter
                      , subq_2.created_at__year
                      , subq_2.created_at__extract_year
                      , subq_2.created_at__extract_quarter
                      , subq_2.created_at__extract_month
                      , subq_2.created_at__extract_day
                      , subq_2.created_at__extract_dow
                      , subq_2.created_at__extract_doy
                      , subq_2.listing__ds__day
                      , subq_2.listing__ds__week
                      , subq_2.listing__ds__month
                      , subq_2.listing__ds__quarter
                      , subq_2.listing__ds__year
                      , subq_2.listing__ds__extract_year
                      , subq_2.listing__ds__extract_quarter
                      , subq_2.listing__ds__extract_month
                      , subq_2.listing__ds__extract_day
                      , subq_2.listing__ds__extract_dow
                      , subq_2.listing__ds__extract_doy
                      , subq_2.listing__created_at__day
                      , subq_2.listing__created_at__week
                      , subq_2.listing__created_at__month
                      , subq_2.listing__created_at__quarter
                      , subq_2.listing__created_at__year
                      , subq_2.listing__created_at__extract_year
                      , subq_2.listing__created_at__extract_quarter
                      , subq_2.listing__created_at__extract_month
                      , subq_2.listing__created_at__extract_day
                      , subq_2.listing__created_at__extract_dow
                      , subq_2.listing__created_at__extract_doy
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
                      , subq_2.user
                      , subq_2.listing__user
                      , subq_2.country_latest
                      , subq_2.is_lux_latest
                      , subq_2.capacity_latest
                      , subq_2.listing__country_latest
                      , subq_2.listing__is_lux_latest
                      , subq_2.listing__capacity_latest
                      , subq_2.listings
                      , subq_2.lux_listings
                      , subq_2.smallest_listing
                      , subq_2.largest_listing
                      , subq_2.active_listings
                    FROM (
                      -- Read Elements From Semantic Model 'listings_latest'
                      SELECT
                        1 AS listings
                        , 1 AS lux_listings
                        , listings_latest_src_28000.capacity AS smallest_listing
                        , listings_latest_src_28000.capacity AS largest_listing
                        , 1 AS active_listings
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
                    ) subq_2
                  ) subq_3
                ) subq_4
                ON
                  subq_1.listing = subq_4.listing
              ) subq_5
              WHERE (listing__is_lux_latest) AND (booking__is_instant)
            ) subq_6
          ) subq_7
        ) subq_8
      ) subq_9
      CROSS JOIN (
        -- Compute Metrics via Expressions
        SELECT
          subq_16.bookings
        FROM (
          -- Aggregate Inputs for Simple Metrics
          SELECT
            SUM(subq_15.bookings) AS bookings
          FROM (
            -- Pass Only Elements: ['bookings']
            SELECT
              subq_14.bookings
            FROM (
              -- Constrain Output with WHERE
              SELECT
                subq_13.ds__day
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
                , subq_13.ds_partitioned__day
                , subq_13.ds_partitioned__week
                , subq_13.ds_partitioned__month
                , subq_13.ds_partitioned__quarter
                , subq_13.ds_partitioned__year
                , subq_13.ds_partitioned__extract_year
                , subq_13.ds_partitioned__extract_quarter
                , subq_13.ds_partitioned__extract_month
                , subq_13.ds_partitioned__extract_day
                , subq_13.ds_partitioned__extract_dow
                , subq_13.ds_partitioned__extract_doy
                , subq_13.paid_at__day
                , subq_13.paid_at__week
                , subq_13.paid_at__month
                , subq_13.paid_at__quarter
                , subq_13.paid_at__year
                , subq_13.paid_at__extract_year
                , subq_13.paid_at__extract_quarter
                , subq_13.paid_at__extract_month
                , subq_13.paid_at__extract_day
                , subq_13.paid_at__extract_dow
                , subq_13.paid_at__extract_doy
                , subq_13.booking__ds__day
                , subq_13.booking__ds__week
                , subq_13.booking__ds__month
                , subq_13.booking__ds__quarter
                , subq_13.booking__ds__year
                , subq_13.booking__ds__extract_year
                , subq_13.booking__ds__extract_quarter
                , subq_13.booking__ds__extract_month
                , subq_13.booking__ds__extract_day
                , subq_13.booking__ds__extract_dow
                , subq_13.booking__ds__extract_doy
                , subq_13.booking__ds_partitioned__day
                , subq_13.booking__ds_partitioned__week
                , subq_13.booking__ds_partitioned__month
                , subq_13.booking__ds_partitioned__quarter
                , subq_13.booking__ds_partitioned__year
                , subq_13.booking__ds_partitioned__extract_year
                , subq_13.booking__ds_partitioned__extract_quarter
                , subq_13.booking__ds_partitioned__extract_month
                , subq_13.booking__ds_partitioned__extract_day
                , subq_13.booking__ds_partitioned__extract_dow
                , subq_13.booking__ds_partitioned__extract_doy
                , subq_13.booking__paid_at__day
                , subq_13.booking__paid_at__week
                , subq_13.booking__paid_at__month
                , subq_13.booking__paid_at__quarter
                , subq_13.booking__paid_at__year
                , subq_13.booking__paid_at__extract_year
                , subq_13.booking__paid_at__extract_quarter
                , subq_13.booking__paid_at__extract_month
                , subq_13.booking__paid_at__extract_day
                , subq_13.booking__paid_at__extract_dow
                , subq_13.booking__paid_at__extract_doy
                , subq_13.metric_time__day
                , subq_13.metric_time__week
                , subq_13.metric_time__month
                , subq_13.metric_time__quarter
                , subq_13.metric_time__year
                , subq_13.metric_time__extract_year
                , subq_13.metric_time__extract_quarter
                , subq_13.metric_time__extract_month
                , subq_13.metric_time__extract_day
                , subq_13.metric_time__extract_dow
                , subq_13.metric_time__extract_doy
                , subq_13.listing
                , subq_13.guest
                , subq_13.host
                , subq_13.booking__listing
                , subq_13.booking__guest
                , subq_13.booking__host
                , subq_13.is_instant
                , subq_13.booking__is_instant
                , subq_13.listing__is_lux_latest
                , subq_13.bookings
                , subq_13.average_booking_value
                , subq_13.instant_bookings
                , subq_13.booking_value
                , subq_13.max_booking_value
                , subq_13.min_booking_value
                , subq_13.instant_booking_value
                , subq_13.average_instant_booking_value
                , subq_13.booking_value_for_non_null_listing_id
                , subq_13.bookers
                , subq_13.referred_bookings
                , subq_13.median_booking_value
                , subq_13.booking_value_p99
                , subq_13.discrete_booking_value_p99
                , subq_13.approximate_continuous_booking_value_p99
                , subq_13.approximate_discrete_booking_value_p99
                , subq_13.bookings_join_to_time_spine
                , subq_13.bookings_fill_nulls_with_0_without_time_spine
                , subq_13.bookings_fill_nulls_with_0
                , subq_13.instant_bookings_with_measure_filter
                , subq_13.bookings_join_to_time_spine_with_tiered_filters
                , subq_13.bookers_fill_nulls_with_0_join_to_timespine
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_12.is_lux_latest AS listing__is_lux_latest
                  , subq_10.ds__day AS ds__day
                  , subq_10.ds__week AS ds__week
                  , subq_10.ds__month AS ds__month
                  , subq_10.ds__quarter AS ds__quarter
                  , subq_10.ds__year AS ds__year
                  , subq_10.ds__extract_year AS ds__extract_year
                  , subq_10.ds__extract_quarter AS ds__extract_quarter
                  , subq_10.ds__extract_month AS ds__extract_month
                  , subq_10.ds__extract_day AS ds__extract_day
                  , subq_10.ds__extract_dow AS ds__extract_dow
                  , subq_10.ds__extract_doy AS ds__extract_doy
                  , subq_10.ds_partitioned__day AS ds_partitioned__day
                  , subq_10.ds_partitioned__week AS ds_partitioned__week
                  , subq_10.ds_partitioned__month AS ds_partitioned__month
                  , subq_10.ds_partitioned__quarter AS ds_partitioned__quarter
                  , subq_10.ds_partitioned__year AS ds_partitioned__year
                  , subq_10.ds_partitioned__extract_year AS ds_partitioned__extract_year
                  , subq_10.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                  , subq_10.ds_partitioned__extract_month AS ds_partitioned__extract_month
                  , subq_10.ds_partitioned__extract_day AS ds_partitioned__extract_day
                  , subq_10.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                  , subq_10.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                  , subq_10.paid_at__day AS paid_at__day
                  , subq_10.paid_at__week AS paid_at__week
                  , subq_10.paid_at__month AS paid_at__month
                  , subq_10.paid_at__quarter AS paid_at__quarter
                  , subq_10.paid_at__year AS paid_at__year
                  , subq_10.paid_at__extract_year AS paid_at__extract_year
                  , subq_10.paid_at__extract_quarter AS paid_at__extract_quarter
                  , subq_10.paid_at__extract_month AS paid_at__extract_month
                  , subq_10.paid_at__extract_day AS paid_at__extract_day
                  , subq_10.paid_at__extract_dow AS paid_at__extract_dow
                  , subq_10.paid_at__extract_doy AS paid_at__extract_doy
                  , subq_10.booking__ds__day AS booking__ds__day
                  , subq_10.booking__ds__week AS booking__ds__week
                  , subq_10.booking__ds__month AS booking__ds__month
                  , subq_10.booking__ds__quarter AS booking__ds__quarter
                  , subq_10.booking__ds__year AS booking__ds__year
                  , subq_10.booking__ds__extract_year AS booking__ds__extract_year
                  , subq_10.booking__ds__extract_quarter AS booking__ds__extract_quarter
                  , subq_10.booking__ds__extract_month AS booking__ds__extract_month
                  , subq_10.booking__ds__extract_day AS booking__ds__extract_day
                  , subq_10.booking__ds__extract_dow AS booking__ds__extract_dow
                  , subq_10.booking__ds__extract_doy AS booking__ds__extract_doy
                  , subq_10.booking__ds_partitioned__day AS booking__ds_partitioned__day
                  , subq_10.booking__ds_partitioned__week AS booking__ds_partitioned__week
                  , subq_10.booking__ds_partitioned__month AS booking__ds_partitioned__month
                  , subq_10.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                  , subq_10.booking__ds_partitioned__year AS booking__ds_partitioned__year
                  , subq_10.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                  , subq_10.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                  , subq_10.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                  , subq_10.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                  , subq_10.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                  , subq_10.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                  , subq_10.booking__paid_at__day AS booking__paid_at__day
                  , subq_10.booking__paid_at__week AS booking__paid_at__week
                  , subq_10.booking__paid_at__month AS booking__paid_at__month
                  , subq_10.booking__paid_at__quarter AS booking__paid_at__quarter
                  , subq_10.booking__paid_at__year AS booking__paid_at__year
                  , subq_10.booking__paid_at__extract_year AS booking__paid_at__extract_year
                  , subq_10.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                  , subq_10.booking__paid_at__extract_month AS booking__paid_at__extract_month
                  , subq_10.booking__paid_at__extract_day AS booking__paid_at__extract_day
                  , subq_10.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                  , subq_10.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
                  , subq_10.metric_time__day AS metric_time__day
                  , subq_10.metric_time__week AS metric_time__week
                  , subq_10.metric_time__month AS metric_time__month
                  , subq_10.metric_time__quarter AS metric_time__quarter
                  , subq_10.metric_time__year AS metric_time__year
                  , subq_10.metric_time__extract_year AS metric_time__extract_year
                  , subq_10.metric_time__extract_quarter AS metric_time__extract_quarter
                  , subq_10.metric_time__extract_month AS metric_time__extract_month
                  , subq_10.metric_time__extract_day AS metric_time__extract_day
                  , subq_10.metric_time__extract_dow AS metric_time__extract_dow
                  , subq_10.metric_time__extract_doy AS metric_time__extract_doy
                  , subq_10.listing AS listing
                  , subq_10.guest AS guest
                  , subq_10.host AS host
                  , subq_10.booking__listing AS booking__listing
                  , subq_10.booking__guest AS booking__guest
                  , subq_10.booking__host AS booking__host
                  , subq_10.is_instant AS is_instant
                  , subq_10.booking__is_instant AS booking__is_instant
                  , subq_10.bookings AS bookings
                  , subq_10.average_booking_value AS average_booking_value
                  , subq_10.instant_bookings AS instant_bookings
                  , subq_10.booking_value AS booking_value
                  , subq_10.max_booking_value AS max_booking_value
                  , subq_10.min_booking_value AS min_booking_value
                  , subq_10.instant_booking_value AS instant_booking_value
                  , subq_10.average_instant_booking_value AS average_instant_booking_value
                  , subq_10.booking_value_for_non_null_listing_id AS booking_value_for_non_null_listing_id
                  , subq_10.bookers AS bookers
                  , subq_10.referred_bookings AS referred_bookings
                  , subq_10.median_booking_value AS median_booking_value
                  , subq_10.booking_value_p99 AS booking_value_p99
                  , subq_10.discrete_booking_value_p99 AS discrete_booking_value_p99
                  , subq_10.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
                  , subq_10.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
                  , subq_10.bookings_join_to_time_spine AS bookings_join_to_time_spine
                  , subq_10.bookings_fill_nulls_with_0_without_time_spine AS bookings_fill_nulls_with_0_without_time_spine
                  , subq_10.bookings_fill_nulls_with_0 AS bookings_fill_nulls_with_0
                  , subq_10.instant_bookings_with_measure_filter AS instant_bookings_with_measure_filter
                  , subq_10.bookings_join_to_time_spine_with_tiered_filters AS bookings_join_to_time_spine_with_tiered_filters
                  , subq_10.bookers_fill_nulls_with_0_join_to_timespine AS bookers_fill_nulls_with_0_join_to_timespine
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
                    , subq_0.bookings
                    , subq_0.average_booking_value
                    , subq_0.instant_bookings
                    , subq_0.booking_value
                    , subq_0.max_booking_value
                    , subq_0.min_booking_value
                    , subq_0.instant_booking_value
                    , subq_0.average_instant_booking_value
                    , subq_0.booking_value_for_non_null_listing_id
                    , subq_0.bookers
                    , subq_0.referred_bookings
                    , subq_0.median_booking_value
                    , subq_0.booking_value_p99
                    , subq_0.discrete_booking_value_p99
                    , subq_0.approximate_continuous_booking_value_p99
                    , subq_0.approximate_discrete_booking_value_p99
                    , subq_0.bookings_join_to_time_spine
                    , subq_0.bookings_fill_nulls_with_0_without_time_spine
                    , subq_0.bookings_fill_nulls_with_0
                    , subq_0.instant_bookings_with_measure_filter
                    , subq_0.bookings_join_to_time_spine_with_tiered_filters
                    , subq_0.bookers_fill_nulls_with_0_join_to_timespine
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
                  ) subq_0
                ) subq_10
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['is_lux_latest', 'listing']
                  SELECT
                    subq_11.listing
                    , subq_11.is_lux_latest
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
                      , subq_2.created_at__day
                      , subq_2.created_at__week
                      , subq_2.created_at__month
                      , subq_2.created_at__quarter
                      , subq_2.created_at__year
                      , subq_2.created_at__extract_year
                      , subq_2.created_at__extract_quarter
                      , subq_2.created_at__extract_month
                      , subq_2.created_at__extract_day
                      , subq_2.created_at__extract_dow
                      , subq_2.created_at__extract_doy
                      , subq_2.listing__ds__day
                      , subq_2.listing__ds__week
                      , subq_2.listing__ds__month
                      , subq_2.listing__ds__quarter
                      , subq_2.listing__ds__year
                      , subq_2.listing__ds__extract_year
                      , subq_2.listing__ds__extract_quarter
                      , subq_2.listing__ds__extract_month
                      , subq_2.listing__ds__extract_day
                      , subq_2.listing__ds__extract_dow
                      , subq_2.listing__ds__extract_doy
                      , subq_2.listing__created_at__day
                      , subq_2.listing__created_at__week
                      , subq_2.listing__created_at__month
                      , subq_2.listing__created_at__quarter
                      , subq_2.listing__created_at__year
                      , subq_2.listing__created_at__extract_year
                      , subq_2.listing__created_at__extract_quarter
                      , subq_2.listing__created_at__extract_month
                      , subq_2.listing__created_at__extract_day
                      , subq_2.listing__created_at__extract_dow
                      , subq_2.listing__created_at__extract_doy
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
                      , subq_2.user
                      , subq_2.listing__user
                      , subq_2.country_latest
                      , subq_2.is_lux_latest
                      , subq_2.capacity_latest
                      , subq_2.listing__country_latest
                      , subq_2.listing__is_lux_latest
                      , subq_2.listing__capacity_latest
                      , subq_2.listings
                      , subq_2.lux_listings
                      , subq_2.smallest_listing
                      , subq_2.largest_listing
                      , subq_2.active_listings
                    FROM (
                      -- Read Elements From Semantic Model 'listings_latest'
                      SELECT
                        1 AS listings
                        , 1 AS lux_listings
                        , listings_latest_src_28000.capacity AS smallest_listing
                        , listings_latest_src_28000.capacity AS largest_listing
                        , 1 AS active_listings
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
                    ) subq_2
                  ) subq_11
                ) subq_12
                ON
                  subq_10.listing = subq_12.listing
              ) subq_13
              WHERE (listing__is_lux_latest) AND (booking__is_instant)
            ) subq_14
          ) subq_15
        ) subq_16
      ) subq_17
      CROSS JOIN (
        -- Compute Metrics via Expressions
        SELECT
          subq_21.booking_value
        FROM (
          -- Aggregate Inputs for Simple Metrics
          SELECT
            SUM(subq_20.booking_value) AS booking_value
          FROM (
            -- Pass Only Elements: ['booking_value']
            SELECT
              subq_19.booking_value
            FROM (
              -- Constrain Output with WHERE
              SELECT
                subq_18.ds__day
                , subq_18.ds__week
                , subq_18.ds__month
                , subq_18.ds__quarter
                , subq_18.ds__year
                , subq_18.ds__extract_year
                , subq_18.ds__extract_quarter
                , subq_18.ds__extract_month
                , subq_18.ds__extract_day
                , subq_18.ds__extract_dow
                , subq_18.ds__extract_doy
                , subq_18.ds_partitioned__day
                , subq_18.ds_partitioned__week
                , subq_18.ds_partitioned__month
                , subq_18.ds_partitioned__quarter
                , subq_18.ds_partitioned__year
                , subq_18.ds_partitioned__extract_year
                , subq_18.ds_partitioned__extract_quarter
                , subq_18.ds_partitioned__extract_month
                , subq_18.ds_partitioned__extract_day
                , subq_18.ds_partitioned__extract_dow
                , subq_18.ds_partitioned__extract_doy
                , subq_18.paid_at__day
                , subq_18.paid_at__week
                , subq_18.paid_at__month
                , subq_18.paid_at__quarter
                , subq_18.paid_at__year
                , subq_18.paid_at__extract_year
                , subq_18.paid_at__extract_quarter
                , subq_18.paid_at__extract_month
                , subq_18.paid_at__extract_day
                , subq_18.paid_at__extract_dow
                , subq_18.paid_at__extract_doy
                , subq_18.booking__ds__day
                , subq_18.booking__ds__week
                , subq_18.booking__ds__month
                , subq_18.booking__ds__quarter
                , subq_18.booking__ds__year
                , subq_18.booking__ds__extract_year
                , subq_18.booking__ds__extract_quarter
                , subq_18.booking__ds__extract_month
                , subq_18.booking__ds__extract_day
                , subq_18.booking__ds__extract_dow
                , subq_18.booking__ds__extract_doy
                , subq_18.booking__ds_partitioned__day
                , subq_18.booking__ds_partitioned__week
                , subq_18.booking__ds_partitioned__month
                , subq_18.booking__ds_partitioned__quarter
                , subq_18.booking__ds_partitioned__year
                , subq_18.booking__ds_partitioned__extract_year
                , subq_18.booking__ds_partitioned__extract_quarter
                , subq_18.booking__ds_partitioned__extract_month
                , subq_18.booking__ds_partitioned__extract_day
                , subq_18.booking__ds_partitioned__extract_dow
                , subq_18.booking__ds_partitioned__extract_doy
                , subq_18.booking__paid_at__day
                , subq_18.booking__paid_at__week
                , subq_18.booking__paid_at__month
                , subq_18.booking__paid_at__quarter
                , subq_18.booking__paid_at__year
                , subq_18.booking__paid_at__extract_year
                , subq_18.booking__paid_at__extract_quarter
                , subq_18.booking__paid_at__extract_month
                , subq_18.booking__paid_at__extract_day
                , subq_18.booking__paid_at__extract_dow
                , subq_18.booking__paid_at__extract_doy
                , subq_18.metric_time__day
                , subq_18.metric_time__week
                , subq_18.metric_time__month
                , subq_18.metric_time__quarter
                , subq_18.metric_time__year
                , subq_18.metric_time__extract_year
                , subq_18.metric_time__extract_quarter
                , subq_18.metric_time__extract_month
                , subq_18.metric_time__extract_day
                , subq_18.metric_time__extract_dow
                , subq_18.metric_time__extract_doy
                , subq_18.listing
                , subq_18.guest
                , subq_18.host
                , subq_18.booking__listing
                , subq_18.booking__guest
                , subq_18.booking__host
                , subq_18.is_instant
                , subq_18.booking__is_instant
                , subq_18.bookings
                , subq_18.average_booking_value
                , subq_18.instant_bookings
                , subq_18.booking_value
                , subq_18.max_booking_value
                , subq_18.min_booking_value
                , subq_18.instant_booking_value
                , subq_18.average_instant_booking_value
                , subq_18.booking_value_for_non_null_listing_id
                , subq_18.bookers
                , subq_18.referred_bookings
                , subq_18.median_booking_value
                , subq_18.booking_value_p99
                , subq_18.discrete_booking_value_p99
                , subq_18.approximate_continuous_booking_value_p99
                , subq_18.approximate_discrete_booking_value_p99
                , subq_18.bookings_join_to_time_spine
                , subq_18.bookings_fill_nulls_with_0_without_time_spine
                , subq_18.bookings_fill_nulls_with_0
                , subq_18.instant_bookings_with_measure_filter
                , subq_18.bookings_join_to_time_spine_with_tiered_filters
                , subq_18.bookers_fill_nulls_with_0_join_to_timespine
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
                  , subq_0.bookings
                  , subq_0.average_booking_value
                  , subq_0.instant_bookings
                  , subq_0.booking_value
                  , subq_0.max_booking_value
                  , subq_0.min_booking_value
                  , subq_0.instant_booking_value
                  , subq_0.average_instant_booking_value
                  , subq_0.booking_value_for_non_null_listing_id
                  , subq_0.bookers
                  , subq_0.referred_bookings
                  , subq_0.median_booking_value
                  , subq_0.booking_value_p99
                  , subq_0.discrete_booking_value_p99
                  , subq_0.approximate_continuous_booking_value_p99
                  , subq_0.approximate_discrete_booking_value_p99
                  , subq_0.bookings_join_to_time_spine
                  , subq_0.bookings_fill_nulls_with_0_without_time_spine
                  , subq_0.bookings_fill_nulls_with_0
                  , subq_0.instant_bookings_with_measure_filter
                  , subq_0.bookings_join_to_time_spine_with_tiered_filters
                  , subq_0.bookers_fill_nulls_with_0_join_to_timespine
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
                ) subq_0
              ) subq_18
              WHERE booking__is_instant
            ) subq_19
          ) subq_20
        ) subq_21
      ) subq_22
    ) subq_23
  ) subq_24
) subq_25
