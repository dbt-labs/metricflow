test_name: test_aliases_with_metrics
test_filename: test_query_rendering.py
docstring:
  Tests a metric query with various aliases.
sql_engine: DuckDB
---
-- Write to DataTable
SELECT
  subq_24.booking_day
  , subq_24.listing_id
  , subq_24.listing_capacity
  , subq_24.bookings_alias
FROM (
  -- Change Column Aliases
  SELECT
    subq_23.listing__capacity_latest AS listing_capacity
    , subq_23.metric_time__day AS booking_day
    , subq_23.listing AS listing_id
    , subq_23.bookings AS bookings_alias
  FROM (
    -- Order By ['bookings', 'metric_time__day', 'listing__capacity_latest', 'listing']
    SELECT
      subq_22.metric_time__day
      , subq_22.listing
      , subq_22.listing__capacity_latest
      , subq_22.bookings
    FROM (
      -- Compute Metrics via Expressions
      SELECT
        subq_21.metric_time__day
        , subq_21.listing
        , subq_21.listing__capacity_latest
        , subq_21.__bookings AS bookings
      FROM (
        -- Aggregate Inputs for Simple Metrics
        SELECT
          subq_20.metric_time__day
          , subq_20.listing
          , subq_20.listing__capacity_latest
          , SUM(subq_20.__bookings) AS __bookings
        FROM (
          -- Pass Only Elements: ['__bookings', 'listing__capacity_latest', 'metric_time__day', 'listing']
          SELECT
            subq_19.metric_time__day
            , subq_19.listing
            , subq_19.listing__capacity_latest
            , subq_19.__bookings
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_18.bookings AS __bookings
              , subq_18.listing__capacity_latest
              , subq_18.metric_time__day
              , subq_18.listing
              , subq_18.listing__booking_fees
            FROM (
              -- Pass Only Elements: ['__bookings', 'listing__capacity_latest', 'metric_time__day', 'listing', 'listing__booking_fees']
              SELECT
                subq_17.metric_time__day
                , subq_17.listing
                , subq_17.listing__capacity_latest
                , subq_17.listing__booking_fees
                , subq_17.__bookings AS bookings
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_13.listing__booking_fees AS listing__booking_fees
                  , subq_16.capacity_latest AS listing__capacity_latest
                  , subq_6.ds__day AS ds__day
                  , subq_6.ds__week AS ds__week
                  , subq_6.ds__month AS ds__month
                  , subq_6.ds__quarter AS ds__quarter
                  , subq_6.ds__year AS ds__year
                  , subq_6.ds__extract_year AS ds__extract_year
                  , subq_6.ds__extract_quarter AS ds__extract_quarter
                  , subq_6.ds__extract_month AS ds__extract_month
                  , subq_6.ds__extract_day AS ds__extract_day
                  , subq_6.ds__extract_dow AS ds__extract_dow
                  , subq_6.ds__extract_doy AS ds__extract_doy
                  , subq_6.ds_partitioned__day AS ds_partitioned__day
                  , subq_6.ds_partitioned__week AS ds_partitioned__week
                  , subq_6.ds_partitioned__month AS ds_partitioned__month
                  , subq_6.ds_partitioned__quarter AS ds_partitioned__quarter
                  , subq_6.ds_partitioned__year AS ds_partitioned__year
                  , subq_6.ds_partitioned__extract_year AS ds_partitioned__extract_year
                  , subq_6.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                  , subq_6.ds_partitioned__extract_month AS ds_partitioned__extract_month
                  , subq_6.ds_partitioned__extract_day AS ds_partitioned__extract_day
                  , subq_6.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                  , subq_6.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                  , subq_6.paid_at__day AS paid_at__day
                  , subq_6.paid_at__week AS paid_at__week
                  , subq_6.paid_at__month AS paid_at__month
                  , subq_6.paid_at__quarter AS paid_at__quarter
                  , subq_6.paid_at__year AS paid_at__year
                  , subq_6.paid_at__extract_year AS paid_at__extract_year
                  , subq_6.paid_at__extract_quarter AS paid_at__extract_quarter
                  , subq_6.paid_at__extract_month AS paid_at__extract_month
                  , subq_6.paid_at__extract_day AS paid_at__extract_day
                  , subq_6.paid_at__extract_dow AS paid_at__extract_dow
                  , subq_6.paid_at__extract_doy AS paid_at__extract_doy
                  , subq_6.booking__ds__day AS booking__ds__day
                  , subq_6.booking__ds__week AS booking__ds__week
                  , subq_6.booking__ds__month AS booking__ds__month
                  , subq_6.booking__ds__quarter AS booking__ds__quarter
                  , subq_6.booking__ds__year AS booking__ds__year
                  , subq_6.booking__ds__extract_year AS booking__ds__extract_year
                  , subq_6.booking__ds__extract_quarter AS booking__ds__extract_quarter
                  , subq_6.booking__ds__extract_month AS booking__ds__extract_month
                  , subq_6.booking__ds__extract_day AS booking__ds__extract_day
                  , subq_6.booking__ds__extract_dow AS booking__ds__extract_dow
                  , subq_6.booking__ds__extract_doy AS booking__ds__extract_doy
                  , subq_6.booking__ds_partitioned__day AS booking__ds_partitioned__day
                  , subq_6.booking__ds_partitioned__week AS booking__ds_partitioned__week
                  , subq_6.booking__ds_partitioned__month AS booking__ds_partitioned__month
                  , subq_6.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                  , subq_6.booking__ds_partitioned__year AS booking__ds_partitioned__year
                  , subq_6.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                  , subq_6.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                  , subq_6.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                  , subq_6.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                  , subq_6.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                  , subq_6.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                  , subq_6.booking__paid_at__day AS booking__paid_at__day
                  , subq_6.booking__paid_at__week AS booking__paid_at__week
                  , subq_6.booking__paid_at__month AS booking__paid_at__month
                  , subq_6.booking__paid_at__quarter AS booking__paid_at__quarter
                  , subq_6.booking__paid_at__year AS booking__paid_at__year
                  , subq_6.booking__paid_at__extract_year AS booking__paid_at__extract_year
                  , subq_6.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                  , subq_6.booking__paid_at__extract_month AS booking__paid_at__extract_month
                  , subq_6.booking__paid_at__extract_day AS booking__paid_at__extract_day
                  , subq_6.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                  , subq_6.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
                  , subq_6.metric_time__day AS metric_time__day
                  , subq_6.metric_time__week AS metric_time__week
                  , subq_6.metric_time__month AS metric_time__month
                  , subq_6.metric_time__quarter AS metric_time__quarter
                  , subq_6.metric_time__year AS metric_time__year
                  , subq_6.metric_time__extract_year AS metric_time__extract_year
                  , subq_6.metric_time__extract_quarter AS metric_time__extract_quarter
                  , subq_6.metric_time__extract_month AS metric_time__extract_month
                  , subq_6.metric_time__extract_day AS metric_time__extract_day
                  , subq_6.metric_time__extract_dow AS metric_time__extract_dow
                  , subq_6.metric_time__extract_doy AS metric_time__extract_doy
                  , subq_6.listing AS listing
                  , subq_6.guest AS guest
                  , subq_6.host AS host
                  , subq_6.booking__listing AS booking__listing
                  , subq_6.booking__guest AS booking__guest
                  , subq_6.booking__host AS booking__host
                  , subq_6.is_instant AS is_instant
                  , subq_6.booking__is_instant AS booking__is_instant
                  , subq_6.__bookings AS __bookings
                  , subq_6.__average_booking_value AS __average_booking_value
                  , subq_6.__instant_bookings AS __instant_bookings
                  , subq_6.__booking_value AS __booking_value
                  , subq_6.__max_booking_value AS __max_booking_value
                  , subq_6.__min_booking_value AS __min_booking_value
                  , subq_6.__instant_booking_value AS __instant_booking_value
                  , subq_6.__average_instant_booking_value AS __average_instant_booking_value
                  , subq_6.__booking_value_for_non_null_listing_id AS __booking_value_for_non_null_listing_id
                  , subq_6.__bookers AS __bookers
                  , subq_6.__referred_bookings AS __referred_bookings
                  , subq_6.__median_booking_value AS __median_booking_value
                  , subq_6.__booking_value_p99 AS __booking_value_p99
                  , subq_6.__discrete_booking_value_p99 AS __discrete_booking_value_p99
                  , subq_6.__approximate_continuous_booking_value_p99 AS __approximate_continuous_booking_value_p99
                  , subq_6.__approximate_discrete_booking_value_p99 AS __approximate_discrete_booking_value_p99
                  , subq_6.__bookings_join_to_time_spine AS __bookings_join_to_time_spine
                  , subq_6.__bookings_fill_nulls_with_0_without_time_spine AS __bookings_fill_nulls_with_0_without_time_spine
                  , subq_6.__bookings_fill_nulls_with_0 AS __bookings_fill_nulls_with_0
                  , subq_6.__instant_bookings_with_measure_filter AS __instant_bookings_with_measure_filter
                  , subq_6.__bookings_join_to_time_spine_with_tiered_filters AS __bookings_join_to_time_spine_with_tiered_filters
                  , subq_6.__bookers_fill_nulls_with_0_join_to_timespine AS __bookers_fill_nulls_with_0_join_to_timespine
                FROM (
                  -- Metric Time Dimension 'ds'
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
                    , subq_5.ds__day AS metric_time__day
                    , subq_5.ds__week AS metric_time__week
                    , subq_5.ds__month AS metric_time__month
                    , subq_5.ds__quarter AS metric_time__quarter
                    , subq_5.ds__year AS metric_time__year
                    , subq_5.ds__extract_year AS metric_time__extract_year
                    , subq_5.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_5.ds__extract_month AS metric_time__extract_month
                    , subq_5.ds__extract_day AS metric_time__extract_day
                    , subq_5.ds__extract_dow AS metric_time__extract_dow
                    , subq_5.ds__extract_doy AS metric_time__extract_doy
                    , subq_5.listing
                    , subq_5.guest
                    , subq_5.host
                    , subq_5.booking__listing
                    , subq_5.booking__guest
                    , subq_5.booking__host
                    , subq_5.is_instant
                    , subq_5.booking__is_instant
                    , subq_5.__bookings
                    , subq_5.__average_booking_value
                    , subq_5.__instant_bookings
                    , subq_5.__booking_value
                    , subq_5.__max_booking_value
                    , subq_5.__min_booking_value
                    , subq_5.__instant_booking_value
                    , subq_5.__average_instant_booking_value
                    , subq_5.__booking_value_for_non_null_listing_id
                    , subq_5.__bookers
                    , subq_5.__referred_bookings
                    , subq_5.__median_booking_value
                    , subq_5.__booking_value_p99
                    , subq_5.__discrete_booking_value_p99
                    , subq_5.__approximate_continuous_booking_value_p99
                    , subq_5.__approximate_discrete_booking_value_p99
                    , subq_5.__bookings_join_to_time_spine
                    , subq_5.__bookings_fill_nulls_with_0_without_time_spine
                    , subq_5.__bookings_fill_nulls_with_0
                    , subq_5.__instant_bookings_with_measure_filter
                    , subq_5.__bookings_join_to_time_spine_with_tiered_filters
                    , subq_5.__bookers_fill_nulls_with_0_join_to_timespine
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
                  ) subq_5
                ) subq_6
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['listing', 'listing__booking_fees']
                  SELECT
                    subq_12.listing
                    , subq_12.listing__booking_fees
                  FROM (
                    -- Compute Metrics via Expressions
                    SELECT
                      subq_11.listing
                      , booking_value * 0.05 AS listing__booking_fees
                    FROM (
                      -- Compute Metrics via Expressions
                      SELECT
                        subq_10.listing
                        , subq_10.__booking_value AS booking_value
                      FROM (
                        -- Aggregate Inputs for Simple Metrics
                        SELECT
                          subq_9.listing
                          , SUM(subq_9.__booking_value) AS __booking_value
                        FROM (
                          -- Pass Only Elements: ['__booking_value', 'listing']
                          SELECT
                            subq_8.listing
                            , subq_8.__booking_value
                          FROM (
                            -- Pass Only Elements: ['__booking_value', 'listing']
                            SELECT
                              subq_7.listing
                              , subq_7.__booking_value
                            FROM (
                              -- Metric Time Dimension 'ds'
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
                                , subq_5.ds__day AS metric_time__day
                                , subq_5.ds__week AS metric_time__week
                                , subq_5.ds__month AS metric_time__month
                                , subq_5.ds__quarter AS metric_time__quarter
                                , subq_5.ds__year AS metric_time__year
                                , subq_5.ds__extract_year AS metric_time__extract_year
                                , subq_5.ds__extract_quarter AS metric_time__extract_quarter
                                , subq_5.ds__extract_month AS metric_time__extract_month
                                , subq_5.ds__extract_day AS metric_time__extract_day
                                , subq_5.ds__extract_dow AS metric_time__extract_dow
                                , subq_5.ds__extract_doy AS metric_time__extract_doy
                                , subq_5.listing
                                , subq_5.guest
                                , subq_5.host
                                , subq_5.booking__listing
                                , subq_5.booking__guest
                                , subq_5.booking__host
                                , subq_5.is_instant
                                , subq_5.booking__is_instant
                                , subq_5.__bookings
                                , subq_5.__average_booking_value
                                , subq_5.__instant_bookings
                                , subq_5.__booking_value
                                , subq_5.__max_booking_value
                                , subq_5.__min_booking_value
                                , subq_5.__instant_booking_value
                                , subq_5.__average_instant_booking_value
                                , subq_5.__booking_value_for_non_null_listing_id
                                , subq_5.__bookers
                                , subq_5.__referred_bookings
                                , subq_5.__median_booking_value
                                , subq_5.__booking_value_p99
                                , subq_5.__discrete_booking_value_p99
                                , subq_5.__approximate_continuous_booking_value_p99
                                , subq_5.__approximate_discrete_booking_value_p99
                                , subq_5.__bookings_join_to_time_spine
                                , subq_5.__bookings_fill_nulls_with_0_without_time_spine
                                , subq_5.__bookings_fill_nulls_with_0
                                , subq_5.__instant_bookings_with_measure_filter
                                , subq_5.__bookings_join_to_time_spine_with_tiered_filters
                                , subq_5.__bookers_fill_nulls_with_0_join_to_timespine
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
                              ) subq_5
                            ) subq_7
                          ) subq_8
                        ) subq_9
                        GROUP BY
                          subq_9.listing
                      ) subq_10
                    ) subq_11
                  ) subq_12
                ) subq_13
                ON
                  subq_6.listing = subq_13.listing
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['capacity_latest', 'listing']
                  SELECT
                    subq_15.listing
                    , subq_15.capacity_latest
                  FROM (
                    -- Metric Time Dimension 'ds'
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
                        , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS ds__extract_dow
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
                        , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
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
                        , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
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
                        , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
                        , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                        , listings_latest_src_28000.country AS listing__country_latest
                        , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                        , listings_latest_src_28000.capacity AS listing__capacity_latest
                        , listings_latest_src_28000.listing_id AS listing
                        , listings_latest_src_28000.user_id AS user
                        , listings_latest_src_28000.user_id AS listing__user
                      FROM ***************************.dim_listings_latest listings_latest_src_28000
                    ) subq_14
                  ) subq_15
                ) subq_16
                ON
                  subq_6.listing = subq_16.listing
              ) subq_17
            ) subq_18
            WHERE listing__booking_fees > 2
          ) subq_19
        ) subq_20
        GROUP BY
          subq_20.metric_time__day
          , subq_20.listing
          , subq_20.listing__capacity_latest
      ) subq_21
    ) subq_22
    ORDER BY subq_22.bookings, subq_22.metric_time__day, subq_22.listing__capacity_latest, subq_22.listing
  ) subq_23
) subq_24
