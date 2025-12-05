test_name: test_filter_by_metric_in_same_semantic_model_as_queried_metric
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a simple metric in the query-level where filter.
sql_engine: Postgres
---
-- Write to DataTable
SELECT
  subq_17.bookers
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_16.__bookers AS bookers
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      COUNT(DISTINCT subq_15.__bookers) AS __bookers
    FROM (
      -- Pass Only Elements: ['__bookers']
      SELECT
        subq_14.__bookers
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_13.bookers AS __bookers
          , subq_13.guest__booking_value
        FROM (
          -- Pass Only Elements: ['__bookers', 'guest__booking_value']
          SELECT
            subq_12.guest__booking_value
            , subq_12.__bookers AS bookers
          FROM (
            -- Join Standard Outputs
            SELECT
              subq_11.guest__booking_value AS guest__booking_value
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
              , subq_5.metric_time__day AS metric_time__day
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
              -- Metric Time Dimension 'ds'
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
              ) subq_4
            ) subq_5
            LEFT OUTER JOIN (
              -- Pass Only Elements: ['guest', 'guest__booking_value']
              SELECT
                subq_10.guest
                , subq_10.guest__booking_value
              FROM (
                -- Compute Metrics via Expressions
                SELECT
                  subq_9.guest
                  , subq_9.__booking_value AS guest__booking_value
                FROM (
                  -- Aggregate Inputs for Simple Metrics
                  SELECT
                    subq_8.guest
                    , SUM(subq_8.__booking_value) AS __booking_value
                  FROM (
                    -- Pass Only Elements: ['__booking_value', 'guest']
                    SELECT
                      subq_7.guest
                      , subq_7.__booking_value
                    FROM (
                      -- Pass Only Elements: ['__booking_value', 'guest']
                      SELECT
                        subq_6.guest
                        , subq_6.__booking_value
                      FROM (
                        -- Metric Time Dimension 'ds'
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
                        ) subq_4
                      ) subq_6
                    ) subq_7
                  ) subq_8
                  GROUP BY
                    subq_8.guest
                ) subq_9
              ) subq_10
            ) subq_11
            ON
              subq_5.guest = subq_11.guest
          ) subq_12
        ) subq_13
        WHERE guest__booking_value > 1.00
      ) subq_14
    ) subq_15
  ) subq_16
) subq_17
