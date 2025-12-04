test_name: test_distinct_values_query_with_metric_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a distinct values query with a metric in the query-level where filter.
sql_engine: Trino
---
-- Write to DataTable
SELECT
  subq_15.listing
FROM (
  -- Pass Only Elements: ['listing']
  SELECT
    subq_14.listing
  FROM (
    -- Constrain Output with WHERE
    SELECT
      subq_13.listing
      , subq_13.listing__bookings
    FROM (
      -- Pass Only Elements: ['listing', 'listing__bookings']
      SELECT
        subq_12.listing
        , subq_12.listing__bookings
      FROM (
        -- Join Standard Outputs
        SELECT
          subq_11.listing__bookings AS listing__bookings
          , subq_4.listing AS listing
          , subq_4.lux_listing AS lux_listing
          , subq_4.listing__lux_listing AS listing__lux_listing
        FROM (
          -- Read Elements From Semantic Model 'lux_listing_mapping'
          SELECT
            lux_listing_mapping_src_28000.listing_id AS listing
            , lux_listing_mapping_src_28000.lux_listing_id AS lux_listing
            , lux_listing_mapping_src_28000.lux_listing_id AS listing__lux_listing
          FROM ***************************.dim_lux_listing_id_mapping lux_listing_mapping_src_28000
        ) subq_4
        FULL OUTER JOIN (
          -- Pass Only Elements: ['listing', 'listing__bookings']
          SELECT
            subq_10.listing
            , subq_10.listing__bookings
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              subq_9.listing
              , subq_9.__bookings AS listing__bookings
            FROM (
              -- Aggregate Inputs for Simple Metrics
              SELECT
                subq_8.listing
                , SUM(subq_8.__bookings) AS __bookings
              FROM (
                -- Pass Only Elements: ['__bookings', 'listing']
                SELECT
                  subq_7.listing
                  , subq_7.__bookings
                FROM (
                  -- Pass Only Elements: ['__bookings', 'listing']
                  SELECT
                    subq_6.listing
                    , subq_6.__bookings
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
                        , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
                        , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
                        , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
                        , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
                        , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
                        , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
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
                ) subq_7
              ) subq_8
              GROUP BY
                subq_8.listing
            ) subq_9
          ) subq_10
        ) subq_11
        ON
          subq_4.listing = subq_11.listing
      ) subq_12
    ) subq_13
    WHERE listing__bookings > 2
  ) subq_14
  GROUP BY
    subq_14.listing
) subq_15
