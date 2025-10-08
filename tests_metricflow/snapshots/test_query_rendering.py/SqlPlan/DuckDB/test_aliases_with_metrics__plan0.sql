test_name: test_aliases_with_metrics
test_filename: test_query_rendering.py
docstring:
  Tests a metric query with various aliases.
sql_engine: DuckDB
---
-- Write to DataTable
SELECT
  subq_21.booking_day
  , subq_21.listing_id
  , subq_21.listing_capacity
  , subq_21.bookings_alias
FROM (
  -- Change Column Aliases
  SELECT
    subq_20.listing__capacity_latest AS listing_capacity
    , subq_20.metric_time__day AS booking_day
    , subq_20.listing AS listing_id
    , subq_20.bookings AS bookings_alias
  FROM (
    -- Order By ['bookings', 'metric_time__day', 'listing__capacity_latest', 'listing']
    SELECT
      subq_19.metric_time__day
      , subq_19.listing
      , subq_19.listing__capacity_latest
      , subq_19.bookings
    FROM (
      -- Compute Metrics via Expressions
      SELECT
        subq_18.metric_time__day
        , subq_18.listing
        , subq_18.listing__capacity_latest
        , subq_18.bookings
      FROM (
        -- Aggregate Inputs for Simple Metrics
        SELECT
          subq_17.metric_time__day
          , subq_17.listing
          , subq_17.listing__capacity_latest
          , SUM(subq_17.bookings) AS bookings
        FROM (
          -- Pass Only Elements: ['bookings', 'listing__capacity_latest', 'metric_time__day', 'listing']
          SELECT
            subq_16.metric_time__day
            , subq_16.listing
            , subq_16.listing__capacity_latest
            , subq_16.bookings
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_15.ds__day
              , subq_15.ds__week
              , subq_15.ds__month
              , subq_15.ds__quarter
              , subq_15.ds__year
              , subq_15.ds__extract_year
              , subq_15.ds__extract_quarter
              , subq_15.ds__extract_month
              , subq_15.ds__extract_day
              , subq_15.ds__extract_dow
              , subq_15.ds__extract_doy
              , subq_15.ds_partitioned__day
              , subq_15.ds_partitioned__week
              , subq_15.ds_partitioned__month
              , subq_15.ds_partitioned__quarter
              , subq_15.ds_partitioned__year
              , subq_15.ds_partitioned__extract_year
              , subq_15.ds_partitioned__extract_quarter
              , subq_15.ds_partitioned__extract_month
              , subq_15.ds_partitioned__extract_day
              , subq_15.ds_partitioned__extract_dow
              , subq_15.ds_partitioned__extract_doy
              , subq_15.paid_at__day
              , subq_15.paid_at__week
              , subq_15.paid_at__month
              , subq_15.paid_at__quarter
              , subq_15.paid_at__year
              , subq_15.paid_at__extract_year
              , subq_15.paid_at__extract_quarter
              , subq_15.paid_at__extract_month
              , subq_15.paid_at__extract_day
              , subq_15.paid_at__extract_dow
              , subq_15.paid_at__extract_doy
              , subq_15.booking__ds__day
              , subq_15.booking__ds__week
              , subq_15.booking__ds__month
              , subq_15.booking__ds__quarter
              , subq_15.booking__ds__year
              , subq_15.booking__ds__extract_year
              , subq_15.booking__ds__extract_quarter
              , subq_15.booking__ds__extract_month
              , subq_15.booking__ds__extract_day
              , subq_15.booking__ds__extract_dow
              , subq_15.booking__ds__extract_doy
              , subq_15.booking__ds_partitioned__day
              , subq_15.booking__ds_partitioned__week
              , subq_15.booking__ds_partitioned__month
              , subq_15.booking__ds_partitioned__quarter
              , subq_15.booking__ds_partitioned__year
              , subq_15.booking__ds_partitioned__extract_year
              , subq_15.booking__ds_partitioned__extract_quarter
              , subq_15.booking__ds_partitioned__extract_month
              , subq_15.booking__ds_partitioned__extract_day
              , subq_15.booking__ds_partitioned__extract_dow
              , subq_15.booking__ds_partitioned__extract_doy
              , subq_15.booking__paid_at__day
              , subq_15.booking__paid_at__week
              , subq_15.booking__paid_at__month
              , subq_15.booking__paid_at__quarter
              , subq_15.booking__paid_at__year
              , subq_15.booking__paid_at__extract_year
              , subq_15.booking__paid_at__extract_quarter
              , subq_15.booking__paid_at__extract_month
              , subq_15.booking__paid_at__extract_day
              , subq_15.booking__paid_at__extract_dow
              , subq_15.booking__paid_at__extract_doy
              , subq_15.metric_time__day
              , subq_15.metric_time__week
              , subq_15.metric_time__month
              , subq_15.metric_time__quarter
              , subq_15.metric_time__year
              , subq_15.metric_time__extract_year
              , subq_15.metric_time__extract_quarter
              , subq_15.metric_time__extract_month
              , subq_15.metric_time__extract_day
              , subq_15.metric_time__extract_dow
              , subq_15.metric_time__extract_doy
              , subq_15.listing
              , subq_15.guest
              , subq_15.host
              , subq_15.booking__listing
              , subq_15.booking__guest
              , subq_15.booking__host
              , subq_15.is_instant
              , subq_15.booking__is_instant
              , subq_15.listing__capacity_latest
              , subq_15.listing__booking_fees
              , subq_15.bookings
              , subq_15.average_booking_value
              , subq_15.instant_bookings
              , subq_15.booking_value
              , subq_15.max_booking_value
              , subq_15.min_booking_value
              , subq_15.instant_booking_value
              , subq_15.average_instant_booking_value
              , subq_15.booking_value_for_non_null_listing_id
              , subq_15.bookers
              , subq_15.referred_bookings
              , subq_15.median_booking_value
              , subq_15.booking_value_p99
              , subq_15.discrete_booking_value_p99
              , subq_15.approximate_continuous_booking_value_p99
              , subq_15.approximate_discrete_booking_value_p99
              , subq_15.bookings_join_to_time_spine
              , subq_15.bookings_fill_nulls_with_0_without_time_spine
              , subq_15.bookings_fill_nulls_with_0
              , subq_15.instant_bookings_with_measure_filter
              , subq_15.bookings_join_to_time_spine_with_tiered_filters
              , subq_15.bookers_fill_nulls_with_0_join_to_timespine
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_11.listing__booking_fees AS listing__booking_fees
                , subq_14.capacity_latest AS listing__capacity_latest
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
                , subq_5.bookings AS bookings
                , subq_5.average_booking_value AS average_booking_value
                , subq_5.instant_bookings AS instant_bookings
                , subq_5.booking_value AS booking_value
                , subq_5.max_booking_value AS max_booking_value
                , subq_5.min_booking_value AS min_booking_value
                , subq_5.instant_booking_value AS instant_booking_value
                , subq_5.average_instant_booking_value AS average_instant_booking_value
                , subq_5.booking_value_for_non_null_listing_id AS booking_value_for_non_null_listing_id
                , subq_5.bookers AS bookers
                , subq_5.referred_bookings AS referred_bookings
                , subq_5.median_booking_value AS median_booking_value
                , subq_5.booking_value_p99 AS booking_value_p99
                , subq_5.discrete_booking_value_p99 AS discrete_booking_value_p99
                , subq_5.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
                , subq_5.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
                , subq_5.bookings_join_to_time_spine AS bookings_join_to_time_spine
                , subq_5.bookings_fill_nulls_with_0_without_time_spine AS bookings_fill_nulls_with_0_without_time_spine
                , subq_5.bookings_fill_nulls_with_0 AS bookings_fill_nulls_with_0
                , subq_5.instant_bookings_with_measure_filter AS instant_bookings_with_measure_filter
                , subq_5.bookings_join_to_time_spine_with_tiered_filters AS bookings_join_to_time_spine_with_tiered_filters
                , subq_5.bookers_fill_nulls_with_0_join_to_timespine AS bookers_fill_nulls_with_0_join_to_timespine
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
                  , subq_4.bookings
                  , subq_4.average_booking_value
                  , subq_4.instant_bookings
                  , subq_4.booking_value
                  , subq_4.max_booking_value
                  , subq_4.min_booking_value
                  , subq_4.instant_booking_value
                  , subq_4.average_instant_booking_value
                  , subq_4.booking_value_for_non_null_listing_id
                  , subq_4.bookers
                  , subq_4.referred_bookings
                  , subq_4.median_booking_value
                  , subq_4.booking_value_p99
                  , subq_4.discrete_booking_value_p99
                  , subq_4.approximate_continuous_booking_value_p99
                  , subq_4.approximate_discrete_booking_value_p99
                  , subq_4.bookings_join_to_time_spine
                  , subq_4.bookings_fill_nulls_with_0_without_time_spine
                  , subq_4.bookings_fill_nulls_with_0
                  , subq_4.instant_bookings_with_measure_filter
                  , subq_4.bookings_join_to_time_spine_with_tiered_filters
                  , subq_4.bookers_fill_nulls_with_0_join_to_timespine
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
                ) subq_4
              ) subq_5
              LEFT OUTER JOIN (
                -- Pass Only Elements: ['listing', 'listing__booking_fees']
                SELECT
                  subq_10.listing
                  , subq_10.listing__booking_fees
                FROM (
                  -- Compute Metrics via Expressions
                  SELECT
                    subq_9.listing
                    , booking_value * 0.05 AS listing__booking_fees
                  FROM (
                    -- Compute Metrics via Expressions
                    SELECT
                      subq_8.listing
                      , subq_8.booking_value
                    FROM (
                      -- Aggregate Inputs for Simple Metrics
                      SELECT
                        subq_7.listing
                        , SUM(subq_7.booking_value) AS booking_value
                      FROM (
                        -- Pass Only Elements: ['booking_value', 'listing']
                        SELECT
                          subq_6.listing
                          , subq_6.booking_value
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
                            , subq_4.bookings
                            , subq_4.average_booking_value
                            , subq_4.instant_bookings
                            , subq_4.booking_value
                            , subq_4.max_booking_value
                            , subq_4.min_booking_value
                            , subq_4.instant_booking_value
                            , subq_4.average_instant_booking_value
                            , subq_4.booking_value_for_non_null_listing_id
                            , subq_4.bookers
                            , subq_4.referred_bookings
                            , subq_4.median_booking_value
                            , subq_4.booking_value_p99
                            , subq_4.discrete_booking_value_p99
                            , subq_4.approximate_continuous_booking_value_p99
                            , subq_4.approximate_discrete_booking_value_p99
                            , subq_4.bookings_join_to_time_spine
                            , subq_4.bookings_fill_nulls_with_0_without_time_spine
                            , subq_4.bookings_fill_nulls_with_0
                            , subq_4.instant_bookings_with_measure_filter
                            , subq_4.bookings_join_to_time_spine_with_tiered_filters
                            , subq_4.bookers_fill_nulls_with_0_join_to_timespine
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
                          ) subq_4
                        ) subq_6
                      ) subq_7
                      GROUP BY
                        subq_7.listing
                    ) subq_8
                  ) subq_9
                ) subq_10
              ) subq_11
              ON
                subq_5.listing = subq_11.listing
              LEFT OUTER JOIN (
                -- Pass Only Elements: ['capacity_latest', 'listing']
                SELECT
                  subq_13.listing
                  , subq_13.capacity_latest
                FROM (
                  -- Metric Time Dimension 'ds'
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
                    , subq_12.created_at__day
                    , subq_12.created_at__week
                    , subq_12.created_at__month
                    , subq_12.created_at__quarter
                    , subq_12.created_at__year
                    , subq_12.created_at__extract_year
                    , subq_12.created_at__extract_quarter
                    , subq_12.created_at__extract_month
                    , subq_12.created_at__extract_day
                    , subq_12.created_at__extract_dow
                    , subq_12.created_at__extract_doy
                    , subq_12.listing__ds__day
                    , subq_12.listing__ds__week
                    , subq_12.listing__ds__month
                    , subq_12.listing__ds__quarter
                    , subq_12.listing__ds__year
                    , subq_12.listing__ds__extract_year
                    , subq_12.listing__ds__extract_quarter
                    , subq_12.listing__ds__extract_month
                    , subq_12.listing__ds__extract_day
                    , subq_12.listing__ds__extract_dow
                    , subq_12.listing__ds__extract_doy
                    , subq_12.listing__created_at__day
                    , subq_12.listing__created_at__week
                    , subq_12.listing__created_at__month
                    , subq_12.listing__created_at__quarter
                    , subq_12.listing__created_at__year
                    , subq_12.listing__created_at__extract_year
                    , subq_12.listing__created_at__extract_quarter
                    , subq_12.listing__created_at__extract_month
                    , subq_12.listing__created_at__extract_day
                    , subq_12.listing__created_at__extract_dow
                    , subq_12.listing__created_at__extract_doy
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
                    , subq_12.user
                    , subq_12.listing__user
                    , subq_12.country_latest
                    , subq_12.is_lux_latest
                    , subq_12.capacity_latest
                    , subq_12.listing__country_latest
                    , subq_12.listing__is_lux_latest
                    , subq_12.listing__capacity_latest
                    , subq_12.listings
                    , subq_12.lux_listings
                    , subq_12.smallest_listing
                    , subq_12.largest_listing
                    , subq_12.active_listings
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
                  ) subq_12
                ) subq_13
              ) subq_14
              ON
                subq_5.listing = subq_14.listing
            ) subq_15
            WHERE listing__booking_fees > 2
          ) subq_16
        ) subq_17
        GROUP BY
          subq_17.metric_time__day
          , subq_17.listing
          , subq_17.listing__capacity_latest
      ) subq_18
    ) subq_19
    ORDER BY subq_19.bookings, subq_19.metric_time__day, subq_19.listing__capacity_latest, subq_19.listing
  ) subq_20
) subq_21
