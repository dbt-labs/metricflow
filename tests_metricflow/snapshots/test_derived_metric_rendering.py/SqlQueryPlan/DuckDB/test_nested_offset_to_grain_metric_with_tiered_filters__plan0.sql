test_name: test_nested_offset_to_grain_metric_with_tiered_filters
test_filename: test_derived_metric_rendering.py
docstring:
  Tests that filters at different tiers are applied appropriately for derived metrics with offset to grain.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  subq_36.metric_time__day
  , bookings_this_month_wtd - bookings AS bookings_offset_to_grain_twice_with_tiered_filters
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_5.metric_time__day, subq_35.metric_time__day) AS metric_time__day
    , MAX(subq_5.bookings) AS bookings
    , MAX(subq_35.bookings_this_month_wtd) AS bookings_this_month_wtd
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_4.metric_time__day
      , subq_4.bookings
    FROM (
      -- Aggregate Measures
      SELECT
        subq_3.metric_time__day
        , SUM(subq_3.bookings) AS bookings
      FROM (
        -- Pass Only Elements: ['bookings', 'metric_time__day']
        SELECT
          subq_2.metric_time__day
          , subq_2.bookings
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_1.ds__day
            , subq_1.ds__week
            , subq_1.ds__month
            , subq_1.ds__quarter
            , subq_1.ds__year
            , subq_1.ds__extract_year
            , subq_1.ds__extract_quarter
            , subq_1.ds__extract_month
            , subq_1.ds__extract_day
            , subq_1.ds__extract_dow
            , subq_1.ds__extract_doy
            , subq_1.ds_partitioned__day
            , subq_1.ds_partitioned__week
            , subq_1.ds_partitioned__month
            , subq_1.ds_partitioned__quarter
            , subq_1.ds_partitioned__year
            , subq_1.ds_partitioned__extract_year
            , subq_1.ds_partitioned__extract_quarter
            , subq_1.ds_partitioned__extract_month
            , subq_1.ds_partitioned__extract_day
            , subq_1.ds_partitioned__extract_dow
            , subq_1.ds_partitioned__extract_doy
            , subq_1.paid_at__day
            , subq_1.paid_at__week
            , subq_1.paid_at__month
            , subq_1.paid_at__quarter
            , subq_1.paid_at__year
            , subq_1.paid_at__extract_year
            , subq_1.paid_at__extract_quarter
            , subq_1.paid_at__extract_month
            , subq_1.paid_at__extract_day
            , subq_1.paid_at__extract_dow
            , subq_1.paid_at__extract_doy
            , subq_1.booking__ds__day
            , subq_1.booking__ds__week
            , subq_1.booking__ds__month
            , subq_1.booking__ds__quarter
            , subq_1.booking__ds__year
            , subq_1.booking__ds__extract_year
            , subq_1.booking__ds__extract_quarter
            , subq_1.booking__ds__extract_month
            , subq_1.booking__ds__extract_day
            , subq_1.booking__ds__extract_dow
            , subq_1.booking__ds__extract_doy
            , subq_1.booking__ds_partitioned__day
            , subq_1.booking__ds_partitioned__week
            , subq_1.booking__ds_partitioned__month
            , subq_1.booking__ds_partitioned__quarter
            , subq_1.booking__ds_partitioned__year
            , subq_1.booking__ds_partitioned__extract_year
            , subq_1.booking__ds_partitioned__extract_quarter
            , subq_1.booking__ds_partitioned__extract_month
            , subq_1.booking__ds_partitioned__extract_day
            , subq_1.booking__ds_partitioned__extract_dow
            , subq_1.booking__ds_partitioned__extract_doy
            , subq_1.booking__paid_at__day
            , subq_1.booking__paid_at__week
            , subq_1.booking__paid_at__month
            , subq_1.booking__paid_at__quarter
            , subq_1.booking__paid_at__year
            , subq_1.booking__paid_at__extract_year
            , subq_1.booking__paid_at__extract_quarter
            , subq_1.booking__paid_at__extract_month
            , subq_1.booking__paid_at__extract_day
            , subq_1.booking__paid_at__extract_dow
            , subq_1.booking__paid_at__extract_doy
            , subq_1.metric_time__day
            , subq_1.metric_time__week
            , subq_1.metric_time__month
            , subq_1.metric_time__quarter
            , subq_1.metric_time__year
            , subq_1.metric_time__extract_year
            , subq_1.metric_time__extract_quarter
            , subq_1.metric_time__extract_month
            , subq_1.metric_time__extract_day
            , subq_1.metric_time__extract_dow
            , subq_1.metric_time__extract_doy
            , subq_1.listing
            , subq_1.guest
            , subq_1.host
            , subq_1.booking__listing
            , subq_1.booking__guest
            , subq_1.booking__host
            , subq_1.is_instant
            , subq_1.booking__is_instant
            , subq_1.bookings
            , subq_1.instant_bookings
            , subq_1.booking_value
            , subq_1.max_booking_value
            , subq_1.min_booking_value
            , subq_1.bookers
            , subq_1.average_booking_value
            , subq_1.referred_bookings
            , subq_1.median_booking_value
            , subq_1.booking_value_p99
            , subq_1.discrete_booking_value_p99
            , subq_1.approximate_continuous_booking_value_p99
            , subq_1.approximate_discrete_booking_value_p99
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
              , subq_0.instant_bookings
              , subq_0.booking_value
              , subq_0.max_booking_value
              , subq_0.min_booking_value
              , subq_0.bookers
              , subq_0.average_booking_value
              , subq_0.referred_bookings
              , subq_0.median_booking_value
              , subq_0.booking_value_p99
              , subq_0.discrete_booking_value_p99
              , subq_0.approximate_continuous_booking_value_p99
              , subq_0.approximate_discrete_booking_value_p99
            FROM (
              -- Read Elements From Semantic Model 'bookings_source'
              SELECT
                1 AS bookings
                , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
                , bookings_source_src_28000.booking_value
                , bookings_source_src_28000.booking_value AS max_booking_value
                , bookings_source_src_28000.booking_value AS min_booking_value
                , bookings_source_src_28000.guest_id AS bookers
                , bookings_source_src_28000.booking_value AS average_booking_value
                , bookings_source_src_28000.booking_value AS booking_payments
                , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
                , bookings_source_src_28000.booking_value AS median_booking_value
                , bookings_source_src_28000.booking_value AS booking_value_p99
                , bookings_source_src_28000.booking_value AS discrete_booking_value_p99
                , bookings_source_src_28000.booking_value AS approximate_continuous_booking_value_p99
                , bookings_source_src_28000.booking_value AS approximate_discrete_booking_value_p99
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
          ) subq_1
          WHERE (((listing IS NOT NULL) AND (metric_time__quarter >= '2020-01-01')) AND (metric_time__month >= '2020-01-01')) AND (booking__is_instant)
        ) subq_2
      ) subq_3
      GROUP BY
        subq_3.metric_time__day
    ) subq_4
  ) subq_5
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    SELECT
      subq_32.metric_time__day AS metric_time__day
      , subq_31.bookings_this_month_wtd AS bookings_this_month_wtd
    FROM (
      -- Filter Time Spine
      SELECT
        subq_34.metric_time__day
      FROM (
        -- Time Spine
        SELECT
          subq_33.ds AS metric_time__day
          , DATE_TRUNC('month', subq_33.ds) AS metric_time__month
          , DATE_TRUNC('quarter', subq_33.ds) AS metric_time__quarter
        FROM ***************************.mf_time_spine subq_33
      ) subq_34
      WHERE (
        metric_time__quarter >= '2020-01-01'
      ) AND (
        metric_time__month >= '2020-01-01'
      )
    ) subq_32
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        subq_30.metric_time__day
        , bookings - bookings_at_start_of_month AS bookings_this_month_wtd
      FROM (
        -- Combine Aggregated Outputs
        SELECT
          COALESCE(subq_15.metric_time__day, subq_29.metric_time__day) AS metric_time__day
          , MAX(subq_15.bookings) AS bookings
          , MAX(subq_29.bookings_at_start_of_month) AS bookings_at_start_of_month
        FROM (
          -- Compute Metrics via Expressions
          SELECT
            subq_14.metric_time__day
            , subq_14.bookings
          FROM (
            -- Aggregate Measures
            SELECT
              subq_13.metric_time__day
              , SUM(subq_13.bookings) AS bookings
            FROM (
              -- Pass Only Elements: ['bookings', 'metric_time__day']
              SELECT
                subq_12.metric_time__day
                , subq_12.bookings
              FROM (
                -- Constrain Output with WHERE
                SELECT
                  subq_11.ds__day
                  , subq_11.ds__week
                  , subq_11.ds__month
                  , subq_11.ds__quarter
                  , subq_11.ds__year
                  , subq_11.ds__extract_year
                  , subq_11.ds__extract_quarter
                  , subq_11.ds__extract_month
                  , subq_11.ds__extract_day
                  , subq_11.ds__extract_dow
                  , subq_11.ds__extract_doy
                  , subq_11.ds_partitioned__day
                  , subq_11.ds_partitioned__week
                  , subq_11.ds_partitioned__month
                  , subq_11.ds_partitioned__quarter
                  , subq_11.ds_partitioned__year
                  , subq_11.ds_partitioned__extract_year
                  , subq_11.ds_partitioned__extract_quarter
                  , subq_11.ds_partitioned__extract_month
                  , subq_11.ds_partitioned__extract_day
                  , subq_11.ds_partitioned__extract_dow
                  , subq_11.ds_partitioned__extract_doy
                  , subq_11.paid_at__day
                  , subq_11.paid_at__week
                  , subq_11.paid_at__month
                  , subq_11.paid_at__quarter
                  , subq_11.paid_at__year
                  , subq_11.paid_at__extract_year
                  , subq_11.paid_at__extract_quarter
                  , subq_11.paid_at__extract_month
                  , subq_11.paid_at__extract_day
                  , subq_11.paid_at__extract_dow
                  , subq_11.paid_at__extract_doy
                  , subq_11.booking__ds__day
                  , subq_11.booking__ds__week
                  , subq_11.booking__ds__month
                  , subq_11.booking__ds__quarter
                  , subq_11.booking__ds__year
                  , subq_11.booking__ds__extract_year
                  , subq_11.booking__ds__extract_quarter
                  , subq_11.booking__ds__extract_month
                  , subq_11.booking__ds__extract_day
                  , subq_11.booking__ds__extract_dow
                  , subq_11.booking__ds__extract_doy
                  , subq_11.booking__ds_partitioned__day
                  , subq_11.booking__ds_partitioned__week
                  , subq_11.booking__ds_partitioned__month
                  , subq_11.booking__ds_partitioned__quarter
                  , subq_11.booking__ds_partitioned__year
                  , subq_11.booking__ds_partitioned__extract_year
                  , subq_11.booking__ds_partitioned__extract_quarter
                  , subq_11.booking__ds_partitioned__extract_month
                  , subq_11.booking__ds_partitioned__extract_day
                  , subq_11.booking__ds_partitioned__extract_dow
                  , subq_11.booking__ds_partitioned__extract_doy
                  , subq_11.booking__paid_at__day
                  , subq_11.booking__paid_at__week
                  , subq_11.booking__paid_at__month
                  , subq_11.booking__paid_at__quarter
                  , subq_11.booking__paid_at__year
                  , subq_11.booking__paid_at__extract_year
                  , subq_11.booking__paid_at__extract_quarter
                  , subq_11.booking__paid_at__extract_month
                  , subq_11.booking__paid_at__extract_day
                  , subq_11.booking__paid_at__extract_dow
                  , subq_11.booking__paid_at__extract_doy
                  , subq_11.metric_time__day
                  , subq_11.metric_time__week
                  , subq_11.metric_time__month
                  , subq_11.metric_time__quarter
                  , subq_11.metric_time__year
                  , subq_11.metric_time__extract_year
                  , subq_11.metric_time__extract_quarter
                  , subq_11.metric_time__extract_month
                  , subq_11.metric_time__extract_day
                  , subq_11.metric_time__extract_dow
                  , subq_11.metric_time__extract_doy
                  , subq_11.listing__created_at__day
                  , subq_11.listing
                  , subq_11.guest
                  , subq_11.host
                  , subq_11.booking__listing
                  , subq_11.booking__guest
                  , subq_11.booking__host
                  , subq_11.is_instant
                  , subq_11.booking__is_instant
                  , subq_11.bookings
                  , subq_11.instant_bookings
                  , subq_11.booking_value
                  , subq_11.max_booking_value
                  , subq_11.min_booking_value
                  , subq_11.bookers
                  , subq_11.average_booking_value
                  , subq_11.referred_bookings
                  , subq_11.median_booking_value
                  , subq_11.booking_value_p99
                  , subq_11.discrete_booking_value_p99
                  , subq_11.approximate_continuous_booking_value_p99
                  , subq_11.approximate_discrete_booking_value_p99
                FROM (
                  -- Join Standard Outputs
                  SELECT
                    subq_10.created_at__day AS listing__created_at__day
                    , subq_7.ds__day AS ds__day
                    , subq_7.ds__week AS ds__week
                    , subq_7.ds__month AS ds__month
                    , subq_7.ds__quarter AS ds__quarter
                    , subq_7.ds__year AS ds__year
                    , subq_7.ds__extract_year AS ds__extract_year
                    , subq_7.ds__extract_quarter AS ds__extract_quarter
                    , subq_7.ds__extract_month AS ds__extract_month
                    , subq_7.ds__extract_day AS ds__extract_day
                    , subq_7.ds__extract_dow AS ds__extract_dow
                    , subq_7.ds__extract_doy AS ds__extract_doy
                    , subq_7.ds_partitioned__day AS ds_partitioned__day
                    , subq_7.ds_partitioned__week AS ds_partitioned__week
                    , subq_7.ds_partitioned__month AS ds_partitioned__month
                    , subq_7.ds_partitioned__quarter AS ds_partitioned__quarter
                    , subq_7.ds_partitioned__year AS ds_partitioned__year
                    , subq_7.ds_partitioned__extract_year AS ds_partitioned__extract_year
                    , subq_7.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                    , subq_7.ds_partitioned__extract_month AS ds_partitioned__extract_month
                    , subq_7.ds_partitioned__extract_day AS ds_partitioned__extract_day
                    , subq_7.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                    , subq_7.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                    , subq_7.paid_at__day AS paid_at__day
                    , subq_7.paid_at__week AS paid_at__week
                    , subq_7.paid_at__month AS paid_at__month
                    , subq_7.paid_at__quarter AS paid_at__quarter
                    , subq_7.paid_at__year AS paid_at__year
                    , subq_7.paid_at__extract_year AS paid_at__extract_year
                    , subq_7.paid_at__extract_quarter AS paid_at__extract_quarter
                    , subq_7.paid_at__extract_month AS paid_at__extract_month
                    , subq_7.paid_at__extract_day AS paid_at__extract_day
                    , subq_7.paid_at__extract_dow AS paid_at__extract_dow
                    , subq_7.paid_at__extract_doy AS paid_at__extract_doy
                    , subq_7.booking__ds__day AS booking__ds__day
                    , subq_7.booking__ds__week AS booking__ds__week
                    , subq_7.booking__ds__month AS booking__ds__month
                    , subq_7.booking__ds__quarter AS booking__ds__quarter
                    , subq_7.booking__ds__year AS booking__ds__year
                    , subq_7.booking__ds__extract_year AS booking__ds__extract_year
                    , subq_7.booking__ds__extract_quarter AS booking__ds__extract_quarter
                    , subq_7.booking__ds__extract_month AS booking__ds__extract_month
                    , subq_7.booking__ds__extract_day AS booking__ds__extract_day
                    , subq_7.booking__ds__extract_dow AS booking__ds__extract_dow
                    , subq_7.booking__ds__extract_doy AS booking__ds__extract_doy
                    , subq_7.booking__ds_partitioned__day AS booking__ds_partitioned__day
                    , subq_7.booking__ds_partitioned__week AS booking__ds_partitioned__week
                    , subq_7.booking__ds_partitioned__month AS booking__ds_partitioned__month
                    , subq_7.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                    , subq_7.booking__ds_partitioned__year AS booking__ds_partitioned__year
                    , subq_7.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                    , subq_7.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                    , subq_7.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                    , subq_7.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                    , subq_7.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                    , subq_7.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                    , subq_7.booking__paid_at__day AS booking__paid_at__day
                    , subq_7.booking__paid_at__week AS booking__paid_at__week
                    , subq_7.booking__paid_at__month AS booking__paid_at__month
                    , subq_7.booking__paid_at__quarter AS booking__paid_at__quarter
                    , subq_7.booking__paid_at__year AS booking__paid_at__year
                    , subq_7.booking__paid_at__extract_year AS booking__paid_at__extract_year
                    , subq_7.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                    , subq_7.booking__paid_at__extract_month AS booking__paid_at__extract_month
                    , subq_7.booking__paid_at__extract_day AS booking__paid_at__extract_day
                    , subq_7.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                    , subq_7.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
                    , subq_7.metric_time__day AS metric_time__day
                    , subq_7.metric_time__week AS metric_time__week
                    , subq_7.metric_time__month AS metric_time__month
                    , subq_7.metric_time__quarter AS metric_time__quarter
                    , subq_7.metric_time__year AS metric_time__year
                    , subq_7.metric_time__extract_year AS metric_time__extract_year
                    , subq_7.metric_time__extract_quarter AS metric_time__extract_quarter
                    , subq_7.metric_time__extract_month AS metric_time__extract_month
                    , subq_7.metric_time__extract_day AS metric_time__extract_day
                    , subq_7.metric_time__extract_dow AS metric_time__extract_dow
                    , subq_7.metric_time__extract_doy AS metric_time__extract_doy
                    , subq_7.listing AS listing
                    , subq_7.guest AS guest
                    , subq_7.host AS host
                    , subq_7.booking__listing AS booking__listing
                    , subq_7.booking__guest AS booking__guest
                    , subq_7.booking__host AS booking__host
                    , subq_7.is_instant AS is_instant
                    , subq_7.booking__is_instant AS booking__is_instant
                    , subq_7.bookings AS bookings
                    , subq_7.instant_bookings AS instant_bookings
                    , subq_7.booking_value AS booking_value
                    , subq_7.max_booking_value AS max_booking_value
                    , subq_7.min_booking_value AS min_booking_value
                    , subq_7.bookers AS bookers
                    , subq_7.average_booking_value AS average_booking_value
                    , subq_7.referred_bookings AS referred_bookings
                    , subq_7.median_booking_value AS median_booking_value
                    , subq_7.booking_value_p99 AS booking_value_p99
                    , subq_7.discrete_booking_value_p99 AS discrete_booking_value_p99
                    , subq_7.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
                    , subq_7.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      subq_6.ds__day
                      , subq_6.ds__week
                      , subq_6.ds__month
                      , subq_6.ds__quarter
                      , subq_6.ds__year
                      , subq_6.ds__extract_year
                      , subq_6.ds__extract_quarter
                      , subq_6.ds__extract_month
                      , subq_6.ds__extract_day
                      , subq_6.ds__extract_dow
                      , subq_6.ds__extract_doy
                      , subq_6.ds_partitioned__day
                      , subq_6.ds_partitioned__week
                      , subq_6.ds_partitioned__month
                      , subq_6.ds_partitioned__quarter
                      , subq_6.ds_partitioned__year
                      , subq_6.ds_partitioned__extract_year
                      , subq_6.ds_partitioned__extract_quarter
                      , subq_6.ds_partitioned__extract_month
                      , subq_6.ds_partitioned__extract_day
                      , subq_6.ds_partitioned__extract_dow
                      , subq_6.ds_partitioned__extract_doy
                      , subq_6.paid_at__day
                      , subq_6.paid_at__week
                      , subq_6.paid_at__month
                      , subq_6.paid_at__quarter
                      , subq_6.paid_at__year
                      , subq_6.paid_at__extract_year
                      , subq_6.paid_at__extract_quarter
                      , subq_6.paid_at__extract_month
                      , subq_6.paid_at__extract_day
                      , subq_6.paid_at__extract_dow
                      , subq_6.paid_at__extract_doy
                      , subq_6.booking__ds__day
                      , subq_6.booking__ds__week
                      , subq_6.booking__ds__month
                      , subq_6.booking__ds__quarter
                      , subq_6.booking__ds__year
                      , subq_6.booking__ds__extract_year
                      , subq_6.booking__ds__extract_quarter
                      , subq_6.booking__ds__extract_month
                      , subq_6.booking__ds__extract_day
                      , subq_6.booking__ds__extract_dow
                      , subq_6.booking__ds__extract_doy
                      , subq_6.booking__ds_partitioned__day
                      , subq_6.booking__ds_partitioned__week
                      , subq_6.booking__ds_partitioned__month
                      , subq_6.booking__ds_partitioned__quarter
                      , subq_6.booking__ds_partitioned__year
                      , subq_6.booking__ds_partitioned__extract_year
                      , subq_6.booking__ds_partitioned__extract_quarter
                      , subq_6.booking__ds_partitioned__extract_month
                      , subq_6.booking__ds_partitioned__extract_day
                      , subq_6.booking__ds_partitioned__extract_dow
                      , subq_6.booking__ds_partitioned__extract_doy
                      , subq_6.booking__paid_at__day
                      , subq_6.booking__paid_at__week
                      , subq_6.booking__paid_at__month
                      , subq_6.booking__paid_at__quarter
                      , subq_6.booking__paid_at__year
                      , subq_6.booking__paid_at__extract_year
                      , subq_6.booking__paid_at__extract_quarter
                      , subq_6.booking__paid_at__extract_month
                      , subq_6.booking__paid_at__extract_day
                      , subq_6.booking__paid_at__extract_dow
                      , subq_6.booking__paid_at__extract_doy
                      , subq_6.ds__day AS metric_time__day
                      , subq_6.ds__week AS metric_time__week
                      , subq_6.ds__month AS metric_time__month
                      , subq_6.ds__quarter AS metric_time__quarter
                      , subq_6.ds__year AS metric_time__year
                      , subq_6.ds__extract_year AS metric_time__extract_year
                      , subq_6.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_6.ds__extract_month AS metric_time__extract_month
                      , subq_6.ds__extract_day AS metric_time__extract_day
                      , subq_6.ds__extract_dow AS metric_time__extract_dow
                      , subq_6.ds__extract_doy AS metric_time__extract_doy
                      , subq_6.listing
                      , subq_6.guest
                      , subq_6.host
                      , subq_6.booking__listing
                      , subq_6.booking__guest
                      , subq_6.booking__host
                      , subq_6.is_instant
                      , subq_6.booking__is_instant
                      , subq_6.bookings
                      , subq_6.instant_bookings
                      , subq_6.booking_value
                      , subq_6.max_booking_value
                      , subq_6.min_booking_value
                      , subq_6.bookers
                      , subq_6.average_booking_value
                      , subq_6.referred_bookings
                      , subq_6.median_booking_value
                      , subq_6.booking_value_p99
                      , subq_6.discrete_booking_value_p99
                      , subq_6.approximate_continuous_booking_value_p99
                      , subq_6.approximate_discrete_booking_value_p99
                    FROM (
                      -- Read Elements From Semantic Model 'bookings_source'
                      SELECT
                        1 AS bookings
                        , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
                        , bookings_source_src_28000.booking_value
                        , bookings_source_src_28000.booking_value AS max_booking_value
                        , bookings_source_src_28000.booking_value AS min_booking_value
                        , bookings_source_src_28000.guest_id AS bookers
                        , bookings_source_src_28000.booking_value AS average_booking_value
                        , bookings_source_src_28000.booking_value AS booking_payments
                        , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
                        , bookings_source_src_28000.booking_value AS median_booking_value
                        , bookings_source_src_28000.booking_value AS booking_value_p99
                        , bookings_source_src_28000.booking_value AS discrete_booking_value_p99
                        , bookings_source_src_28000.booking_value AS approximate_continuous_booking_value_p99
                        , bookings_source_src_28000.booking_value AS approximate_discrete_booking_value_p99
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
                    ) subq_6
                  ) subq_7
                  LEFT OUTER JOIN (
                    -- Pass Only Elements: ['created_at__day', 'listing']
                    SELECT
                      subq_9.created_at__day
                      , subq_9.listing
                    FROM (
                      -- Metric Time Dimension 'ds'
                      SELECT
                        subq_8.ds__day
                        , subq_8.ds__week
                        , subq_8.ds__month
                        , subq_8.ds__quarter
                        , subq_8.ds__year
                        , subq_8.ds__extract_year
                        , subq_8.ds__extract_quarter
                        , subq_8.ds__extract_month
                        , subq_8.ds__extract_day
                        , subq_8.ds__extract_dow
                        , subq_8.ds__extract_doy
                        , subq_8.created_at__day
                        , subq_8.created_at__week
                        , subq_8.created_at__month
                        , subq_8.created_at__quarter
                        , subq_8.created_at__year
                        , subq_8.created_at__extract_year
                        , subq_8.created_at__extract_quarter
                        , subq_8.created_at__extract_month
                        , subq_8.created_at__extract_day
                        , subq_8.created_at__extract_dow
                        , subq_8.created_at__extract_doy
                        , subq_8.listing__ds__day
                        , subq_8.listing__ds__week
                        , subq_8.listing__ds__month
                        , subq_8.listing__ds__quarter
                        , subq_8.listing__ds__year
                        , subq_8.listing__ds__extract_year
                        , subq_8.listing__ds__extract_quarter
                        , subq_8.listing__ds__extract_month
                        , subq_8.listing__ds__extract_day
                        , subq_8.listing__ds__extract_dow
                        , subq_8.listing__ds__extract_doy
                        , subq_8.listing__created_at__day
                        , subq_8.listing__created_at__week
                        , subq_8.listing__created_at__month
                        , subq_8.listing__created_at__quarter
                        , subq_8.listing__created_at__year
                        , subq_8.listing__created_at__extract_year
                        , subq_8.listing__created_at__extract_quarter
                        , subq_8.listing__created_at__extract_month
                        , subq_8.listing__created_at__extract_day
                        , subq_8.listing__created_at__extract_dow
                        , subq_8.listing__created_at__extract_doy
                        , subq_8.ds__day AS metric_time__day
                        , subq_8.ds__week AS metric_time__week
                        , subq_8.ds__month AS metric_time__month
                        , subq_8.ds__quarter AS metric_time__quarter
                        , subq_8.ds__year AS metric_time__year
                        , subq_8.ds__extract_year AS metric_time__extract_year
                        , subq_8.ds__extract_quarter AS metric_time__extract_quarter
                        , subq_8.ds__extract_month AS metric_time__extract_month
                        , subq_8.ds__extract_day AS metric_time__extract_day
                        , subq_8.ds__extract_dow AS metric_time__extract_dow
                        , subq_8.ds__extract_doy AS metric_time__extract_doy
                        , subq_8.listing
                        , subq_8.user
                        , subq_8.listing__user
                        , subq_8.country_latest
                        , subq_8.is_lux_latest
                        , subq_8.capacity_latest
                        , subq_8.listing__country_latest
                        , subq_8.listing__is_lux_latest
                        , subq_8.listing__capacity_latest
                        , subq_8.listings
                        , subq_8.largest_listing
                        , subq_8.smallest_listing
                      FROM (
                        -- Read Elements From Semantic Model 'listings_latest'
                        SELECT
                          1 AS listings
                          , listings_latest_src_28000.capacity AS largest_listing
                          , listings_latest_src_28000.capacity AS smallest_listing
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
                      ) subq_8
                    ) subq_9
                  ) subq_10
                  ON
                    subq_7.listing = subq_10.listing
                ) subq_11
                WHERE (((listing IS NOT NULL) AND (booking__is_instant)) AND (booking__ds__year >= '2019-01-01')) AND (listing__created_at__day >= '2020-01-02')
              ) subq_12
            ) subq_13
            GROUP BY
              subq_13.metric_time__day
          ) subq_14
        ) subq_15
        FULL OUTER JOIN (
          -- Compute Metrics via Expressions
          SELECT
            subq_28.metric_time__day
            , subq_28.bookings AS bookings_at_start_of_month
          FROM (
            -- Aggregate Measures
            SELECT
              subq_27.metric_time__day
              , SUM(subq_27.bookings) AS bookings
            FROM (
              -- Pass Only Elements: ['bookings', 'metric_time__day']
              SELECT
                subq_26.metric_time__day
                , subq_26.bookings
              FROM (
                -- Constrain Output with WHERE
                SELECT
                  subq_25.metric_time__day
                  , subq_25.metric_time__week
                  , subq_25.metric_time__month
                  , subq_25.metric_time__quarter
                  , subq_25.metric_time__year
                  , subq_25.metric_time__extract_year
                  , subq_25.metric_time__extract_quarter
                  , subq_25.metric_time__extract_month
                  , subq_25.metric_time__extract_day
                  , subq_25.metric_time__extract_dow
                  , subq_25.metric_time__extract_doy
                  , subq_25.ds__day
                  , subq_25.ds__week
                  , subq_25.ds__month
                  , subq_25.ds__quarter
                  , subq_25.ds__year
                  , subq_25.ds__extract_year
                  , subq_25.ds__extract_quarter
                  , subq_25.ds__extract_month
                  , subq_25.ds__extract_day
                  , subq_25.ds__extract_dow
                  , subq_25.ds__extract_doy
                  , subq_25.ds_partitioned__day
                  , subq_25.ds_partitioned__week
                  , subq_25.ds_partitioned__month
                  , subq_25.ds_partitioned__quarter
                  , subq_25.ds_partitioned__year
                  , subq_25.ds_partitioned__extract_year
                  , subq_25.ds_partitioned__extract_quarter
                  , subq_25.ds_partitioned__extract_month
                  , subq_25.ds_partitioned__extract_day
                  , subq_25.ds_partitioned__extract_dow
                  , subq_25.ds_partitioned__extract_doy
                  , subq_25.paid_at__day
                  , subq_25.paid_at__week
                  , subq_25.paid_at__month
                  , subq_25.paid_at__quarter
                  , subq_25.paid_at__year
                  , subq_25.paid_at__extract_year
                  , subq_25.paid_at__extract_quarter
                  , subq_25.paid_at__extract_month
                  , subq_25.paid_at__extract_day
                  , subq_25.paid_at__extract_dow
                  , subq_25.paid_at__extract_doy
                  , subq_25.booking__ds__day
                  , subq_25.booking__ds__week
                  , subq_25.booking__ds__month
                  , subq_25.booking__ds__quarter
                  , subq_25.booking__ds__year
                  , subq_25.booking__ds__extract_year
                  , subq_25.booking__ds__extract_quarter
                  , subq_25.booking__ds__extract_month
                  , subq_25.booking__ds__extract_day
                  , subq_25.booking__ds__extract_dow
                  , subq_25.booking__ds__extract_doy
                  , subq_25.booking__ds_partitioned__day
                  , subq_25.booking__ds_partitioned__week
                  , subq_25.booking__ds_partitioned__month
                  , subq_25.booking__ds_partitioned__quarter
                  , subq_25.booking__ds_partitioned__year
                  , subq_25.booking__ds_partitioned__extract_year
                  , subq_25.booking__ds_partitioned__extract_quarter
                  , subq_25.booking__ds_partitioned__extract_month
                  , subq_25.booking__ds_partitioned__extract_day
                  , subq_25.booking__ds_partitioned__extract_dow
                  , subq_25.booking__ds_partitioned__extract_doy
                  , subq_25.booking__paid_at__day
                  , subq_25.booking__paid_at__week
                  , subq_25.booking__paid_at__month
                  , subq_25.booking__paid_at__quarter
                  , subq_25.booking__paid_at__year
                  , subq_25.booking__paid_at__extract_year
                  , subq_25.booking__paid_at__extract_quarter
                  , subq_25.booking__paid_at__extract_month
                  , subq_25.booking__paid_at__extract_day
                  , subq_25.booking__paid_at__extract_dow
                  , subq_25.booking__paid_at__extract_doy
                  , subq_25.listing__created_at__day
                  , subq_25.listing
                  , subq_25.guest
                  , subq_25.host
                  , subq_25.booking__listing
                  , subq_25.booking__guest
                  , subq_25.booking__host
                  , subq_25.is_instant
                  , subq_25.booking__is_instant
                  , subq_25.bookings
                  , subq_25.instant_bookings
                  , subq_25.booking_value
                  , subq_25.max_booking_value
                  , subq_25.min_booking_value
                  , subq_25.bookers
                  , subq_25.average_booking_value
                  , subq_25.referred_bookings
                  , subq_25.median_booking_value
                  , subq_25.booking_value_p99
                  , subq_25.discrete_booking_value_p99
                  , subq_25.approximate_continuous_booking_value_p99
                  , subq_25.approximate_discrete_booking_value_p99
                FROM (
                  -- Join Standard Outputs
                  SELECT
                    subq_24.created_at__day AS listing__created_at__day
                    , subq_21.metric_time__day AS metric_time__day
                    , subq_21.metric_time__week AS metric_time__week
                    , subq_21.metric_time__month AS metric_time__month
                    , subq_21.metric_time__quarter AS metric_time__quarter
                    , subq_21.metric_time__year AS metric_time__year
                    , subq_21.metric_time__extract_year AS metric_time__extract_year
                    , subq_21.metric_time__extract_quarter AS metric_time__extract_quarter
                    , subq_21.metric_time__extract_month AS metric_time__extract_month
                    , subq_21.metric_time__extract_day AS metric_time__extract_day
                    , subq_21.metric_time__extract_dow AS metric_time__extract_dow
                    , subq_21.metric_time__extract_doy AS metric_time__extract_doy
                    , subq_21.ds__day AS ds__day
                    , subq_21.ds__week AS ds__week
                    , subq_21.ds__month AS ds__month
                    , subq_21.ds__quarter AS ds__quarter
                    , subq_21.ds__year AS ds__year
                    , subq_21.ds__extract_year AS ds__extract_year
                    , subq_21.ds__extract_quarter AS ds__extract_quarter
                    , subq_21.ds__extract_month AS ds__extract_month
                    , subq_21.ds__extract_day AS ds__extract_day
                    , subq_21.ds__extract_dow AS ds__extract_dow
                    , subq_21.ds__extract_doy AS ds__extract_doy
                    , subq_21.ds_partitioned__day AS ds_partitioned__day
                    , subq_21.ds_partitioned__week AS ds_partitioned__week
                    , subq_21.ds_partitioned__month AS ds_partitioned__month
                    , subq_21.ds_partitioned__quarter AS ds_partitioned__quarter
                    , subq_21.ds_partitioned__year AS ds_partitioned__year
                    , subq_21.ds_partitioned__extract_year AS ds_partitioned__extract_year
                    , subq_21.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                    , subq_21.ds_partitioned__extract_month AS ds_partitioned__extract_month
                    , subq_21.ds_partitioned__extract_day AS ds_partitioned__extract_day
                    , subq_21.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                    , subq_21.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                    , subq_21.paid_at__day AS paid_at__day
                    , subq_21.paid_at__week AS paid_at__week
                    , subq_21.paid_at__month AS paid_at__month
                    , subq_21.paid_at__quarter AS paid_at__quarter
                    , subq_21.paid_at__year AS paid_at__year
                    , subq_21.paid_at__extract_year AS paid_at__extract_year
                    , subq_21.paid_at__extract_quarter AS paid_at__extract_quarter
                    , subq_21.paid_at__extract_month AS paid_at__extract_month
                    , subq_21.paid_at__extract_day AS paid_at__extract_day
                    , subq_21.paid_at__extract_dow AS paid_at__extract_dow
                    , subq_21.paid_at__extract_doy AS paid_at__extract_doy
                    , subq_21.booking__ds__day AS booking__ds__day
                    , subq_21.booking__ds__week AS booking__ds__week
                    , subq_21.booking__ds__month AS booking__ds__month
                    , subq_21.booking__ds__quarter AS booking__ds__quarter
                    , subq_21.booking__ds__year AS booking__ds__year
                    , subq_21.booking__ds__extract_year AS booking__ds__extract_year
                    , subq_21.booking__ds__extract_quarter AS booking__ds__extract_quarter
                    , subq_21.booking__ds__extract_month AS booking__ds__extract_month
                    , subq_21.booking__ds__extract_day AS booking__ds__extract_day
                    , subq_21.booking__ds__extract_dow AS booking__ds__extract_dow
                    , subq_21.booking__ds__extract_doy AS booking__ds__extract_doy
                    , subq_21.booking__ds_partitioned__day AS booking__ds_partitioned__day
                    , subq_21.booking__ds_partitioned__week AS booking__ds_partitioned__week
                    , subq_21.booking__ds_partitioned__month AS booking__ds_partitioned__month
                    , subq_21.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                    , subq_21.booking__ds_partitioned__year AS booking__ds_partitioned__year
                    , subq_21.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                    , subq_21.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                    , subq_21.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                    , subq_21.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                    , subq_21.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                    , subq_21.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                    , subq_21.booking__paid_at__day AS booking__paid_at__day
                    , subq_21.booking__paid_at__week AS booking__paid_at__week
                    , subq_21.booking__paid_at__month AS booking__paid_at__month
                    , subq_21.booking__paid_at__quarter AS booking__paid_at__quarter
                    , subq_21.booking__paid_at__year AS booking__paid_at__year
                    , subq_21.booking__paid_at__extract_year AS booking__paid_at__extract_year
                    , subq_21.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                    , subq_21.booking__paid_at__extract_month AS booking__paid_at__extract_month
                    , subq_21.booking__paid_at__extract_day AS booking__paid_at__extract_day
                    , subq_21.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                    , subq_21.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
                    , subq_21.listing AS listing
                    , subq_21.guest AS guest
                    , subq_21.host AS host
                    , subq_21.booking__listing AS booking__listing
                    , subq_21.booking__guest AS booking__guest
                    , subq_21.booking__host AS booking__host
                    , subq_21.is_instant AS is_instant
                    , subq_21.booking__is_instant AS booking__is_instant
                    , subq_21.bookings AS bookings
                    , subq_21.instant_bookings AS instant_bookings
                    , subq_21.booking_value AS booking_value
                    , subq_21.max_booking_value AS max_booking_value
                    , subq_21.min_booking_value AS min_booking_value
                    , subq_21.bookers AS bookers
                    , subq_21.average_booking_value AS average_booking_value
                    , subq_21.referred_bookings AS referred_bookings
                    , subq_21.median_booking_value AS median_booking_value
                    , subq_21.booking_value_p99 AS booking_value_p99
                    , subq_21.discrete_booking_value_p99 AS discrete_booking_value_p99
                    , subq_21.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
                    , subq_21.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
                  FROM (
                    -- Join to Time Spine Dataset
                    SELECT
                      subq_18.metric_time__day AS metric_time__day
                      , DATE_TRUNC('week', subq_18.metric_time__day) AS metric_time__week
                      , DATE_TRUNC('month', subq_18.metric_time__day) AS metric_time__month
                      , DATE_TRUNC('quarter', subq_18.metric_time__day) AS metric_time__quarter
                      , DATE_TRUNC('year', subq_18.metric_time__day) AS metric_time__year
                      , EXTRACT(year FROM subq_18.metric_time__day) AS metric_time__extract_year
                      , EXTRACT(quarter FROM subq_18.metric_time__day) AS metric_time__extract_quarter
                      , EXTRACT(month FROM subq_18.metric_time__day) AS metric_time__extract_month
                      , EXTRACT(day FROM subq_18.metric_time__day) AS metric_time__extract_day
                      , EXTRACT(isodow FROM subq_18.metric_time__day) AS metric_time__extract_dow
                      , EXTRACT(doy FROM subq_18.metric_time__day) AS metric_time__extract_doy
                      , subq_17.ds__day AS ds__day
                      , subq_17.ds__week AS ds__week
                      , subq_17.ds__month AS ds__month
                      , subq_17.ds__quarter AS ds__quarter
                      , subq_17.ds__year AS ds__year
                      , subq_17.ds__extract_year AS ds__extract_year
                      , subq_17.ds__extract_quarter AS ds__extract_quarter
                      , subq_17.ds__extract_month AS ds__extract_month
                      , subq_17.ds__extract_day AS ds__extract_day
                      , subq_17.ds__extract_dow AS ds__extract_dow
                      , subq_17.ds__extract_doy AS ds__extract_doy
                      , subq_17.ds_partitioned__day AS ds_partitioned__day
                      , subq_17.ds_partitioned__week AS ds_partitioned__week
                      , subq_17.ds_partitioned__month AS ds_partitioned__month
                      , subq_17.ds_partitioned__quarter AS ds_partitioned__quarter
                      , subq_17.ds_partitioned__year AS ds_partitioned__year
                      , subq_17.ds_partitioned__extract_year AS ds_partitioned__extract_year
                      , subq_17.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                      , subq_17.ds_partitioned__extract_month AS ds_partitioned__extract_month
                      , subq_17.ds_partitioned__extract_day AS ds_partitioned__extract_day
                      , subq_17.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                      , subq_17.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                      , subq_17.paid_at__day AS paid_at__day
                      , subq_17.paid_at__week AS paid_at__week
                      , subq_17.paid_at__month AS paid_at__month
                      , subq_17.paid_at__quarter AS paid_at__quarter
                      , subq_17.paid_at__year AS paid_at__year
                      , subq_17.paid_at__extract_year AS paid_at__extract_year
                      , subq_17.paid_at__extract_quarter AS paid_at__extract_quarter
                      , subq_17.paid_at__extract_month AS paid_at__extract_month
                      , subq_17.paid_at__extract_day AS paid_at__extract_day
                      , subq_17.paid_at__extract_dow AS paid_at__extract_dow
                      , subq_17.paid_at__extract_doy AS paid_at__extract_doy
                      , subq_17.booking__ds__day AS booking__ds__day
                      , subq_17.booking__ds__week AS booking__ds__week
                      , subq_17.booking__ds__month AS booking__ds__month
                      , subq_17.booking__ds__quarter AS booking__ds__quarter
                      , subq_17.booking__ds__year AS booking__ds__year
                      , subq_17.booking__ds__extract_year AS booking__ds__extract_year
                      , subq_17.booking__ds__extract_quarter AS booking__ds__extract_quarter
                      , subq_17.booking__ds__extract_month AS booking__ds__extract_month
                      , subq_17.booking__ds__extract_day AS booking__ds__extract_day
                      , subq_17.booking__ds__extract_dow AS booking__ds__extract_dow
                      , subq_17.booking__ds__extract_doy AS booking__ds__extract_doy
                      , subq_17.booking__ds_partitioned__day AS booking__ds_partitioned__day
                      , subq_17.booking__ds_partitioned__week AS booking__ds_partitioned__week
                      , subq_17.booking__ds_partitioned__month AS booking__ds_partitioned__month
                      , subq_17.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                      , subq_17.booking__ds_partitioned__year AS booking__ds_partitioned__year
                      , subq_17.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                      , subq_17.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                      , subq_17.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                      , subq_17.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                      , subq_17.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                      , subq_17.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                      , subq_17.booking__paid_at__day AS booking__paid_at__day
                      , subq_17.booking__paid_at__week AS booking__paid_at__week
                      , subq_17.booking__paid_at__month AS booking__paid_at__month
                      , subq_17.booking__paid_at__quarter AS booking__paid_at__quarter
                      , subq_17.booking__paid_at__year AS booking__paid_at__year
                      , subq_17.booking__paid_at__extract_year AS booking__paid_at__extract_year
                      , subq_17.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                      , subq_17.booking__paid_at__extract_month AS booking__paid_at__extract_month
                      , subq_17.booking__paid_at__extract_day AS booking__paid_at__extract_day
                      , subq_17.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                      , subq_17.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
                      , subq_17.listing AS listing
                      , subq_17.guest AS guest
                      , subq_17.host AS host
                      , subq_17.booking__listing AS booking__listing
                      , subq_17.booking__guest AS booking__guest
                      , subq_17.booking__host AS booking__host
                      , subq_17.is_instant AS is_instant
                      , subq_17.booking__is_instant AS booking__is_instant
                      , subq_17.bookings AS bookings
                      , subq_17.instant_bookings AS instant_bookings
                      , subq_17.booking_value AS booking_value
                      , subq_17.max_booking_value AS max_booking_value
                      , subq_17.min_booking_value AS min_booking_value
                      , subq_17.bookers AS bookers
                      , subq_17.average_booking_value AS average_booking_value
                      , subq_17.referred_bookings AS referred_bookings
                      , subq_17.median_booking_value AS median_booking_value
                      , subq_17.booking_value_p99 AS booking_value_p99
                      , subq_17.discrete_booking_value_p99 AS discrete_booking_value_p99
                      , subq_17.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
                      , subq_17.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
                    FROM (
                      -- Filter Time Spine
                      SELECT
                        subq_20.metric_time__day
                      FROM (
                        -- Time Spine
                        SELECT
                          DATE_TRUNC('year', subq_19.ds) AS booking__ds__year
                          , subq_19.ds AS metric_time__day
                        FROM ***************************.mf_time_spine subq_19
                      ) subq_20
                      WHERE booking__ds__year >= '2019-01-01'
                    ) subq_18
                    INNER JOIN (
                      -- Metric Time Dimension 'ds'
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
                        , subq_16.paid_at__day
                        , subq_16.paid_at__week
                        , subq_16.paid_at__month
                        , subq_16.paid_at__quarter
                        , subq_16.paid_at__year
                        , subq_16.paid_at__extract_year
                        , subq_16.paid_at__extract_quarter
                        , subq_16.paid_at__extract_month
                        , subq_16.paid_at__extract_day
                        , subq_16.paid_at__extract_dow
                        , subq_16.paid_at__extract_doy
                        , subq_16.booking__ds__day
                        , subq_16.booking__ds__week
                        , subq_16.booking__ds__month
                        , subq_16.booking__ds__quarter
                        , subq_16.booking__ds__year
                        , subq_16.booking__ds__extract_year
                        , subq_16.booking__ds__extract_quarter
                        , subq_16.booking__ds__extract_month
                        , subq_16.booking__ds__extract_day
                        , subq_16.booking__ds__extract_dow
                        , subq_16.booking__ds__extract_doy
                        , subq_16.booking__ds_partitioned__day
                        , subq_16.booking__ds_partitioned__week
                        , subq_16.booking__ds_partitioned__month
                        , subq_16.booking__ds_partitioned__quarter
                        , subq_16.booking__ds_partitioned__year
                        , subq_16.booking__ds_partitioned__extract_year
                        , subq_16.booking__ds_partitioned__extract_quarter
                        , subq_16.booking__ds_partitioned__extract_month
                        , subq_16.booking__ds_partitioned__extract_day
                        , subq_16.booking__ds_partitioned__extract_dow
                        , subq_16.booking__ds_partitioned__extract_doy
                        , subq_16.booking__paid_at__day
                        , subq_16.booking__paid_at__week
                        , subq_16.booking__paid_at__month
                        , subq_16.booking__paid_at__quarter
                        , subq_16.booking__paid_at__year
                        , subq_16.booking__paid_at__extract_year
                        , subq_16.booking__paid_at__extract_quarter
                        , subq_16.booking__paid_at__extract_month
                        , subq_16.booking__paid_at__extract_day
                        , subq_16.booking__paid_at__extract_dow
                        , subq_16.booking__paid_at__extract_doy
                        , subq_16.ds__day AS metric_time__day
                        , subq_16.ds__week AS metric_time__week
                        , subq_16.ds__month AS metric_time__month
                        , subq_16.ds__quarter AS metric_time__quarter
                        , subq_16.ds__year AS metric_time__year
                        , subq_16.ds__extract_year AS metric_time__extract_year
                        , subq_16.ds__extract_quarter AS metric_time__extract_quarter
                        , subq_16.ds__extract_month AS metric_time__extract_month
                        , subq_16.ds__extract_day AS metric_time__extract_day
                        , subq_16.ds__extract_dow AS metric_time__extract_dow
                        , subq_16.ds__extract_doy AS metric_time__extract_doy
                        , subq_16.listing
                        , subq_16.guest
                        , subq_16.host
                        , subq_16.booking__listing
                        , subq_16.booking__guest
                        , subq_16.booking__host
                        , subq_16.is_instant
                        , subq_16.booking__is_instant
                        , subq_16.bookings
                        , subq_16.instant_bookings
                        , subq_16.booking_value
                        , subq_16.max_booking_value
                        , subq_16.min_booking_value
                        , subq_16.bookers
                        , subq_16.average_booking_value
                        , subq_16.referred_bookings
                        , subq_16.median_booking_value
                        , subq_16.booking_value_p99
                        , subq_16.discrete_booking_value_p99
                        , subq_16.approximate_continuous_booking_value_p99
                        , subq_16.approximate_discrete_booking_value_p99
                      FROM (
                        -- Read Elements From Semantic Model 'bookings_source'
                        SELECT
                          1 AS bookings
                          , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
                          , bookings_source_src_28000.booking_value
                          , bookings_source_src_28000.booking_value AS max_booking_value
                          , bookings_source_src_28000.booking_value AS min_booking_value
                          , bookings_source_src_28000.guest_id AS bookers
                          , bookings_source_src_28000.booking_value AS average_booking_value
                          , bookings_source_src_28000.booking_value AS booking_payments
                          , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
                          , bookings_source_src_28000.booking_value AS median_booking_value
                          , bookings_source_src_28000.booking_value AS booking_value_p99
                          , bookings_source_src_28000.booking_value AS discrete_booking_value_p99
                          , bookings_source_src_28000.booking_value AS approximate_continuous_booking_value_p99
                          , bookings_source_src_28000.booking_value AS approximate_discrete_booking_value_p99
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
                      ) subq_16
                    ) subq_17
                    ON
                      DATE_TRUNC('month', subq_18.metric_time__day) = subq_17.metric_time__day
                  ) subq_21
                  LEFT OUTER JOIN (
                    -- Pass Only Elements: ['created_at__day', 'listing']
                    SELECT
                      subq_23.created_at__day
                      , subq_23.listing
                    FROM (
                      -- Metric Time Dimension 'ds'
                      SELECT
                        subq_22.ds__day
                        , subq_22.ds__week
                        , subq_22.ds__month
                        , subq_22.ds__quarter
                        , subq_22.ds__year
                        , subq_22.ds__extract_year
                        , subq_22.ds__extract_quarter
                        , subq_22.ds__extract_month
                        , subq_22.ds__extract_day
                        , subq_22.ds__extract_dow
                        , subq_22.ds__extract_doy
                        , subq_22.created_at__day
                        , subq_22.created_at__week
                        , subq_22.created_at__month
                        , subq_22.created_at__quarter
                        , subq_22.created_at__year
                        , subq_22.created_at__extract_year
                        , subq_22.created_at__extract_quarter
                        , subq_22.created_at__extract_month
                        , subq_22.created_at__extract_day
                        , subq_22.created_at__extract_dow
                        , subq_22.created_at__extract_doy
                        , subq_22.listing__ds__day
                        , subq_22.listing__ds__week
                        , subq_22.listing__ds__month
                        , subq_22.listing__ds__quarter
                        , subq_22.listing__ds__year
                        , subq_22.listing__ds__extract_year
                        , subq_22.listing__ds__extract_quarter
                        , subq_22.listing__ds__extract_month
                        , subq_22.listing__ds__extract_day
                        , subq_22.listing__ds__extract_dow
                        , subq_22.listing__ds__extract_doy
                        , subq_22.listing__created_at__day
                        , subq_22.listing__created_at__week
                        , subq_22.listing__created_at__month
                        , subq_22.listing__created_at__quarter
                        , subq_22.listing__created_at__year
                        , subq_22.listing__created_at__extract_year
                        , subq_22.listing__created_at__extract_quarter
                        , subq_22.listing__created_at__extract_month
                        , subq_22.listing__created_at__extract_day
                        , subq_22.listing__created_at__extract_dow
                        , subq_22.listing__created_at__extract_doy
                        , subq_22.ds__day AS metric_time__day
                        , subq_22.ds__week AS metric_time__week
                        , subq_22.ds__month AS metric_time__month
                        , subq_22.ds__quarter AS metric_time__quarter
                        , subq_22.ds__year AS metric_time__year
                        , subq_22.ds__extract_year AS metric_time__extract_year
                        , subq_22.ds__extract_quarter AS metric_time__extract_quarter
                        , subq_22.ds__extract_month AS metric_time__extract_month
                        , subq_22.ds__extract_day AS metric_time__extract_day
                        , subq_22.ds__extract_dow AS metric_time__extract_dow
                        , subq_22.ds__extract_doy AS metric_time__extract_doy
                        , subq_22.listing
                        , subq_22.user
                        , subq_22.listing__user
                        , subq_22.country_latest
                        , subq_22.is_lux_latest
                        , subq_22.capacity_latest
                        , subq_22.listing__country_latest
                        , subq_22.listing__is_lux_latest
                        , subq_22.listing__capacity_latest
                        , subq_22.listings
                        , subq_22.largest_listing
                        , subq_22.smallest_listing
                      FROM (
                        -- Read Elements From Semantic Model 'listings_latest'
                        SELECT
                          1 AS listings
                          , listings_latest_src_28000.capacity AS largest_listing
                          , listings_latest_src_28000.capacity AS smallest_listing
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
                      ) subq_22
                    ) subq_23
                  ) subq_24
                  ON
                    subq_21.listing = subq_24.listing
                ) subq_25
                WHERE ((listing IS NOT NULL) AND (booking__is_instant)) AND (listing__created_at__day >= '2020-01-02')
              ) subq_26
            ) subq_27
            GROUP BY
              subq_27.metric_time__day
          ) subq_28
        ) subq_29
        ON
          subq_15.metric_time__day = subq_29.metric_time__day
        GROUP BY
          COALESCE(subq_15.metric_time__day, subq_29.metric_time__day)
      ) subq_30
    ) subq_31
    ON
      DATE_TRUNC('week', subq_32.metric_time__day) = subq_31.metric_time__day
  ) subq_35
  ON
    subq_5.metric_time__day = subq_35.metric_time__day
  GROUP BY
    COALESCE(subq_5.metric_time__day, subq_35.metric_time__day)
) subq_36
