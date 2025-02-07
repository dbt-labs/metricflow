test_name: test_nested_offset_metric_with_tiered_filters
test_filename: test_derived_metric_rendering.py
docstring:
  Tests that filters at different tiers are applied appropriately for derived metrics.

      This includes filters at the input metric, metric, and query level. At each tier there are filters on both
      metric_time / agg time and another dimension, which might have different behaviors.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  subq_19.metric_time__day
  , bookings_offset_once AS bookings_offset_twice_with_tiered_filters
FROM (
  -- Pass Only Elements: ['metric_time__day', 'bookings_offset_once']
  SELECT
    subq_18.metric_time__day
    , subq_18.bookings_offset_once
  FROM (
    -- Constrain Output with WHERE
    SELECT
      subq_17.metric_time__month
      , subq_17.booking__ds__quarter
      , subq_17.metric_time__year
      , subq_17.listing__created_at__day
      , subq_17.metric_time__day
      , subq_17.listing
      , subq_17.booking__is_instant
      , subq_17.bookings_offset_once
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_16.metric_time__day AS metric_time__day
        , subq_13.metric_time__month AS metric_time__month
        , subq_13.booking__ds__quarter AS booking__ds__quarter
        , subq_13.metric_time__year AS metric_time__year
        , subq_13.listing__created_at__day AS listing__created_at__day
        , subq_13.listing AS listing
        , subq_13.booking__is_instant AS booking__is_instant
        , subq_13.bookings_offset_once AS bookings_offset_once
      FROM (
        -- Pass Only Elements: ['metric_time__day',]
        SELECT
          subq_15.metric_time__day
        FROM (
          -- Change Column Aliases
          SELECT
            subq_14.ds__day AS metric_time__day
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
            , subq_14.ds__alien_day
          FROM (
            -- Read From Time Spine 'mf_time_spine'
            SELECT
              time_spine_src_28006.ds AS ds__day
              , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
              , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
              , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
              , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
              , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
              , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
              , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
              , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
              , EXTRACT(isodow FROM time_spine_src_28006.ds) AS ds__extract_dow
              , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
              , time_spine_src_28006.alien_day AS ds__alien_day
            FROM ***************************.mf_time_spine time_spine_src_28006
          ) subq_14
        ) subq_15
      ) subq_16
      INNER JOIN (
        -- Compute Metrics via Expressions
        SELECT
          subq_12.metric_time__day
          , subq_12.metric_time__month
          , subq_12.booking__ds__quarter
          , subq_12.metric_time__year
          , subq_12.listing__created_at__day
          , subq_12.listing
          , subq_12.booking__is_instant
          , 2 * bookings AS bookings_offset_once
        FROM (
          -- Compute Metrics via Expressions
          SELECT
            subq_11.metric_time__day
            , subq_11.metric_time__month
            , subq_11.booking__ds__quarter
            , subq_11.metric_time__year
            , subq_11.listing__created_at__day
            , subq_11.listing
            , subq_11.booking__is_instant
            , subq_11.bookings
          FROM (
            -- Aggregate Measures
            SELECT
              subq_10.metric_time__day
              , subq_10.metric_time__month
              , subq_10.booking__ds__quarter
              , subq_10.metric_time__year
              , subq_10.listing__created_at__day
              , subq_10.listing
              , subq_10.booking__is_instant
              , SUM(subq_10.bookings) AS bookings
            FROM (
              -- Pass Only Elements: [
              --   'bookings',
              --   'booking__is_instant',
              --   'metric_time__day',
              --   'metric_time__year',
              --   'metric_time__month',
              --   'booking__ds__quarter',
              --   'listing__created_at__day',
              --   'listing',
              -- ]
              SELECT
                subq_9.metric_time__day
                , subq_9.metric_time__month
                , subq_9.booking__ds__quarter
                , subq_9.metric_time__year
                , subq_9.listing__created_at__day
                , subq_9.listing
                , subq_9.booking__is_instant
                , subq_9.bookings
              FROM (
                -- Join Standard Outputs
                SELECT
                  subq_8.created_at__day AS listing__created_at__day
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
                  , subq_5.metric_time__week AS metric_time__week
                  , subq_5.metric_time__quarter AS metric_time__quarter
                  , subq_5.metric_time__extract_year AS metric_time__extract_year
                  , subq_5.metric_time__extract_quarter AS metric_time__extract_quarter
                  , subq_5.metric_time__extract_month AS metric_time__extract_month
                  , subq_5.metric_time__extract_day AS metric_time__extract_day
                  , subq_5.metric_time__extract_dow AS metric_time__extract_dow
                  , subq_5.metric_time__extract_doy AS metric_time__extract_doy
                  , subq_5.metric_time__day AS metric_time__day
                  , subq_5.metric_time__month AS metric_time__month
                  , subq_5.booking__ds__quarter AS booking__ds__quarter
                  , subq_5.metric_time__year AS metric_time__year
                  , subq_5.listing AS listing
                  , subq_5.guest AS guest
                  , subq_5.host AS host
                  , subq_5.booking__listing AS booking__listing
                  , subq_5.booking__guest AS booking__guest
                  , subq_5.booking__host AS booking__host
                  , subq_5.is_instant AS is_instant
                  , subq_5.booking__is_instant AS booking__is_instant
                  , subq_5.bookings AS bookings
                  , subq_5.instant_bookings AS instant_bookings
                  , subq_5.booking_value AS booking_value
                  , subq_5.max_booking_value AS max_booking_value
                  , subq_5.min_booking_value AS min_booking_value
                  , subq_5.bookers AS bookers
                  , subq_5.average_booking_value AS average_booking_value
                  , subq_5.referred_bookings AS referred_bookings
                  , subq_5.median_booking_value AS median_booking_value
                  , subq_5.booking_value_p99 AS booking_value_p99
                  , subq_5.discrete_booking_value_p99 AS discrete_booking_value_p99
                  , subq_5.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
                  , subq_5.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
                FROM (
                  -- Join to Time Spine Dataset
                  SELECT
                    subq_4.metric_time__day AS metric_time__day
                    , subq_4.metric_time__month AS metric_time__month
                    , subq_4.booking__ds__quarter AS booking__ds__quarter
                    , subq_4.metric_time__year AS metric_time__year
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
                    , subq_1.metric_time__week AS metric_time__week
                    , subq_1.metric_time__quarter AS metric_time__quarter
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
                    , subq_1.instant_bookings AS instant_bookings
                    , subq_1.booking_value AS booking_value
                    , subq_1.max_booking_value AS max_booking_value
                    , subq_1.min_booking_value AS min_booking_value
                    , subq_1.bookers AS bookers
                    , subq_1.average_booking_value AS average_booking_value
                    , subq_1.referred_bookings AS referred_bookings
                    , subq_1.median_booking_value AS median_booking_value
                    , subq_1.booking_value_p99 AS booking_value_p99
                    , subq_1.discrete_booking_value_p99 AS discrete_booking_value_p99
                    , subq_1.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
                    , subq_1.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
                  FROM (
                    -- Pass Only Elements: ['booking__ds__quarter', 'metric_time__day', 'metric_time__year', 'metric_time__month']
                    SELECT
                      subq_3.metric_time__day
                      , subq_3.metric_time__month
                      , subq_3.booking__ds__quarter
                      , subq_3.metric_time__year
                    FROM (
                      -- Change Column Aliases
                      SELECT
                        subq_2.ds__day AS metric_time__day
                        , subq_2.ds__week
                        , subq_2.ds__month AS metric_time__month
                        , subq_2.ds__quarter AS booking__ds__quarter
                        , subq_2.ds__year AS metric_time__year
                        , subq_2.ds__extract_year
                        , subq_2.ds__extract_quarter
                        , subq_2.ds__extract_month
                        , subq_2.ds__extract_day
                        , subq_2.ds__extract_dow
                        , subq_2.ds__extract_doy
                        , subq_2.ds__alien_day
                      FROM (
                        -- Read From Time Spine 'mf_time_spine'
                        SELECT
                          time_spine_src_28006.ds AS ds__day
                          , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
                          , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
                          , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
                          , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
                          , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
                          , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
                          , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
                          , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
                          , EXTRACT(isodow FROM time_spine_src_28006.ds) AS ds__extract_dow
                          , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
                          , time_spine_src_28006.alien_day AS ds__alien_day
                        FROM ***************************.mf_time_spine time_spine_src_28006
                      ) subq_2
                    ) subq_3
                  ) subq_4
                  INNER JOIN (
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
                  ON
                    subq_4.metric_time__day - INTERVAL 5 day = subq_1.metric_time__day
                ) subq_5
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['created_at__day', 'listing']
                  SELECT
                    subq_7.created_at__day
                    , subq_7.listing
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
                      , subq_6.created_at__day
                      , subq_6.created_at__week
                      , subq_6.created_at__month
                      , subq_6.created_at__quarter
                      , subq_6.created_at__year
                      , subq_6.created_at__extract_year
                      , subq_6.created_at__extract_quarter
                      , subq_6.created_at__extract_month
                      , subq_6.created_at__extract_day
                      , subq_6.created_at__extract_dow
                      , subq_6.created_at__extract_doy
                      , subq_6.listing__ds__day
                      , subq_6.listing__ds__week
                      , subq_6.listing__ds__month
                      , subq_6.listing__ds__quarter
                      , subq_6.listing__ds__year
                      , subq_6.listing__ds__extract_year
                      , subq_6.listing__ds__extract_quarter
                      , subq_6.listing__ds__extract_month
                      , subq_6.listing__ds__extract_day
                      , subq_6.listing__ds__extract_dow
                      , subq_6.listing__ds__extract_doy
                      , subq_6.listing__created_at__day
                      , subq_6.listing__created_at__week
                      , subq_6.listing__created_at__month
                      , subq_6.listing__created_at__quarter
                      , subq_6.listing__created_at__year
                      , subq_6.listing__created_at__extract_year
                      , subq_6.listing__created_at__extract_quarter
                      , subq_6.listing__created_at__extract_month
                      , subq_6.listing__created_at__extract_day
                      , subq_6.listing__created_at__extract_dow
                      , subq_6.listing__created_at__extract_doy
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
                      , subq_6.user
                      , subq_6.listing__user
                      , subq_6.country_latest
                      , subq_6.is_lux_latest
                      , subq_6.capacity_latest
                      , subq_6.listing__country_latest
                      , subq_6.listing__is_lux_latest
                      , subq_6.listing__capacity_latest
                      , subq_6.listings
                      , subq_6.largest_listing
                      , subq_6.smallest_listing
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
                    ) subq_6
                  ) subq_7
                ) subq_8
                ON
                  subq_5.listing = subq_8.listing
              ) subq_9
            ) subq_10
            GROUP BY
              subq_10.metric_time__day
              , subq_10.metric_time__month
              , subq_10.booking__ds__quarter
              , subq_10.metric_time__year
              , subq_10.listing__created_at__day
              , subq_10.listing
              , subq_10.booking__is_instant
          ) subq_11
        ) subq_12
      ) subq_13
      ON
        subq_16.metric_time__day - INTERVAL 1 month = subq_13.metric_time__day
    ) subq_17
    WHERE (((((metric_time__year >= '2020-01-01') AND (listing IS NOT NULL)) AND (metric_time__month >= '2019-01-01')) AND (booking__is_instant)) AND (booking__ds__quarter = '2021-01-01')) AND (listing__created_at__day = '2021-01-01')
  ) subq_18
) subq_19
