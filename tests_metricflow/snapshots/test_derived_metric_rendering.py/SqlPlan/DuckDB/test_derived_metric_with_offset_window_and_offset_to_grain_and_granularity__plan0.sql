test_name: test_derived_metric_with_offset_window_and_offset_to_grain_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_16.metric_time__year
  , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_7.metric_time__year, nr_subq_15.metric_time__year) AS metric_time__year
    , MAX(nr_subq_7.month_start_bookings) AS month_start_bookings
    , MAX(nr_subq_15.bookings_1_month_ago) AS bookings_1_month_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      nr_subq_6.metric_time__year
      , nr_subq_6.bookings AS month_start_bookings
    FROM (
      -- Aggregate Measures
      SELECT
        nr_subq_5.metric_time__year
        , SUM(nr_subq_5.bookings) AS bookings
      FROM (
        -- Pass Only Elements: ['bookings', 'metric_time__year']
        SELECT
          nr_subq_4.metric_time__year
          , nr_subq_4.bookings
        FROM (
          -- Join to Time Spine Dataset
          SELECT
            nr_subq_3.metric_time__day AS metric_time__day
            , nr_subq_3.metric_time__year AS metric_time__year
            , nr_subq_0.ds__day AS ds__day
            , nr_subq_0.ds__week AS ds__week
            , nr_subq_0.ds__month AS ds__month
            , nr_subq_0.ds__quarter AS ds__quarter
            , nr_subq_0.ds__year AS ds__year
            , nr_subq_0.ds__extract_year AS ds__extract_year
            , nr_subq_0.ds__extract_quarter AS ds__extract_quarter
            , nr_subq_0.ds__extract_month AS ds__extract_month
            , nr_subq_0.ds__extract_day AS ds__extract_day
            , nr_subq_0.ds__extract_dow AS ds__extract_dow
            , nr_subq_0.ds__extract_doy AS ds__extract_doy
            , nr_subq_0.ds_partitioned__day AS ds_partitioned__day
            , nr_subq_0.ds_partitioned__week AS ds_partitioned__week
            , nr_subq_0.ds_partitioned__month AS ds_partitioned__month
            , nr_subq_0.ds_partitioned__quarter AS ds_partitioned__quarter
            , nr_subq_0.ds_partitioned__year AS ds_partitioned__year
            , nr_subq_0.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , nr_subq_0.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , nr_subq_0.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , nr_subq_0.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , nr_subq_0.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , nr_subq_0.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , nr_subq_0.paid_at__day AS paid_at__day
            , nr_subq_0.paid_at__week AS paid_at__week
            , nr_subq_0.paid_at__month AS paid_at__month
            , nr_subq_0.paid_at__quarter AS paid_at__quarter
            , nr_subq_0.paid_at__year AS paid_at__year
            , nr_subq_0.paid_at__extract_year AS paid_at__extract_year
            , nr_subq_0.paid_at__extract_quarter AS paid_at__extract_quarter
            , nr_subq_0.paid_at__extract_month AS paid_at__extract_month
            , nr_subq_0.paid_at__extract_day AS paid_at__extract_day
            , nr_subq_0.paid_at__extract_dow AS paid_at__extract_dow
            , nr_subq_0.paid_at__extract_doy AS paid_at__extract_doy
            , nr_subq_0.booking__ds__day AS booking__ds__day
            , nr_subq_0.booking__ds__week AS booking__ds__week
            , nr_subq_0.booking__ds__month AS booking__ds__month
            , nr_subq_0.booking__ds__quarter AS booking__ds__quarter
            , nr_subq_0.booking__ds__year AS booking__ds__year
            , nr_subq_0.booking__ds__extract_year AS booking__ds__extract_year
            , nr_subq_0.booking__ds__extract_quarter AS booking__ds__extract_quarter
            , nr_subq_0.booking__ds__extract_month AS booking__ds__extract_month
            , nr_subq_0.booking__ds__extract_day AS booking__ds__extract_day
            , nr_subq_0.booking__ds__extract_dow AS booking__ds__extract_dow
            , nr_subq_0.booking__ds__extract_doy AS booking__ds__extract_doy
            , nr_subq_0.booking__ds_partitioned__day AS booking__ds_partitioned__day
            , nr_subq_0.booking__ds_partitioned__week AS booking__ds_partitioned__week
            , nr_subq_0.booking__ds_partitioned__month AS booking__ds_partitioned__month
            , nr_subq_0.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
            , nr_subq_0.booking__ds_partitioned__year AS booking__ds_partitioned__year
            , nr_subq_0.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
            , nr_subq_0.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
            , nr_subq_0.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
            , nr_subq_0.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
            , nr_subq_0.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
            , nr_subq_0.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
            , nr_subq_0.booking__paid_at__day AS booking__paid_at__day
            , nr_subq_0.booking__paid_at__week AS booking__paid_at__week
            , nr_subq_0.booking__paid_at__month AS booking__paid_at__month
            , nr_subq_0.booking__paid_at__quarter AS booking__paid_at__quarter
            , nr_subq_0.booking__paid_at__year AS booking__paid_at__year
            , nr_subq_0.booking__paid_at__extract_year AS booking__paid_at__extract_year
            , nr_subq_0.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
            , nr_subq_0.booking__paid_at__extract_month AS booking__paid_at__extract_month
            , nr_subq_0.booking__paid_at__extract_day AS booking__paid_at__extract_day
            , nr_subq_0.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
            , nr_subq_0.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
            , nr_subq_0.metric_time__week AS metric_time__week
            , nr_subq_0.metric_time__month AS metric_time__month
            , nr_subq_0.metric_time__quarter AS metric_time__quarter
            , nr_subq_0.metric_time__extract_year AS metric_time__extract_year
            , nr_subq_0.metric_time__extract_quarter AS metric_time__extract_quarter
            , nr_subq_0.metric_time__extract_month AS metric_time__extract_month
            , nr_subq_0.metric_time__extract_day AS metric_time__extract_day
            , nr_subq_0.metric_time__extract_dow AS metric_time__extract_dow
            , nr_subq_0.metric_time__extract_doy AS metric_time__extract_doy
            , nr_subq_0.listing AS listing
            , nr_subq_0.guest AS guest
            , nr_subq_0.host AS host
            , nr_subq_0.booking__listing AS booking__listing
            , nr_subq_0.booking__guest AS booking__guest
            , nr_subq_0.booking__host AS booking__host
            , nr_subq_0.is_instant AS is_instant
            , nr_subq_0.booking__is_instant AS booking__is_instant
            , nr_subq_0.bookings AS bookings
            , nr_subq_0.instant_bookings AS instant_bookings
            , nr_subq_0.booking_value AS booking_value
            , nr_subq_0.max_booking_value AS max_booking_value
            , nr_subq_0.min_booking_value AS min_booking_value
            , nr_subq_0.bookers AS bookers
            , nr_subq_0.average_booking_value AS average_booking_value
            , nr_subq_0.referred_bookings AS referred_bookings
            , nr_subq_0.median_booking_value AS median_booking_value
            , nr_subq_0.booking_value_p99 AS booking_value_p99
            , nr_subq_0.discrete_booking_value_p99 AS discrete_booking_value_p99
            , nr_subq_0.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
            , nr_subq_0.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
          FROM (
            -- Pass Only Elements: ['metric_time__day', 'metric_time__year']
            SELECT
              nr_subq_2.metric_time__day
              , nr_subq_2.metric_time__year
            FROM (
              -- Change Column Aliases
              SELECT
                nr_subq_1.ds__day AS metric_time__day
                , nr_subq_1.ds__week
                , nr_subq_1.ds__month
                , nr_subq_1.ds__quarter
                , nr_subq_1.ds__year AS metric_time__year
                , nr_subq_1.ds__extract_year
                , nr_subq_1.ds__extract_quarter
                , nr_subq_1.ds__extract_month
                , nr_subq_1.ds__extract_day
                , nr_subq_1.ds__extract_dow
                , nr_subq_1.ds__extract_doy
                , nr_subq_1.ds__martian_day
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
                  , time_spine_src_28006.martian_day AS ds__martian_day
                FROM ***************************.mf_time_spine time_spine_src_28006
              ) nr_subq_1
            ) nr_subq_2
          ) nr_subq_3
          INNER JOIN (
            -- Metric Time Dimension 'ds'
            SELECT
              nr_subq_28002.ds__day
              , nr_subq_28002.ds__week
              , nr_subq_28002.ds__month
              , nr_subq_28002.ds__quarter
              , nr_subq_28002.ds__year
              , nr_subq_28002.ds__extract_year
              , nr_subq_28002.ds__extract_quarter
              , nr_subq_28002.ds__extract_month
              , nr_subq_28002.ds__extract_day
              , nr_subq_28002.ds__extract_dow
              , nr_subq_28002.ds__extract_doy
              , nr_subq_28002.ds_partitioned__day
              , nr_subq_28002.ds_partitioned__week
              , nr_subq_28002.ds_partitioned__month
              , nr_subq_28002.ds_partitioned__quarter
              , nr_subq_28002.ds_partitioned__year
              , nr_subq_28002.ds_partitioned__extract_year
              , nr_subq_28002.ds_partitioned__extract_quarter
              , nr_subq_28002.ds_partitioned__extract_month
              , nr_subq_28002.ds_partitioned__extract_day
              , nr_subq_28002.ds_partitioned__extract_dow
              , nr_subq_28002.ds_partitioned__extract_doy
              , nr_subq_28002.paid_at__day
              , nr_subq_28002.paid_at__week
              , nr_subq_28002.paid_at__month
              , nr_subq_28002.paid_at__quarter
              , nr_subq_28002.paid_at__year
              , nr_subq_28002.paid_at__extract_year
              , nr_subq_28002.paid_at__extract_quarter
              , nr_subq_28002.paid_at__extract_month
              , nr_subq_28002.paid_at__extract_day
              , nr_subq_28002.paid_at__extract_dow
              , nr_subq_28002.paid_at__extract_doy
              , nr_subq_28002.booking__ds__day
              , nr_subq_28002.booking__ds__week
              , nr_subq_28002.booking__ds__month
              , nr_subq_28002.booking__ds__quarter
              , nr_subq_28002.booking__ds__year
              , nr_subq_28002.booking__ds__extract_year
              , nr_subq_28002.booking__ds__extract_quarter
              , nr_subq_28002.booking__ds__extract_month
              , nr_subq_28002.booking__ds__extract_day
              , nr_subq_28002.booking__ds__extract_dow
              , nr_subq_28002.booking__ds__extract_doy
              , nr_subq_28002.booking__ds_partitioned__day
              , nr_subq_28002.booking__ds_partitioned__week
              , nr_subq_28002.booking__ds_partitioned__month
              , nr_subq_28002.booking__ds_partitioned__quarter
              , nr_subq_28002.booking__ds_partitioned__year
              , nr_subq_28002.booking__ds_partitioned__extract_year
              , nr_subq_28002.booking__ds_partitioned__extract_quarter
              , nr_subq_28002.booking__ds_partitioned__extract_month
              , nr_subq_28002.booking__ds_partitioned__extract_day
              , nr_subq_28002.booking__ds_partitioned__extract_dow
              , nr_subq_28002.booking__ds_partitioned__extract_doy
              , nr_subq_28002.booking__paid_at__day
              , nr_subq_28002.booking__paid_at__week
              , nr_subq_28002.booking__paid_at__month
              , nr_subq_28002.booking__paid_at__quarter
              , nr_subq_28002.booking__paid_at__year
              , nr_subq_28002.booking__paid_at__extract_year
              , nr_subq_28002.booking__paid_at__extract_quarter
              , nr_subq_28002.booking__paid_at__extract_month
              , nr_subq_28002.booking__paid_at__extract_day
              , nr_subq_28002.booking__paid_at__extract_dow
              , nr_subq_28002.booking__paid_at__extract_doy
              , nr_subq_28002.ds__day AS metric_time__day
              , nr_subq_28002.ds__week AS metric_time__week
              , nr_subq_28002.ds__month AS metric_time__month
              , nr_subq_28002.ds__quarter AS metric_time__quarter
              , nr_subq_28002.ds__year AS metric_time__year
              , nr_subq_28002.ds__extract_year AS metric_time__extract_year
              , nr_subq_28002.ds__extract_quarter AS metric_time__extract_quarter
              , nr_subq_28002.ds__extract_month AS metric_time__extract_month
              , nr_subq_28002.ds__extract_day AS metric_time__extract_day
              , nr_subq_28002.ds__extract_dow AS metric_time__extract_dow
              , nr_subq_28002.ds__extract_doy AS metric_time__extract_doy
              , nr_subq_28002.listing
              , nr_subq_28002.guest
              , nr_subq_28002.host
              , nr_subq_28002.booking__listing
              , nr_subq_28002.booking__guest
              , nr_subq_28002.booking__host
              , nr_subq_28002.is_instant
              , nr_subq_28002.booking__is_instant
              , nr_subq_28002.bookings
              , nr_subq_28002.instant_bookings
              , nr_subq_28002.booking_value
              , nr_subq_28002.max_booking_value
              , nr_subq_28002.min_booking_value
              , nr_subq_28002.bookers
              , nr_subq_28002.average_booking_value
              , nr_subq_28002.referred_bookings
              , nr_subq_28002.median_booking_value
              , nr_subq_28002.booking_value_p99
              , nr_subq_28002.discrete_booking_value_p99
              , nr_subq_28002.approximate_continuous_booking_value_p99
              , nr_subq_28002.approximate_discrete_booking_value_p99
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
            ) nr_subq_28002
          ) nr_subq_0
          ON
            DATE_TRUNC('month', nr_subq_3.metric_time__day) = nr_subq_0.metric_time__day
          WHERE nr_subq_3.metric_time__year = nr_subq_3.metric_time__day
        ) nr_subq_4
      ) nr_subq_5
      GROUP BY
        nr_subq_5.metric_time__year
    ) nr_subq_6
  ) nr_subq_7
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      nr_subq_14.metric_time__year
      , nr_subq_14.bookings AS bookings_1_month_ago
    FROM (
      -- Aggregate Measures
      SELECT
        nr_subq_13.metric_time__year
        , SUM(nr_subq_13.bookings) AS bookings
      FROM (
        -- Pass Only Elements: ['bookings', 'metric_time__year']
        SELECT
          nr_subq_12.metric_time__year
          , nr_subq_12.bookings
        FROM (
          -- Join to Time Spine Dataset
          SELECT
            nr_subq_11.metric_time__day AS metric_time__day
            , nr_subq_11.metric_time__year AS metric_time__year
            , nr_subq_8.ds__day AS ds__day
            , nr_subq_8.ds__week AS ds__week
            , nr_subq_8.ds__month AS ds__month
            , nr_subq_8.ds__quarter AS ds__quarter
            , nr_subq_8.ds__year AS ds__year
            , nr_subq_8.ds__extract_year AS ds__extract_year
            , nr_subq_8.ds__extract_quarter AS ds__extract_quarter
            , nr_subq_8.ds__extract_month AS ds__extract_month
            , nr_subq_8.ds__extract_day AS ds__extract_day
            , nr_subq_8.ds__extract_dow AS ds__extract_dow
            , nr_subq_8.ds__extract_doy AS ds__extract_doy
            , nr_subq_8.ds_partitioned__day AS ds_partitioned__day
            , nr_subq_8.ds_partitioned__week AS ds_partitioned__week
            , nr_subq_8.ds_partitioned__month AS ds_partitioned__month
            , nr_subq_8.ds_partitioned__quarter AS ds_partitioned__quarter
            , nr_subq_8.ds_partitioned__year AS ds_partitioned__year
            , nr_subq_8.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , nr_subq_8.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , nr_subq_8.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , nr_subq_8.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , nr_subq_8.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , nr_subq_8.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , nr_subq_8.paid_at__day AS paid_at__day
            , nr_subq_8.paid_at__week AS paid_at__week
            , nr_subq_8.paid_at__month AS paid_at__month
            , nr_subq_8.paid_at__quarter AS paid_at__quarter
            , nr_subq_8.paid_at__year AS paid_at__year
            , nr_subq_8.paid_at__extract_year AS paid_at__extract_year
            , nr_subq_8.paid_at__extract_quarter AS paid_at__extract_quarter
            , nr_subq_8.paid_at__extract_month AS paid_at__extract_month
            , nr_subq_8.paid_at__extract_day AS paid_at__extract_day
            , nr_subq_8.paid_at__extract_dow AS paid_at__extract_dow
            , nr_subq_8.paid_at__extract_doy AS paid_at__extract_doy
            , nr_subq_8.booking__ds__day AS booking__ds__day
            , nr_subq_8.booking__ds__week AS booking__ds__week
            , nr_subq_8.booking__ds__month AS booking__ds__month
            , nr_subq_8.booking__ds__quarter AS booking__ds__quarter
            , nr_subq_8.booking__ds__year AS booking__ds__year
            , nr_subq_8.booking__ds__extract_year AS booking__ds__extract_year
            , nr_subq_8.booking__ds__extract_quarter AS booking__ds__extract_quarter
            , nr_subq_8.booking__ds__extract_month AS booking__ds__extract_month
            , nr_subq_8.booking__ds__extract_day AS booking__ds__extract_day
            , nr_subq_8.booking__ds__extract_dow AS booking__ds__extract_dow
            , nr_subq_8.booking__ds__extract_doy AS booking__ds__extract_doy
            , nr_subq_8.booking__ds_partitioned__day AS booking__ds_partitioned__day
            , nr_subq_8.booking__ds_partitioned__week AS booking__ds_partitioned__week
            , nr_subq_8.booking__ds_partitioned__month AS booking__ds_partitioned__month
            , nr_subq_8.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
            , nr_subq_8.booking__ds_partitioned__year AS booking__ds_partitioned__year
            , nr_subq_8.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
            , nr_subq_8.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
            , nr_subq_8.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
            , nr_subq_8.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
            , nr_subq_8.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
            , nr_subq_8.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
            , nr_subq_8.booking__paid_at__day AS booking__paid_at__day
            , nr_subq_8.booking__paid_at__week AS booking__paid_at__week
            , nr_subq_8.booking__paid_at__month AS booking__paid_at__month
            , nr_subq_8.booking__paid_at__quarter AS booking__paid_at__quarter
            , nr_subq_8.booking__paid_at__year AS booking__paid_at__year
            , nr_subq_8.booking__paid_at__extract_year AS booking__paid_at__extract_year
            , nr_subq_8.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
            , nr_subq_8.booking__paid_at__extract_month AS booking__paid_at__extract_month
            , nr_subq_8.booking__paid_at__extract_day AS booking__paid_at__extract_day
            , nr_subq_8.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
            , nr_subq_8.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
            , nr_subq_8.metric_time__week AS metric_time__week
            , nr_subq_8.metric_time__month AS metric_time__month
            , nr_subq_8.metric_time__quarter AS metric_time__quarter
            , nr_subq_8.metric_time__extract_year AS metric_time__extract_year
            , nr_subq_8.metric_time__extract_quarter AS metric_time__extract_quarter
            , nr_subq_8.metric_time__extract_month AS metric_time__extract_month
            , nr_subq_8.metric_time__extract_day AS metric_time__extract_day
            , nr_subq_8.metric_time__extract_dow AS metric_time__extract_dow
            , nr_subq_8.metric_time__extract_doy AS metric_time__extract_doy
            , nr_subq_8.listing AS listing
            , nr_subq_8.guest AS guest
            , nr_subq_8.host AS host
            , nr_subq_8.booking__listing AS booking__listing
            , nr_subq_8.booking__guest AS booking__guest
            , nr_subq_8.booking__host AS booking__host
            , nr_subq_8.is_instant AS is_instant
            , nr_subq_8.booking__is_instant AS booking__is_instant
            , nr_subq_8.bookings AS bookings
            , nr_subq_8.instant_bookings AS instant_bookings
            , nr_subq_8.booking_value AS booking_value
            , nr_subq_8.max_booking_value AS max_booking_value
            , nr_subq_8.min_booking_value AS min_booking_value
            , nr_subq_8.bookers AS bookers
            , nr_subq_8.average_booking_value AS average_booking_value
            , nr_subq_8.referred_bookings AS referred_bookings
            , nr_subq_8.median_booking_value AS median_booking_value
            , nr_subq_8.booking_value_p99 AS booking_value_p99
            , nr_subq_8.discrete_booking_value_p99 AS discrete_booking_value_p99
            , nr_subq_8.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
            , nr_subq_8.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
          FROM (
            -- Pass Only Elements: ['metric_time__day', 'metric_time__year']
            SELECT
              nr_subq_10.metric_time__day
              , nr_subq_10.metric_time__year
            FROM (
              -- Change Column Aliases
              SELECT
                nr_subq_9.ds__day AS metric_time__day
                , nr_subq_9.ds__week
                , nr_subq_9.ds__month
                , nr_subq_9.ds__quarter
                , nr_subq_9.ds__year AS metric_time__year
                , nr_subq_9.ds__extract_year
                , nr_subq_9.ds__extract_quarter
                , nr_subq_9.ds__extract_month
                , nr_subq_9.ds__extract_day
                , nr_subq_9.ds__extract_dow
                , nr_subq_9.ds__extract_doy
                , nr_subq_9.ds__martian_day
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
                  , time_spine_src_28006.martian_day AS ds__martian_day
                FROM ***************************.mf_time_spine time_spine_src_28006
              ) nr_subq_9
            ) nr_subq_10
          ) nr_subq_11
          INNER JOIN (
            -- Metric Time Dimension 'ds'
            SELECT
              nr_subq_28002.ds__day
              , nr_subq_28002.ds__week
              , nr_subq_28002.ds__month
              , nr_subq_28002.ds__quarter
              , nr_subq_28002.ds__year
              , nr_subq_28002.ds__extract_year
              , nr_subq_28002.ds__extract_quarter
              , nr_subq_28002.ds__extract_month
              , nr_subq_28002.ds__extract_day
              , nr_subq_28002.ds__extract_dow
              , nr_subq_28002.ds__extract_doy
              , nr_subq_28002.ds_partitioned__day
              , nr_subq_28002.ds_partitioned__week
              , nr_subq_28002.ds_partitioned__month
              , nr_subq_28002.ds_partitioned__quarter
              , nr_subq_28002.ds_partitioned__year
              , nr_subq_28002.ds_partitioned__extract_year
              , nr_subq_28002.ds_partitioned__extract_quarter
              , nr_subq_28002.ds_partitioned__extract_month
              , nr_subq_28002.ds_partitioned__extract_day
              , nr_subq_28002.ds_partitioned__extract_dow
              , nr_subq_28002.ds_partitioned__extract_doy
              , nr_subq_28002.paid_at__day
              , nr_subq_28002.paid_at__week
              , nr_subq_28002.paid_at__month
              , nr_subq_28002.paid_at__quarter
              , nr_subq_28002.paid_at__year
              , nr_subq_28002.paid_at__extract_year
              , nr_subq_28002.paid_at__extract_quarter
              , nr_subq_28002.paid_at__extract_month
              , nr_subq_28002.paid_at__extract_day
              , nr_subq_28002.paid_at__extract_dow
              , nr_subq_28002.paid_at__extract_doy
              , nr_subq_28002.booking__ds__day
              , nr_subq_28002.booking__ds__week
              , nr_subq_28002.booking__ds__month
              , nr_subq_28002.booking__ds__quarter
              , nr_subq_28002.booking__ds__year
              , nr_subq_28002.booking__ds__extract_year
              , nr_subq_28002.booking__ds__extract_quarter
              , nr_subq_28002.booking__ds__extract_month
              , nr_subq_28002.booking__ds__extract_day
              , nr_subq_28002.booking__ds__extract_dow
              , nr_subq_28002.booking__ds__extract_doy
              , nr_subq_28002.booking__ds_partitioned__day
              , nr_subq_28002.booking__ds_partitioned__week
              , nr_subq_28002.booking__ds_partitioned__month
              , nr_subq_28002.booking__ds_partitioned__quarter
              , nr_subq_28002.booking__ds_partitioned__year
              , nr_subq_28002.booking__ds_partitioned__extract_year
              , nr_subq_28002.booking__ds_partitioned__extract_quarter
              , nr_subq_28002.booking__ds_partitioned__extract_month
              , nr_subq_28002.booking__ds_partitioned__extract_day
              , nr_subq_28002.booking__ds_partitioned__extract_dow
              , nr_subq_28002.booking__ds_partitioned__extract_doy
              , nr_subq_28002.booking__paid_at__day
              , nr_subq_28002.booking__paid_at__week
              , nr_subq_28002.booking__paid_at__month
              , nr_subq_28002.booking__paid_at__quarter
              , nr_subq_28002.booking__paid_at__year
              , nr_subq_28002.booking__paid_at__extract_year
              , nr_subq_28002.booking__paid_at__extract_quarter
              , nr_subq_28002.booking__paid_at__extract_month
              , nr_subq_28002.booking__paid_at__extract_day
              , nr_subq_28002.booking__paid_at__extract_dow
              , nr_subq_28002.booking__paid_at__extract_doy
              , nr_subq_28002.ds__day AS metric_time__day
              , nr_subq_28002.ds__week AS metric_time__week
              , nr_subq_28002.ds__month AS metric_time__month
              , nr_subq_28002.ds__quarter AS metric_time__quarter
              , nr_subq_28002.ds__year AS metric_time__year
              , nr_subq_28002.ds__extract_year AS metric_time__extract_year
              , nr_subq_28002.ds__extract_quarter AS metric_time__extract_quarter
              , nr_subq_28002.ds__extract_month AS metric_time__extract_month
              , nr_subq_28002.ds__extract_day AS metric_time__extract_day
              , nr_subq_28002.ds__extract_dow AS metric_time__extract_dow
              , nr_subq_28002.ds__extract_doy AS metric_time__extract_doy
              , nr_subq_28002.listing
              , nr_subq_28002.guest
              , nr_subq_28002.host
              , nr_subq_28002.booking__listing
              , nr_subq_28002.booking__guest
              , nr_subq_28002.booking__host
              , nr_subq_28002.is_instant
              , nr_subq_28002.booking__is_instant
              , nr_subq_28002.bookings
              , nr_subq_28002.instant_bookings
              , nr_subq_28002.booking_value
              , nr_subq_28002.max_booking_value
              , nr_subq_28002.min_booking_value
              , nr_subq_28002.bookers
              , nr_subq_28002.average_booking_value
              , nr_subq_28002.referred_bookings
              , nr_subq_28002.median_booking_value
              , nr_subq_28002.booking_value_p99
              , nr_subq_28002.discrete_booking_value_p99
              , nr_subq_28002.approximate_continuous_booking_value_p99
              , nr_subq_28002.approximate_discrete_booking_value_p99
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
            ) nr_subq_28002
          ) nr_subq_8
          ON
            nr_subq_11.metric_time__day - INTERVAL 1 month = nr_subq_8.metric_time__day
        ) nr_subq_12
      ) nr_subq_13
      GROUP BY
        nr_subq_13.metric_time__year
    ) nr_subq_14
  ) nr_subq_15
  ON
    nr_subq_7.metric_time__year = nr_subq_15.metric_time__year
  GROUP BY
    COALESCE(nr_subq_7.metric_time__year, nr_subq_15.metric_time__year)
) nr_subq_16
