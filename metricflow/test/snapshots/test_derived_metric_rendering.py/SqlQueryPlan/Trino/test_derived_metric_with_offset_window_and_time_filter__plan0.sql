-- Compute Metrics via Expressions
SELECT
  subq_15.metric_time__day
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_5.metric_time__day, subq_14.metric_time__day) AS metric_time__day
    , MAX(subq_5.bookings) AS bookings
    , MAX(subq_14.bookings_2_weeks_ago) AS bookings_2_weeks_ago
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
        -- Constrain Output with WHERE
        SELECT
          subq_2.metric_time__day
          , subq_2.bookings
        FROM (
          -- Pass Only Elements:
          --   ['bookings', 'metric_time__day']
          SELECT
            subq_1.metric_time__day
            , subq_1.bookings
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
                , bookings_source_src_10001.booking_value
                , bookings_source_src_10001.booking_value AS max_booking_value
                , bookings_source_src_10001.booking_value AS min_booking_value
                , bookings_source_src_10001.guest_id AS bookers
                , bookings_source_src_10001.booking_value AS average_booking_value
                , bookings_source_src_10001.booking_value AS booking_payments
                , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
                , bookings_source_src_10001.booking_value AS median_booking_value
                , bookings_source_src_10001.booking_value AS booking_value_p99
                , bookings_source_src_10001.booking_value AS discrete_booking_value_p99
                , bookings_source_src_10001.booking_value AS approximate_continuous_booking_value_p99
                , bookings_source_src_10001.booking_value AS approximate_discrete_booking_value_p99
                , bookings_source_src_10001.is_instant
                , DATE_TRUNC('day', bookings_source_src_10001.ds) AS ds__day
                , DATE_TRUNC('week', bookings_source_src_10001.ds) AS ds__week
                , DATE_TRUNC('month', bookings_source_src_10001.ds) AS ds__month
                , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS ds__quarter
                , DATE_TRUNC('year', bookings_source_src_10001.ds) AS ds__year
                , EXTRACT(year FROM bookings_source_src_10001.ds) AS ds__extract_year
                , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS ds__extract_quarter
                , EXTRACT(month FROM bookings_source_src_10001.ds) AS ds__extract_month
                , EXTRACT(day FROM bookings_source_src_10001.ds) AS ds__extract_day
                , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.ds) AS ds__extract_dow
                , EXTRACT(doy FROM bookings_source_src_10001.ds) AS ds__extract_doy
                , DATE_TRUNC('day', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__day
                , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__week
                , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__month
                , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__quarter
                , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__year
                , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_year
                , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_quarter
                , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_month
                , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_day
                , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_dow
                , EXTRACT(doy FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_doy
                , DATE_TRUNC('day', bookings_source_src_10001.paid_at) AS paid_at__day
                , DATE_TRUNC('week', bookings_source_src_10001.paid_at) AS paid_at__week
                , DATE_TRUNC('month', bookings_source_src_10001.paid_at) AS paid_at__month
                , DATE_TRUNC('quarter', bookings_source_src_10001.paid_at) AS paid_at__quarter
                , DATE_TRUNC('year', bookings_source_src_10001.paid_at) AS paid_at__year
                , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS paid_at__extract_year
                , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS paid_at__extract_quarter
                , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS paid_at__extract_month
                , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS paid_at__extract_day
                , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.paid_at) AS paid_at__extract_dow
                , EXTRACT(doy FROM bookings_source_src_10001.paid_at) AS paid_at__extract_doy
                , bookings_source_src_10001.is_instant AS booking__is_instant
                , DATE_TRUNC('day', bookings_source_src_10001.ds) AS booking__ds__day
                , DATE_TRUNC('week', bookings_source_src_10001.ds) AS booking__ds__week
                , DATE_TRUNC('month', bookings_source_src_10001.ds) AS booking__ds__month
                , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS booking__ds__quarter
                , DATE_TRUNC('year', bookings_source_src_10001.ds) AS booking__ds__year
                , EXTRACT(year FROM bookings_source_src_10001.ds) AS booking__ds__extract_year
                , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS booking__ds__extract_quarter
                , EXTRACT(month FROM bookings_source_src_10001.ds) AS booking__ds__extract_month
                , EXTRACT(day FROM bookings_source_src_10001.ds) AS booking__ds__extract_day
                , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.ds) AS booking__ds__extract_dow
                , EXTRACT(doy FROM bookings_source_src_10001.ds) AS booking__ds__extract_doy
                , DATE_TRUNC('day', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__day
                , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__week
                , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__month
                , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__quarter
                , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__year
                , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_year
                , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_month
                , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_day
                , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_dow
                , EXTRACT(doy FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_doy
                , DATE_TRUNC('day', bookings_source_src_10001.paid_at) AS booking__paid_at__day
                , DATE_TRUNC('week', bookings_source_src_10001.paid_at) AS booking__paid_at__week
                , DATE_TRUNC('month', bookings_source_src_10001.paid_at) AS booking__paid_at__month
                , DATE_TRUNC('quarter', bookings_source_src_10001.paid_at) AS booking__paid_at__quarter
                , DATE_TRUNC('year', bookings_source_src_10001.paid_at) AS booking__paid_at__year
                , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_year
                , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_quarter
                , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_month
                , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_day
                , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_dow
                , EXTRACT(doy FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_doy
                , bookings_source_src_10001.listing_id AS listing
                , bookings_source_src_10001.guest_id AS guest
                , bookings_source_src_10001.host_id AS host
                , bookings_source_src_10001.listing_id AS booking__listing
                , bookings_source_src_10001.guest_id AS booking__guest
                , bookings_source_src_10001.host_id AS booking__host
              FROM ***************************.fct_bookings bookings_source_src_10001
            ) subq_0
          ) subq_1
        ) subq_2
        WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
      ) subq_3
      GROUP BY
        subq_3.metric_time__day
    ) subq_4
  ) subq_5
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_13.metric_time__day
      , subq_13.bookings AS bookings_2_weeks_ago
    FROM (
      -- Aggregate Measures
      SELECT
        subq_12.metric_time__day
        , SUM(subq_12.bookings) AS bookings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_11.metric_time__day
          , subq_11.bookings
        FROM (
          -- Pass Only Elements:
          --   ['bookings', 'metric_time__day']
          SELECT
            subq_10.metric_time__day
            , subq_10.bookings
          FROM (
            -- Join to Time Spine Dataset
            SELECT
              subq_8.metric_time__day AS metric_time__day
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
              -- Date Spine
              SELECT
                subq_9.ds AS metric_time__day
              FROM ***************************.mf_time_spine subq_9
            ) subq_8
            INNER JOIN (
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
                  , bookings_source_src_10001.booking_value
                  , bookings_source_src_10001.booking_value AS max_booking_value
                  , bookings_source_src_10001.booking_value AS min_booking_value
                  , bookings_source_src_10001.guest_id AS bookers
                  , bookings_source_src_10001.booking_value AS average_booking_value
                  , bookings_source_src_10001.booking_value AS booking_payments
                  , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
                  , bookings_source_src_10001.booking_value AS median_booking_value
                  , bookings_source_src_10001.booking_value AS booking_value_p99
                  , bookings_source_src_10001.booking_value AS discrete_booking_value_p99
                  , bookings_source_src_10001.booking_value AS approximate_continuous_booking_value_p99
                  , bookings_source_src_10001.booking_value AS approximate_discrete_booking_value_p99
                  , bookings_source_src_10001.is_instant
                  , DATE_TRUNC('day', bookings_source_src_10001.ds) AS ds__day
                  , DATE_TRUNC('week', bookings_source_src_10001.ds) AS ds__week
                  , DATE_TRUNC('month', bookings_source_src_10001.ds) AS ds__month
                  , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS ds__quarter
                  , DATE_TRUNC('year', bookings_source_src_10001.ds) AS ds__year
                  , EXTRACT(year FROM bookings_source_src_10001.ds) AS ds__extract_year
                  , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS ds__extract_quarter
                  , EXTRACT(month FROM bookings_source_src_10001.ds) AS ds__extract_month
                  , EXTRACT(day FROM bookings_source_src_10001.ds) AS ds__extract_day
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.ds) AS ds__extract_dow
                  , EXTRACT(doy FROM bookings_source_src_10001.ds) AS ds__extract_doy
                  , DATE_TRUNC('day', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__day
                  , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__week
                  , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__month
                  , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__quarter
                  , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__year
                  , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_year
                  , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_quarter
                  , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_month
                  , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_day
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_dow
                  , EXTRACT(doy FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_doy
                  , DATE_TRUNC('day', bookings_source_src_10001.paid_at) AS paid_at__day
                  , DATE_TRUNC('week', bookings_source_src_10001.paid_at) AS paid_at__week
                  , DATE_TRUNC('month', bookings_source_src_10001.paid_at) AS paid_at__month
                  , DATE_TRUNC('quarter', bookings_source_src_10001.paid_at) AS paid_at__quarter
                  , DATE_TRUNC('year', bookings_source_src_10001.paid_at) AS paid_at__year
                  , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS paid_at__extract_year
                  , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS paid_at__extract_quarter
                  , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS paid_at__extract_month
                  , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS paid_at__extract_day
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.paid_at) AS paid_at__extract_dow
                  , EXTRACT(doy FROM bookings_source_src_10001.paid_at) AS paid_at__extract_doy
                  , bookings_source_src_10001.is_instant AS booking__is_instant
                  , DATE_TRUNC('day', bookings_source_src_10001.ds) AS booking__ds__day
                  , DATE_TRUNC('week', bookings_source_src_10001.ds) AS booking__ds__week
                  , DATE_TRUNC('month', bookings_source_src_10001.ds) AS booking__ds__month
                  , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS booking__ds__quarter
                  , DATE_TRUNC('year', bookings_source_src_10001.ds) AS booking__ds__year
                  , EXTRACT(year FROM bookings_source_src_10001.ds) AS booking__ds__extract_year
                  , EXTRACT(quarter FROM bookings_source_src_10001.ds) AS booking__ds__extract_quarter
                  , EXTRACT(month FROM bookings_source_src_10001.ds) AS booking__ds__extract_month
                  , EXTRACT(day FROM bookings_source_src_10001.ds) AS booking__ds__extract_day
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.ds) AS booking__ds__extract_dow
                  , EXTRACT(doy FROM bookings_source_src_10001.ds) AS booking__ds__extract_doy
                  , DATE_TRUNC('day', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__day
                  , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__week
                  , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__month
                  , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__quarter
                  , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__year
                  , EXTRACT(year FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_year
                  , EXTRACT(quarter FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                  , EXTRACT(month FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_month
                  , EXTRACT(day FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_day
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_dow
                  , EXTRACT(doy FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_doy
                  , DATE_TRUNC('day', bookings_source_src_10001.paid_at) AS booking__paid_at__day
                  , DATE_TRUNC('week', bookings_source_src_10001.paid_at) AS booking__paid_at__week
                  , DATE_TRUNC('month', bookings_source_src_10001.paid_at) AS booking__paid_at__month
                  , DATE_TRUNC('quarter', bookings_source_src_10001.paid_at) AS booking__paid_at__quarter
                  , DATE_TRUNC('year', bookings_source_src_10001.paid_at) AS booking__paid_at__year
                  , EXTRACT(year FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_year
                  , EXTRACT(quarter FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_quarter
                  , EXTRACT(month FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_month
                  , EXTRACT(day FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_day
                  , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_dow
                  , EXTRACT(doy FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_doy
                  , bookings_source_src_10001.listing_id AS listing
                  , bookings_source_src_10001.guest_id AS guest
                  , bookings_source_src_10001.host_id AS host
                  , bookings_source_src_10001.listing_id AS booking__listing
                  , bookings_source_src_10001.guest_id AS booking__guest
                  , bookings_source_src_10001.host_id AS booking__host
                FROM ***************************.fct_bookings bookings_source_src_10001
              ) subq_6
            ) subq_7
            ON
              DATE_ADD('day', -14, subq_8.metric_time__day) = subq_7.metric_time__day
          ) subq_10
        ) subq_11
        WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
      ) subq_12
      GROUP BY
        subq_12.metric_time__day
    ) subq_13
  ) subq_14
  ON
    subq_5.metric_time__day = subq_14.metric_time__day
  GROUP BY
    COALESCE(subq_5.metric_time__day, subq_14.metric_time__day)
) subq_15
