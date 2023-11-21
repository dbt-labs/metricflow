-- Compute Metrics via Expressions
SELECT
  subq_15.metric_time__day
  , booking_fees - booking_fees_start_of_month AS booking_fees_since_start_of_month
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_8.metric_time__day, subq_14.metric_time__day) AS metric_time__day
    , MAX(subq_8.booking_fees_start_of_month) AS booking_fees_start_of_month
    , MAX(subq_14.booking_fees) AS booking_fees
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_6.metric_time__day AS metric_time__day
      , subq_5.booking_fees_start_of_month AS booking_fees_start_of_month
    FROM (
      -- Date Spine
      SELECT
        subq_7.ds AS metric_time__day
      FROM ***************************.mf_time_spine subq_7
    ) subq_6
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        subq_4.metric_time__day
        , booking_value * 0.05 AS booking_fees_start_of_month
      FROM (
        -- Compute Metrics via Expressions
        SELECT
          subq_3.metric_time__day
          , subq_3.booking_value
        FROM (
          -- Aggregate Measures
          SELECT
            subq_2.metric_time__day
            , SUM(subq_2.booking_value) AS booking_value
          FROM (
            -- Pass Only Elements:
            --   ['booking_value', 'metric_time__day']
            SELECT
              subq_1.metric_time__day
              , subq_1.booking_value
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
                  , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_10001.ds) AS ds__extract_dow
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
                  , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_dow
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
                  , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_10001.paid_at) AS paid_at__extract_dow
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
                  , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_10001.ds) AS booking__ds__extract_dow
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
                  , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
                  , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_dow
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
          GROUP BY
            subq_2.metric_time__day
        ) subq_3
      ) subq_4
    ) subq_5
    ON
      DATE_TRUNC('month', subq_6.metric_time__day) = subq_5.metric_time__day
  ) subq_8
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_13.metric_time__day
      , booking_value * 0.05 AS booking_fees
    FROM (
      -- Compute Metrics via Expressions
      SELECT
        subq_12.metric_time__day
        , subq_12.booking_value
      FROM (
        -- Aggregate Measures
        SELECT
          subq_11.metric_time__day
          , SUM(subq_11.booking_value) AS booking_value
        FROM (
          -- Pass Only Elements:
          --   ['booking_value', 'metric_time__day']
          SELECT
            subq_10.metric_time__day
            , subq_10.booking_value
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
              , subq_9.ds_partitioned__day
              , subq_9.ds_partitioned__week
              , subq_9.ds_partitioned__month
              , subq_9.ds_partitioned__quarter
              , subq_9.ds_partitioned__year
              , subq_9.ds_partitioned__extract_year
              , subq_9.ds_partitioned__extract_quarter
              , subq_9.ds_partitioned__extract_month
              , subq_9.ds_partitioned__extract_day
              , subq_9.ds_partitioned__extract_dow
              , subq_9.ds_partitioned__extract_doy
              , subq_9.paid_at__day
              , subq_9.paid_at__week
              , subq_9.paid_at__month
              , subq_9.paid_at__quarter
              , subq_9.paid_at__year
              , subq_9.paid_at__extract_year
              , subq_9.paid_at__extract_quarter
              , subq_9.paid_at__extract_month
              , subq_9.paid_at__extract_day
              , subq_9.paid_at__extract_dow
              , subq_9.paid_at__extract_doy
              , subq_9.booking__ds__day
              , subq_9.booking__ds__week
              , subq_9.booking__ds__month
              , subq_9.booking__ds__quarter
              , subq_9.booking__ds__year
              , subq_9.booking__ds__extract_year
              , subq_9.booking__ds__extract_quarter
              , subq_9.booking__ds__extract_month
              , subq_9.booking__ds__extract_day
              , subq_9.booking__ds__extract_dow
              , subq_9.booking__ds__extract_doy
              , subq_9.booking__ds_partitioned__day
              , subq_9.booking__ds_partitioned__week
              , subq_9.booking__ds_partitioned__month
              , subq_9.booking__ds_partitioned__quarter
              , subq_9.booking__ds_partitioned__year
              , subq_9.booking__ds_partitioned__extract_year
              , subq_9.booking__ds_partitioned__extract_quarter
              , subq_9.booking__ds_partitioned__extract_month
              , subq_9.booking__ds_partitioned__extract_day
              , subq_9.booking__ds_partitioned__extract_dow
              , subq_9.booking__ds_partitioned__extract_doy
              , subq_9.booking__paid_at__day
              , subq_9.booking__paid_at__week
              , subq_9.booking__paid_at__month
              , subq_9.booking__paid_at__quarter
              , subq_9.booking__paid_at__year
              , subq_9.booking__paid_at__extract_year
              , subq_9.booking__paid_at__extract_quarter
              , subq_9.booking__paid_at__extract_month
              , subq_9.booking__paid_at__extract_day
              , subq_9.booking__paid_at__extract_dow
              , subq_9.booking__paid_at__extract_doy
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
              , subq_9.guest
              , subq_9.host
              , subq_9.booking__listing
              , subq_9.booking__guest
              , subq_9.booking__host
              , subq_9.is_instant
              , subq_9.booking__is_instant
              , subq_9.bookings
              , subq_9.instant_bookings
              , subq_9.booking_value
              , subq_9.max_booking_value
              , subq_9.min_booking_value
              , subq_9.bookers
              , subq_9.average_booking_value
              , subq_9.referred_bookings
              , subq_9.median_booking_value
              , subq_9.booking_value_p99
              , subq_9.discrete_booking_value_p99
              , subq_9.approximate_continuous_booking_value_p99
              , subq_9.approximate_discrete_booking_value_p99
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
                , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_10001.ds) AS ds__extract_dow
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
                , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_10001.ds_partitioned) AS ds_partitioned__extract_dow
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
                , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_10001.paid_at) AS paid_at__extract_dow
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
                , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_10001.ds) AS booking__ds__extract_dow
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
                , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_10001.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
                , EXTRACT(DAYOFWEEK_ISO FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_dow
                , EXTRACT(doy FROM bookings_source_src_10001.paid_at) AS booking__paid_at__extract_doy
                , bookings_source_src_10001.listing_id AS listing
                , bookings_source_src_10001.guest_id AS guest
                , bookings_source_src_10001.host_id AS host
                , bookings_source_src_10001.listing_id AS booking__listing
                , bookings_source_src_10001.guest_id AS booking__guest
                , bookings_source_src_10001.host_id AS booking__host
              FROM ***************************.fct_bookings bookings_source_src_10001
            ) subq_9
          ) subq_10
        ) subq_11
        GROUP BY
          subq_11.metric_time__day
      ) subq_12
    ) subq_13
  ) subq_14
  ON
    subq_8.metric_time__day = subq_14.metric_time__day
  GROUP BY
    COALESCE(subq_8.metric_time__day, subq_14.metric_time__day)
) subq_15
