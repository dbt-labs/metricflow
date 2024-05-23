-- Compute Metrics via Expressions
SELECT
  subq_15.metric_time__day
  , CAST(subq_15.average_booking_value AS DOUBLE) / CAST(NULLIF(subq_15.max_booking_value, 0) AS DOUBLE) AS instant_booking_fraction_of_max_value
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_9.metric_time__day, subq_14.metric_time__day) AS metric_time__day
    , MAX(subq_9.average_booking_value) AS average_booking_value
    , MAX(subq_14.max_booking_value) AS max_booking_value
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_8.metric_time__day
      , subq_8.average_booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        subq_7.metric_time__day
        , AVG(subq_7.average_booking_value) AS average_booking_value
      FROM (
        -- Pass Only Elements: ['average_booking_value', 'metric_time__day']
        SELECT
          subq_6.metric_time__day
          , subq_6.average_booking_value
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_5.metric_time__day
            , subq_5.booking__is_instant
            , subq_5.average_booking_value
          FROM (
            -- Pass Only Elements: ['average_booking_value', 'booking__is_instant', 'metric_time__day']
            SELECT
              subq_4.metric_time__day
              , subq_4.booking__is_instant
              , subq_4.average_booking_value
            FROM (
              -- Constrain Output with WHERE
              SELECT
                subq_3.ds__day
                , subq_3.ds__week
                , subq_3.ds__month
                , subq_3.ds__quarter
                , subq_3.ds__year
                , subq_3.ds__extract_year
                , subq_3.ds__extract_quarter
                , subq_3.ds__extract_month
                , subq_3.ds__extract_day
                , subq_3.ds__extract_dow
                , subq_3.ds__extract_doy
                , subq_3.ds_partitioned__day
                , subq_3.ds_partitioned__week
                , subq_3.ds_partitioned__month
                , subq_3.ds_partitioned__quarter
                , subq_3.ds_partitioned__year
                , subq_3.ds_partitioned__extract_year
                , subq_3.ds_partitioned__extract_quarter
                , subq_3.ds_partitioned__extract_month
                , subq_3.ds_partitioned__extract_day
                , subq_3.ds_partitioned__extract_dow
                , subq_3.ds_partitioned__extract_doy
                , subq_3.paid_at__day
                , subq_3.paid_at__week
                , subq_3.paid_at__month
                , subq_3.paid_at__quarter
                , subq_3.paid_at__year
                , subq_3.paid_at__extract_year
                , subq_3.paid_at__extract_quarter
                , subq_3.paid_at__extract_month
                , subq_3.paid_at__extract_day
                , subq_3.paid_at__extract_dow
                , subq_3.paid_at__extract_doy
                , subq_3.booking__ds__day
                , subq_3.booking__ds__week
                , subq_3.booking__ds__month
                , subq_3.booking__ds__quarter
                , subq_3.booking__ds__year
                , subq_3.booking__ds__extract_year
                , subq_3.booking__ds__extract_quarter
                , subq_3.booking__ds__extract_month
                , subq_3.booking__ds__extract_day
                , subq_3.booking__ds__extract_dow
                , subq_3.booking__ds__extract_doy
                , subq_3.booking__ds_partitioned__day
                , subq_3.booking__ds_partitioned__week
                , subq_3.booking__ds_partitioned__month
                , subq_3.booking__ds_partitioned__quarter
                , subq_3.booking__ds_partitioned__year
                , subq_3.booking__ds_partitioned__extract_year
                , subq_3.booking__ds_partitioned__extract_quarter
                , subq_3.booking__ds_partitioned__extract_month
                , subq_3.booking__ds_partitioned__extract_day
                , subq_3.booking__ds_partitioned__extract_dow
                , subq_3.booking__ds_partitioned__extract_doy
                , subq_3.booking__paid_at__day
                , subq_3.booking__paid_at__week
                , subq_3.booking__paid_at__month
                , subq_3.booking__paid_at__quarter
                , subq_3.booking__paid_at__year
                , subq_3.booking__paid_at__extract_year
                , subq_3.booking__paid_at__extract_quarter
                , subq_3.booking__paid_at__extract_month
                , subq_3.booking__paid_at__extract_day
                , subq_3.booking__paid_at__extract_dow
                , subq_3.booking__paid_at__extract_doy
                , subq_3.metric_time__day
                , subq_3.metric_time__week
                , subq_3.metric_time__month
                , subq_3.metric_time__quarter
                , subq_3.metric_time__year
                , subq_3.metric_time__extract_year
                , subq_3.metric_time__extract_quarter
                , subq_3.metric_time__extract_month
                , subq_3.metric_time__extract_day
                , subq_3.metric_time__extract_dow
                , subq_3.metric_time__extract_doy
                , subq_3.listing
                , subq_3.guest
                , subq_3.host
                , subq_3.booking__listing
                , subq_3.booking__guest
                , subq_3.booking__host
                , subq_3.is_instant
                , subq_3.booking__is_instant
                , subq_3.bookings
                , subq_3.instant_bookings
                , subq_3.booking_value
                , subq_3.max_booking_value
                , subq_3.min_booking_value
                , subq_3.bookers
                , subq_3.average_booking_value
                , subq_3.referred_bookings
                , subq_3.median_booking_value
                , subq_3.booking_value_p99
                , subq_3.discrete_booking_value_p99
                , subq_3.approximate_continuous_booking_value_p99
                , subq_3.approximate_discrete_booking_value_p99
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
                  , subq_2.ds_partitioned__day
                  , subq_2.ds_partitioned__week
                  , subq_2.ds_partitioned__month
                  , subq_2.ds_partitioned__quarter
                  , subq_2.ds_partitioned__year
                  , subq_2.ds_partitioned__extract_year
                  , subq_2.ds_partitioned__extract_quarter
                  , subq_2.ds_partitioned__extract_month
                  , subq_2.ds_partitioned__extract_day
                  , subq_2.ds_partitioned__extract_dow
                  , subq_2.ds_partitioned__extract_doy
                  , subq_2.paid_at__day
                  , subq_2.paid_at__week
                  , subq_2.paid_at__month
                  , subq_2.paid_at__quarter
                  , subq_2.paid_at__year
                  , subq_2.paid_at__extract_year
                  , subq_2.paid_at__extract_quarter
                  , subq_2.paid_at__extract_month
                  , subq_2.paid_at__extract_day
                  , subq_2.paid_at__extract_dow
                  , subq_2.paid_at__extract_doy
                  , subq_2.booking__ds__day
                  , subq_2.booking__ds__week
                  , subq_2.booking__ds__month
                  , subq_2.booking__ds__quarter
                  , subq_2.booking__ds__year
                  , subq_2.booking__ds__extract_year
                  , subq_2.booking__ds__extract_quarter
                  , subq_2.booking__ds__extract_month
                  , subq_2.booking__ds__extract_day
                  , subq_2.booking__ds__extract_dow
                  , subq_2.booking__ds__extract_doy
                  , subq_2.booking__ds_partitioned__day
                  , subq_2.booking__ds_partitioned__week
                  , subq_2.booking__ds_partitioned__month
                  , subq_2.booking__ds_partitioned__quarter
                  , subq_2.booking__ds_partitioned__year
                  , subq_2.booking__ds_partitioned__extract_year
                  , subq_2.booking__ds_partitioned__extract_quarter
                  , subq_2.booking__ds_partitioned__extract_month
                  , subq_2.booking__ds_partitioned__extract_day
                  , subq_2.booking__ds_partitioned__extract_dow
                  , subq_2.booking__ds_partitioned__extract_doy
                  , subq_2.booking__paid_at__day
                  , subq_2.booking__paid_at__week
                  , subq_2.booking__paid_at__month
                  , subq_2.booking__paid_at__quarter
                  , subq_2.booking__paid_at__year
                  , subq_2.booking__paid_at__extract_year
                  , subq_2.booking__paid_at__extract_quarter
                  , subq_2.booking__paid_at__extract_month
                  , subq_2.booking__paid_at__extract_day
                  , subq_2.booking__paid_at__extract_dow
                  , subq_2.booking__paid_at__extract_doy
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
                  , subq_2.guest
                  , subq_2.host
                  , subq_2.booking__listing
                  , subq_2.booking__guest
                  , subq_2.booking__host
                  , subq_2.is_instant
                  , subq_2.booking__is_instant
                  , subq_2.bookings
                  , subq_2.instant_bookings
                  , subq_2.booking_value
                  , subq_2.max_booking_value
                  , subq_2.min_booking_value
                  , subq_2.bookers
                  , subq_2.average_booking_value
                  , subq_2.referred_bookings
                  , subq_2.median_booking_value
                  , subq_2.booking_value_p99
                  , subq_2.discrete_booking_value_p99
                  , subq_2.approximate_continuous_booking_value_p99
                  , subq_2.approximate_discrete_booking_value_p99
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
                ) subq_2
              ) subq_3
              WHERE booking__is_instant
            ) subq_4
          ) subq_5
          WHERE booking__is_instant
        ) subq_6
      ) subq_7
      GROUP BY
        subq_7.metric_time__day
    ) subq_8
  ) subq_9
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_13.metric_time__day
      , subq_13.max_booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        subq_12.metric_time__day
        , MAX(subq_12.max_booking_value) AS max_booking_value
      FROM (
        -- Pass Only Elements: ['max_booking_value', 'metric_time__day']
        SELECT
          subq_11.metric_time__day
          , subq_11.max_booking_value
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_10.ds__day
            , subq_10.ds__week
            , subq_10.ds__month
            , subq_10.ds__quarter
            , subq_10.ds__year
            , subq_10.ds__extract_year
            , subq_10.ds__extract_quarter
            , subq_10.ds__extract_month
            , subq_10.ds__extract_day
            , subq_10.ds__extract_dow
            , subq_10.ds__extract_doy
            , subq_10.ds_partitioned__day
            , subq_10.ds_partitioned__week
            , subq_10.ds_partitioned__month
            , subq_10.ds_partitioned__quarter
            , subq_10.ds_partitioned__year
            , subq_10.ds_partitioned__extract_year
            , subq_10.ds_partitioned__extract_quarter
            , subq_10.ds_partitioned__extract_month
            , subq_10.ds_partitioned__extract_day
            , subq_10.ds_partitioned__extract_dow
            , subq_10.ds_partitioned__extract_doy
            , subq_10.paid_at__day
            , subq_10.paid_at__week
            , subq_10.paid_at__month
            , subq_10.paid_at__quarter
            , subq_10.paid_at__year
            , subq_10.paid_at__extract_year
            , subq_10.paid_at__extract_quarter
            , subq_10.paid_at__extract_month
            , subq_10.paid_at__extract_day
            , subq_10.paid_at__extract_dow
            , subq_10.paid_at__extract_doy
            , subq_10.booking__ds__day
            , subq_10.booking__ds__week
            , subq_10.booking__ds__month
            , subq_10.booking__ds__quarter
            , subq_10.booking__ds__year
            , subq_10.booking__ds__extract_year
            , subq_10.booking__ds__extract_quarter
            , subq_10.booking__ds__extract_month
            , subq_10.booking__ds__extract_day
            , subq_10.booking__ds__extract_dow
            , subq_10.booking__ds__extract_doy
            , subq_10.booking__ds_partitioned__day
            , subq_10.booking__ds_partitioned__week
            , subq_10.booking__ds_partitioned__month
            , subq_10.booking__ds_partitioned__quarter
            , subq_10.booking__ds_partitioned__year
            , subq_10.booking__ds_partitioned__extract_year
            , subq_10.booking__ds_partitioned__extract_quarter
            , subq_10.booking__ds_partitioned__extract_month
            , subq_10.booking__ds_partitioned__extract_day
            , subq_10.booking__ds_partitioned__extract_dow
            , subq_10.booking__ds_partitioned__extract_doy
            , subq_10.booking__paid_at__day
            , subq_10.booking__paid_at__week
            , subq_10.booking__paid_at__month
            , subq_10.booking__paid_at__quarter
            , subq_10.booking__paid_at__year
            , subq_10.booking__paid_at__extract_year
            , subq_10.booking__paid_at__extract_quarter
            , subq_10.booking__paid_at__extract_month
            , subq_10.booking__paid_at__extract_day
            , subq_10.booking__paid_at__extract_dow
            , subq_10.booking__paid_at__extract_doy
            , subq_10.ds__day AS metric_time__day
            , subq_10.ds__week AS metric_time__week
            , subq_10.ds__month AS metric_time__month
            , subq_10.ds__quarter AS metric_time__quarter
            , subq_10.ds__year AS metric_time__year
            , subq_10.ds__extract_year AS metric_time__extract_year
            , subq_10.ds__extract_quarter AS metric_time__extract_quarter
            , subq_10.ds__extract_month AS metric_time__extract_month
            , subq_10.ds__extract_day AS metric_time__extract_day
            , subq_10.ds__extract_dow AS metric_time__extract_dow
            , subq_10.ds__extract_doy AS metric_time__extract_doy
            , subq_10.listing
            , subq_10.guest
            , subq_10.host
            , subq_10.booking__listing
            , subq_10.booking__guest
            , subq_10.booking__host
            , subq_10.is_instant
            , subq_10.booking__is_instant
            , subq_10.bookings
            , subq_10.instant_bookings
            , subq_10.booking_value
            , subq_10.max_booking_value
            , subq_10.min_booking_value
            , subq_10.bookers
            , subq_10.average_booking_value
            , subq_10.referred_bookings
            , subq_10.median_booking_value
            , subq_10.booking_value_p99
            , subq_10.discrete_booking_value_p99
            , subq_10.approximate_continuous_booking_value_p99
            , subq_10.approximate_discrete_booking_value_p99
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
          ) subq_10
        ) subq_11
      ) subq_12
      GROUP BY
        subq_12.metric_time__day
    ) subq_13
  ) subq_14
  ON
    subq_9.metric_time__day = subq_14.metric_time__day
  GROUP BY
    COALESCE(subq_9.metric_time__day, subq_14.metric_time__day)
) subq_15
