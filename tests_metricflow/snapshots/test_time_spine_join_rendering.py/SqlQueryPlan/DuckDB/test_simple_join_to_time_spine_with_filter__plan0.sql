-- Compute Metrics via Expressions
SELECT
  subq_15.metric_time__day
  , COALESCE(subq_15.bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_13.metric_time__day AS metric_time__day
    , subq_12.bookings AS bookings
  FROM (
    -- Time Spine
    SELECT
      subq_14.ds AS metric_time__day
    FROM ***************************.mf_time_spine subq_14
  ) subq_13
  LEFT OUTER JOIN (
    -- Aggregate Measures
    SELECT
      subq_11.metric_time__day
      , SUM(subq_11.bookings) AS bookings
    FROM (
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      SELECT
        subq_10.metric_time__day
        , subq_10.bookings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_9.metric_time__day
          , subq_9.booking__is_instant
          , subq_9.bookings
        FROM (
          -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day']
          SELECT
            subq_8.metric_time__day
            , subq_8.booking__is_instant
            , subq_8.bookings
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_7.ds__day
              , subq_7.ds__week
              , subq_7.ds__month
              , subq_7.ds__quarter
              , subq_7.ds__year
              , subq_7.ds__extract_year
              , subq_7.ds__extract_quarter
              , subq_7.ds__extract_month
              , subq_7.ds__extract_day
              , subq_7.ds__extract_dow
              , subq_7.ds__extract_doy
              , subq_7.ds_partitioned__day
              , subq_7.ds_partitioned__week
              , subq_7.ds_partitioned__month
              , subq_7.ds_partitioned__quarter
              , subq_7.ds_partitioned__year
              , subq_7.ds_partitioned__extract_year
              , subq_7.ds_partitioned__extract_quarter
              , subq_7.ds_partitioned__extract_month
              , subq_7.ds_partitioned__extract_day
              , subq_7.ds_partitioned__extract_dow
              , subq_7.ds_partitioned__extract_doy
              , subq_7.paid_at__day
              , subq_7.paid_at__week
              , subq_7.paid_at__month
              , subq_7.paid_at__quarter
              , subq_7.paid_at__year
              , subq_7.paid_at__extract_year
              , subq_7.paid_at__extract_quarter
              , subq_7.paid_at__extract_month
              , subq_7.paid_at__extract_day
              , subq_7.paid_at__extract_dow
              , subq_7.paid_at__extract_doy
              , subq_7.booking__ds__day
              , subq_7.booking__ds__week
              , subq_7.booking__ds__month
              , subq_7.booking__ds__quarter
              , subq_7.booking__ds__year
              , subq_7.booking__ds__extract_year
              , subq_7.booking__ds__extract_quarter
              , subq_7.booking__ds__extract_month
              , subq_7.booking__ds__extract_day
              , subq_7.booking__ds__extract_dow
              , subq_7.booking__ds__extract_doy
              , subq_7.booking__ds_partitioned__day
              , subq_7.booking__ds_partitioned__week
              , subq_7.booking__ds_partitioned__month
              , subq_7.booking__ds_partitioned__quarter
              , subq_7.booking__ds_partitioned__year
              , subq_7.booking__ds_partitioned__extract_year
              , subq_7.booking__ds_partitioned__extract_quarter
              , subq_7.booking__ds_partitioned__extract_month
              , subq_7.booking__ds_partitioned__extract_day
              , subq_7.booking__ds_partitioned__extract_dow
              , subq_7.booking__ds_partitioned__extract_doy
              , subq_7.booking__paid_at__day
              , subq_7.booking__paid_at__week
              , subq_7.booking__paid_at__month
              , subq_7.booking__paid_at__quarter
              , subq_7.booking__paid_at__year
              , subq_7.booking__paid_at__extract_year
              , subq_7.booking__paid_at__extract_quarter
              , subq_7.booking__paid_at__extract_month
              , subq_7.booking__paid_at__extract_day
              , subq_7.booking__paid_at__extract_dow
              , subq_7.booking__paid_at__extract_doy
              , subq_7.metric_time__day
              , subq_7.metric_time__week
              , subq_7.metric_time__month
              , subq_7.metric_time__quarter
              , subq_7.metric_time__year
              , subq_7.metric_time__extract_year
              , subq_7.metric_time__extract_quarter
              , subq_7.metric_time__extract_month
              , subq_7.metric_time__extract_day
              , subq_7.metric_time__extract_dow
              , subq_7.metric_time__extract_doy
              , subq_7.listing
              , subq_7.guest
              , subq_7.host
              , subq_7.booking__listing
              , subq_7.booking__guest
              , subq_7.booking__host
              , subq_7.is_instant
              , subq_7.booking__is_instant
              , subq_7.bookings
              , subq_7.instant_bookings
              , subq_7.booking_value
              , subq_7.max_booking_value
              , subq_7.min_booking_value
              , subq_7.bookers
              , subq_7.average_booking_value
              , subq_7.referred_bookings
              , subq_7.median_booking_value
              , subq_7.booking_value_p99
              , subq_7.discrete_booking_value_p99
              , subq_7.approximate_continuous_booking_value_p99
              , subq_7.approximate_discrete_booking_value_p99
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
            WHERE booking__is_instant
          ) subq_8
        ) subq_9
        WHERE booking__is_instant
      ) subq_10
    ) subq_11
    GROUP BY
      subq_11.metric_time__day
  ) subq_12
  ON
    subq_13.metric_time__day = subq_12.metric_time__day
) subq_15
