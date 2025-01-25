test_name: test_join_to_time_spine_with_filter_not_in_group_by
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Postgres
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_8.metric_time__day
  , nr_subq_8.bookings AS bookings_join_to_time_spine_with_tiered_filters
FROM (
  -- Join to Time Spine Dataset
  SELECT
    nr_subq_7.metric_time__day AS metric_time__day
    , nr_subq_3.bookings AS bookings
  FROM (
    -- Pass Only Elements: ['metric_time__day',]
    SELECT
      nr_subq_6.metric_time__day
    FROM (
      -- Constrain Output with WHERE
      SELECT
        nr_subq_5.metric_time__day
        , nr_subq_5.ds__week
        , nr_subq_5.metric_time__month
        , nr_subq_5.ds__quarter
        , nr_subq_5.ds__year
        , nr_subq_5.ds__extract_year
        , nr_subq_5.ds__extract_quarter
        , nr_subq_5.ds__extract_month
        , nr_subq_5.ds__extract_day
        , nr_subq_5.ds__extract_dow
        , nr_subq_5.ds__extract_doy
        , nr_subq_5.ds__martian_day
      FROM (
        -- Change Column Aliases
        SELECT
          nr_subq_4.ds__day AS metric_time__day
          , nr_subq_4.ds__week
          , nr_subq_4.ds__month AS metric_time__month
          , nr_subq_4.ds__quarter
          , nr_subq_4.ds__year
          , nr_subq_4.ds__extract_year
          , nr_subq_4.ds__extract_quarter
          , nr_subq_4.ds__extract_month
          , nr_subq_4.ds__extract_day
          , nr_subq_4.ds__extract_dow
          , nr_subq_4.ds__extract_doy
          , nr_subq_4.ds__martian_day
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
        ) nr_subq_4
      ) nr_subq_5
      WHERE (metric_time__day <= '2020-01-02') AND (metric_time__month > '2020-01-01')
    ) nr_subq_6
  ) nr_subq_7
  LEFT OUTER JOIN (
    -- Aggregate Measures
    SELECT
      nr_subq_2.metric_time__day
      , SUM(nr_subq_2.bookings) AS bookings
    FROM (
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      SELECT
        nr_subq_1.metric_time__day
        , nr_subq_1.bookings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          nr_subq_0.ds__day
          , nr_subq_0.ds__week
          , nr_subq_0.ds__month
          , nr_subq_0.ds__quarter
          , nr_subq_0.ds__year
          , nr_subq_0.ds__extract_year
          , nr_subq_0.ds__extract_quarter
          , nr_subq_0.ds__extract_month
          , nr_subq_0.ds__extract_day
          , nr_subq_0.ds__extract_dow
          , nr_subq_0.ds__extract_doy
          , nr_subq_0.ds_partitioned__day
          , nr_subq_0.ds_partitioned__week
          , nr_subq_0.ds_partitioned__month
          , nr_subq_0.ds_partitioned__quarter
          , nr_subq_0.ds_partitioned__year
          , nr_subq_0.ds_partitioned__extract_year
          , nr_subq_0.ds_partitioned__extract_quarter
          , nr_subq_0.ds_partitioned__extract_month
          , nr_subq_0.ds_partitioned__extract_day
          , nr_subq_0.ds_partitioned__extract_dow
          , nr_subq_0.ds_partitioned__extract_doy
          , nr_subq_0.paid_at__day
          , nr_subq_0.paid_at__week
          , nr_subq_0.paid_at__month
          , nr_subq_0.paid_at__quarter
          , nr_subq_0.paid_at__year
          , nr_subq_0.paid_at__extract_year
          , nr_subq_0.paid_at__extract_quarter
          , nr_subq_0.paid_at__extract_month
          , nr_subq_0.paid_at__extract_day
          , nr_subq_0.paid_at__extract_dow
          , nr_subq_0.paid_at__extract_doy
          , nr_subq_0.booking__ds__day
          , nr_subq_0.booking__ds__week
          , nr_subq_0.booking__ds__month
          , nr_subq_0.booking__ds__quarter
          , nr_subq_0.booking__ds__year
          , nr_subq_0.booking__ds__extract_year
          , nr_subq_0.booking__ds__extract_quarter
          , nr_subq_0.booking__ds__extract_month
          , nr_subq_0.booking__ds__extract_day
          , nr_subq_0.booking__ds__extract_dow
          , nr_subq_0.booking__ds__extract_doy
          , nr_subq_0.booking__ds_partitioned__day
          , nr_subq_0.booking__ds_partitioned__week
          , nr_subq_0.booking__ds_partitioned__month
          , nr_subq_0.booking__ds_partitioned__quarter
          , nr_subq_0.booking__ds_partitioned__year
          , nr_subq_0.booking__ds_partitioned__extract_year
          , nr_subq_0.booking__ds_partitioned__extract_quarter
          , nr_subq_0.booking__ds_partitioned__extract_month
          , nr_subq_0.booking__ds_partitioned__extract_day
          , nr_subq_0.booking__ds_partitioned__extract_dow
          , nr_subq_0.booking__ds_partitioned__extract_doy
          , nr_subq_0.booking__paid_at__day
          , nr_subq_0.booking__paid_at__week
          , nr_subq_0.booking__paid_at__month
          , nr_subq_0.booking__paid_at__quarter
          , nr_subq_0.booking__paid_at__year
          , nr_subq_0.booking__paid_at__extract_year
          , nr_subq_0.booking__paid_at__extract_quarter
          , nr_subq_0.booking__paid_at__extract_month
          , nr_subq_0.booking__paid_at__extract_day
          , nr_subq_0.booking__paid_at__extract_dow
          , nr_subq_0.booking__paid_at__extract_doy
          , nr_subq_0.metric_time__day
          , nr_subq_0.metric_time__week
          , nr_subq_0.metric_time__month
          , nr_subq_0.metric_time__quarter
          , nr_subq_0.metric_time__year
          , nr_subq_0.metric_time__extract_year
          , nr_subq_0.metric_time__extract_quarter
          , nr_subq_0.metric_time__extract_month
          , nr_subq_0.metric_time__extract_day
          , nr_subq_0.metric_time__extract_dow
          , nr_subq_0.metric_time__extract_doy
          , nr_subq_0.listing
          , nr_subq_0.guest
          , nr_subq_0.host
          , nr_subq_0.booking__listing
          , nr_subq_0.booking__guest
          , nr_subq_0.booking__host
          , nr_subq_0.is_instant
          , nr_subq_0.booking__is_instant
          , nr_subq_0.bookings
          , nr_subq_0.instant_bookings
          , nr_subq_0.booking_value
          , nr_subq_0.max_booking_value
          , nr_subq_0.min_booking_value
          , nr_subq_0.bookers
          , nr_subq_0.average_booking_value
          , nr_subq_0.referred_bookings
          , nr_subq_0.median_booking_value
          , nr_subq_0.booking_value_p99
          , nr_subq_0.discrete_booking_value_p99
          , nr_subq_0.approximate_continuous_booking_value_p99
          , nr_subq_0.approximate_discrete_booking_value_p99
        FROM (
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
        WHERE ((metric_time__day >= '2020-01-02') AND (metric_time__day <= '2020-01-02')) AND (metric_time__month > '2020-01-01')
      ) nr_subq_1
    ) nr_subq_2
    GROUP BY
      nr_subq_2.metric_time__day
  ) nr_subq_3
  ON
    nr_subq_7.metric_time__day = nr_subq_3.metric_time__day
) nr_subq_8
