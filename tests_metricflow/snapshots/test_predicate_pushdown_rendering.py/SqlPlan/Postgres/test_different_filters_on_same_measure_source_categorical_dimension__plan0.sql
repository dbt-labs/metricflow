test_name: test_different_filters_on_same_measure_source_categorical_dimension
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where multiple filters against the same measure dimension need to be an effective OR.

      This can be an issue where a derived metric takes in two filters that refer to the same dimension from the input
      measure source. If these filters are disjoint the predicate pushdown needs to ensure that all matching rows are
      returned, so we cannot simply push one filter or the other down, nor can we push them down as an AND - they
      must be an OR, since all relevant rows need to be returned to the requesting metrics.

      The metric listed here has one input that filters on bookings__is_instant and another that does not, which means
      the source input for the latter input must NOT have the filter applied to it.
sql_engine: Postgres
---
-- Compute Metrics via Expressions
SELECT
  subq_10.metric_time__day
  , CAST(subq_10.average_booking_value AS DOUBLE PRECISION) / CAST(NULLIF(subq_10.max_booking_value, 0) AS DOUBLE PRECISION) AS instant_booking_fraction_of_max_value
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_5.metric_time__day, subq_9.metric_time__day) AS metric_time__day
    , MAX(subq_5.average_booking_value) AS average_booking_value
    , MAX(subq_9.max_booking_value) AS max_booking_value
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_4.metric_time__day
      , subq_4.average_booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        subq_3.metric_time__day
        , AVG(subq_3.average_booking_value) AS average_booking_value
      FROM (
        -- Pass Only Elements: ['average_booking_value', 'metric_time__day']
        SELECT
          subq_2.metric_time__day
          , subq_2.average_booking_value
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
          WHERE booking__is_instant
        ) subq_2
      ) subq_3
      GROUP BY
        subq_3.metric_time__day
    ) subq_4
  ) subq_5
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_8.metric_time__day
      , subq_8.max_booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        subq_7.metric_time__day
        , MAX(subq_7.max_booking_value) AS max_booking_value
      FROM (
        -- Pass Only Elements: ['max_booking_value', 'metric_time__day']
        SELECT
          subq_6.metric_time__day
          , subq_6.max_booking_value
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
        ) subq_6
      ) subq_7
      GROUP BY
        subq_7.metric_time__day
    ) subq_8
  ) subq_9
  ON
    subq_5.metric_time__day = subq_9.metric_time__day
  GROUP BY
    COALESCE(subq_5.metric_time__day, subq_9.metric_time__day)
) subq_10