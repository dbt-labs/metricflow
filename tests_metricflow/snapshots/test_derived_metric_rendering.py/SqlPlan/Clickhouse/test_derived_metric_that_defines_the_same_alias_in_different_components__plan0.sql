test_name: test_derived_metric_that_defines_the_same_alias_in_different_components
test_filename: test_derived_metric_rendering.py
docstring:
  Tests querying a derived metric which give the same alias to its components.
sql_engine: Clickhouse
---
-- Order By [] Limit 1
SELECT
  subq_12.booking__is_instant
  , subq_12.derived_shared_alias_1a
  , subq_12.derived_shared_alias_2
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_5.booking__is_instant, subq_11.booking__is_instant) AS booking__is_instant
    , MAX(subq_5.derived_shared_alias_1a) AS derived_shared_alias_1a
    , MAX(subq_11.derived_shared_alias_2) AS derived_shared_alias_2
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_4.booking__is_instant
      , shared_alias - 10 AS derived_shared_alias_1a
    FROM (
      -- Compute Metrics via Expressions
      SELECT
        subq_3.booking__is_instant
        , subq_3.bookings AS shared_alias
      FROM (
        -- Aggregate Measures
        SELECT
          subq_2.booking__is_instant
          , SUM(subq_2.bookings) AS bookings
        FROM (
          -- Pass Only Elements: ['bookings', 'booking__is_instant']
          SELECT
            subq_1.booking__is_instant
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
                , EXTRACT(toYear FROM bookings_source_src_28000.ds) AS ds__extract_year
                , EXTRACT(toQuarter FROM bookings_source_src_28000.ds) AS ds__extract_quarter
                , EXTRACT(toMonth FROM bookings_source_src_28000.ds) AS ds__extract_month
                , EXTRACT(toDayOfMonth FROM bookings_source_src_28000.ds) AS ds__extract_day
                , EXTRACT(toDayOfWeek FROM bookings_source_src_28000.ds) AS ds__extract_dow
                , EXTRACT(toDayOfYear FROM bookings_source_src_28000.ds) AS ds__extract_doy
                , DATE_TRUNC('day', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__day
                , DATE_TRUNC('week', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__week
                , DATE_TRUNC('month', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__month
                , DATE_TRUNC('quarter', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                , DATE_TRUNC('year', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__year
                , EXTRACT(toYear FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                , EXTRACT(toQuarter FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                , EXTRACT(toMonth FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                , EXTRACT(toDayOfMonth FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                , EXTRACT(toDayOfWeek FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
                , EXTRACT(toDayOfYear FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                , DATE_TRUNC('day', bookings_source_src_28000.paid_at) AS paid_at__day
                , DATE_TRUNC('week', bookings_source_src_28000.paid_at) AS paid_at__week
                , DATE_TRUNC('month', bookings_source_src_28000.paid_at) AS paid_at__month
                , DATE_TRUNC('quarter', bookings_source_src_28000.paid_at) AS paid_at__quarter
                , DATE_TRUNC('year', bookings_source_src_28000.paid_at) AS paid_at__year
                , EXTRACT(toYear FROM bookings_source_src_28000.paid_at) AS paid_at__extract_year
                , EXTRACT(toQuarter FROM bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
                , EXTRACT(toMonth FROM bookings_source_src_28000.paid_at) AS paid_at__extract_month
                , EXTRACT(toDayOfMonth FROM bookings_source_src_28000.paid_at) AS paid_at__extract_day
                , EXTRACT(toDayOfWeek FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
                , EXTRACT(toDayOfYear FROM bookings_source_src_28000.paid_at) AS paid_at__extract_doy
                , bookings_source_src_28000.is_instant AS booking__is_instant
                , DATE_TRUNC('day', bookings_source_src_28000.ds) AS booking__ds__day
                , DATE_TRUNC('week', bookings_source_src_28000.ds) AS booking__ds__week
                , DATE_TRUNC('month', bookings_source_src_28000.ds) AS booking__ds__month
                , DATE_TRUNC('quarter', bookings_source_src_28000.ds) AS booking__ds__quarter
                , DATE_TRUNC('year', bookings_source_src_28000.ds) AS booking__ds__year
                , EXTRACT(toYear FROM bookings_source_src_28000.ds) AS booking__ds__extract_year
                , EXTRACT(toQuarter FROM bookings_source_src_28000.ds) AS booking__ds__extract_quarter
                , EXTRACT(toMonth FROM bookings_source_src_28000.ds) AS booking__ds__extract_month
                , EXTRACT(toDayOfMonth FROM bookings_source_src_28000.ds) AS booking__ds__extract_day
                , EXTRACT(toDayOfWeek FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
                , EXTRACT(toDayOfYear FROM bookings_source_src_28000.ds) AS booking__ds__extract_doy
                , DATE_TRUNC('day', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__day
                , DATE_TRUNC('week', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__week
                , DATE_TRUNC('month', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__month
                , DATE_TRUNC('quarter', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__quarter
                , DATE_TRUNC('year', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__year
                , EXTRACT(toYear FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
                , EXTRACT(toQuarter FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                , EXTRACT(toMonth FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
                , EXTRACT(toDayOfMonth FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
                , EXTRACT(toDayOfWeek FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
                , EXTRACT(toDayOfYear FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
                , DATE_TRUNC('day', bookings_source_src_28000.paid_at) AS booking__paid_at__day
                , DATE_TRUNC('week', bookings_source_src_28000.paid_at) AS booking__paid_at__week
                , DATE_TRUNC('month', bookings_source_src_28000.paid_at) AS booking__paid_at__month
                , DATE_TRUNC('quarter', bookings_source_src_28000.paid_at) AS booking__paid_at__quarter
                , DATE_TRUNC('year', bookings_source_src_28000.paid_at) AS booking__paid_at__year
                , EXTRACT(toYear FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_year
                , EXTRACT(toQuarter FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_quarter
                , EXTRACT(toMonth FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_month
                , EXTRACT(toDayOfMonth FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_day
                , EXTRACT(toDayOfWeek FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
                , EXTRACT(toDayOfYear FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
                , bookings_source_src_28000.listing_id AS listing
                , bookings_source_src_28000.guest_id AS guest
                , bookings_source_src_28000.host_id AS host
                , bookings_source_src_28000.listing_id AS booking__listing
                , bookings_source_src_28000.guest_id AS booking__guest
                , bookings_source_src_28000.host_id AS booking__host
              FROM ***************************.fct_bookings bookings_source_src_28000
              SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
            ) subq_0
            SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
          ) subq_1
          SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
        ) subq_2
        GROUP BY
          subq_2.booking__is_instant
        SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
      ) subq_3
      SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
    ) subq_4
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_5
  FULL OUTER JOIN
  (
    -- Compute Metrics via Expressions
    SELECT
      subq_10.booking__is_instant
      , shared_alias + 10 AS derived_shared_alias_2
    FROM (
      -- Compute Metrics via Expressions
      SELECT
        subq_9.booking__is_instant
        , subq_9.instant_bookings AS shared_alias
      FROM (
        -- Aggregate Measures
        SELECT
          subq_8.booking__is_instant
          , SUM(subq_8.instant_bookings) AS instant_bookings
        FROM (
          -- Pass Only Elements: ['instant_bookings', 'booking__is_instant']
          SELECT
            subq_7.booking__is_instant
            , subq_7.instant_bookings
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
                , EXTRACT(toYear FROM bookings_source_src_28000.ds) AS ds__extract_year
                , EXTRACT(toQuarter FROM bookings_source_src_28000.ds) AS ds__extract_quarter
                , EXTRACT(toMonth FROM bookings_source_src_28000.ds) AS ds__extract_month
                , EXTRACT(toDayOfMonth FROM bookings_source_src_28000.ds) AS ds__extract_day
                , EXTRACT(toDayOfWeek FROM bookings_source_src_28000.ds) AS ds__extract_dow
                , EXTRACT(toDayOfYear FROM bookings_source_src_28000.ds) AS ds__extract_doy
                , DATE_TRUNC('day', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__day
                , DATE_TRUNC('week', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__week
                , DATE_TRUNC('month', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__month
                , DATE_TRUNC('quarter', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                , DATE_TRUNC('year', bookings_source_src_28000.ds_partitioned) AS ds_partitioned__year
                , EXTRACT(toYear FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                , EXTRACT(toQuarter FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                , EXTRACT(toMonth FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                , EXTRACT(toDayOfMonth FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                , EXTRACT(toDayOfWeek FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
                , EXTRACT(toDayOfYear FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                , DATE_TRUNC('day', bookings_source_src_28000.paid_at) AS paid_at__day
                , DATE_TRUNC('week', bookings_source_src_28000.paid_at) AS paid_at__week
                , DATE_TRUNC('month', bookings_source_src_28000.paid_at) AS paid_at__month
                , DATE_TRUNC('quarter', bookings_source_src_28000.paid_at) AS paid_at__quarter
                , DATE_TRUNC('year', bookings_source_src_28000.paid_at) AS paid_at__year
                , EXTRACT(toYear FROM bookings_source_src_28000.paid_at) AS paid_at__extract_year
                , EXTRACT(toQuarter FROM bookings_source_src_28000.paid_at) AS paid_at__extract_quarter
                , EXTRACT(toMonth FROM bookings_source_src_28000.paid_at) AS paid_at__extract_month
                , EXTRACT(toDayOfMonth FROM bookings_source_src_28000.paid_at) AS paid_at__extract_day
                , EXTRACT(toDayOfWeek FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
                , EXTRACT(toDayOfYear FROM bookings_source_src_28000.paid_at) AS paid_at__extract_doy
                , bookings_source_src_28000.is_instant AS booking__is_instant
                , DATE_TRUNC('day', bookings_source_src_28000.ds) AS booking__ds__day
                , DATE_TRUNC('week', bookings_source_src_28000.ds) AS booking__ds__week
                , DATE_TRUNC('month', bookings_source_src_28000.ds) AS booking__ds__month
                , DATE_TRUNC('quarter', bookings_source_src_28000.ds) AS booking__ds__quarter
                , DATE_TRUNC('year', bookings_source_src_28000.ds) AS booking__ds__year
                , EXTRACT(toYear FROM bookings_source_src_28000.ds) AS booking__ds__extract_year
                , EXTRACT(toQuarter FROM bookings_source_src_28000.ds) AS booking__ds__extract_quarter
                , EXTRACT(toMonth FROM bookings_source_src_28000.ds) AS booking__ds__extract_month
                , EXTRACT(toDayOfMonth FROM bookings_source_src_28000.ds) AS booking__ds__extract_day
                , EXTRACT(toDayOfWeek FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
                , EXTRACT(toDayOfYear FROM bookings_source_src_28000.ds) AS booking__ds__extract_doy
                , DATE_TRUNC('day', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__day
                , DATE_TRUNC('week', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__week
                , DATE_TRUNC('month', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__month
                , DATE_TRUNC('quarter', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__quarter
                , DATE_TRUNC('year', bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__year
                , EXTRACT(toYear FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_year
                , EXTRACT(toQuarter FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
                , EXTRACT(toMonth FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_month
                , EXTRACT(toDayOfMonth FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_day
                , EXTRACT(toDayOfWeek FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
                , EXTRACT(toDayOfYear FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_doy
                , DATE_TRUNC('day', bookings_source_src_28000.paid_at) AS booking__paid_at__day
                , DATE_TRUNC('week', bookings_source_src_28000.paid_at) AS booking__paid_at__week
                , DATE_TRUNC('month', bookings_source_src_28000.paid_at) AS booking__paid_at__month
                , DATE_TRUNC('quarter', bookings_source_src_28000.paid_at) AS booking__paid_at__quarter
                , DATE_TRUNC('year', bookings_source_src_28000.paid_at) AS booking__paid_at__year
                , EXTRACT(toYear FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_year
                , EXTRACT(toQuarter FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_quarter
                , EXTRACT(toMonth FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_month
                , EXTRACT(toDayOfMonth FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_day
                , EXTRACT(toDayOfWeek FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
                , EXTRACT(toDayOfYear FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
                , bookings_source_src_28000.listing_id AS listing
                , bookings_source_src_28000.guest_id AS guest
                , bookings_source_src_28000.host_id AS host
                , bookings_source_src_28000.listing_id AS booking__listing
                , bookings_source_src_28000.guest_id AS booking__guest
                , bookings_source_src_28000.host_id AS booking__host
              FROM ***************************.fct_bookings bookings_source_src_28000
              SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
            ) subq_6
            SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
          ) subq_7
          SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
        ) subq_8
        GROUP BY
          subq_8.booking__is_instant
        SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
      ) subq_9
      SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
    ) subq_10
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_11
  ON
    subq_5.booking__is_instant = subq_11.booking__is_instant
  GROUP BY
    COALESCE(subq_5.booking__is_instant, subq_11.booking__is_instant)
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_12
LIMIT 1
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
