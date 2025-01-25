test_name: test_simple_metric_with_joined_sub_daily_dimension
test_filename: test_granularity_date_part_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_20.listing__user__bio_added_ts__minute
  , nr_subq_20.bookings
FROM (
  -- Aggregate Measures
  SELECT
    nr_subq_19.listing__user__bio_added_ts__minute
    , SUM(nr_subq_19.bookings) AS bookings
  FROM (
    -- Pass Only Elements: ['bookings', 'listing__user__bio_added_ts__minute']
    SELECT
      nr_subq_18.listing__user__bio_added_ts__minute
      , nr_subq_18.bookings
    FROM (
      -- Join Standard Outputs
      SELECT
        nr_subq_17.user__ds_partitioned__day AS listing__user__ds_partitioned__day
        , nr_subq_17.user__bio_added_ts__minute AS listing__user__bio_added_ts__minute
        , nr_subq_12.ds__day AS ds__day
        , nr_subq_12.ds__week AS ds__week
        , nr_subq_12.ds__month AS ds__month
        , nr_subq_12.ds__quarter AS ds__quarter
        , nr_subq_12.ds__year AS ds__year
        , nr_subq_12.ds__extract_year AS ds__extract_year
        , nr_subq_12.ds__extract_quarter AS ds__extract_quarter
        , nr_subq_12.ds__extract_month AS ds__extract_month
        , nr_subq_12.ds__extract_day AS ds__extract_day
        , nr_subq_12.ds__extract_dow AS ds__extract_dow
        , nr_subq_12.ds__extract_doy AS ds__extract_doy
        , nr_subq_12.ds_partitioned__day AS ds_partitioned__day
        , nr_subq_12.ds_partitioned__week AS ds_partitioned__week
        , nr_subq_12.ds_partitioned__month AS ds_partitioned__month
        , nr_subq_12.ds_partitioned__quarter AS ds_partitioned__quarter
        , nr_subq_12.ds_partitioned__year AS ds_partitioned__year
        , nr_subq_12.ds_partitioned__extract_year AS ds_partitioned__extract_year
        , nr_subq_12.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
        , nr_subq_12.ds_partitioned__extract_month AS ds_partitioned__extract_month
        , nr_subq_12.ds_partitioned__extract_day AS ds_partitioned__extract_day
        , nr_subq_12.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
        , nr_subq_12.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
        , nr_subq_12.paid_at__day AS paid_at__day
        , nr_subq_12.paid_at__week AS paid_at__week
        , nr_subq_12.paid_at__month AS paid_at__month
        , nr_subq_12.paid_at__quarter AS paid_at__quarter
        , nr_subq_12.paid_at__year AS paid_at__year
        , nr_subq_12.paid_at__extract_year AS paid_at__extract_year
        , nr_subq_12.paid_at__extract_quarter AS paid_at__extract_quarter
        , nr_subq_12.paid_at__extract_month AS paid_at__extract_month
        , nr_subq_12.paid_at__extract_day AS paid_at__extract_day
        , nr_subq_12.paid_at__extract_dow AS paid_at__extract_dow
        , nr_subq_12.paid_at__extract_doy AS paid_at__extract_doy
        , nr_subq_12.booking__ds__day AS booking__ds__day
        , nr_subq_12.booking__ds__week AS booking__ds__week
        , nr_subq_12.booking__ds__month AS booking__ds__month
        , nr_subq_12.booking__ds__quarter AS booking__ds__quarter
        , nr_subq_12.booking__ds__year AS booking__ds__year
        , nr_subq_12.booking__ds__extract_year AS booking__ds__extract_year
        , nr_subq_12.booking__ds__extract_quarter AS booking__ds__extract_quarter
        , nr_subq_12.booking__ds__extract_month AS booking__ds__extract_month
        , nr_subq_12.booking__ds__extract_day AS booking__ds__extract_day
        , nr_subq_12.booking__ds__extract_dow AS booking__ds__extract_dow
        , nr_subq_12.booking__ds__extract_doy AS booking__ds__extract_doy
        , nr_subq_12.booking__ds_partitioned__day AS booking__ds_partitioned__day
        , nr_subq_12.booking__ds_partitioned__week AS booking__ds_partitioned__week
        , nr_subq_12.booking__ds_partitioned__month AS booking__ds_partitioned__month
        , nr_subq_12.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
        , nr_subq_12.booking__ds_partitioned__year AS booking__ds_partitioned__year
        , nr_subq_12.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
        , nr_subq_12.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
        , nr_subq_12.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
        , nr_subq_12.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
        , nr_subq_12.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
        , nr_subq_12.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
        , nr_subq_12.booking__paid_at__day AS booking__paid_at__day
        , nr_subq_12.booking__paid_at__week AS booking__paid_at__week
        , nr_subq_12.booking__paid_at__month AS booking__paid_at__month
        , nr_subq_12.booking__paid_at__quarter AS booking__paid_at__quarter
        , nr_subq_12.booking__paid_at__year AS booking__paid_at__year
        , nr_subq_12.booking__paid_at__extract_year AS booking__paid_at__extract_year
        , nr_subq_12.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
        , nr_subq_12.booking__paid_at__extract_month AS booking__paid_at__extract_month
        , nr_subq_12.booking__paid_at__extract_day AS booking__paid_at__extract_day
        , nr_subq_12.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
        , nr_subq_12.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
        , nr_subq_12.metric_time__day AS metric_time__day
        , nr_subq_12.metric_time__week AS metric_time__week
        , nr_subq_12.metric_time__month AS metric_time__month
        , nr_subq_12.metric_time__quarter AS metric_time__quarter
        , nr_subq_12.metric_time__year AS metric_time__year
        , nr_subq_12.metric_time__extract_year AS metric_time__extract_year
        , nr_subq_12.metric_time__extract_quarter AS metric_time__extract_quarter
        , nr_subq_12.metric_time__extract_month AS metric_time__extract_month
        , nr_subq_12.metric_time__extract_day AS metric_time__extract_day
        , nr_subq_12.metric_time__extract_dow AS metric_time__extract_dow
        , nr_subq_12.metric_time__extract_doy AS metric_time__extract_doy
        , nr_subq_12.listing AS listing
        , nr_subq_12.guest AS guest
        , nr_subq_12.host AS host
        , nr_subq_12.booking__listing AS booking__listing
        , nr_subq_12.booking__guest AS booking__guest
        , nr_subq_12.booking__host AS booking__host
        , nr_subq_12.is_instant AS is_instant
        , nr_subq_12.booking__is_instant AS booking__is_instant
        , nr_subq_12.bookings AS bookings
        , nr_subq_12.instant_bookings AS instant_bookings
        , nr_subq_12.booking_value AS booking_value
        , nr_subq_12.max_booking_value AS max_booking_value
        , nr_subq_12.min_booking_value AS min_booking_value
        , nr_subq_12.bookers AS bookers
        , nr_subq_12.average_booking_value AS average_booking_value
        , nr_subq_12.referred_bookings AS referred_bookings
        , nr_subq_12.median_booking_value AS median_booking_value
        , nr_subq_12.booking_value_p99 AS booking_value_p99
        , nr_subq_12.discrete_booking_value_p99 AS discrete_booking_value_p99
        , nr_subq_12.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
        , nr_subq_12.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
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
      ) nr_subq_12
      LEFT OUTER JOIN (
        -- Pass Only Elements: ['user__ds_partitioned__day', 'user__bio_added_ts__minute', 'listing']
        SELECT
          nr_subq_16.user__ds_partitioned__day
          , nr_subq_16.user__bio_added_ts__minute
          , nr_subq_16.listing
        FROM (
          -- Join Standard Outputs
          SELECT
            nr_subq_15.home_state AS user__home_state
            , nr_subq_15.ds__day AS user__ds__day
            , nr_subq_15.ds__week AS user__ds__week
            , nr_subq_15.ds__month AS user__ds__month
            , nr_subq_15.ds__quarter AS user__ds__quarter
            , nr_subq_15.ds__year AS user__ds__year
            , nr_subq_15.ds__extract_year AS user__ds__extract_year
            , nr_subq_15.ds__extract_quarter AS user__ds__extract_quarter
            , nr_subq_15.ds__extract_month AS user__ds__extract_month
            , nr_subq_15.ds__extract_day AS user__ds__extract_day
            , nr_subq_15.ds__extract_dow AS user__ds__extract_dow
            , nr_subq_15.ds__extract_doy AS user__ds__extract_doy
            , nr_subq_15.created_at__day AS user__created_at__day
            , nr_subq_15.created_at__week AS user__created_at__week
            , nr_subq_15.created_at__month AS user__created_at__month
            , nr_subq_15.created_at__quarter AS user__created_at__quarter
            , nr_subq_15.created_at__year AS user__created_at__year
            , nr_subq_15.created_at__extract_year AS user__created_at__extract_year
            , nr_subq_15.created_at__extract_quarter AS user__created_at__extract_quarter
            , nr_subq_15.created_at__extract_month AS user__created_at__extract_month
            , nr_subq_15.created_at__extract_day AS user__created_at__extract_day
            , nr_subq_15.created_at__extract_dow AS user__created_at__extract_dow
            , nr_subq_15.created_at__extract_doy AS user__created_at__extract_doy
            , nr_subq_15.ds_partitioned__day AS user__ds_partitioned__day
            , nr_subq_15.ds_partitioned__week AS user__ds_partitioned__week
            , nr_subq_15.ds_partitioned__month AS user__ds_partitioned__month
            , nr_subq_15.ds_partitioned__quarter AS user__ds_partitioned__quarter
            , nr_subq_15.ds_partitioned__year AS user__ds_partitioned__year
            , nr_subq_15.ds_partitioned__extract_year AS user__ds_partitioned__extract_year
            , nr_subq_15.ds_partitioned__extract_quarter AS user__ds_partitioned__extract_quarter
            , nr_subq_15.ds_partitioned__extract_month AS user__ds_partitioned__extract_month
            , nr_subq_15.ds_partitioned__extract_day AS user__ds_partitioned__extract_day
            , nr_subq_15.ds_partitioned__extract_dow AS user__ds_partitioned__extract_dow
            , nr_subq_15.ds_partitioned__extract_doy AS user__ds_partitioned__extract_doy
            , nr_subq_15.last_profile_edit_ts__millisecond AS user__last_profile_edit_ts__millisecond
            , nr_subq_15.last_profile_edit_ts__second AS user__last_profile_edit_ts__second
            , nr_subq_15.last_profile_edit_ts__minute AS user__last_profile_edit_ts__minute
            , nr_subq_15.last_profile_edit_ts__hour AS user__last_profile_edit_ts__hour
            , nr_subq_15.last_profile_edit_ts__day AS user__last_profile_edit_ts__day
            , nr_subq_15.last_profile_edit_ts__week AS user__last_profile_edit_ts__week
            , nr_subq_15.last_profile_edit_ts__month AS user__last_profile_edit_ts__month
            , nr_subq_15.last_profile_edit_ts__quarter AS user__last_profile_edit_ts__quarter
            , nr_subq_15.last_profile_edit_ts__year AS user__last_profile_edit_ts__year
            , nr_subq_15.last_profile_edit_ts__extract_year AS user__last_profile_edit_ts__extract_year
            , nr_subq_15.last_profile_edit_ts__extract_quarter AS user__last_profile_edit_ts__extract_quarter
            , nr_subq_15.last_profile_edit_ts__extract_month AS user__last_profile_edit_ts__extract_month
            , nr_subq_15.last_profile_edit_ts__extract_day AS user__last_profile_edit_ts__extract_day
            , nr_subq_15.last_profile_edit_ts__extract_dow AS user__last_profile_edit_ts__extract_dow
            , nr_subq_15.last_profile_edit_ts__extract_doy AS user__last_profile_edit_ts__extract_doy
            , nr_subq_15.bio_added_ts__second AS user__bio_added_ts__second
            , nr_subq_15.bio_added_ts__minute AS user__bio_added_ts__minute
            , nr_subq_15.bio_added_ts__hour AS user__bio_added_ts__hour
            , nr_subq_15.bio_added_ts__day AS user__bio_added_ts__day
            , nr_subq_15.bio_added_ts__week AS user__bio_added_ts__week
            , nr_subq_15.bio_added_ts__month AS user__bio_added_ts__month
            , nr_subq_15.bio_added_ts__quarter AS user__bio_added_ts__quarter
            , nr_subq_15.bio_added_ts__year AS user__bio_added_ts__year
            , nr_subq_15.bio_added_ts__extract_year AS user__bio_added_ts__extract_year
            , nr_subq_15.bio_added_ts__extract_quarter AS user__bio_added_ts__extract_quarter
            , nr_subq_15.bio_added_ts__extract_month AS user__bio_added_ts__extract_month
            , nr_subq_15.bio_added_ts__extract_day AS user__bio_added_ts__extract_day
            , nr_subq_15.bio_added_ts__extract_dow AS user__bio_added_ts__extract_dow
            , nr_subq_15.bio_added_ts__extract_doy AS user__bio_added_ts__extract_doy
            , nr_subq_15.last_login_ts__minute AS user__last_login_ts__minute
            , nr_subq_15.last_login_ts__hour AS user__last_login_ts__hour
            , nr_subq_15.last_login_ts__day AS user__last_login_ts__day
            , nr_subq_15.last_login_ts__week AS user__last_login_ts__week
            , nr_subq_15.last_login_ts__month AS user__last_login_ts__month
            , nr_subq_15.last_login_ts__quarter AS user__last_login_ts__quarter
            , nr_subq_15.last_login_ts__year AS user__last_login_ts__year
            , nr_subq_15.last_login_ts__extract_year AS user__last_login_ts__extract_year
            , nr_subq_15.last_login_ts__extract_quarter AS user__last_login_ts__extract_quarter
            , nr_subq_15.last_login_ts__extract_month AS user__last_login_ts__extract_month
            , nr_subq_15.last_login_ts__extract_day AS user__last_login_ts__extract_day
            , nr_subq_15.last_login_ts__extract_dow AS user__last_login_ts__extract_dow
            , nr_subq_15.last_login_ts__extract_doy AS user__last_login_ts__extract_doy
            , nr_subq_15.archived_at__hour AS user__archived_at__hour
            , nr_subq_15.archived_at__day AS user__archived_at__day
            , nr_subq_15.archived_at__week AS user__archived_at__week
            , nr_subq_15.archived_at__month AS user__archived_at__month
            , nr_subq_15.archived_at__quarter AS user__archived_at__quarter
            , nr_subq_15.archived_at__year AS user__archived_at__year
            , nr_subq_15.archived_at__extract_year AS user__archived_at__extract_year
            , nr_subq_15.archived_at__extract_quarter AS user__archived_at__extract_quarter
            , nr_subq_15.archived_at__extract_month AS user__archived_at__extract_month
            , nr_subq_15.archived_at__extract_day AS user__archived_at__extract_day
            , nr_subq_15.archived_at__extract_dow AS user__archived_at__extract_dow
            , nr_subq_15.archived_at__extract_doy AS user__archived_at__extract_doy
            , nr_subq_15.metric_time__day AS user__metric_time__day
            , nr_subq_15.metric_time__week AS user__metric_time__week
            , nr_subq_15.metric_time__month AS user__metric_time__month
            , nr_subq_15.metric_time__quarter AS user__metric_time__quarter
            , nr_subq_15.metric_time__year AS user__metric_time__year
            , nr_subq_15.metric_time__extract_year AS user__metric_time__extract_year
            , nr_subq_15.metric_time__extract_quarter AS user__metric_time__extract_quarter
            , nr_subq_15.metric_time__extract_month AS user__metric_time__extract_month
            , nr_subq_15.metric_time__extract_day AS user__metric_time__extract_day
            , nr_subq_15.metric_time__extract_dow AS user__metric_time__extract_dow
            , nr_subq_15.metric_time__extract_doy AS user__metric_time__extract_doy
            , nr_subq_13.ds__day AS ds__day
            , nr_subq_13.ds__week AS ds__week
            , nr_subq_13.ds__month AS ds__month
            , nr_subq_13.ds__quarter AS ds__quarter
            , nr_subq_13.ds__year AS ds__year
            , nr_subq_13.ds__extract_year AS ds__extract_year
            , nr_subq_13.ds__extract_quarter AS ds__extract_quarter
            , nr_subq_13.ds__extract_month AS ds__extract_month
            , nr_subq_13.ds__extract_day AS ds__extract_day
            , nr_subq_13.ds__extract_dow AS ds__extract_dow
            , nr_subq_13.ds__extract_doy AS ds__extract_doy
            , nr_subq_13.created_at__day AS created_at__day
            , nr_subq_13.created_at__week AS created_at__week
            , nr_subq_13.created_at__month AS created_at__month
            , nr_subq_13.created_at__quarter AS created_at__quarter
            , nr_subq_13.created_at__year AS created_at__year
            , nr_subq_13.created_at__extract_year AS created_at__extract_year
            , nr_subq_13.created_at__extract_quarter AS created_at__extract_quarter
            , nr_subq_13.created_at__extract_month AS created_at__extract_month
            , nr_subq_13.created_at__extract_day AS created_at__extract_day
            , nr_subq_13.created_at__extract_dow AS created_at__extract_dow
            , nr_subq_13.created_at__extract_doy AS created_at__extract_doy
            , nr_subq_13.listing__ds__day AS listing__ds__day
            , nr_subq_13.listing__ds__week AS listing__ds__week
            , nr_subq_13.listing__ds__month AS listing__ds__month
            , nr_subq_13.listing__ds__quarter AS listing__ds__quarter
            , nr_subq_13.listing__ds__year AS listing__ds__year
            , nr_subq_13.listing__ds__extract_year AS listing__ds__extract_year
            , nr_subq_13.listing__ds__extract_quarter AS listing__ds__extract_quarter
            , nr_subq_13.listing__ds__extract_month AS listing__ds__extract_month
            , nr_subq_13.listing__ds__extract_day AS listing__ds__extract_day
            , nr_subq_13.listing__ds__extract_dow AS listing__ds__extract_dow
            , nr_subq_13.listing__ds__extract_doy AS listing__ds__extract_doy
            , nr_subq_13.listing__created_at__day AS listing__created_at__day
            , nr_subq_13.listing__created_at__week AS listing__created_at__week
            , nr_subq_13.listing__created_at__month AS listing__created_at__month
            , nr_subq_13.listing__created_at__quarter AS listing__created_at__quarter
            , nr_subq_13.listing__created_at__year AS listing__created_at__year
            , nr_subq_13.listing__created_at__extract_year AS listing__created_at__extract_year
            , nr_subq_13.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
            , nr_subq_13.listing__created_at__extract_month AS listing__created_at__extract_month
            , nr_subq_13.listing__created_at__extract_day AS listing__created_at__extract_day
            , nr_subq_13.listing__created_at__extract_dow AS listing__created_at__extract_dow
            , nr_subq_13.listing__created_at__extract_doy AS listing__created_at__extract_doy
            , nr_subq_13.metric_time__day AS metric_time__day
            , nr_subq_13.metric_time__week AS metric_time__week
            , nr_subq_13.metric_time__month AS metric_time__month
            , nr_subq_13.metric_time__quarter AS metric_time__quarter
            , nr_subq_13.metric_time__year AS metric_time__year
            , nr_subq_13.metric_time__extract_year AS metric_time__extract_year
            , nr_subq_13.metric_time__extract_quarter AS metric_time__extract_quarter
            , nr_subq_13.metric_time__extract_month AS metric_time__extract_month
            , nr_subq_13.metric_time__extract_day AS metric_time__extract_day
            , nr_subq_13.metric_time__extract_dow AS metric_time__extract_dow
            , nr_subq_13.metric_time__extract_doy AS metric_time__extract_doy
            , nr_subq_13.listing AS listing
            , nr_subq_13.user AS user
            , nr_subq_13.listing__user AS listing__user
            , nr_subq_13.country_latest AS country_latest
            , nr_subq_13.is_lux_latest AS is_lux_latest
            , nr_subq_13.capacity_latest AS capacity_latest
            , nr_subq_13.listing__country_latest AS listing__country_latest
            , nr_subq_13.listing__is_lux_latest AS listing__is_lux_latest
            , nr_subq_13.listing__capacity_latest AS listing__capacity_latest
            , nr_subq_13.listings AS listings
            , nr_subq_13.largest_listing AS largest_listing
            , nr_subq_13.smallest_listing AS smallest_listing
          FROM (
            -- Metric Time Dimension 'ds'
            SELECT
              nr_subq_28007.ds__day
              , nr_subq_28007.ds__week
              , nr_subq_28007.ds__month
              , nr_subq_28007.ds__quarter
              , nr_subq_28007.ds__year
              , nr_subq_28007.ds__extract_year
              , nr_subq_28007.ds__extract_quarter
              , nr_subq_28007.ds__extract_month
              , nr_subq_28007.ds__extract_day
              , nr_subq_28007.ds__extract_dow
              , nr_subq_28007.ds__extract_doy
              , nr_subq_28007.created_at__day
              , nr_subq_28007.created_at__week
              , nr_subq_28007.created_at__month
              , nr_subq_28007.created_at__quarter
              , nr_subq_28007.created_at__year
              , nr_subq_28007.created_at__extract_year
              , nr_subq_28007.created_at__extract_quarter
              , nr_subq_28007.created_at__extract_month
              , nr_subq_28007.created_at__extract_day
              , nr_subq_28007.created_at__extract_dow
              , nr_subq_28007.created_at__extract_doy
              , nr_subq_28007.listing__ds__day
              , nr_subq_28007.listing__ds__week
              , nr_subq_28007.listing__ds__month
              , nr_subq_28007.listing__ds__quarter
              , nr_subq_28007.listing__ds__year
              , nr_subq_28007.listing__ds__extract_year
              , nr_subq_28007.listing__ds__extract_quarter
              , nr_subq_28007.listing__ds__extract_month
              , nr_subq_28007.listing__ds__extract_day
              , nr_subq_28007.listing__ds__extract_dow
              , nr_subq_28007.listing__ds__extract_doy
              , nr_subq_28007.listing__created_at__day
              , nr_subq_28007.listing__created_at__week
              , nr_subq_28007.listing__created_at__month
              , nr_subq_28007.listing__created_at__quarter
              , nr_subq_28007.listing__created_at__year
              , nr_subq_28007.listing__created_at__extract_year
              , nr_subq_28007.listing__created_at__extract_quarter
              , nr_subq_28007.listing__created_at__extract_month
              , nr_subq_28007.listing__created_at__extract_day
              , nr_subq_28007.listing__created_at__extract_dow
              , nr_subq_28007.listing__created_at__extract_doy
              , nr_subq_28007.ds__day AS metric_time__day
              , nr_subq_28007.ds__week AS metric_time__week
              , nr_subq_28007.ds__month AS metric_time__month
              , nr_subq_28007.ds__quarter AS metric_time__quarter
              , nr_subq_28007.ds__year AS metric_time__year
              , nr_subq_28007.ds__extract_year AS metric_time__extract_year
              , nr_subq_28007.ds__extract_quarter AS metric_time__extract_quarter
              , nr_subq_28007.ds__extract_month AS metric_time__extract_month
              , nr_subq_28007.ds__extract_day AS metric_time__extract_day
              , nr_subq_28007.ds__extract_dow AS metric_time__extract_dow
              , nr_subq_28007.ds__extract_doy AS metric_time__extract_doy
              , nr_subq_28007.listing
              , nr_subq_28007.user
              , nr_subq_28007.listing__user
              , nr_subq_28007.country_latest
              , nr_subq_28007.is_lux_latest
              , nr_subq_28007.capacity_latest
              , nr_subq_28007.listing__country_latest
              , nr_subq_28007.listing__is_lux_latest
              , nr_subq_28007.listing__capacity_latest
              , nr_subq_28007.listings
              , nr_subq_28007.largest_listing
              , nr_subq_28007.smallest_listing
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
            ) nr_subq_28007
          ) nr_subq_13
          LEFT OUTER JOIN (
            -- Pass Only Elements: [
            --   'home_state',
            --   'user__home_state',
            --   'ds__day',
            --   'ds__week',
            --   'ds__month',
            --   'ds__quarter',
            --   'ds__year',
            --   'ds__extract_year',
            --   'ds__extract_quarter',
            --   'ds__extract_month',
            --   'ds__extract_day',
            --   'ds__extract_dow',
            --   'ds__extract_doy',
            --   'created_at__day',
            --   'created_at__week',
            --   'created_at__month',
            --   'created_at__quarter',
            --   'created_at__year',
            --   'created_at__extract_year',
            --   'created_at__extract_quarter',
            --   'created_at__extract_month',
            --   'created_at__extract_day',
            --   'created_at__extract_dow',
            --   'created_at__extract_doy',
            --   'ds_partitioned__day',
            --   'ds_partitioned__week',
            --   'ds_partitioned__month',
            --   'ds_partitioned__quarter',
            --   'ds_partitioned__year',
            --   'ds_partitioned__extract_year',
            --   'ds_partitioned__extract_quarter',
            --   'ds_partitioned__extract_month',
            --   'ds_partitioned__extract_day',
            --   'ds_partitioned__extract_dow',
            --   'ds_partitioned__extract_doy',
            --   'last_profile_edit_ts__millisecond',
            --   'last_profile_edit_ts__second',
            --   'last_profile_edit_ts__minute',
            --   'last_profile_edit_ts__hour',
            --   'last_profile_edit_ts__day',
            --   'last_profile_edit_ts__week',
            --   'last_profile_edit_ts__month',
            --   'last_profile_edit_ts__quarter',
            --   'last_profile_edit_ts__year',
            --   'last_profile_edit_ts__extract_year',
            --   'last_profile_edit_ts__extract_quarter',
            --   'last_profile_edit_ts__extract_month',
            --   'last_profile_edit_ts__extract_day',
            --   'last_profile_edit_ts__extract_dow',
            --   'last_profile_edit_ts__extract_doy',
            --   'bio_added_ts__second',
            --   'bio_added_ts__minute',
            --   'bio_added_ts__hour',
            --   'bio_added_ts__day',
            --   'bio_added_ts__week',
            --   'bio_added_ts__month',
            --   'bio_added_ts__quarter',
            --   'bio_added_ts__year',
            --   'bio_added_ts__extract_year',
            --   'bio_added_ts__extract_quarter',
            --   'bio_added_ts__extract_month',
            --   'bio_added_ts__extract_day',
            --   'bio_added_ts__extract_dow',
            --   'bio_added_ts__extract_doy',
            --   'last_login_ts__minute',
            --   'last_login_ts__hour',
            --   'last_login_ts__day',
            --   'last_login_ts__week',
            --   'last_login_ts__month',
            --   'last_login_ts__quarter',
            --   'last_login_ts__year',
            --   'last_login_ts__extract_year',
            --   'last_login_ts__extract_quarter',
            --   'last_login_ts__extract_month',
            --   'last_login_ts__extract_day',
            --   'last_login_ts__extract_dow',
            --   'last_login_ts__extract_doy',
            --   'archived_at__hour',
            --   'archived_at__day',
            --   'archived_at__week',
            --   'archived_at__month',
            --   'archived_at__quarter',
            --   'archived_at__year',
            --   'archived_at__extract_year',
            --   'archived_at__extract_quarter',
            --   'archived_at__extract_month',
            --   'archived_at__extract_day',
            --   'archived_at__extract_dow',
            --   'archived_at__extract_doy',
            --   'user__ds__day',
            --   'user__ds__week',
            --   'user__ds__month',
            --   'user__ds__quarter',
            --   'user__ds__year',
            --   'user__ds__extract_year',
            --   'user__ds__extract_quarter',
            --   'user__ds__extract_month',
            --   'user__ds__extract_day',
            --   'user__ds__extract_dow',
            --   'user__ds__extract_doy',
            --   'user__created_at__day',
            --   'user__created_at__week',
            --   'user__created_at__month',
            --   'user__created_at__quarter',
            --   'user__created_at__year',
            --   'user__created_at__extract_year',
            --   'user__created_at__extract_quarter',
            --   'user__created_at__extract_month',
            --   'user__created_at__extract_day',
            --   'user__created_at__extract_dow',
            --   'user__created_at__extract_doy',
            --   'user__ds_partitioned__day',
            --   'user__ds_partitioned__week',
            --   'user__ds_partitioned__month',
            --   'user__ds_partitioned__quarter',
            --   'user__ds_partitioned__year',
            --   'user__ds_partitioned__extract_year',
            --   'user__ds_partitioned__extract_quarter',
            --   'user__ds_partitioned__extract_month',
            --   'user__ds_partitioned__extract_day',
            --   'user__ds_partitioned__extract_dow',
            --   'user__ds_partitioned__extract_doy',
            --   'user__last_profile_edit_ts__millisecond',
            --   'user__last_profile_edit_ts__second',
            --   'user__last_profile_edit_ts__minute',
            --   'user__last_profile_edit_ts__hour',
            --   'user__last_profile_edit_ts__day',
            --   'user__last_profile_edit_ts__week',
            --   'user__last_profile_edit_ts__month',
            --   'user__last_profile_edit_ts__quarter',
            --   'user__last_profile_edit_ts__year',
            --   'user__last_profile_edit_ts__extract_year',
            --   'user__last_profile_edit_ts__extract_quarter',
            --   'user__last_profile_edit_ts__extract_month',
            --   'user__last_profile_edit_ts__extract_day',
            --   'user__last_profile_edit_ts__extract_dow',
            --   'user__last_profile_edit_ts__extract_doy',
            --   'user__bio_added_ts__second',
            --   'user__bio_added_ts__minute',
            --   'user__bio_added_ts__hour',
            --   'user__bio_added_ts__day',
            --   'user__bio_added_ts__week',
            --   'user__bio_added_ts__month',
            --   'user__bio_added_ts__quarter',
            --   'user__bio_added_ts__year',
            --   'user__bio_added_ts__extract_year',
            --   'user__bio_added_ts__extract_quarter',
            --   'user__bio_added_ts__extract_month',
            --   'user__bio_added_ts__extract_day',
            --   'user__bio_added_ts__extract_dow',
            --   'user__bio_added_ts__extract_doy',
            --   'user__last_login_ts__minute',
            --   'user__last_login_ts__hour',
            --   'user__last_login_ts__day',
            --   'user__last_login_ts__week',
            --   'user__last_login_ts__month',
            --   'user__last_login_ts__quarter',
            --   'user__last_login_ts__year',
            --   'user__last_login_ts__extract_year',
            --   'user__last_login_ts__extract_quarter',
            --   'user__last_login_ts__extract_month',
            --   'user__last_login_ts__extract_day',
            --   'user__last_login_ts__extract_dow',
            --   'user__last_login_ts__extract_doy',
            --   'user__archived_at__hour',
            --   'user__archived_at__day',
            --   'user__archived_at__week',
            --   'user__archived_at__month',
            --   'user__archived_at__quarter',
            --   'user__archived_at__year',
            --   'user__archived_at__extract_year',
            --   'user__archived_at__extract_quarter',
            --   'user__archived_at__extract_month',
            --   'user__archived_at__extract_day',
            --   'user__archived_at__extract_dow',
            --   'user__archived_at__extract_doy',
            --   'metric_time__day',
            --   'metric_time__week',
            --   'metric_time__month',
            --   'metric_time__quarter',
            --   'metric_time__year',
            --   'metric_time__extract_year',
            --   'metric_time__extract_quarter',
            --   'metric_time__extract_month',
            --   'metric_time__extract_day',
            --   'metric_time__extract_dow',
            --   'metric_time__extract_doy',
            --   'user',
            -- ]
            SELECT
              nr_subq_14.ds__day
              , nr_subq_14.ds__week
              , nr_subq_14.ds__month
              , nr_subq_14.ds__quarter
              , nr_subq_14.ds__year
              , nr_subq_14.ds__extract_year
              , nr_subq_14.ds__extract_quarter
              , nr_subq_14.ds__extract_month
              , nr_subq_14.ds__extract_day
              , nr_subq_14.ds__extract_dow
              , nr_subq_14.ds__extract_doy
              , nr_subq_14.created_at__day
              , nr_subq_14.created_at__week
              , nr_subq_14.created_at__month
              , nr_subq_14.created_at__quarter
              , nr_subq_14.created_at__year
              , nr_subq_14.created_at__extract_year
              , nr_subq_14.created_at__extract_quarter
              , nr_subq_14.created_at__extract_month
              , nr_subq_14.created_at__extract_day
              , nr_subq_14.created_at__extract_dow
              , nr_subq_14.created_at__extract_doy
              , nr_subq_14.ds_partitioned__day
              , nr_subq_14.ds_partitioned__week
              , nr_subq_14.ds_partitioned__month
              , nr_subq_14.ds_partitioned__quarter
              , nr_subq_14.ds_partitioned__year
              , nr_subq_14.ds_partitioned__extract_year
              , nr_subq_14.ds_partitioned__extract_quarter
              , nr_subq_14.ds_partitioned__extract_month
              , nr_subq_14.ds_partitioned__extract_day
              , nr_subq_14.ds_partitioned__extract_dow
              , nr_subq_14.ds_partitioned__extract_doy
              , nr_subq_14.last_profile_edit_ts__millisecond
              , nr_subq_14.last_profile_edit_ts__second
              , nr_subq_14.last_profile_edit_ts__minute
              , nr_subq_14.last_profile_edit_ts__hour
              , nr_subq_14.last_profile_edit_ts__day
              , nr_subq_14.last_profile_edit_ts__week
              , nr_subq_14.last_profile_edit_ts__month
              , nr_subq_14.last_profile_edit_ts__quarter
              , nr_subq_14.last_profile_edit_ts__year
              , nr_subq_14.last_profile_edit_ts__extract_year
              , nr_subq_14.last_profile_edit_ts__extract_quarter
              , nr_subq_14.last_profile_edit_ts__extract_month
              , nr_subq_14.last_profile_edit_ts__extract_day
              , nr_subq_14.last_profile_edit_ts__extract_dow
              , nr_subq_14.last_profile_edit_ts__extract_doy
              , nr_subq_14.bio_added_ts__second
              , nr_subq_14.bio_added_ts__minute
              , nr_subq_14.bio_added_ts__hour
              , nr_subq_14.bio_added_ts__day
              , nr_subq_14.bio_added_ts__week
              , nr_subq_14.bio_added_ts__month
              , nr_subq_14.bio_added_ts__quarter
              , nr_subq_14.bio_added_ts__year
              , nr_subq_14.bio_added_ts__extract_year
              , nr_subq_14.bio_added_ts__extract_quarter
              , nr_subq_14.bio_added_ts__extract_month
              , nr_subq_14.bio_added_ts__extract_day
              , nr_subq_14.bio_added_ts__extract_dow
              , nr_subq_14.bio_added_ts__extract_doy
              , nr_subq_14.last_login_ts__minute
              , nr_subq_14.last_login_ts__hour
              , nr_subq_14.last_login_ts__day
              , nr_subq_14.last_login_ts__week
              , nr_subq_14.last_login_ts__month
              , nr_subq_14.last_login_ts__quarter
              , nr_subq_14.last_login_ts__year
              , nr_subq_14.last_login_ts__extract_year
              , nr_subq_14.last_login_ts__extract_quarter
              , nr_subq_14.last_login_ts__extract_month
              , nr_subq_14.last_login_ts__extract_day
              , nr_subq_14.last_login_ts__extract_dow
              , nr_subq_14.last_login_ts__extract_doy
              , nr_subq_14.archived_at__hour
              , nr_subq_14.archived_at__day
              , nr_subq_14.archived_at__week
              , nr_subq_14.archived_at__month
              , nr_subq_14.archived_at__quarter
              , nr_subq_14.archived_at__year
              , nr_subq_14.archived_at__extract_year
              , nr_subq_14.archived_at__extract_quarter
              , nr_subq_14.archived_at__extract_month
              , nr_subq_14.archived_at__extract_day
              , nr_subq_14.archived_at__extract_dow
              , nr_subq_14.archived_at__extract_doy
              , nr_subq_14.user__ds__day
              , nr_subq_14.user__ds__week
              , nr_subq_14.user__ds__month
              , nr_subq_14.user__ds__quarter
              , nr_subq_14.user__ds__year
              , nr_subq_14.user__ds__extract_year
              , nr_subq_14.user__ds__extract_quarter
              , nr_subq_14.user__ds__extract_month
              , nr_subq_14.user__ds__extract_day
              , nr_subq_14.user__ds__extract_dow
              , nr_subq_14.user__ds__extract_doy
              , nr_subq_14.user__created_at__day
              , nr_subq_14.user__created_at__week
              , nr_subq_14.user__created_at__month
              , nr_subq_14.user__created_at__quarter
              , nr_subq_14.user__created_at__year
              , nr_subq_14.user__created_at__extract_year
              , nr_subq_14.user__created_at__extract_quarter
              , nr_subq_14.user__created_at__extract_month
              , nr_subq_14.user__created_at__extract_day
              , nr_subq_14.user__created_at__extract_dow
              , nr_subq_14.user__created_at__extract_doy
              , nr_subq_14.user__ds_partitioned__day
              , nr_subq_14.user__ds_partitioned__week
              , nr_subq_14.user__ds_partitioned__month
              , nr_subq_14.user__ds_partitioned__quarter
              , nr_subq_14.user__ds_partitioned__year
              , nr_subq_14.user__ds_partitioned__extract_year
              , nr_subq_14.user__ds_partitioned__extract_quarter
              , nr_subq_14.user__ds_partitioned__extract_month
              , nr_subq_14.user__ds_partitioned__extract_day
              , nr_subq_14.user__ds_partitioned__extract_dow
              , nr_subq_14.user__ds_partitioned__extract_doy
              , nr_subq_14.user__last_profile_edit_ts__millisecond
              , nr_subq_14.user__last_profile_edit_ts__second
              , nr_subq_14.user__last_profile_edit_ts__minute
              , nr_subq_14.user__last_profile_edit_ts__hour
              , nr_subq_14.user__last_profile_edit_ts__day
              , nr_subq_14.user__last_profile_edit_ts__week
              , nr_subq_14.user__last_profile_edit_ts__month
              , nr_subq_14.user__last_profile_edit_ts__quarter
              , nr_subq_14.user__last_profile_edit_ts__year
              , nr_subq_14.user__last_profile_edit_ts__extract_year
              , nr_subq_14.user__last_profile_edit_ts__extract_quarter
              , nr_subq_14.user__last_profile_edit_ts__extract_month
              , nr_subq_14.user__last_profile_edit_ts__extract_day
              , nr_subq_14.user__last_profile_edit_ts__extract_dow
              , nr_subq_14.user__last_profile_edit_ts__extract_doy
              , nr_subq_14.user__bio_added_ts__second
              , nr_subq_14.user__bio_added_ts__minute
              , nr_subq_14.user__bio_added_ts__hour
              , nr_subq_14.user__bio_added_ts__day
              , nr_subq_14.user__bio_added_ts__week
              , nr_subq_14.user__bio_added_ts__month
              , nr_subq_14.user__bio_added_ts__quarter
              , nr_subq_14.user__bio_added_ts__year
              , nr_subq_14.user__bio_added_ts__extract_year
              , nr_subq_14.user__bio_added_ts__extract_quarter
              , nr_subq_14.user__bio_added_ts__extract_month
              , nr_subq_14.user__bio_added_ts__extract_day
              , nr_subq_14.user__bio_added_ts__extract_dow
              , nr_subq_14.user__bio_added_ts__extract_doy
              , nr_subq_14.user__last_login_ts__minute
              , nr_subq_14.user__last_login_ts__hour
              , nr_subq_14.user__last_login_ts__day
              , nr_subq_14.user__last_login_ts__week
              , nr_subq_14.user__last_login_ts__month
              , nr_subq_14.user__last_login_ts__quarter
              , nr_subq_14.user__last_login_ts__year
              , nr_subq_14.user__last_login_ts__extract_year
              , nr_subq_14.user__last_login_ts__extract_quarter
              , nr_subq_14.user__last_login_ts__extract_month
              , nr_subq_14.user__last_login_ts__extract_day
              , nr_subq_14.user__last_login_ts__extract_dow
              , nr_subq_14.user__last_login_ts__extract_doy
              , nr_subq_14.user__archived_at__hour
              , nr_subq_14.user__archived_at__day
              , nr_subq_14.user__archived_at__week
              , nr_subq_14.user__archived_at__month
              , nr_subq_14.user__archived_at__quarter
              , nr_subq_14.user__archived_at__year
              , nr_subq_14.user__archived_at__extract_year
              , nr_subq_14.user__archived_at__extract_quarter
              , nr_subq_14.user__archived_at__extract_month
              , nr_subq_14.user__archived_at__extract_day
              , nr_subq_14.user__archived_at__extract_dow
              , nr_subq_14.user__archived_at__extract_doy
              , nr_subq_14.metric_time__day
              , nr_subq_14.metric_time__week
              , nr_subq_14.metric_time__month
              , nr_subq_14.metric_time__quarter
              , nr_subq_14.metric_time__year
              , nr_subq_14.metric_time__extract_year
              , nr_subq_14.metric_time__extract_quarter
              , nr_subq_14.metric_time__extract_month
              , nr_subq_14.metric_time__extract_day
              , nr_subq_14.metric_time__extract_dow
              , nr_subq_14.metric_time__extract_doy
              , nr_subq_14.user
              , nr_subq_14.home_state
              , nr_subq_14.user__home_state
            FROM (
              -- Metric Time Dimension 'created_at'
              SELECT
                nr_subq_28009.ds__day
                , nr_subq_28009.ds__week
                , nr_subq_28009.ds__month
                , nr_subq_28009.ds__quarter
                , nr_subq_28009.ds__year
                , nr_subq_28009.ds__extract_year
                , nr_subq_28009.ds__extract_quarter
                , nr_subq_28009.ds__extract_month
                , nr_subq_28009.ds__extract_day
                , nr_subq_28009.ds__extract_dow
                , nr_subq_28009.ds__extract_doy
                , nr_subq_28009.created_at__day
                , nr_subq_28009.created_at__week
                , nr_subq_28009.created_at__month
                , nr_subq_28009.created_at__quarter
                , nr_subq_28009.created_at__year
                , nr_subq_28009.created_at__extract_year
                , nr_subq_28009.created_at__extract_quarter
                , nr_subq_28009.created_at__extract_month
                , nr_subq_28009.created_at__extract_day
                , nr_subq_28009.created_at__extract_dow
                , nr_subq_28009.created_at__extract_doy
                , nr_subq_28009.ds_partitioned__day
                , nr_subq_28009.ds_partitioned__week
                , nr_subq_28009.ds_partitioned__month
                , nr_subq_28009.ds_partitioned__quarter
                , nr_subq_28009.ds_partitioned__year
                , nr_subq_28009.ds_partitioned__extract_year
                , nr_subq_28009.ds_partitioned__extract_quarter
                , nr_subq_28009.ds_partitioned__extract_month
                , nr_subq_28009.ds_partitioned__extract_day
                , nr_subq_28009.ds_partitioned__extract_dow
                , nr_subq_28009.ds_partitioned__extract_doy
                , nr_subq_28009.last_profile_edit_ts__millisecond
                , nr_subq_28009.last_profile_edit_ts__second
                , nr_subq_28009.last_profile_edit_ts__minute
                , nr_subq_28009.last_profile_edit_ts__hour
                , nr_subq_28009.last_profile_edit_ts__day
                , nr_subq_28009.last_profile_edit_ts__week
                , nr_subq_28009.last_profile_edit_ts__month
                , nr_subq_28009.last_profile_edit_ts__quarter
                , nr_subq_28009.last_profile_edit_ts__year
                , nr_subq_28009.last_profile_edit_ts__extract_year
                , nr_subq_28009.last_profile_edit_ts__extract_quarter
                , nr_subq_28009.last_profile_edit_ts__extract_month
                , nr_subq_28009.last_profile_edit_ts__extract_day
                , nr_subq_28009.last_profile_edit_ts__extract_dow
                , nr_subq_28009.last_profile_edit_ts__extract_doy
                , nr_subq_28009.bio_added_ts__second
                , nr_subq_28009.bio_added_ts__minute
                , nr_subq_28009.bio_added_ts__hour
                , nr_subq_28009.bio_added_ts__day
                , nr_subq_28009.bio_added_ts__week
                , nr_subq_28009.bio_added_ts__month
                , nr_subq_28009.bio_added_ts__quarter
                , nr_subq_28009.bio_added_ts__year
                , nr_subq_28009.bio_added_ts__extract_year
                , nr_subq_28009.bio_added_ts__extract_quarter
                , nr_subq_28009.bio_added_ts__extract_month
                , nr_subq_28009.bio_added_ts__extract_day
                , nr_subq_28009.bio_added_ts__extract_dow
                , nr_subq_28009.bio_added_ts__extract_doy
                , nr_subq_28009.last_login_ts__minute
                , nr_subq_28009.last_login_ts__hour
                , nr_subq_28009.last_login_ts__day
                , nr_subq_28009.last_login_ts__week
                , nr_subq_28009.last_login_ts__month
                , nr_subq_28009.last_login_ts__quarter
                , nr_subq_28009.last_login_ts__year
                , nr_subq_28009.last_login_ts__extract_year
                , nr_subq_28009.last_login_ts__extract_quarter
                , nr_subq_28009.last_login_ts__extract_month
                , nr_subq_28009.last_login_ts__extract_day
                , nr_subq_28009.last_login_ts__extract_dow
                , nr_subq_28009.last_login_ts__extract_doy
                , nr_subq_28009.archived_at__hour
                , nr_subq_28009.archived_at__day
                , nr_subq_28009.archived_at__week
                , nr_subq_28009.archived_at__month
                , nr_subq_28009.archived_at__quarter
                , nr_subq_28009.archived_at__year
                , nr_subq_28009.archived_at__extract_year
                , nr_subq_28009.archived_at__extract_quarter
                , nr_subq_28009.archived_at__extract_month
                , nr_subq_28009.archived_at__extract_day
                , nr_subq_28009.archived_at__extract_dow
                , nr_subq_28009.archived_at__extract_doy
                , nr_subq_28009.user__ds__day
                , nr_subq_28009.user__ds__week
                , nr_subq_28009.user__ds__month
                , nr_subq_28009.user__ds__quarter
                , nr_subq_28009.user__ds__year
                , nr_subq_28009.user__ds__extract_year
                , nr_subq_28009.user__ds__extract_quarter
                , nr_subq_28009.user__ds__extract_month
                , nr_subq_28009.user__ds__extract_day
                , nr_subq_28009.user__ds__extract_dow
                , nr_subq_28009.user__ds__extract_doy
                , nr_subq_28009.user__created_at__day
                , nr_subq_28009.user__created_at__week
                , nr_subq_28009.user__created_at__month
                , nr_subq_28009.user__created_at__quarter
                , nr_subq_28009.user__created_at__year
                , nr_subq_28009.user__created_at__extract_year
                , nr_subq_28009.user__created_at__extract_quarter
                , nr_subq_28009.user__created_at__extract_month
                , nr_subq_28009.user__created_at__extract_day
                , nr_subq_28009.user__created_at__extract_dow
                , nr_subq_28009.user__created_at__extract_doy
                , nr_subq_28009.user__ds_partitioned__day
                , nr_subq_28009.user__ds_partitioned__week
                , nr_subq_28009.user__ds_partitioned__month
                , nr_subq_28009.user__ds_partitioned__quarter
                , nr_subq_28009.user__ds_partitioned__year
                , nr_subq_28009.user__ds_partitioned__extract_year
                , nr_subq_28009.user__ds_partitioned__extract_quarter
                , nr_subq_28009.user__ds_partitioned__extract_month
                , nr_subq_28009.user__ds_partitioned__extract_day
                , nr_subq_28009.user__ds_partitioned__extract_dow
                , nr_subq_28009.user__ds_partitioned__extract_doy
                , nr_subq_28009.user__last_profile_edit_ts__millisecond
                , nr_subq_28009.user__last_profile_edit_ts__second
                , nr_subq_28009.user__last_profile_edit_ts__minute
                , nr_subq_28009.user__last_profile_edit_ts__hour
                , nr_subq_28009.user__last_profile_edit_ts__day
                , nr_subq_28009.user__last_profile_edit_ts__week
                , nr_subq_28009.user__last_profile_edit_ts__month
                , nr_subq_28009.user__last_profile_edit_ts__quarter
                , nr_subq_28009.user__last_profile_edit_ts__year
                , nr_subq_28009.user__last_profile_edit_ts__extract_year
                , nr_subq_28009.user__last_profile_edit_ts__extract_quarter
                , nr_subq_28009.user__last_profile_edit_ts__extract_month
                , nr_subq_28009.user__last_profile_edit_ts__extract_day
                , nr_subq_28009.user__last_profile_edit_ts__extract_dow
                , nr_subq_28009.user__last_profile_edit_ts__extract_doy
                , nr_subq_28009.user__bio_added_ts__second
                , nr_subq_28009.user__bio_added_ts__minute
                , nr_subq_28009.user__bio_added_ts__hour
                , nr_subq_28009.user__bio_added_ts__day
                , nr_subq_28009.user__bio_added_ts__week
                , nr_subq_28009.user__bio_added_ts__month
                , nr_subq_28009.user__bio_added_ts__quarter
                , nr_subq_28009.user__bio_added_ts__year
                , nr_subq_28009.user__bio_added_ts__extract_year
                , nr_subq_28009.user__bio_added_ts__extract_quarter
                , nr_subq_28009.user__bio_added_ts__extract_month
                , nr_subq_28009.user__bio_added_ts__extract_day
                , nr_subq_28009.user__bio_added_ts__extract_dow
                , nr_subq_28009.user__bio_added_ts__extract_doy
                , nr_subq_28009.user__last_login_ts__minute
                , nr_subq_28009.user__last_login_ts__hour
                , nr_subq_28009.user__last_login_ts__day
                , nr_subq_28009.user__last_login_ts__week
                , nr_subq_28009.user__last_login_ts__month
                , nr_subq_28009.user__last_login_ts__quarter
                , nr_subq_28009.user__last_login_ts__year
                , nr_subq_28009.user__last_login_ts__extract_year
                , nr_subq_28009.user__last_login_ts__extract_quarter
                , nr_subq_28009.user__last_login_ts__extract_month
                , nr_subq_28009.user__last_login_ts__extract_day
                , nr_subq_28009.user__last_login_ts__extract_dow
                , nr_subq_28009.user__last_login_ts__extract_doy
                , nr_subq_28009.user__archived_at__hour
                , nr_subq_28009.user__archived_at__day
                , nr_subq_28009.user__archived_at__week
                , nr_subq_28009.user__archived_at__month
                , nr_subq_28009.user__archived_at__quarter
                , nr_subq_28009.user__archived_at__year
                , nr_subq_28009.user__archived_at__extract_year
                , nr_subq_28009.user__archived_at__extract_quarter
                , nr_subq_28009.user__archived_at__extract_month
                , nr_subq_28009.user__archived_at__extract_day
                , nr_subq_28009.user__archived_at__extract_dow
                , nr_subq_28009.user__archived_at__extract_doy
                , nr_subq_28009.created_at__day AS metric_time__day
                , nr_subq_28009.created_at__week AS metric_time__week
                , nr_subq_28009.created_at__month AS metric_time__month
                , nr_subq_28009.created_at__quarter AS metric_time__quarter
                , nr_subq_28009.created_at__year AS metric_time__year
                , nr_subq_28009.created_at__extract_year AS metric_time__extract_year
                , nr_subq_28009.created_at__extract_quarter AS metric_time__extract_quarter
                , nr_subq_28009.created_at__extract_month AS metric_time__extract_month
                , nr_subq_28009.created_at__extract_day AS metric_time__extract_day
                , nr_subq_28009.created_at__extract_dow AS metric_time__extract_dow
                , nr_subq_28009.created_at__extract_doy AS metric_time__extract_doy
                , nr_subq_28009.user
                , nr_subq_28009.home_state
                , nr_subq_28009.user__home_state
                , nr_subq_28009.new_users
              FROM (
                -- Read Elements From Semantic Model 'users_ds_source'
                SELECT
                  1 AS new_users
                  , 1 AS archived_users
                  , DATE_TRUNC('day', users_ds_source_src_28000.ds) AS ds__day
                  , DATE_TRUNC('week', users_ds_source_src_28000.ds) AS ds__week
                  , DATE_TRUNC('month', users_ds_source_src_28000.ds) AS ds__month
                  , DATE_TRUNC('quarter', users_ds_source_src_28000.ds) AS ds__quarter
                  , DATE_TRUNC('year', users_ds_source_src_28000.ds) AS ds__year
                  , EXTRACT(year FROM users_ds_source_src_28000.ds) AS ds__extract_year
                  , EXTRACT(quarter FROM users_ds_source_src_28000.ds) AS ds__extract_quarter
                  , EXTRACT(month FROM users_ds_source_src_28000.ds) AS ds__extract_month
                  , EXTRACT(day FROM users_ds_source_src_28000.ds) AS ds__extract_day
                  , EXTRACT(isodow FROM users_ds_source_src_28000.ds) AS ds__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.ds) AS ds__extract_doy
                  , DATE_TRUNC('day', users_ds_source_src_28000.created_at) AS created_at__day
                  , DATE_TRUNC('week', users_ds_source_src_28000.created_at) AS created_at__week
                  , DATE_TRUNC('month', users_ds_source_src_28000.created_at) AS created_at__month
                  , DATE_TRUNC('quarter', users_ds_source_src_28000.created_at) AS created_at__quarter
                  , DATE_TRUNC('year', users_ds_source_src_28000.created_at) AS created_at__year
                  , EXTRACT(year FROM users_ds_source_src_28000.created_at) AS created_at__extract_year
                  , EXTRACT(quarter FROM users_ds_source_src_28000.created_at) AS created_at__extract_quarter
                  , EXTRACT(month FROM users_ds_source_src_28000.created_at) AS created_at__extract_month
                  , EXTRACT(day FROM users_ds_source_src_28000.created_at) AS created_at__extract_day
                  , EXTRACT(isodow FROM users_ds_source_src_28000.created_at) AS created_at__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.created_at) AS created_at__extract_doy
                  , DATE_TRUNC('day', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__day
                  , DATE_TRUNC('week', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__week
                  , DATE_TRUNC('month', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__month
                  , DATE_TRUNC('quarter', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                  , DATE_TRUNC('year', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__year
                  , EXTRACT(year FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                  , EXTRACT(quarter FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                  , EXTRACT(month FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                  , EXTRACT(day FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                  , EXTRACT(isodow FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                  , users_ds_source_src_28000.home_state
                  , DATE_TRUNC('millisecond', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__millisecond
                  , DATE_TRUNC('second', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__second
                  , DATE_TRUNC('minute', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__minute
                  , DATE_TRUNC('hour', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__hour
                  , DATE_TRUNC('day', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__day
                  , DATE_TRUNC('week', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__week
                  , DATE_TRUNC('month', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__month
                  , DATE_TRUNC('quarter', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__quarter
                  , DATE_TRUNC('year', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__year
                  , EXTRACT(year FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_year
                  , EXTRACT(quarter FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_quarter
                  , EXTRACT(month FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_month
                  , EXTRACT(day FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_day
                  , EXTRACT(isodow FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_doy
                  , DATE_TRUNC('second', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__second
                  , DATE_TRUNC('minute', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__minute
                  , DATE_TRUNC('hour', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__hour
                  , DATE_TRUNC('day', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__day
                  , DATE_TRUNC('week', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__week
                  , DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__month
                  , DATE_TRUNC('quarter', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__quarter
                  , DATE_TRUNC('year', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__year
                  , EXTRACT(year FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_year
                  , EXTRACT(quarter FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_quarter
                  , EXTRACT(month FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_month
                  , EXTRACT(day FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_day
                  , EXTRACT(isodow FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_doy
                  , DATE_TRUNC('minute', users_ds_source_src_28000.last_login_ts) AS last_login_ts__minute
                  , DATE_TRUNC('hour', users_ds_source_src_28000.last_login_ts) AS last_login_ts__hour
                  , DATE_TRUNC('day', users_ds_source_src_28000.last_login_ts) AS last_login_ts__day
                  , DATE_TRUNC('week', users_ds_source_src_28000.last_login_ts) AS last_login_ts__week
                  , DATE_TRUNC('month', users_ds_source_src_28000.last_login_ts) AS last_login_ts__month
                  , DATE_TRUNC('quarter', users_ds_source_src_28000.last_login_ts) AS last_login_ts__quarter
                  , DATE_TRUNC('year', users_ds_source_src_28000.last_login_ts) AS last_login_ts__year
                  , EXTRACT(year FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_year
                  , EXTRACT(quarter FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_quarter
                  , EXTRACT(month FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_month
                  , EXTRACT(day FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_day
                  , EXTRACT(isodow FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_doy
                  , DATE_TRUNC('hour', users_ds_source_src_28000.archived_at) AS archived_at__hour
                  , DATE_TRUNC('day', users_ds_source_src_28000.archived_at) AS archived_at__day
                  , DATE_TRUNC('week', users_ds_source_src_28000.archived_at) AS archived_at__week
                  , DATE_TRUNC('month', users_ds_source_src_28000.archived_at) AS archived_at__month
                  , DATE_TRUNC('quarter', users_ds_source_src_28000.archived_at) AS archived_at__quarter
                  , DATE_TRUNC('year', users_ds_source_src_28000.archived_at) AS archived_at__year
                  , EXTRACT(year FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_year
                  , EXTRACT(quarter FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_quarter
                  , EXTRACT(month FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_month
                  , EXTRACT(day FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_day
                  , EXTRACT(isodow FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_doy
                  , DATE_TRUNC('day', users_ds_source_src_28000.ds) AS user__ds__day
                  , DATE_TRUNC('week', users_ds_source_src_28000.ds) AS user__ds__week
                  , DATE_TRUNC('month', users_ds_source_src_28000.ds) AS user__ds__month
                  , DATE_TRUNC('quarter', users_ds_source_src_28000.ds) AS user__ds__quarter
                  , DATE_TRUNC('year', users_ds_source_src_28000.ds) AS user__ds__year
                  , EXTRACT(year FROM users_ds_source_src_28000.ds) AS user__ds__extract_year
                  , EXTRACT(quarter FROM users_ds_source_src_28000.ds) AS user__ds__extract_quarter
                  , EXTRACT(month FROM users_ds_source_src_28000.ds) AS user__ds__extract_month
                  , EXTRACT(day FROM users_ds_source_src_28000.ds) AS user__ds__extract_day
                  , EXTRACT(isodow FROM users_ds_source_src_28000.ds) AS user__ds__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.ds) AS user__ds__extract_doy
                  , DATE_TRUNC('day', users_ds_source_src_28000.created_at) AS user__created_at__day
                  , DATE_TRUNC('week', users_ds_source_src_28000.created_at) AS user__created_at__week
                  , DATE_TRUNC('month', users_ds_source_src_28000.created_at) AS user__created_at__month
                  , DATE_TRUNC('quarter', users_ds_source_src_28000.created_at) AS user__created_at__quarter
                  , DATE_TRUNC('year', users_ds_source_src_28000.created_at) AS user__created_at__year
                  , EXTRACT(year FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_year
                  , EXTRACT(quarter FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_quarter
                  , EXTRACT(month FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_month
                  , EXTRACT(day FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_day
                  , EXTRACT(isodow FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_doy
                  , DATE_TRUNC('day', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__day
                  , DATE_TRUNC('week', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__week
                  , DATE_TRUNC('month', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__month
                  , DATE_TRUNC('quarter', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__quarter
                  , DATE_TRUNC('year', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__year
                  , EXTRACT(year FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_year
                  , EXTRACT(quarter FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_quarter
                  , EXTRACT(month FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_month
                  , EXTRACT(day FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_day
                  , EXTRACT(isodow FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_doy
                  , users_ds_source_src_28000.home_state AS user__home_state
                  , DATE_TRUNC('millisecond', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__millisecond
                  , DATE_TRUNC('second', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__second
                  , DATE_TRUNC('minute', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__minute
                  , DATE_TRUNC('hour', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__hour
                  , DATE_TRUNC('day', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__day
                  , DATE_TRUNC('week', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__week
                  , DATE_TRUNC('month', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__month
                  , DATE_TRUNC('quarter', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__quarter
                  , DATE_TRUNC('year', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__year
                  , EXTRACT(year FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_year
                  , EXTRACT(quarter FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_quarter
                  , EXTRACT(month FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_month
                  , EXTRACT(day FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_day
                  , EXTRACT(isodow FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_doy
                  , DATE_TRUNC('second', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__second
                  , DATE_TRUNC('minute', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__minute
                  , DATE_TRUNC('hour', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__hour
                  , DATE_TRUNC('day', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__day
                  , DATE_TRUNC('week', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__week
                  , DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__month
                  , DATE_TRUNC('quarter', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__quarter
                  , DATE_TRUNC('year', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__year
                  , EXTRACT(year FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_year
                  , EXTRACT(quarter FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_quarter
                  , EXTRACT(month FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_month
                  , EXTRACT(day FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_day
                  , EXTRACT(isodow FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_doy
                  , DATE_TRUNC('minute', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__minute
                  , DATE_TRUNC('hour', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__hour
                  , DATE_TRUNC('day', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__day
                  , DATE_TRUNC('week', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__week
                  , DATE_TRUNC('month', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__month
                  , DATE_TRUNC('quarter', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__quarter
                  , DATE_TRUNC('year', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__year
                  , EXTRACT(year FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_year
                  , EXTRACT(quarter FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_quarter
                  , EXTRACT(month FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_month
                  , EXTRACT(day FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_day
                  , EXTRACT(isodow FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_doy
                  , DATE_TRUNC('hour', users_ds_source_src_28000.archived_at) AS user__archived_at__hour
                  , DATE_TRUNC('day', users_ds_source_src_28000.archived_at) AS user__archived_at__day
                  , DATE_TRUNC('week', users_ds_source_src_28000.archived_at) AS user__archived_at__week
                  , DATE_TRUNC('month', users_ds_source_src_28000.archived_at) AS user__archived_at__month
                  , DATE_TRUNC('quarter', users_ds_source_src_28000.archived_at) AS user__archived_at__quarter
                  , DATE_TRUNC('year', users_ds_source_src_28000.archived_at) AS user__archived_at__year
                  , EXTRACT(year FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_year
                  , EXTRACT(quarter FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_quarter
                  , EXTRACT(month FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_month
                  , EXTRACT(day FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_day
                  , EXTRACT(isodow FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_doy
                  , users_ds_source_src_28000.user_id AS user
                FROM ***************************.dim_users users_ds_source_src_28000
              ) nr_subq_28009
            ) nr_subq_14
          ) nr_subq_15
          ON
            nr_subq_13.user = nr_subq_15.user
        ) nr_subq_16
      ) nr_subq_17
      ON
        (
          nr_subq_12.listing = nr_subq_17.listing
        ) AND (
          nr_subq_12.ds_partitioned__day = nr_subq_17.user__ds_partitioned__day
        )
    ) nr_subq_18
  ) nr_subq_19
  GROUP BY
    nr_subq_19.listing__user__bio_added_ts__minute
) nr_subq_20
