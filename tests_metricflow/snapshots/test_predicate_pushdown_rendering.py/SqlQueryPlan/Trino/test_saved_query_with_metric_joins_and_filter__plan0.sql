-- Combine Aggregated Outputs
SELECT
  COALESCE(subq_9.listing__capacity_latest, subq_19.listing__capacity_latest, subq_41.listing__capacity_latest) AS listing__capacity_latest
  , MAX(subq_9.bookings) AS bookings
  , MAX(subq_19.views) AS views
  , MAX(subq_41.bookings_per_view) AS bookings_per_view
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_8.listing__capacity_latest
    , subq_8.bookings
  FROM (
    -- Aggregate Measures
    SELECT
      subq_7.listing__capacity_latest
      , SUM(subq_7.bookings) AS bookings
    FROM (
      -- Pass Only Elements: ['bookings', 'listing__capacity_latest']
      SELECT
        subq_6.listing__capacity_latest
        , subq_6.bookings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_5.ds__day
          , subq_5.ds__week
          , subq_5.ds__month
          , subq_5.ds__quarter
          , subq_5.ds__year
          , subq_5.ds__extract_year
          , subq_5.ds__extract_quarter
          , subq_5.ds__extract_month
          , subq_5.ds__extract_day
          , subq_5.ds__extract_dow
          , subq_5.ds__extract_doy
          , subq_5.ds_partitioned__day
          , subq_5.ds_partitioned__week
          , subq_5.ds_partitioned__month
          , subq_5.ds_partitioned__quarter
          , subq_5.ds_partitioned__year
          , subq_5.ds_partitioned__extract_year
          , subq_5.ds_partitioned__extract_quarter
          , subq_5.ds_partitioned__extract_month
          , subq_5.ds_partitioned__extract_day
          , subq_5.ds_partitioned__extract_dow
          , subq_5.ds_partitioned__extract_doy
          , subq_5.paid_at__day
          , subq_5.paid_at__week
          , subq_5.paid_at__month
          , subq_5.paid_at__quarter
          , subq_5.paid_at__year
          , subq_5.paid_at__extract_year
          , subq_5.paid_at__extract_quarter
          , subq_5.paid_at__extract_month
          , subq_5.paid_at__extract_day
          , subq_5.paid_at__extract_dow
          , subq_5.paid_at__extract_doy
          , subq_5.booking__ds__day
          , subq_5.booking__ds__week
          , subq_5.booking__ds__month
          , subq_5.booking__ds__quarter
          , subq_5.booking__ds__year
          , subq_5.booking__ds__extract_year
          , subq_5.booking__ds__extract_quarter
          , subq_5.booking__ds__extract_month
          , subq_5.booking__ds__extract_day
          , subq_5.booking__ds__extract_dow
          , subq_5.booking__ds__extract_doy
          , subq_5.booking__ds_partitioned__day
          , subq_5.booking__ds_partitioned__week
          , subq_5.booking__ds_partitioned__month
          , subq_5.booking__ds_partitioned__quarter
          , subq_5.booking__ds_partitioned__year
          , subq_5.booking__ds_partitioned__extract_year
          , subq_5.booking__ds_partitioned__extract_quarter
          , subq_5.booking__ds_partitioned__extract_month
          , subq_5.booking__ds_partitioned__extract_day
          , subq_5.booking__ds_partitioned__extract_dow
          , subq_5.booking__ds_partitioned__extract_doy
          , subq_5.booking__paid_at__day
          , subq_5.booking__paid_at__week
          , subq_5.booking__paid_at__month
          , subq_5.booking__paid_at__quarter
          , subq_5.booking__paid_at__year
          , subq_5.booking__paid_at__extract_year
          , subq_5.booking__paid_at__extract_quarter
          , subq_5.booking__paid_at__extract_month
          , subq_5.booking__paid_at__extract_day
          , subq_5.booking__paid_at__extract_dow
          , subq_5.booking__paid_at__extract_doy
          , subq_5.metric_time__day
          , subq_5.metric_time__week
          , subq_5.metric_time__month
          , subq_5.metric_time__quarter
          , subq_5.metric_time__year
          , subq_5.metric_time__extract_year
          , subq_5.metric_time__extract_quarter
          , subq_5.metric_time__extract_month
          , subq_5.metric_time__extract_day
          , subq_5.metric_time__extract_dow
          , subq_5.metric_time__extract_doy
          , subq_5.listing
          , subq_5.guest
          , subq_5.host
          , subq_5.booking__listing
          , subq_5.booking__guest
          , subq_5.booking__host
          , subq_5.is_instant
          , subq_5.booking__is_instant
          , subq_5.listing__is_lux_latest
          , subq_5.listing__capacity_latest
          , subq_5.bookings
          , subq_5.instant_bookings
          , subq_5.booking_value
          , subq_5.max_booking_value
          , subq_5.min_booking_value
          , subq_5.bookers
          , subq_5.average_booking_value
          , subq_5.referred_bookings
          , subq_5.median_booking_value
          , subq_5.booking_value_p99
          , subq_5.discrete_booking_value_p99
          , subq_5.approximate_continuous_booking_value_p99
          , subq_5.approximate_discrete_booking_value_p99
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_4.is_lux_latest AS listing__is_lux_latest
            , subq_4.capacity_latest AS listing__capacity_latest
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
            , subq_1.booking__ds__quarter AS booking__ds__quarter
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
            , subq_1.metric_time__day AS metric_time__day
            , subq_1.metric_time__week AS metric_time__week
            , subq_1.metric_time__month AS metric_time__month
            , subq_1.metric_time__quarter AS metric_time__quarter
            , subq_1.metric_time__year AS metric_time__year
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
                , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
                , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
                , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
                , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
                , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
                , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
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
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['capacity_latest', 'is_lux_latest', 'listing', 'listing']
            SELECT
              subq_3.listing
              , subq_3.is_lux_latest
              , subq_3.capacity_latest
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
                , subq_2.created_at__day
                , subq_2.created_at__week
                , subq_2.created_at__month
                , subq_2.created_at__quarter
                , subq_2.created_at__year
                , subq_2.created_at__extract_year
                , subq_2.created_at__extract_quarter
                , subq_2.created_at__extract_month
                , subq_2.created_at__extract_day
                , subq_2.created_at__extract_dow
                , subq_2.created_at__extract_doy
                , subq_2.listing__ds__day
                , subq_2.listing__ds__week
                , subq_2.listing__ds__month
                , subq_2.listing__ds__quarter
                , subq_2.listing__ds__year
                , subq_2.listing__ds__extract_year
                , subq_2.listing__ds__extract_quarter
                , subq_2.listing__ds__extract_month
                , subq_2.listing__ds__extract_day
                , subq_2.listing__ds__extract_dow
                , subq_2.listing__ds__extract_doy
                , subq_2.listing__created_at__day
                , subq_2.listing__created_at__week
                , subq_2.listing__created_at__month
                , subq_2.listing__created_at__quarter
                , subq_2.listing__created_at__year
                , subq_2.listing__created_at__extract_year
                , subq_2.listing__created_at__extract_quarter
                , subq_2.listing__created_at__extract_month
                , subq_2.listing__created_at__extract_day
                , subq_2.listing__created_at__extract_dow
                , subq_2.listing__created_at__extract_doy
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
                , subq_2.user
                , subq_2.listing__user
                , subq_2.country_latest
                , subq_2.is_lux_latest
                , subq_2.capacity_latest
                , subq_2.listing__country_latest
                , subq_2.listing__is_lux_latest
                , subq_2.listing__capacity_latest
                , subq_2.listings
                , subq_2.largest_listing
                , subq_2.smallest_listing
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
                  , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS ds__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
                  , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                  , listings_latest_src_28000.country AS listing__country_latest
                  , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                  , listings_latest_src_28000.capacity AS listing__capacity_latest
                  , listings_latest_src_28000.listing_id AS listing
                  , listings_latest_src_28000.user_id AS user
                  , listings_latest_src_28000.user_id AS listing__user
                FROM ***************************.dim_listings_latest listings_latest_src_28000
              ) subq_2
            ) subq_3
          ) subq_4
          ON
            subq_1.listing = subq_4.listing
        ) subq_5
        WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
      ) subq_6
    ) subq_7
    GROUP BY
      subq_7.listing__capacity_latest
  ) subq_8
) subq_9
FULL OUTER JOIN (
  -- Compute Metrics via Expressions
  SELECT
    subq_18.listing__capacity_latest
    , subq_18.views
  FROM (
    -- Aggregate Measures
    SELECT
      subq_17.listing__capacity_latest
      , SUM(subq_17.views) AS views
    FROM (
      -- Pass Only Elements: ['views', 'listing__capacity_latest']
      SELECT
        subq_16.listing__capacity_latest
        , subq_16.views
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_15.ds__day
          , subq_15.ds__week
          , subq_15.ds__month
          , subq_15.ds__quarter
          , subq_15.ds__year
          , subq_15.ds__extract_year
          , subq_15.ds__extract_quarter
          , subq_15.ds__extract_month
          , subq_15.ds__extract_day
          , subq_15.ds__extract_dow
          , subq_15.ds__extract_doy
          , subq_15.ds_partitioned__day
          , subq_15.ds_partitioned__week
          , subq_15.ds_partitioned__month
          , subq_15.ds_partitioned__quarter
          , subq_15.ds_partitioned__year
          , subq_15.ds_partitioned__extract_year
          , subq_15.ds_partitioned__extract_quarter
          , subq_15.ds_partitioned__extract_month
          , subq_15.ds_partitioned__extract_day
          , subq_15.ds_partitioned__extract_dow
          , subq_15.ds_partitioned__extract_doy
          , subq_15.view__ds__day
          , subq_15.view__ds__week
          , subq_15.view__ds__month
          , subq_15.view__ds__quarter
          , subq_15.view__ds__year
          , subq_15.view__ds__extract_year
          , subq_15.view__ds__extract_quarter
          , subq_15.view__ds__extract_month
          , subq_15.view__ds__extract_day
          , subq_15.view__ds__extract_dow
          , subq_15.view__ds__extract_doy
          , subq_15.view__ds_partitioned__day
          , subq_15.view__ds_partitioned__week
          , subq_15.view__ds_partitioned__month
          , subq_15.view__ds_partitioned__quarter
          , subq_15.view__ds_partitioned__year
          , subq_15.view__ds_partitioned__extract_year
          , subq_15.view__ds_partitioned__extract_quarter
          , subq_15.view__ds_partitioned__extract_month
          , subq_15.view__ds_partitioned__extract_day
          , subq_15.view__ds_partitioned__extract_dow
          , subq_15.view__ds_partitioned__extract_doy
          , subq_15.metric_time__day
          , subq_15.metric_time__week
          , subq_15.metric_time__month
          , subq_15.metric_time__quarter
          , subq_15.metric_time__year
          , subq_15.metric_time__extract_year
          , subq_15.metric_time__extract_quarter
          , subq_15.metric_time__extract_month
          , subq_15.metric_time__extract_day
          , subq_15.metric_time__extract_dow
          , subq_15.metric_time__extract_doy
          , subq_15.listing
          , subq_15.user
          , subq_15.view__listing
          , subq_15.view__user
          , subq_15.listing__is_lux_latest
          , subq_15.listing__capacity_latest
          , subq_15.views
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_14.is_lux_latest AS listing__is_lux_latest
            , subq_14.capacity_latest AS listing__capacity_latest
            , subq_11.ds__day AS ds__day
            , subq_11.ds__week AS ds__week
            , subq_11.ds__month AS ds__month
            , subq_11.ds__quarter AS ds__quarter
            , subq_11.ds__year AS ds__year
            , subq_11.ds__extract_year AS ds__extract_year
            , subq_11.ds__extract_quarter AS ds__extract_quarter
            , subq_11.ds__extract_month AS ds__extract_month
            , subq_11.ds__extract_day AS ds__extract_day
            , subq_11.ds__extract_dow AS ds__extract_dow
            , subq_11.ds__extract_doy AS ds__extract_doy
            , subq_11.ds_partitioned__day AS ds_partitioned__day
            , subq_11.ds_partitioned__week AS ds_partitioned__week
            , subq_11.ds_partitioned__month AS ds_partitioned__month
            , subq_11.ds_partitioned__quarter AS ds_partitioned__quarter
            , subq_11.ds_partitioned__year AS ds_partitioned__year
            , subq_11.ds_partitioned__extract_year AS ds_partitioned__extract_year
            , subq_11.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
            , subq_11.ds_partitioned__extract_month AS ds_partitioned__extract_month
            , subq_11.ds_partitioned__extract_day AS ds_partitioned__extract_day
            , subq_11.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
            , subq_11.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
            , subq_11.view__ds__day AS view__ds__day
            , subq_11.view__ds__week AS view__ds__week
            , subq_11.view__ds__month AS view__ds__month
            , subq_11.view__ds__quarter AS view__ds__quarter
            , subq_11.view__ds__year AS view__ds__year
            , subq_11.view__ds__extract_year AS view__ds__extract_year
            , subq_11.view__ds__extract_quarter AS view__ds__extract_quarter
            , subq_11.view__ds__extract_month AS view__ds__extract_month
            , subq_11.view__ds__extract_day AS view__ds__extract_day
            , subq_11.view__ds__extract_dow AS view__ds__extract_dow
            , subq_11.view__ds__extract_doy AS view__ds__extract_doy
            , subq_11.view__ds_partitioned__day AS view__ds_partitioned__day
            , subq_11.view__ds_partitioned__week AS view__ds_partitioned__week
            , subq_11.view__ds_partitioned__month AS view__ds_partitioned__month
            , subq_11.view__ds_partitioned__quarter AS view__ds_partitioned__quarter
            , subq_11.view__ds_partitioned__year AS view__ds_partitioned__year
            , subq_11.view__ds_partitioned__extract_year AS view__ds_partitioned__extract_year
            , subq_11.view__ds_partitioned__extract_quarter AS view__ds_partitioned__extract_quarter
            , subq_11.view__ds_partitioned__extract_month AS view__ds_partitioned__extract_month
            , subq_11.view__ds_partitioned__extract_day AS view__ds_partitioned__extract_day
            , subq_11.view__ds_partitioned__extract_dow AS view__ds_partitioned__extract_dow
            , subq_11.view__ds_partitioned__extract_doy AS view__ds_partitioned__extract_doy
            , subq_11.metric_time__day AS metric_time__day
            , subq_11.metric_time__week AS metric_time__week
            , subq_11.metric_time__month AS metric_time__month
            , subq_11.metric_time__quarter AS metric_time__quarter
            , subq_11.metric_time__year AS metric_time__year
            , subq_11.metric_time__extract_year AS metric_time__extract_year
            , subq_11.metric_time__extract_quarter AS metric_time__extract_quarter
            , subq_11.metric_time__extract_month AS metric_time__extract_month
            , subq_11.metric_time__extract_day AS metric_time__extract_day
            , subq_11.metric_time__extract_dow AS metric_time__extract_dow
            , subq_11.metric_time__extract_doy AS metric_time__extract_doy
            , subq_11.listing AS listing
            , subq_11.user AS user
            , subq_11.view__listing AS view__listing
            , subq_11.view__user AS view__user
            , subq_11.views AS views
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
              , subq_10.view__ds__day
              , subq_10.view__ds__week
              , subq_10.view__ds__month
              , subq_10.view__ds__quarter
              , subq_10.view__ds__year
              , subq_10.view__ds__extract_year
              , subq_10.view__ds__extract_quarter
              , subq_10.view__ds__extract_month
              , subq_10.view__ds__extract_day
              , subq_10.view__ds__extract_dow
              , subq_10.view__ds__extract_doy
              , subq_10.view__ds_partitioned__day
              , subq_10.view__ds_partitioned__week
              , subq_10.view__ds_partitioned__month
              , subq_10.view__ds_partitioned__quarter
              , subq_10.view__ds_partitioned__year
              , subq_10.view__ds_partitioned__extract_year
              , subq_10.view__ds_partitioned__extract_quarter
              , subq_10.view__ds_partitioned__extract_month
              , subq_10.view__ds_partitioned__extract_day
              , subq_10.view__ds_partitioned__extract_dow
              , subq_10.view__ds_partitioned__extract_doy
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
              , subq_10.user
              , subq_10.view__listing
              , subq_10.view__user
              , subq_10.views
            FROM (
              -- Read Elements From Semantic Model 'views_source'
              SELECT
                1 AS views
                , DATE_TRUNC('day', views_source_src_28000.ds) AS ds__day
                , DATE_TRUNC('week', views_source_src_28000.ds) AS ds__week
                , DATE_TRUNC('month', views_source_src_28000.ds) AS ds__month
                , DATE_TRUNC('quarter', views_source_src_28000.ds) AS ds__quarter
                , DATE_TRUNC('year', views_source_src_28000.ds) AS ds__year
                , EXTRACT(year FROM views_source_src_28000.ds) AS ds__extract_year
                , EXTRACT(quarter FROM views_source_src_28000.ds) AS ds__extract_quarter
                , EXTRACT(month FROM views_source_src_28000.ds) AS ds__extract_month
                , EXTRACT(day FROM views_source_src_28000.ds) AS ds__extract_day
                , EXTRACT(DAY_OF_WEEK FROM views_source_src_28000.ds) AS ds__extract_dow
                , EXTRACT(doy FROM views_source_src_28000.ds) AS ds__extract_doy
                , DATE_TRUNC('day', views_source_src_28000.ds_partitioned) AS ds_partitioned__day
                , DATE_TRUNC('week', views_source_src_28000.ds_partitioned) AS ds_partitioned__week
                , DATE_TRUNC('month', views_source_src_28000.ds_partitioned) AS ds_partitioned__month
                , DATE_TRUNC('quarter', views_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                , DATE_TRUNC('year', views_source_src_28000.ds_partitioned) AS ds_partitioned__year
                , EXTRACT(year FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                , EXTRACT(quarter FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                , EXTRACT(month FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                , EXTRACT(day FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                , EXTRACT(DAY_OF_WEEK FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
                , EXTRACT(doy FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                , DATE_TRUNC('day', views_source_src_28000.ds) AS view__ds__day
                , DATE_TRUNC('week', views_source_src_28000.ds) AS view__ds__week
                , DATE_TRUNC('month', views_source_src_28000.ds) AS view__ds__month
                , DATE_TRUNC('quarter', views_source_src_28000.ds) AS view__ds__quarter
                , DATE_TRUNC('year', views_source_src_28000.ds) AS view__ds__year
                , EXTRACT(year FROM views_source_src_28000.ds) AS view__ds__extract_year
                , EXTRACT(quarter FROM views_source_src_28000.ds) AS view__ds__extract_quarter
                , EXTRACT(month FROM views_source_src_28000.ds) AS view__ds__extract_month
                , EXTRACT(day FROM views_source_src_28000.ds) AS view__ds__extract_day
                , EXTRACT(DAY_OF_WEEK FROM views_source_src_28000.ds) AS view__ds__extract_dow
                , EXTRACT(doy FROM views_source_src_28000.ds) AS view__ds__extract_doy
                , DATE_TRUNC('day', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__day
                , DATE_TRUNC('week', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__week
                , DATE_TRUNC('month', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__month
                , DATE_TRUNC('quarter', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__quarter
                , DATE_TRUNC('year', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__year
                , EXTRACT(year FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_year
                , EXTRACT(quarter FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_quarter
                , EXTRACT(month FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_month
                , EXTRACT(day FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_day
                , EXTRACT(DAY_OF_WEEK FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_dow
                , EXTRACT(doy FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_doy
                , views_source_src_28000.listing_id AS listing
                , views_source_src_28000.user_id AS user
                , views_source_src_28000.listing_id AS view__listing
                , views_source_src_28000.user_id AS view__user
              FROM ***************************.fct_views views_source_src_28000
            ) subq_10
          ) subq_11
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['capacity_latest', 'is_lux_latest', 'listing', 'listing']
            SELECT
              subq_13.listing
              , subq_13.is_lux_latest
              , subq_13.capacity_latest
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_12.ds__day
                , subq_12.ds__week
                , subq_12.ds__month
                , subq_12.ds__quarter
                , subq_12.ds__year
                , subq_12.ds__extract_year
                , subq_12.ds__extract_quarter
                , subq_12.ds__extract_month
                , subq_12.ds__extract_day
                , subq_12.ds__extract_dow
                , subq_12.ds__extract_doy
                , subq_12.created_at__day
                , subq_12.created_at__week
                , subq_12.created_at__month
                , subq_12.created_at__quarter
                , subq_12.created_at__year
                , subq_12.created_at__extract_year
                , subq_12.created_at__extract_quarter
                , subq_12.created_at__extract_month
                , subq_12.created_at__extract_day
                , subq_12.created_at__extract_dow
                , subq_12.created_at__extract_doy
                , subq_12.listing__ds__day
                , subq_12.listing__ds__week
                , subq_12.listing__ds__month
                , subq_12.listing__ds__quarter
                , subq_12.listing__ds__year
                , subq_12.listing__ds__extract_year
                , subq_12.listing__ds__extract_quarter
                , subq_12.listing__ds__extract_month
                , subq_12.listing__ds__extract_day
                , subq_12.listing__ds__extract_dow
                , subq_12.listing__ds__extract_doy
                , subq_12.listing__created_at__day
                , subq_12.listing__created_at__week
                , subq_12.listing__created_at__month
                , subq_12.listing__created_at__quarter
                , subq_12.listing__created_at__year
                , subq_12.listing__created_at__extract_year
                , subq_12.listing__created_at__extract_quarter
                , subq_12.listing__created_at__extract_month
                , subq_12.listing__created_at__extract_day
                , subq_12.listing__created_at__extract_dow
                , subq_12.listing__created_at__extract_doy
                , subq_12.ds__day AS metric_time__day
                , subq_12.ds__week AS metric_time__week
                , subq_12.ds__month AS metric_time__month
                , subq_12.ds__quarter AS metric_time__quarter
                , subq_12.ds__year AS metric_time__year
                , subq_12.ds__extract_year AS metric_time__extract_year
                , subq_12.ds__extract_quarter AS metric_time__extract_quarter
                , subq_12.ds__extract_month AS metric_time__extract_month
                , subq_12.ds__extract_day AS metric_time__extract_day
                , subq_12.ds__extract_dow AS metric_time__extract_dow
                , subq_12.ds__extract_doy AS metric_time__extract_doy
                , subq_12.listing
                , subq_12.user
                , subq_12.listing__user
                , subq_12.country_latest
                , subq_12.is_lux_latest
                , subq_12.capacity_latest
                , subq_12.listing__country_latest
                , subq_12.listing__is_lux_latest
                , subq_12.listing__capacity_latest
                , subq_12.listings
                , subq_12.largest_listing
                , subq_12.smallest_listing
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
                  , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS ds__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
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
                  , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
                  , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                  , listings_latest_src_28000.country AS listing__country_latest
                  , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                  , listings_latest_src_28000.capacity AS listing__capacity_latest
                  , listings_latest_src_28000.listing_id AS listing
                  , listings_latest_src_28000.user_id AS user
                  , listings_latest_src_28000.user_id AS listing__user
                FROM ***************************.dim_listings_latest listings_latest_src_28000
              ) subq_12
            ) subq_13
          ) subq_14
          ON
            subq_11.listing = subq_14.listing
        ) subq_15
        WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
      ) subq_16
    ) subq_17
    GROUP BY
      subq_17.listing__capacity_latest
  ) subq_18
) subq_19
ON
  subq_9.listing__capacity_latest = subq_19.listing__capacity_latest
FULL OUTER JOIN (
  -- Compute Metrics via Expressions
  SELECT
    subq_40.listing__capacity_latest
    , CAST(subq_40.bookings AS DOUBLE) / CAST(NULLIF(subq_40.views, 0) AS DOUBLE) AS bookings_per_view
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_29.listing__capacity_latest, subq_39.listing__capacity_latest) AS listing__capacity_latest
      , MAX(subq_29.bookings) AS bookings
      , MAX(subq_39.views) AS views
    FROM (
      -- Compute Metrics via Expressions
      SELECT
        subq_28.listing__capacity_latest
        , subq_28.bookings
      FROM (
        -- Aggregate Measures
        SELECT
          subq_27.listing__capacity_latest
          , SUM(subq_27.bookings) AS bookings
        FROM (
          -- Pass Only Elements: ['bookings', 'listing__capacity_latest']
          SELECT
            subq_26.listing__capacity_latest
            , subq_26.bookings
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_25.ds__day
              , subq_25.ds__week
              , subq_25.ds__month
              , subq_25.ds__quarter
              , subq_25.ds__year
              , subq_25.ds__extract_year
              , subq_25.ds__extract_quarter
              , subq_25.ds__extract_month
              , subq_25.ds__extract_day
              , subq_25.ds__extract_dow
              , subq_25.ds__extract_doy
              , subq_25.ds_partitioned__day
              , subq_25.ds_partitioned__week
              , subq_25.ds_partitioned__month
              , subq_25.ds_partitioned__quarter
              , subq_25.ds_partitioned__year
              , subq_25.ds_partitioned__extract_year
              , subq_25.ds_partitioned__extract_quarter
              , subq_25.ds_partitioned__extract_month
              , subq_25.ds_partitioned__extract_day
              , subq_25.ds_partitioned__extract_dow
              , subq_25.ds_partitioned__extract_doy
              , subq_25.paid_at__day
              , subq_25.paid_at__week
              , subq_25.paid_at__month
              , subq_25.paid_at__quarter
              , subq_25.paid_at__year
              , subq_25.paid_at__extract_year
              , subq_25.paid_at__extract_quarter
              , subq_25.paid_at__extract_month
              , subq_25.paid_at__extract_day
              , subq_25.paid_at__extract_dow
              , subq_25.paid_at__extract_doy
              , subq_25.booking__ds__day
              , subq_25.booking__ds__week
              , subq_25.booking__ds__month
              , subq_25.booking__ds__quarter
              , subq_25.booking__ds__year
              , subq_25.booking__ds__extract_year
              , subq_25.booking__ds__extract_quarter
              , subq_25.booking__ds__extract_month
              , subq_25.booking__ds__extract_day
              , subq_25.booking__ds__extract_dow
              , subq_25.booking__ds__extract_doy
              , subq_25.booking__ds_partitioned__day
              , subq_25.booking__ds_partitioned__week
              , subq_25.booking__ds_partitioned__month
              , subq_25.booking__ds_partitioned__quarter
              , subq_25.booking__ds_partitioned__year
              , subq_25.booking__ds_partitioned__extract_year
              , subq_25.booking__ds_partitioned__extract_quarter
              , subq_25.booking__ds_partitioned__extract_month
              , subq_25.booking__ds_partitioned__extract_day
              , subq_25.booking__ds_partitioned__extract_dow
              , subq_25.booking__ds_partitioned__extract_doy
              , subq_25.booking__paid_at__day
              , subq_25.booking__paid_at__week
              , subq_25.booking__paid_at__month
              , subq_25.booking__paid_at__quarter
              , subq_25.booking__paid_at__year
              , subq_25.booking__paid_at__extract_year
              , subq_25.booking__paid_at__extract_quarter
              , subq_25.booking__paid_at__extract_month
              , subq_25.booking__paid_at__extract_day
              , subq_25.booking__paid_at__extract_dow
              , subq_25.booking__paid_at__extract_doy
              , subq_25.metric_time__day
              , subq_25.metric_time__week
              , subq_25.metric_time__month
              , subq_25.metric_time__quarter
              , subq_25.metric_time__year
              , subq_25.metric_time__extract_year
              , subq_25.metric_time__extract_quarter
              , subq_25.metric_time__extract_month
              , subq_25.metric_time__extract_day
              , subq_25.metric_time__extract_dow
              , subq_25.metric_time__extract_doy
              , subq_25.listing
              , subq_25.guest
              , subq_25.host
              , subq_25.booking__listing
              , subq_25.booking__guest
              , subq_25.booking__host
              , subq_25.is_instant
              , subq_25.booking__is_instant
              , subq_25.listing__is_lux_latest
              , subq_25.listing__capacity_latest
              , subq_25.bookings
              , subq_25.instant_bookings
              , subq_25.booking_value
              , subq_25.max_booking_value
              , subq_25.min_booking_value
              , subq_25.bookers
              , subq_25.average_booking_value
              , subq_25.referred_bookings
              , subq_25.median_booking_value
              , subq_25.booking_value_p99
              , subq_25.discrete_booking_value_p99
              , subq_25.approximate_continuous_booking_value_p99
              , subq_25.approximate_discrete_booking_value_p99
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_24.is_lux_latest AS listing__is_lux_latest
                , subq_24.capacity_latest AS listing__capacity_latest
                , subq_21.ds__day AS ds__day
                , subq_21.ds__week AS ds__week
                , subq_21.ds__month AS ds__month
                , subq_21.ds__quarter AS ds__quarter
                , subq_21.ds__year AS ds__year
                , subq_21.ds__extract_year AS ds__extract_year
                , subq_21.ds__extract_quarter AS ds__extract_quarter
                , subq_21.ds__extract_month AS ds__extract_month
                , subq_21.ds__extract_day AS ds__extract_day
                , subq_21.ds__extract_dow AS ds__extract_dow
                , subq_21.ds__extract_doy AS ds__extract_doy
                , subq_21.ds_partitioned__day AS ds_partitioned__day
                , subq_21.ds_partitioned__week AS ds_partitioned__week
                , subq_21.ds_partitioned__month AS ds_partitioned__month
                , subq_21.ds_partitioned__quarter AS ds_partitioned__quarter
                , subq_21.ds_partitioned__year AS ds_partitioned__year
                , subq_21.ds_partitioned__extract_year AS ds_partitioned__extract_year
                , subq_21.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                , subq_21.ds_partitioned__extract_month AS ds_partitioned__extract_month
                , subq_21.ds_partitioned__extract_day AS ds_partitioned__extract_day
                , subq_21.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                , subq_21.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                , subq_21.paid_at__day AS paid_at__day
                , subq_21.paid_at__week AS paid_at__week
                , subq_21.paid_at__month AS paid_at__month
                , subq_21.paid_at__quarter AS paid_at__quarter
                , subq_21.paid_at__year AS paid_at__year
                , subq_21.paid_at__extract_year AS paid_at__extract_year
                , subq_21.paid_at__extract_quarter AS paid_at__extract_quarter
                , subq_21.paid_at__extract_month AS paid_at__extract_month
                , subq_21.paid_at__extract_day AS paid_at__extract_day
                , subq_21.paid_at__extract_dow AS paid_at__extract_dow
                , subq_21.paid_at__extract_doy AS paid_at__extract_doy
                , subq_21.booking__ds__day AS booking__ds__day
                , subq_21.booking__ds__week AS booking__ds__week
                , subq_21.booking__ds__month AS booking__ds__month
                , subq_21.booking__ds__quarter AS booking__ds__quarter
                , subq_21.booking__ds__year AS booking__ds__year
                , subq_21.booking__ds__extract_year AS booking__ds__extract_year
                , subq_21.booking__ds__extract_quarter AS booking__ds__extract_quarter
                , subq_21.booking__ds__extract_month AS booking__ds__extract_month
                , subq_21.booking__ds__extract_day AS booking__ds__extract_day
                , subq_21.booking__ds__extract_dow AS booking__ds__extract_dow
                , subq_21.booking__ds__extract_doy AS booking__ds__extract_doy
                , subq_21.booking__ds_partitioned__day AS booking__ds_partitioned__day
                , subq_21.booking__ds_partitioned__week AS booking__ds_partitioned__week
                , subq_21.booking__ds_partitioned__month AS booking__ds_partitioned__month
                , subq_21.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
                , subq_21.booking__ds_partitioned__year AS booking__ds_partitioned__year
                , subq_21.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
                , subq_21.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
                , subq_21.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
                , subq_21.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
                , subq_21.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
                , subq_21.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
                , subq_21.booking__paid_at__day AS booking__paid_at__day
                , subq_21.booking__paid_at__week AS booking__paid_at__week
                , subq_21.booking__paid_at__month AS booking__paid_at__month
                , subq_21.booking__paid_at__quarter AS booking__paid_at__quarter
                , subq_21.booking__paid_at__year AS booking__paid_at__year
                , subq_21.booking__paid_at__extract_year AS booking__paid_at__extract_year
                , subq_21.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
                , subq_21.booking__paid_at__extract_month AS booking__paid_at__extract_month
                , subq_21.booking__paid_at__extract_day AS booking__paid_at__extract_day
                , subq_21.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
                , subq_21.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
                , subq_21.metric_time__day AS metric_time__day
                , subq_21.metric_time__week AS metric_time__week
                , subq_21.metric_time__month AS metric_time__month
                , subq_21.metric_time__quarter AS metric_time__quarter
                , subq_21.metric_time__year AS metric_time__year
                , subq_21.metric_time__extract_year AS metric_time__extract_year
                , subq_21.metric_time__extract_quarter AS metric_time__extract_quarter
                , subq_21.metric_time__extract_month AS metric_time__extract_month
                , subq_21.metric_time__extract_day AS metric_time__extract_day
                , subq_21.metric_time__extract_dow AS metric_time__extract_dow
                , subq_21.metric_time__extract_doy AS metric_time__extract_doy
                , subq_21.listing AS listing
                , subq_21.guest AS guest
                , subq_21.host AS host
                , subq_21.booking__listing AS booking__listing
                , subq_21.booking__guest AS booking__guest
                , subq_21.booking__host AS booking__host
                , subq_21.is_instant AS is_instant
                , subq_21.booking__is_instant AS booking__is_instant
                , subq_21.bookings AS bookings
                , subq_21.instant_bookings AS instant_bookings
                , subq_21.booking_value AS booking_value
                , subq_21.max_booking_value AS max_booking_value
                , subq_21.min_booking_value AS min_booking_value
                , subq_21.bookers AS bookers
                , subq_21.average_booking_value AS average_booking_value
                , subq_21.referred_bookings AS referred_bookings
                , subq_21.median_booking_value AS median_booking_value
                , subq_21.booking_value_p99 AS booking_value_p99
                , subq_21.discrete_booking_value_p99 AS discrete_booking_value_p99
                , subq_21.approximate_continuous_booking_value_p99 AS approximate_continuous_booking_value_p99
                , subq_21.approximate_discrete_booking_value_p99 AS approximate_discrete_booking_value_p99
              FROM (
                -- Metric Time Dimension 'ds'
                SELECT
                  subq_20.ds__day
                  , subq_20.ds__week
                  , subq_20.ds__month
                  , subq_20.ds__quarter
                  , subq_20.ds__year
                  , subq_20.ds__extract_year
                  , subq_20.ds__extract_quarter
                  , subq_20.ds__extract_month
                  , subq_20.ds__extract_day
                  , subq_20.ds__extract_dow
                  , subq_20.ds__extract_doy
                  , subq_20.ds_partitioned__day
                  , subq_20.ds_partitioned__week
                  , subq_20.ds_partitioned__month
                  , subq_20.ds_partitioned__quarter
                  , subq_20.ds_partitioned__year
                  , subq_20.ds_partitioned__extract_year
                  , subq_20.ds_partitioned__extract_quarter
                  , subq_20.ds_partitioned__extract_month
                  , subq_20.ds_partitioned__extract_day
                  , subq_20.ds_partitioned__extract_dow
                  , subq_20.ds_partitioned__extract_doy
                  , subq_20.paid_at__day
                  , subq_20.paid_at__week
                  , subq_20.paid_at__month
                  , subq_20.paid_at__quarter
                  , subq_20.paid_at__year
                  , subq_20.paid_at__extract_year
                  , subq_20.paid_at__extract_quarter
                  , subq_20.paid_at__extract_month
                  , subq_20.paid_at__extract_day
                  , subq_20.paid_at__extract_dow
                  , subq_20.paid_at__extract_doy
                  , subq_20.booking__ds__day
                  , subq_20.booking__ds__week
                  , subq_20.booking__ds__month
                  , subq_20.booking__ds__quarter
                  , subq_20.booking__ds__year
                  , subq_20.booking__ds__extract_year
                  , subq_20.booking__ds__extract_quarter
                  , subq_20.booking__ds__extract_month
                  , subq_20.booking__ds__extract_day
                  , subq_20.booking__ds__extract_dow
                  , subq_20.booking__ds__extract_doy
                  , subq_20.booking__ds_partitioned__day
                  , subq_20.booking__ds_partitioned__week
                  , subq_20.booking__ds_partitioned__month
                  , subq_20.booking__ds_partitioned__quarter
                  , subq_20.booking__ds_partitioned__year
                  , subq_20.booking__ds_partitioned__extract_year
                  , subq_20.booking__ds_partitioned__extract_quarter
                  , subq_20.booking__ds_partitioned__extract_month
                  , subq_20.booking__ds_partitioned__extract_day
                  , subq_20.booking__ds_partitioned__extract_dow
                  , subq_20.booking__ds_partitioned__extract_doy
                  , subq_20.booking__paid_at__day
                  , subq_20.booking__paid_at__week
                  , subq_20.booking__paid_at__month
                  , subq_20.booking__paid_at__quarter
                  , subq_20.booking__paid_at__year
                  , subq_20.booking__paid_at__extract_year
                  , subq_20.booking__paid_at__extract_quarter
                  , subq_20.booking__paid_at__extract_month
                  , subq_20.booking__paid_at__extract_day
                  , subq_20.booking__paid_at__extract_dow
                  , subq_20.booking__paid_at__extract_doy
                  , subq_20.ds__day AS metric_time__day
                  , subq_20.ds__week AS metric_time__week
                  , subq_20.ds__month AS metric_time__month
                  , subq_20.ds__quarter AS metric_time__quarter
                  , subq_20.ds__year AS metric_time__year
                  , subq_20.ds__extract_year AS metric_time__extract_year
                  , subq_20.ds__extract_quarter AS metric_time__extract_quarter
                  , subq_20.ds__extract_month AS metric_time__extract_month
                  , subq_20.ds__extract_day AS metric_time__extract_day
                  , subq_20.ds__extract_dow AS metric_time__extract_dow
                  , subq_20.ds__extract_doy AS metric_time__extract_doy
                  , subq_20.listing
                  , subq_20.guest
                  , subq_20.host
                  , subq_20.booking__listing
                  , subq_20.booking__guest
                  , subq_20.booking__host
                  , subq_20.is_instant
                  , subq_20.booking__is_instant
                  , subq_20.bookings
                  , subq_20.instant_bookings
                  , subq_20.booking_value
                  , subq_20.max_booking_value
                  , subq_20.min_booking_value
                  , subq_20.bookers
                  , subq_20.average_booking_value
                  , subq_20.referred_bookings
                  , subq_20.median_booking_value
                  , subq_20.booking_value_p99
                  , subq_20.discrete_booking_value_p99
                  , subq_20.approximate_continuous_booking_value_p99
                  , subq_20.approximate_discrete_booking_value_p99
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS ds__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS paid_at__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds) AS booking__ds__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.ds_partitioned) AS booking__ds_partitioned__extract_dow
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
                    , EXTRACT(DAY_OF_WEEK FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_dow
                    , EXTRACT(doy FROM bookings_source_src_28000.paid_at) AS booking__paid_at__extract_doy
                    , bookings_source_src_28000.listing_id AS listing
                    , bookings_source_src_28000.guest_id AS guest
                    , bookings_source_src_28000.host_id AS host
                    , bookings_source_src_28000.listing_id AS booking__listing
                    , bookings_source_src_28000.guest_id AS booking__guest
                    , bookings_source_src_28000.host_id AS booking__host
                  FROM ***************************.fct_bookings bookings_source_src_28000
                ) subq_20
              ) subq_21
              LEFT OUTER JOIN (
                -- Pass Only Elements: ['capacity_latest', 'is_lux_latest', 'listing', 'listing']
                SELECT
                  subq_23.listing
                  , subq_23.is_lux_latest
                  , subq_23.capacity_latest
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_22.ds__day
                    , subq_22.ds__week
                    , subq_22.ds__month
                    , subq_22.ds__quarter
                    , subq_22.ds__year
                    , subq_22.ds__extract_year
                    , subq_22.ds__extract_quarter
                    , subq_22.ds__extract_month
                    , subq_22.ds__extract_day
                    , subq_22.ds__extract_dow
                    , subq_22.ds__extract_doy
                    , subq_22.created_at__day
                    , subq_22.created_at__week
                    , subq_22.created_at__month
                    , subq_22.created_at__quarter
                    , subq_22.created_at__year
                    , subq_22.created_at__extract_year
                    , subq_22.created_at__extract_quarter
                    , subq_22.created_at__extract_month
                    , subq_22.created_at__extract_day
                    , subq_22.created_at__extract_dow
                    , subq_22.created_at__extract_doy
                    , subq_22.listing__ds__day
                    , subq_22.listing__ds__week
                    , subq_22.listing__ds__month
                    , subq_22.listing__ds__quarter
                    , subq_22.listing__ds__year
                    , subq_22.listing__ds__extract_year
                    , subq_22.listing__ds__extract_quarter
                    , subq_22.listing__ds__extract_month
                    , subq_22.listing__ds__extract_day
                    , subq_22.listing__ds__extract_dow
                    , subq_22.listing__ds__extract_doy
                    , subq_22.listing__created_at__day
                    , subq_22.listing__created_at__week
                    , subq_22.listing__created_at__month
                    , subq_22.listing__created_at__quarter
                    , subq_22.listing__created_at__year
                    , subq_22.listing__created_at__extract_year
                    , subq_22.listing__created_at__extract_quarter
                    , subq_22.listing__created_at__extract_month
                    , subq_22.listing__created_at__extract_day
                    , subq_22.listing__created_at__extract_dow
                    , subq_22.listing__created_at__extract_doy
                    , subq_22.ds__day AS metric_time__day
                    , subq_22.ds__week AS metric_time__week
                    , subq_22.ds__month AS metric_time__month
                    , subq_22.ds__quarter AS metric_time__quarter
                    , subq_22.ds__year AS metric_time__year
                    , subq_22.ds__extract_year AS metric_time__extract_year
                    , subq_22.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_22.ds__extract_month AS metric_time__extract_month
                    , subq_22.ds__extract_day AS metric_time__extract_day
                    , subq_22.ds__extract_dow AS metric_time__extract_dow
                    , subq_22.ds__extract_doy AS metric_time__extract_doy
                    , subq_22.listing
                    , subq_22.user
                    , subq_22.listing__user
                    , subq_22.country_latest
                    , subq_22.is_lux_latest
                    , subq_22.capacity_latest
                    , subq_22.listing__country_latest
                    , subq_22.listing__is_lux_latest
                    , subq_22.listing__capacity_latest
                    , subq_22.listings
                    , subq_22.largest_listing
                    , subq_22.smallest_listing
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
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS ds__extract_dow
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
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
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
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
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
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                      , listings_latest_src_28000.country AS listing__country_latest
                      , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                      , listings_latest_src_28000.capacity AS listing__capacity_latest
                      , listings_latest_src_28000.listing_id AS listing
                      , listings_latest_src_28000.user_id AS user
                      , listings_latest_src_28000.user_id AS listing__user
                    FROM ***************************.dim_listings_latest listings_latest_src_28000
                  ) subq_22
                ) subq_23
              ) subq_24
              ON
                subq_21.listing = subq_24.listing
            ) subq_25
            WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
          ) subq_26
        ) subq_27
        GROUP BY
          subq_27.listing__capacity_latest
      ) subq_28
    ) subq_29
    FULL OUTER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        subq_38.listing__capacity_latest
        , subq_38.views
      FROM (
        -- Aggregate Measures
        SELECT
          subq_37.listing__capacity_latest
          , SUM(subq_37.views) AS views
        FROM (
          -- Pass Only Elements: ['views', 'listing__capacity_latest']
          SELECT
            subq_36.listing__capacity_latest
            , subq_36.views
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_35.ds__day
              , subq_35.ds__week
              , subq_35.ds__month
              , subq_35.ds__quarter
              , subq_35.ds__year
              , subq_35.ds__extract_year
              , subq_35.ds__extract_quarter
              , subq_35.ds__extract_month
              , subq_35.ds__extract_day
              , subq_35.ds__extract_dow
              , subq_35.ds__extract_doy
              , subq_35.ds_partitioned__day
              , subq_35.ds_partitioned__week
              , subq_35.ds_partitioned__month
              , subq_35.ds_partitioned__quarter
              , subq_35.ds_partitioned__year
              , subq_35.ds_partitioned__extract_year
              , subq_35.ds_partitioned__extract_quarter
              , subq_35.ds_partitioned__extract_month
              , subq_35.ds_partitioned__extract_day
              , subq_35.ds_partitioned__extract_dow
              , subq_35.ds_partitioned__extract_doy
              , subq_35.view__ds__day
              , subq_35.view__ds__week
              , subq_35.view__ds__month
              , subq_35.view__ds__quarter
              , subq_35.view__ds__year
              , subq_35.view__ds__extract_year
              , subq_35.view__ds__extract_quarter
              , subq_35.view__ds__extract_month
              , subq_35.view__ds__extract_day
              , subq_35.view__ds__extract_dow
              , subq_35.view__ds__extract_doy
              , subq_35.view__ds_partitioned__day
              , subq_35.view__ds_partitioned__week
              , subq_35.view__ds_partitioned__month
              , subq_35.view__ds_partitioned__quarter
              , subq_35.view__ds_partitioned__year
              , subq_35.view__ds_partitioned__extract_year
              , subq_35.view__ds_partitioned__extract_quarter
              , subq_35.view__ds_partitioned__extract_month
              , subq_35.view__ds_partitioned__extract_day
              , subq_35.view__ds_partitioned__extract_dow
              , subq_35.view__ds_partitioned__extract_doy
              , subq_35.metric_time__day
              , subq_35.metric_time__week
              , subq_35.metric_time__month
              , subq_35.metric_time__quarter
              , subq_35.metric_time__year
              , subq_35.metric_time__extract_year
              , subq_35.metric_time__extract_quarter
              , subq_35.metric_time__extract_month
              , subq_35.metric_time__extract_day
              , subq_35.metric_time__extract_dow
              , subq_35.metric_time__extract_doy
              , subq_35.listing
              , subq_35.user
              , subq_35.view__listing
              , subq_35.view__user
              , subq_35.listing__is_lux_latest
              , subq_35.listing__capacity_latest
              , subq_35.views
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_34.is_lux_latest AS listing__is_lux_latest
                , subq_34.capacity_latest AS listing__capacity_latest
                , subq_31.ds__day AS ds__day
                , subq_31.ds__week AS ds__week
                , subq_31.ds__month AS ds__month
                , subq_31.ds__quarter AS ds__quarter
                , subq_31.ds__year AS ds__year
                , subq_31.ds__extract_year AS ds__extract_year
                , subq_31.ds__extract_quarter AS ds__extract_quarter
                , subq_31.ds__extract_month AS ds__extract_month
                , subq_31.ds__extract_day AS ds__extract_day
                , subq_31.ds__extract_dow AS ds__extract_dow
                , subq_31.ds__extract_doy AS ds__extract_doy
                , subq_31.ds_partitioned__day AS ds_partitioned__day
                , subq_31.ds_partitioned__week AS ds_partitioned__week
                , subq_31.ds_partitioned__month AS ds_partitioned__month
                , subq_31.ds_partitioned__quarter AS ds_partitioned__quarter
                , subq_31.ds_partitioned__year AS ds_partitioned__year
                , subq_31.ds_partitioned__extract_year AS ds_partitioned__extract_year
                , subq_31.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
                , subq_31.ds_partitioned__extract_month AS ds_partitioned__extract_month
                , subq_31.ds_partitioned__extract_day AS ds_partitioned__extract_day
                , subq_31.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
                , subq_31.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
                , subq_31.view__ds__day AS view__ds__day
                , subq_31.view__ds__week AS view__ds__week
                , subq_31.view__ds__month AS view__ds__month
                , subq_31.view__ds__quarter AS view__ds__quarter
                , subq_31.view__ds__year AS view__ds__year
                , subq_31.view__ds__extract_year AS view__ds__extract_year
                , subq_31.view__ds__extract_quarter AS view__ds__extract_quarter
                , subq_31.view__ds__extract_month AS view__ds__extract_month
                , subq_31.view__ds__extract_day AS view__ds__extract_day
                , subq_31.view__ds__extract_dow AS view__ds__extract_dow
                , subq_31.view__ds__extract_doy AS view__ds__extract_doy
                , subq_31.view__ds_partitioned__day AS view__ds_partitioned__day
                , subq_31.view__ds_partitioned__week AS view__ds_partitioned__week
                , subq_31.view__ds_partitioned__month AS view__ds_partitioned__month
                , subq_31.view__ds_partitioned__quarter AS view__ds_partitioned__quarter
                , subq_31.view__ds_partitioned__year AS view__ds_partitioned__year
                , subq_31.view__ds_partitioned__extract_year AS view__ds_partitioned__extract_year
                , subq_31.view__ds_partitioned__extract_quarter AS view__ds_partitioned__extract_quarter
                , subq_31.view__ds_partitioned__extract_month AS view__ds_partitioned__extract_month
                , subq_31.view__ds_partitioned__extract_day AS view__ds_partitioned__extract_day
                , subq_31.view__ds_partitioned__extract_dow AS view__ds_partitioned__extract_dow
                , subq_31.view__ds_partitioned__extract_doy AS view__ds_partitioned__extract_doy
                , subq_31.metric_time__day AS metric_time__day
                , subq_31.metric_time__week AS metric_time__week
                , subq_31.metric_time__month AS metric_time__month
                , subq_31.metric_time__quarter AS metric_time__quarter
                , subq_31.metric_time__year AS metric_time__year
                , subq_31.metric_time__extract_year AS metric_time__extract_year
                , subq_31.metric_time__extract_quarter AS metric_time__extract_quarter
                , subq_31.metric_time__extract_month AS metric_time__extract_month
                , subq_31.metric_time__extract_day AS metric_time__extract_day
                , subq_31.metric_time__extract_dow AS metric_time__extract_dow
                , subq_31.metric_time__extract_doy AS metric_time__extract_doy
                , subq_31.listing AS listing
                , subq_31.user AS user
                , subq_31.view__listing AS view__listing
                , subq_31.view__user AS view__user
                , subq_31.views AS views
              FROM (
                -- Metric Time Dimension 'ds'
                SELECT
                  subq_30.ds__day
                  , subq_30.ds__week
                  , subq_30.ds__month
                  , subq_30.ds__quarter
                  , subq_30.ds__year
                  , subq_30.ds__extract_year
                  , subq_30.ds__extract_quarter
                  , subq_30.ds__extract_month
                  , subq_30.ds__extract_day
                  , subq_30.ds__extract_dow
                  , subq_30.ds__extract_doy
                  , subq_30.ds_partitioned__day
                  , subq_30.ds_partitioned__week
                  , subq_30.ds_partitioned__month
                  , subq_30.ds_partitioned__quarter
                  , subq_30.ds_partitioned__year
                  , subq_30.ds_partitioned__extract_year
                  , subq_30.ds_partitioned__extract_quarter
                  , subq_30.ds_partitioned__extract_month
                  , subq_30.ds_partitioned__extract_day
                  , subq_30.ds_partitioned__extract_dow
                  , subq_30.ds_partitioned__extract_doy
                  , subq_30.view__ds__day
                  , subq_30.view__ds__week
                  , subq_30.view__ds__month
                  , subq_30.view__ds__quarter
                  , subq_30.view__ds__year
                  , subq_30.view__ds__extract_year
                  , subq_30.view__ds__extract_quarter
                  , subq_30.view__ds__extract_month
                  , subq_30.view__ds__extract_day
                  , subq_30.view__ds__extract_dow
                  , subq_30.view__ds__extract_doy
                  , subq_30.view__ds_partitioned__day
                  , subq_30.view__ds_partitioned__week
                  , subq_30.view__ds_partitioned__month
                  , subq_30.view__ds_partitioned__quarter
                  , subq_30.view__ds_partitioned__year
                  , subq_30.view__ds_partitioned__extract_year
                  , subq_30.view__ds_partitioned__extract_quarter
                  , subq_30.view__ds_partitioned__extract_month
                  , subq_30.view__ds_partitioned__extract_day
                  , subq_30.view__ds_partitioned__extract_dow
                  , subq_30.view__ds_partitioned__extract_doy
                  , subq_30.ds__day AS metric_time__day
                  , subq_30.ds__week AS metric_time__week
                  , subq_30.ds__month AS metric_time__month
                  , subq_30.ds__quarter AS metric_time__quarter
                  , subq_30.ds__year AS metric_time__year
                  , subq_30.ds__extract_year AS metric_time__extract_year
                  , subq_30.ds__extract_quarter AS metric_time__extract_quarter
                  , subq_30.ds__extract_month AS metric_time__extract_month
                  , subq_30.ds__extract_day AS metric_time__extract_day
                  , subq_30.ds__extract_dow AS metric_time__extract_dow
                  , subq_30.ds__extract_doy AS metric_time__extract_doy
                  , subq_30.listing
                  , subq_30.user
                  , subq_30.view__listing
                  , subq_30.view__user
                  , subq_30.views
                FROM (
                  -- Read Elements From Semantic Model 'views_source'
                  SELECT
                    1 AS views
                    , DATE_TRUNC('day', views_source_src_28000.ds) AS ds__day
                    , DATE_TRUNC('week', views_source_src_28000.ds) AS ds__week
                    , DATE_TRUNC('month', views_source_src_28000.ds) AS ds__month
                    , DATE_TRUNC('quarter', views_source_src_28000.ds) AS ds__quarter
                    , DATE_TRUNC('year', views_source_src_28000.ds) AS ds__year
                    , EXTRACT(year FROM views_source_src_28000.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM views_source_src_28000.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM views_source_src_28000.ds) AS ds__extract_month
                    , EXTRACT(day FROM views_source_src_28000.ds) AS ds__extract_day
                    , EXTRACT(DAY_OF_WEEK FROM views_source_src_28000.ds) AS ds__extract_dow
                    , EXTRACT(doy FROM views_source_src_28000.ds) AS ds__extract_doy
                    , DATE_TRUNC('day', views_source_src_28000.ds_partitioned) AS ds_partitioned__day
                    , DATE_TRUNC('week', views_source_src_28000.ds_partitioned) AS ds_partitioned__week
                    , DATE_TRUNC('month', views_source_src_28000.ds_partitioned) AS ds_partitioned__month
                    , DATE_TRUNC('quarter', views_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                    , DATE_TRUNC('year', views_source_src_28000.ds_partitioned) AS ds_partitioned__year
                    , EXTRACT(year FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                    , EXTRACT(quarter FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                    , EXTRACT(month FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                    , EXTRACT(day FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                    , EXTRACT(DAY_OF_WEEK FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
                    , EXTRACT(doy FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                    , DATE_TRUNC('day', views_source_src_28000.ds) AS view__ds__day
                    , DATE_TRUNC('week', views_source_src_28000.ds) AS view__ds__week
                    , DATE_TRUNC('month', views_source_src_28000.ds) AS view__ds__month
                    , DATE_TRUNC('quarter', views_source_src_28000.ds) AS view__ds__quarter
                    , DATE_TRUNC('year', views_source_src_28000.ds) AS view__ds__year
                    , EXTRACT(year FROM views_source_src_28000.ds) AS view__ds__extract_year
                    , EXTRACT(quarter FROM views_source_src_28000.ds) AS view__ds__extract_quarter
                    , EXTRACT(month FROM views_source_src_28000.ds) AS view__ds__extract_month
                    , EXTRACT(day FROM views_source_src_28000.ds) AS view__ds__extract_day
                    , EXTRACT(DAY_OF_WEEK FROM views_source_src_28000.ds) AS view__ds__extract_dow
                    , EXTRACT(doy FROM views_source_src_28000.ds) AS view__ds__extract_doy
                    , DATE_TRUNC('day', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__day
                    , DATE_TRUNC('week', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__week
                    , DATE_TRUNC('month', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__month
                    , DATE_TRUNC('quarter', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__quarter
                    , DATE_TRUNC('year', views_source_src_28000.ds_partitioned) AS view__ds_partitioned__year
                    , EXTRACT(year FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_year
                    , EXTRACT(quarter FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_quarter
                    , EXTRACT(month FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_month
                    , EXTRACT(day FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_day
                    , EXTRACT(DAY_OF_WEEK FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_dow
                    , EXTRACT(doy FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_doy
                    , views_source_src_28000.listing_id AS listing
                    , views_source_src_28000.user_id AS user
                    , views_source_src_28000.listing_id AS view__listing
                    , views_source_src_28000.user_id AS view__user
                  FROM ***************************.fct_views views_source_src_28000
                ) subq_30
              ) subq_31
              LEFT OUTER JOIN (
                -- Pass Only Elements: ['capacity_latest', 'is_lux_latest', 'listing', 'listing']
                SELECT
                  subq_33.listing
                  , subq_33.is_lux_latest
                  , subq_33.capacity_latest
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_32.ds__day
                    , subq_32.ds__week
                    , subq_32.ds__month
                    , subq_32.ds__quarter
                    , subq_32.ds__year
                    , subq_32.ds__extract_year
                    , subq_32.ds__extract_quarter
                    , subq_32.ds__extract_month
                    , subq_32.ds__extract_day
                    , subq_32.ds__extract_dow
                    , subq_32.ds__extract_doy
                    , subq_32.created_at__day
                    , subq_32.created_at__week
                    , subq_32.created_at__month
                    , subq_32.created_at__quarter
                    , subq_32.created_at__year
                    , subq_32.created_at__extract_year
                    , subq_32.created_at__extract_quarter
                    , subq_32.created_at__extract_month
                    , subq_32.created_at__extract_day
                    , subq_32.created_at__extract_dow
                    , subq_32.created_at__extract_doy
                    , subq_32.listing__ds__day
                    , subq_32.listing__ds__week
                    , subq_32.listing__ds__month
                    , subq_32.listing__ds__quarter
                    , subq_32.listing__ds__year
                    , subq_32.listing__ds__extract_year
                    , subq_32.listing__ds__extract_quarter
                    , subq_32.listing__ds__extract_month
                    , subq_32.listing__ds__extract_day
                    , subq_32.listing__ds__extract_dow
                    , subq_32.listing__ds__extract_doy
                    , subq_32.listing__created_at__day
                    , subq_32.listing__created_at__week
                    , subq_32.listing__created_at__month
                    , subq_32.listing__created_at__quarter
                    , subq_32.listing__created_at__year
                    , subq_32.listing__created_at__extract_year
                    , subq_32.listing__created_at__extract_quarter
                    , subq_32.listing__created_at__extract_month
                    , subq_32.listing__created_at__extract_day
                    , subq_32.listing__created_at__extract_dow
                    , subq_32.listing__created_at__extract_doy
                    , subq_32.ds__day AS metric_time__day
                    , subq_32.ds__week AS metric_time__week
                    , subq_32.ds__month AS metric_time__month
                    , subq_32.ds__quarter AS metric_time__quarter
                    , subq_32.ds__year AS metric_time__year
                    , subq_32.ds__extract_year AS metric_time__extract_year
                    , subq_32.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_32.ds__extract_month AS metric_time__extract_month
                    , subq_32.ds__extract_day AS metric_time__extract_day
                    , subq_32.ds__extract_dow AS metric_time__extract_dow
                    , subq_32.ds__extract_doy AS metric_time__extract_doy
                    , subq_32.listing
                    , subq_32.user
                    , subq_32.listing__user
                    , subq_32.country_latest
                    , subq_32.is_lux_latest
                    , subq_32.capacity_latest
                    , subq_32.listing__country_latest
                    , subq_32.listing__is_lux_latest
                    , subq_32.listing__capacity_latest
                    , subq_32.listings
                    , subq_32.largest_listing
                    , subq_32.smallest_listing
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
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS ds__extract_dow
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
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
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
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
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
                      , EXTRACT(DAY_OF_WEEK FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
                      , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                      , listings_latest_src_28000.country AS listing__country_latest
                      , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                      , listings_latest_src_28000.capacity AS listing__capacity_latest
                      , listings_latest_src_28000.listing_id AS listing
                      , listings_latest_src_28000.user_id AS user
                      , listings_latest_src_28000.user_id AS listing__user
                    FROM ***************************.dim_listings_latest listings_latest_src_28000
                  ) subq_32
                ) subq_33
              ) subq_34
              ON
                subq_31.listing = subq_34.listing
            ) subq_35
            WHERE (listing__is_lux_latest) AND (metric_time__day >= '2020-01-02')
          ) subq_36
        ) subq_37
        GROUP BY
          subq_37.listing__capacity_latest
      ) subq_38
    ) subq_39
    ON
      subq_29.listing__capacity_latest = subq_39.listing__capacity_latest
    GROUP BY
      COALESCE(subq_29.listing__capacity_latest, subq_39.listing__capacity_latest)
  ) subq_40
) subq_41
ON
  COALESCE(subq_9.listing__capacity_latest, subq_19.listing__capacity_latest) = subq_41.listing__capacity_latest
GROUP BY
  COALESCE(subq_9.listing__capacity_latest, subq_19.listing__capacity_latest, subq_41.listing__capacity_latest)
