test_name: test_multi_hop_to_scd_dimension
test_filename: test_query_rendering.py
docstring:
  Tests conversion of a plan using an SCD dimension that is reached through another table.
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  subq_9.metric_time__day
  , subq_9.listing__lux_listing__is_confirmed_lux
  , subq_9.bookings
FROM (
  -- Aggregate Measures
  SELECT
    subq_8.metric_time__day
    , subq_8.listing__lux_listing__is_confirmed_lux
    , SUM(subq_8.bookings) AS bookings
  FROM (
    -- Pass Only Elements: ['bookings', 'listing__lux_listing__is_confirmed_lux', 'metric_time__day']
    SELECT
      subq_7.metric_time__day
      , subq_7.listing__lux_listing__is_confirmed_lux
      , subq_7.bookings
    FROM (
      -- Join Standard Outputs
      SELECT
        subq_6.lux_listing__is_confirmed_lux AS listing__lux_listing__is_confirmed_lux
        , subq_6.lux_listing__window_start__day AS listing__lux_listing__window_start__day
        , subq_6.lux_listing__window_end__day AS listing__lux_listing__window_end__day
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
        , subq_1.user AS user
        , subq_1.booking__listing AS booking__listing
        , subq_1.booking__guest AS booking__guest
        , subq_1.booking__host AS booking__host
        , subq_1.booking__user AS booking__user
        , subq_1.is_instant AS is_instant
        , subq_1.booking__is_instant AS booking__is_instant
        , subq_1.bookings AS bookings
        , subq_1.instant_bookings AS instant_bookings
        , subq_1.booking_value AS booking_value
        , subq_1.bookers AS bookers
        , subq_1.average_booking_value AS average_booking_value
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
          , subq_0.user
          , subq_0.booking__listing
          , subq_0.booking__guest
          , subq_0.booking__host
          , subq_0.booking__user
          , subq_0.is_instant
          , subq_0.booking__is_instant
          , subq_0.bookings
          , subq_0.instant_bookings
          , subq_0.booking_value
          , subq_0.bookers
          , subq_0.average_booking_value
        FROM (
          -- Read Elements From Semantic Model 'bookings_source'
          SELECT
            1 AS bookings
            , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
            , bookings_source_src_26000.booking_value
            , bookings_source_src_26000.guest_id AS bookers
            , bookings_source_src_26000.booking_value AS average_booking_value
            , bookings_source_src_26000.booking_value AS booking_payments
            , bookings_source_src_26000.is_instant
            , DATETIME_TRUNC(bookings_source_src_26000.ds, day) AS ds__day
            , DATETIME_TRUNC(bookings_source_src_26000.ds, isoweek) AS ds__week
            , DATETIME_TRUNC(bookings_source_src_26000.ds, month) AS ds__month
            , DATETIME_TRUNC(bookings_source_src_26000.ds, quarter) AS ds__quarter
            , DATETIME_TRUNC(bookings_source_src_26000.ds, year) AS ds__year
            , EXTRACT(year FROM bookings_source_src_26000.ds) AS ds__extract_year
            , EXTRACT(quarter FROM bookings_source_src_26000.ds) AS ds__extract_quarter
            , EXTRACT(month FROM bookings_source_src_26000.ds) AS ds__extract_month
            , EXTRACT(day FROM bookings_source_src_26000.ds) AS ds__extract_day
            , IF(EXTRACT(dayofweek FROM bookings_source_src_26000.ds) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_26000.ds) - 1) AS ds__extract_dow
            , EXTRACT(dayofyear FROM bookings_source_src_26000.ds) AS ds__extract_doy
            , DATETIME_TRUNC(bookings_source_src_26000.ds_partitioned, day) AS ds_partitioned__day
            , DATETIME_TRUNC(bookings_source_src_26000.ds_partitioned, isoweek) AS ds_partitioned__week
            , DATETIME_TRUNC(bookings_source_src_26000.ds_partitioned, month) AS ds_partitioned__month
            , DATETIME_TRUNC(bookings_source_src_26000.ds_partitioned, quarter) AS ds_partitioned__quarter
            , DATETIME_TRUNC(bookings_source_src_26000.ds_partitioned, year) AS ds_partitioned__year
            , EXTRACT(year FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_year
            , EXTRACT(quarter FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_quarter
            , EXTRACT(month FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_month
            , EXTRACT(day FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_day
            , IF(EXTRACT(dayofweek FROM bookings_source_src_26000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_26000.ds_partitioned) - 1) AS ds_partitioned__extract_dow
            , EXTRACT(dayofyear FROM bookings_source_src_26000.ds_partitioned) AS ds_partitioned__extract_doy
            , DATETIME_TRUNC(bookings_source_src_26000.paid_at, day) AS paid_at__day
            , DATETIME_TRUNC(bookings_source_src_26000.paid_at, isoweek) AS paid_at__week
            , DATETIME_TRUNC(bookings_source_src_26000.paid_at, month) AS paid_at__month
            , DATETIME_TRUNC(bookings_source_src_26000.paid_at, quarter) AS paid_at__quarter
            , DATETIME_TRUNC(bookings_source_src_26000.paid_at, year) AS paid_at__year
            , EXTRACT(year FROM bookings_source_src_26000.paid_at) AS paid_at__extract_year
            , EXTRACT(quarter FROM bookings_source_src_26000.paid_at) AS paid_at__extract_quarter
            , EXTRACT(month FROM bookings_source_src_26000.paid_at) AS paid_at__extract_month
            , EXTRACT(day FROM bookings_source_src_26000.paid_at) AS paid_at__extract_day
            , IF(EXTRACT(dayofweek FROM bookings_source_src_26000.paid_at) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_26000.paid_at) - 1) AS paid_at__extract_dow
            , EXTRACT(dayofyear FROM bookings_source_src_26000.paid_at) AS paid_at__extract_doy
            , bookings_source_src_26000.is_instant AS booking__is_instant
            , DATETIME_TRUNC(bookings_source_src_26000.ds, day) AS booking__ds__day
            , DATETIME_TRUNC(bookings_source_src_26000.ds, isoweek) AS booking__ds__week
            , DATETIME_TRUNC(bookings_source_src_26000.ds, month) AS booking__ds__month
            , DATETIME_TRUNC(bookings_source_src_26000.ds, quarter) AS booking__ds__quarter
            , DATETIME_TRUNC(bookings_source_src_26000.ds, year) AS booking__ds__year
            , EXTRACT(year FROM bookings_source_src_26000.ds) AS booking__ds__extract_year
            , EXTRACT(quarter FROM bookings_source_src_26000.ds) AS booking__ds__extract_quarter
            , EXTRACT(month FROM bookings_source_src_26000.ds) AS booking__ds__extract_month
            , EXTRACT(day FROM bookings_source_src_26000.ds) AS booking__ds__extract_day
            , IF(EXTRACT(dayofweek FROM bookings_source_src_26000.ds) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_26000.ds) - 1) AS booking__ds__extract_dow
            , EXTRACT(dayofyear FROM bookings_source_src_26000.ds) AS booking__ds__extract_doy
            , DATETIME_TRUNC(bookings_source_src_26000.ds_partitioned, day) AS booking__ds_partitioned__day
            , DATETIME_TRUNC(bookings_source_src_26000.ds_partitioned, isoweek) AS booking__ds_partitioned__week
            , DATETIME_TRUNC(bookings_source_src_26000.ds_partitioned, month) AS booking__ds_partitioned__month
            , DATETIME_TRUNC(bookings_source_src_26000.ds_partitioned, quarter) AS booking__ds_partitioned__quarter
            , DATETIME_TRUNC(bookings_source_src_26000.ds_partitioned, year) AS booking__ds_partitioned__year
            , EXTRACT(year FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_year
            , EXTRACT(quarter FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_quarter
            , EXTRACT(month FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_month
            , EXTRACT(day FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_day
            , IF(EXTRACT(dayofweek FROM bookings_source_src_26000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_26000.ds_partitioned) - 1) AS booking__ds_partitioned__extract_dow
            , EXTRACT(dayofyear FROM bookings_source_src_26000.ds_partitioned) AS booking__ds_partitioned__extract_doy
            , DATETIME_TRUNC(bookings_source_src_26000.paid_at, day) AS booking__paid_at__day
            , DATETIME_TRUNC(bookings_source_src_26000.paid_at, isoweek) AS booking__paid_at__week
            , DATETIME_TRUNC(bookings_source_src_26000.paid_at, month) AS booking__paid_at__month
            , DATETIME_TRUNC(bookings_source_src_26000.paid_at, quarter) AS booking__paid_at__quarter
            , DATETIME_TRUNC(bookings_source_src_26000.paid_at, year) AS booking__paid_at__year
            , EXTRACT(year FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_year
            , EXTRACT(quarter FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_quarter
            , EXTRACT(month FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_month
            , EXTRACT(day FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_day
            , IF(EXTRACT(dayofweek FROM bookings_source_src_26000.paid_at) = 1, 7, EXTRACT(dayofweek FROM bookings_source_src_26000.paid_at) - 1) AS booking__paid_at__extract_dow
            , EXTRACT(dayofyear FROM bookings_source_src_26000.paid_at) AS booking__paid_at__extract_doy
            , bookings_source_src_26000.listing_id AS listing
            , bookings_source_src_26000.guest_id AS guest
            , bookings_source_src_26000.host_id AS host
            , bookings_source_src_26000.guest_id AS user
            , bookings_source_src_26000.listing_id AS booking__listing
            , bookings_source_src_26000.guest_id AS booking__guest
            , bookings_source_src_26000.host_id AS booking__host
            , bookings_source_src_26000.guest_id AS booking__user
          FROM ***************************.fct_bookings bookings_source_src_26000
        ) subq_0
      ) subq_1
      LEFT OUTER JOIN (
        -- Pass Only Elements: ['lux_listing__is_confirmed_lux', 'lux_listing__window_start__day', 'lux_listing__window_end__day', 'listing']
        SELECT
          subq_5.lux_listing__window_start__day
          , subq_5.lux_listing__window_end__day
          , subq_5.listing
          , subq_5.lux_listing__is_confirmed_lux
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_4.is_confirmed_lux AS lux_listing__is_confirmed_lux
            , subq_4.window_start__day AS lux_listing__window_start__day
            , subq_4.window_start__week AS lux_listing__window_start__week
            , subq_4.window_start__month AS lux_listing__window_start__month
            , subq_4.window_start__quarter AS lux_listing__window_start__quarter
            , subq_4.window_start__year AS lux_listing__window_start__year
            , subq_4.window_start__extract_year AS lux_listing__window_start__extract_year
            , subq_4.window_start__extract_quarter AS lux_listing__window_start__extract_quarter
            , subq_4.window_start__extract_month AS lux_listing__window_start__extract_month
            , subq_4.window_start__extract_day AS lux_listing__window_start__extract_day
            , subq_4.window_start__extract_dow AS lux_listing__window_start__extract_dow
            , subq_4.window_start__extract_doy AS lux_listing__window_start__extract_doy
            , subq_4.window_end__day AS lux_listing__window_end__day
            , subq_4.window_end__week AS lux_listing__window_end__week
            , subq_4.window_end__month AS lux_listing__window_end__month
            , subq_4.window_end__quarter AS lux_listing__window_end__quarter
            , subq_4.window_end__year AS lux_listing__window_end__year
            , subq_4.window_end__extract_year AS lux_listing__window_end__extract_year
            , subq_4.window_end__extract_quarter AS lux_listing__window_end__extract_quarter
            , subq_4.window_end__extract_month AS lux_listing__window_end__extract_month
            , subq_4.window_end__extract_day AS lux_listing__window_end__extract_day
            , subq_4.window_end__extract_dow AS lux_listing__window_end__extract_dow
            , subq_4.window_end__extract_doy AS lux_listing__window_end__extract_doy
            , subq_2.listing AS listing
            , subq_2.lux_listing AS lux_listing
            , subq_2.listing__lux_listing AS listing__lux_listing
          FROM (
            -- Read Elements From Semantic Model 'lux_listing_mapping'
            SELECT
              lux_listing_mapping_src_26000.listing_id AS listing
              , lux_listing_mapping_src_26000.lux_listing_id AS lux_listing
              , lux_listing_mapping_src_26000.lux_listing_id AS listing__lux_listing
            FROM ***************************.dim_lux_listing_id_mapping lux_listing_mapping_src_26000
          ) subq_2
          LEFT OUTER JOIN (
            -- Pass Only Elements: [
            --   'is_confirmed_lux',
            --   'lux_listing__is_confirmed_lux',
            --   'window_start__day',
            --   'window_start__week',
            --   'window_start__month',
            --   'window_start__quarter',
            --   'window_start__year',
            --   'window_start__extract_year',
            --   'window_start__extract_quarter',
            --   'window_start__extract_month',
            --   'window_start__extract_day',
            --   'window_start__extract_dow',
            --   'window_start__extract_doy',
            --   'window_end__day',
            --   'window_end__week',
            --   'window_end__month',
            --   'window_end__quarter',
            --   'window_end__year',
            --   'window_end__extract_year',
            --   'window_end__extract_quarter',
            --   'window_end__extract_month',
            --   'window_end__extract_day',
            --   'window_end__extract_dow',
            --   'window_end__extract_doy',
            --   'lux_listing__window_start__day',
            --   'lux_listing__window_start__week',
            --   'lux_listing__window_start__month',
            --   'lux_listing__window_start__quarter',
            --   'lux_listing__window_start__year',
            --   'lux_listing__window_start__extract_year',
            --   'lux_listing__window_start__extract_quarter',
            --   'lux_listing__window_start__extract_month',
            --   'lux_listing__window_start__extract_day',
            --   'lux_listing__window_start__extract_dow',
            --   'lux_listing__window_start__extract_doy',
            --   'lux_listing__window_end__day',
            --   'lux_listing__window_end__week',
            --   'lux_listing__window_end__month',
            --   'lux_listing__window_end__quarter',
            --   'lux_listing__window_end__year',
            --   'lux_listing__window_end__extract_year',
            --   'lux_listing__window_end__extract_quarter',
            --   'lux_listing__window_end__extract_month',
            --   'lux_listing__window_end__extract_day',
            --   'lux_listing__window_end__extract_dow',
            --   'lux_listing__window_end__extract_doy',
            --   'lux_listing',
            -- ]
            SELECT
              subq_3.window_start__day
              , subq_3.window_start__week
              , subq_3.window_start__month
              , subq_3.window_start__quarter
              , subq_3.window_start__year
              , subq_3.window_start__extract_year
              , subq_3.window_start__extract_quarter
              , subq_3.window_start__extract_month
              , subq_3.window_start__extract_day
              , subq_3.window_start__extract_dow
              , subq_3.window_start__extract_doy
              , subq_3.window_end__day
              , subq_3.window_end__week
              , subq_3.window_end__month
              , subq_3.window_end__quarter
              , subq_3.window_end__year
              , subq_3.window_end__extract_year
              , subq_3.window_end__extract_quarter
              , subq_3.window_end__extract_month
              , subq_3.window_end__extract_day
              , subq_3.window_end__extract_dow
              , subq_3.window_end__extract_doy
              , subq_3.lux_listing__window_start__day
              , subq_3.lux_listing__window_start__week
              , subq_3.lux_listing__window_start__month
              , subq_3.lux_listing__window_start__quarter
              , subq_3.lux_listing__window_start__year
              , subq_3.lux_listing__window_start__extract_year
              , subq_3.lux_listing__window_start__extract_quarter
              , subq_3.lux_listing__window_start__extract_month
              , subq_3.lux_listing__window_start__extract_day
              , subq_3.lux_listing__window_start__extract_dow
              , subq_3.lux_listing__window_start__extract_doy
              , subq_3.lux_listing__window_end__day
              , subq_3.lux_listing__window_end__week
              , subq_3.lux_listing__window_end__month
              , subq_3.lux_listing__window_end__quarter
              , subq_3.lux_listing__window_end__year
              , subq_3.lux_listing__window_end__extract_year
              , subq_3.lux_listing__window_end__extract_quarter
              , subq_3.lux_listing__window_end__extract_month
              , subq_3.lux_listing__window_end__extract_day
              , subq_3.lux_listing__window_end__extract_dow
              , subq_3.lux_listing__window_end__extract_doy
              , subq_3.lux_listing
              , subq_3.is_confirmed_lux
              , subq_3.lux_listing__is_confirmed_lux
            FROM (
              -- Read Elements From Semantic Model 'lux_listings'
              SELECT
                lux_listings_src_26000.valid_from AS window_start__day
                , DATETIME_TRUNC(lux_listings_src_26000.valid_from, isoweek) AS window_start__week
                , DATETIME_TRUNC(lux_listings_src_26000.valid_from, month) AS window_start__month
                , DATETIME_TRUNC(lux_listings_src_26000.valid_from, quarter) AS window_start__quarter
                , DATETIME_TRUNC(lux_listings_src_26000.valid_from, year) AS window_start__year
                , EXTRACT(year FROM lux_listings_src_26000.valid_from) AS window_start__extract_year
                , EXTRACT(quarter FROM lux_listings_src_26000.valid_from) AS window_start__extract_quarter
                , EXTRACT(month FROM lux_listings_src_26000.valid_from) AS window_start__extract_month
                , EXTRACT(day FROM lux_listings_src_26000.valid_from) AS window_start__extract_day
                , IF(EXTRACT(dayofweek FROM lux_listings_src_26000.valid_from) = 1, 7, EXTRACT(dayofweek FROM lux_listings_src_26000.valid_from) - 1) AS window_start__extract_dow
                , EXTRACT(dayofyear FROM lux_listings_src_26000.valid_from) AS window_start__extract_doy
                , lux_listings_src_26000.valid_to AS window_end__day
                , DATETIME_TRUNC(lux_listings_src_26000.valid_to, isoweek) AS window_end__week
                , DATETIME_TRUNC(lux_listings_src_26000.valid_to, month) AS window_end__month
                , DATETIME_TRUNC(lux_listings_src_26000.valid_to, quarter) AS window_end__quarter
                , DATETIME_TRUNC(lux_listings_src_26000.valid_to, year) AS window_end__year
                , EXTRACT(year FROM lux_listings_src_26000.valid_to) AS window_end__extract_year
                , EXTRACT(quarter FROM lux_listings_src_26000.valid_to) AS window_end__extract_quarter
                , EXTRACT(month FROM lux_listings_src_26000.valid_to) AS window_end__extract_month
                , EXTRACT(day FROM lux_listings_src_26000.valid_to) AS window_end__extract_day
                , IF(EXTRACT(dayofweek FROM lux_listings_src_26000.valid_to) = 1, 7, EXTRACT(dayofweek FROM lux_listings_src_26000.valid_to) - 1) AS window_end__extract_dow
                , EXTRACT(dayofyear FROM lux_listings_src_26000.valid_to) AS window_end__extract_doy
                , lux_listings_src_26000.is_confirmed_lux
                , lux_listings_src_26000.valid_from AS lux_listing__window_start__day
                , DATETIME_TRUNC(lux_listings_src_26000.valid_from, isoweek) AS lux_listing__window_start__week
                , DATETIME_TRUNC(lux_listings_src_26000.valid_from, month) AS lux_listing__window_start__month
                , DATETIME_TRUNC(lux_listings_src_26000.valid_from, quarter) AS lux_listing__window_start__quarter
                , DATETIME_TRUNC(lux_listings_src_26000.valid_from, year) AS lux_listing__window_start__year
                , EXTRACT(year FROM lux_listings_src_26000.valid_from) AS lux_listing__window_start__extract_year
                , EXTRACT(quarter FROM lux_listings_src_26000.valid_from) AS lux_listing__window_start__extract_quarter
                , EXTRACT(month FROM lux_listings_src_26000.valid_from) AS lux_listing__window_start__extract_month
                , EXTRACT(day FROM lux_listings_src_26000.valid_from) AS lux_listing__window_start__extract_day
                , IF(EXTRACT(dayofweek FROM lux_listings_src_26000.valid_from) = 1, 7, EXTRACT(dayofweek FROM lux_listings_src_26000.valid_from) - 1) AS lux_listing__window_start__extract_dow
                , EXTRACT(dayofyear FROM lux_listings_src_26000.valid_from) AS lux_listing__window_start__extract_doy
                , lux_listings_src_26000.valid_to AS lux_listing__window_end__day
                , DATETIME_TRUNC(lux_listings_src_26000.valid_to, isoweek) AS lux_listing__window_end__week
                , DATETIME_TRUNC(lux_listings_src_26000.valid_to, month) AS lux_listing__window_end__month
                , DATETIME_TRUNC(lux_listings_src_26000.valid_to, quarter) AS lux_listing__window_end__quarter
                , DATETIME_TRUNC(lux_listings_src_26000.valid_to, year) AS lux_listing__window_end__year
                , EXTRACT(year FROM lux_listings_src_26000.valid_to) AS lux_listing__window_end__extract_year
                , EXTRACT(quarter FROM lux_listings_src_26000.valid_to) AS lux_listing__window_end__extract_quarter
                , EXTRACT(month FROM lux_listings_src_26000.valid_to) AS lux_listing__window_end__extract_month
                , EXTRACT(day FROM lux_listings_src_26000.valid_to) AS lux_listing__window_end__extract_day
                , IF(EXTRACT(dayofweek FROM lux_listings_src_26000.valid_to) = 1, 7, EXTRACT(dayofweek FROM lux_listings_src_26000.valid_to) - 1) AS lux_listing__window_end__extract_dow
                , EXTRACT(dayofyear FROM lux_listings_src_26000.valid_to) AS lux_listing__window_end__extract_doy
                , lux_listings_src_26000.is_confirmed_lux AS lux_listing__is_confirmed_lux
                , lux_listings_src_26000.lux_listing_id AS lux_listing
              FROM ***************************.dim_lux_listings lux_listings_src_26000
            ) subq_3
          ) subq_4
          ON
            subq_2.lux_listing = subq_4.lux_listing
        ) subq_5
      ) subq_6
      ON
        (
          subq_1.listing = subq_6.listing
        ) AND (
          (
            subq_1.metric_time__day >= subq_6.lux_listing__window_start__day
          ) AND (
            (
              subq_1.metric_time__day < subq_6.lux_listing__window_end__day
            ) OR (
              subq_6.lux_listing__window_end__day IS NULL
            )
          )
        )
    ) subq_7
  ) subq_8
  GROUP BY
    metric_time__day
    , listing__lux_listing__is_confirmed_lux
) subq_9
