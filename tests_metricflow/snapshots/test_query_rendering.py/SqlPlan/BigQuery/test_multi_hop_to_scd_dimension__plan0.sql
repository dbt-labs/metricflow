test_name: test_multi_hop_to_scd_dimension
test_filename: test_query_rendering.py
docstring:
  Tests conversion of a plan using an SCD dimension that is reached through another table.
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_11.metric_time__day
  , nr_subq_11.listing__lux_listing__is_confirmed_lux
  , nr_subq_11.bookings
FROM (
  -- Aggregate Measures
  SELECT
    nr_subq_10.metric_time__day
    , nr_subq_10.listing__lux_listing__is_confirmed_lux
    , SUM(nr_subq_10.bookings) AS bookings
  FROM (
    -- Pass Only Elements: ['bookings', 'listing__lux_listing__is_confirmed_lux', 'metric_time__day']
    SELECT
      nr_subq_9.metric_time__day
      , nr_subq_9.listing__lux_listing__is_confirmed_lux
      , nr_subq_9.bookings
    FROM (
      -- Join Standard Outputs
      SELECT
        nr_subq_8.lux_listing__is_confirmed_lux AS listing__lux_listing__is_confirmed_lux
        , nr_subq_8.lux_listing__window_start__day AS listing__lux_listing__window_start__day
        , nr_subq_8.lux_listing__window_end__day AS listing__lux_listing__window_end__day
        , nr_subq_3.ds__day AS ds__day
        , nr_subq_3.ds__week AS ds__week
        , nr_subq_3.ds__month AS ds__month
        , nr_subq_3.ds__quarter AS ds__quarter
        , nr_subq_3.ds__year AS ds__year
        , nr_subq_3.ds__extract_year AS ds__extract_year
        , nr_subq_3.ds__extract_quarter AS ds__extract_quarter
        , nr_subq_3.ds__extract_month AS ds__extract_month
        , nr_subq_3.ds__extract_day AS ds__extract_day
        , nr_subq_3.ds__extract_dow AS ds__extract_dow
        , nr_subq_3.ds__extract_doy AS ds__extract_doy
        , nr_subq_3.ds_partitioned__day AS ds_partitioned__day
        , nr_subq_3.ds_partitioned__week AS ds_partitioned__week
        , nr_subq_3.ds_partitioned__month AS ds_partitioned__month
        , nr_subq_3.ds_partitioned__quarter AS ds_partitioned__quarter
        , nr_subq_3.ds_partitioned__year AS ds_partitioned__year
        , nr_subq_3.ds_partitioned__extract_year AS ds_partitioned__extract_year
        , nr_subq_3.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
        , nr_subq_3.ds_partitioned__extract_month AS ds_partitioned__extract_month
        , nr_subq_3.ds_partitioned__extract_day AS ds_partitioned__extract_day
        , nr_subq_3.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
        , nr_subq_3.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
        , nr_subq_3.paid_at__day AS paid_at__day
        , nr_subq_3.paid_at__week AS paid_at__week
        , nr_subq_3.paid_at__month AS paid_at__month
        , nr_subq_3.paid_at__quarter AS paid_at__quarter
        , nr_subq_3.paid_at__year AS paid_at__year
        , nr_subq_3.paid_at__extract_year AS paid_at__extract_year
        , nr_subq_3.paid_at__extract_quarter AS paid_at__extract_quarter
        , nr_subq_3.paid_at__extract_month AS paid_at__extract_month
        , nr_subq_3.paid_at__extract_day AS paid_at__extract_day
        , nr_subq_3.paid_at__extract_dow AS paid_at__extract_dow
        , nr_subq_3.paid_at__extract_doy AS paid_at__extract_doy
        , nr_subq_3.booking__ds__day AS booking__ds__day
        , nr_subq_3.booking__ds__week AS booking__ds__week
        , nr_subq_3.booking__ds__month AS booking__ds__month
        , nr_subq_3.booking__ds__quarter AS booking__ds__quarter
        , nr_subq_3.booking__ds__year AS booking__ds__year
        , nr_subq_3.booking__ds__extract_year AS booking__ds__extract_year
        , nr_subq_3.booking__ds__extract_quarter AS booking__ds__extract_quarter
        , nr_subq_3.booking__ds__extract_month AS booking__ds__extract_month
        , nr_subq_3.booking__ds__extract_day AS booking__ds__extract_day
        , nr_subq_3.booking__ds__extract_dow AS booking__ds__extract_dow
        , nr_subq_3.booking__ds__extract_doy AS booking__ds__extract_doy
        , nr_subq_3.booking__ds_partitioned__day AS booking__ds_partitioned__day
        , nr_subq_3.booking__ds_partitioned__week AS booking__ds_partitioned__week
        , nr_subq_3.booking__ds_partitioned__month AS booking__ds_partitioned__month
        , nr_subq_3.booking__ds_partitioned__quarter AS booking__ds_partitioned__quarter
        , nr_subq_3.booking__ds_partitioned__year AS booking__ds_partitioned__year
        , nr_subq_3.booking__ds_partitioned__extract_year AS booking__ds_partitioned__extract_year
        , nr_subq_3.booking__ds_partitioned__extract_quarter AS booking__ds_partitioned__extract_quarter
        , nr_subq_3.booking__ds_partitioned__extract_month AS booking__ds_partitioned__extract_month
        , nr_subq_3.booking__ds_partitioned__extract_day AS booking__ds_partitioned__extract_day
        , nr_subq_3.booking__ds_partitioned__extract_dow AS booking__ds_partitioned__extract_dow
        , nr_subq_3.booking__ds_partitioned__extract_doy AS booking__ds_partitioned__extract_doy
        , nr_subq_3.booking__paid_at__day AS booking__paid_at__day
        , nr_subq_3.booking__paid_at__week AS booking__paid_at__week
        , nr_subq_3.booking__paid_at__month AS booking__paid_at__month
        , nr_subq_3.booking__paid_at__quarter AS booking__paid_at__quarter
        , nr_subq_3.booking__paid_at__year AS booking__paid_at__year
        , nr_subq_3.booking__paid_at__extract_year AS booking__paid_at__extract_year
        , nr_subq_3.booking__paid_at__extract_quarter AS booking__paid_at__extract_quarter
        , nr_subq_3.booking__paid_at__extract_month AS booking__paid_at__extract_month
        , nr_subq_3.booking__paid_at__extract_day AS booking__paid_at__extract_day
        , nr_subq_3.booking__paid_at__extract_dow AS booking__paid_at__extract_dow
        , nr_subq_3.booking__paid_at__extract_doy AS booking__paid_at__extract_doy
        , nr_subq_3.metric_time__day AS metric_time__day
        , nr_subq_3.metric_time__week AS metric_time__week
        , nr_subq_3.metric_time__month AS metric_time__month
        , nr_subq_3.metric_time__quarter AS metric_time__quarter
        , nr_subq_3.metric_time__year AS metric_time__year
        , nr_subq_3.metric_time__extract_year AS metric_time__extract_year
        , nr_subq_3.metric_time__extract_quarter AS metric_time__extract_quarter
        , nr_subq_3.metric_time__extract_month AS metric_time__extract_month
        , nr_subq_3.metric_time__extract_day AS metric_time__extract_day
        , nr_subq_3.metric_time__extract_dow AS metric_time__extract_dow
        , nr_subq_3.metric_time__extract_doy AS metric_time__extract_doy
        , nr_subq_3.listing AS listing
        , nr_subq_3.guest AS guest
        , nr_subq_3.host AS host
        , nr_subq_3.user AS user
        , nr_subq_3.booking__listing AS booking__listing
        , nr_subq_3.booking__guest AS booking__guest
        , nr_subq_3.booking__host AS booking__host
        , nr_subq_3.booking__user AS booking__user
        , nr_subq_3.is_instant AS is_instant
        , nr_subq_3.booking__is_instant AS booking__is_instant
        , nr_subq_3.bookings AS bookings
        , nr_subq_3.instant_bookings AS instant_bookings
        , nr_subq_3.booking_value AS booking_value
        , nr_subq_3.bookers AS bookers
        , nr_subq_3.average_booking_value AS average_booking_value
      FROM (
        -- Metric Time Dimension 'ds'
        SELECT
          nr_subq_26000.ds__day
          , nr_subq_26000.ds__week
          , nr_subq_26000.ds__month
          , nr_subq_26000.ds__quarter
          , nr_subq_26000.ds__year
          , nr_subq_26000.ds__extract_year
          , nr_subq_26000.ds__extract_quarter
          , nr_subq_26000.ds__extract_month
          , nr_subq_26000.ds__extract_day
          , nr_subq_26000.ds__extract_dow
          , nr_subq_26000.ds__extract_doy
          , nr_subq_26000.ds_partitioned__day
          , nr_subq_26000.ds_partitioned__week
          , nr_subq_26000.ds_partitioned__month
          , nr_subq_26000.ds_partitioned__quarter
          , nr_subq_26000.ds_partitioned__year
          , nr_subq_26000.ds_partitioned__extract_year
          , nr_subq_26000.ds_partitioned__extract_quarter
          , nr_subq_26000.ds_partitioned__extract_month
          , nr_subq_26000.ds_partitioned__extract_day
          , nr_subq_26000.ds_partitioned__extract_dow
          , nr_subq_26000.ds_partitioned__extract_doy
          , nr_subq_26000.paid_at__day
          , nr_subq_26000.paid_at__week
          , nr_subq_26000.paid_at__month
          , nr_subq_26000.paid_at__quarter
          , nr_subq_26000.paid_at__year
          , nr_subq_26000.paid_at__extract_year
          , nr_subq_26000.paid_at__extract_quarter
          , nr_subq_26000.paid_at__extract_month
          , nr_subq_26000.paid_at__extract_day
          , nr_subq_26000.paid_at__extract_dow
          , nr_subq_26000.paid_at__extract_doy
          , nr_subq_26000.booking__ds__day
          , nr_subq_26000.booking__ds__week
          , nr_subq_26000.booking__ds__month
          , nr_subq_26000.booking__ds__quarter
          , nr_subq_26000.booking__ds__year
          , nr_subq_26000.booking__ds__extract_year
          , nr_subq_26000.booking__ds__extract_quarter
          , nr_subq_26000.booking__ds__extract_month
          , nr_subq_26000.booking__ds__extract_day
          , nr_subq_26000.booking__ds__extract_dow
          , nr_subq_26000.booking__ds__extract_doy
          , nr_subq_26000.booking__ds_partitioned__day
          , nr_subq_26000.booking__ds_partitioned__week
          , nr_subq_26000.booking__ds_partitioned__month
          , nr_subq_26000.booking__ds_partitioned__quarter
          , nr_subq_26000.booking__ds_partitioned__year
          , nr_subq_26000.booking__ds_partitioned__extract_year
          , nr_subq_26000.booking__ds_partitioned__extract_quarter
          , nr_subq_26000.booking__ds_partitioned__extract_month
          , nr_subq_26000.booking__ds_partitioned__extract_day
          , nr_subq_26000.booking__ds_partitioned__extract_dow
          , nr_subq_26000.booking__ds_partitioned__extract_doy
          , nr_subq_26000.booking__paid_at__day
          , nr_subq_26000.booking__paid_at__week
          , nr_subq_26000.booking__paid_at__month
          , nr_subq_26000.booking__paid_at__quarter
          , nr_subq_26000.booking__paid_at__year
          , nr_subq_26000.booking__paid_at__extract_year
          , nr_subq_26000.booking__paid_at__extract_quarter
          , nr_subq_26000.booking__paid_at__extract_month
          , nr_subq_26000.booking__paid_at__extract_day
          , nr_subq_26000.booking__paid_at__extract_dow
          , nr_subq_26000.booking__paid_at__extract_doy
          , nr_subq_26000.ds__day AS metric_time__day
          , nr_subq_26000.ds__week AS metric_time__week
          , nr_subq_26000.ds__month AS metric_time__month
          , nr_subq_26000.ds__quarter AS metric_time__quarter
          , nr_subq_26000.ds__year AS metric_time__year
          , nr_subq_26000.ds__extract_year AS metric_time__extract_year
          , nr_subq_26000.ds__extract_quarter AS metric_time__extract_quarter
          , nr_subq_26000.ds__extract_month AS metric_time__extract_month
          , nr_subq_26000.ds__extract_day AS metric_time__extract_day
          , nr_subq_26000.ds__extract_dow AS metric_time__extract_dow
          , nr_subq_26000.ds__extract_doy AS metric_time__extract_doy
          , nr_subq_26000.listing
          , nr_subq_26000.guest
          , nr_subq_26000.host
          , nr_subq_26000.user
          , nr_subq_26000.booking__listing
          , nr_subq_26000.booking__guest
          , nr_subq_26000.booking__host
          , nr_subq_26000.booking__user
          , nr_subq_26000.is_instant
          , nr_subq_26000.booking__is_instant
          , nr_subq_26000.bookings
          , nr_subq_26000.instant_bookings
          , nr_subq_26000.booking_value
          , nr_subq_26000.bookers
          , nr_subq_26000.average_booking_value
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
        ) nr_subq_26000
      ) nr_subq_3
      LEFT OUTER JOIN (
        -- Pass Only Elements: ['lux_listing__is_confirmed_lux', 'lux_listing__window_start__day', 'lux_listing__window_end__day', 'listing']
        SELECT
          nr_subq_7.lux_listing__window_start__day
          , nr_subq_7.lux_listing__window_end__day
          , nr_subq_7.listing
          , nr_subq_7.lux_listing__is_confirmed_lux
        FROM (
          -- Join Standard Outputs
          SELECT
            nr_subq_6.is_confirmed_lux AS lux_listing__is_confirmed_lux
            , nr_subq_6.window_start__day AS lux_listing__window_start__day
            , nr_subq_6.window_start__week AS lux_listing__window_start__week
            , nr_subq_6.window_start__month AS lux_listing__window_start__month
            , nr_subq_6.window_start__quarter AS lux_listing__window_start__quarter
            , nr_subq_6.window_start__year AS lux_listing__window_start__year
            , nr_subq_6.window_start__extract_year AS lux_listing__window_start__extract_year
            , nr_subq_6.window_start__extract_quarter AS lux_listing__window_start__extract_quarter
            , nr_subq_6.window_start__extract_month AS lux_listing__window_start__extract_month
            , nr_subq_6.window_start__extract_day AS lux_listing__window_start__extract_day
            , nr_subq_6.window_start__extract_dow AS lux_listing__window_start__extract_dow
            , nr_subq_6.window_start__extract_doy AS lux_listing__window_start__extract_doy
            , nr_subq_6.window_end__day AS lux_listing__window_end__day
            , nr_subq_6.window_end__week AS lux_listing__window_end__week
            , nr_subq_6.window_end__month AS lux_listing__window_end__month
            , nr_subq_6.window_end__quarter AS lux_listing__window_end__quarter
            , nr_subq_6.window_end__year AS lux_listing__window_end__year
            , nr_subq_6.window_end__extract_year AS lux_listing__window_end__extract_year
            , nr_subq_6.window_end__extract_quarter AS lux_listing__window_end__extract_quarter
            , nr_subq_6.window_end__extract_month AS lux_listing__window_end__extract_month
            , nr_subq_6.window_end__extract_day AS lux_listing__window_end__extract_day
            , nr_subq_6.window_end__extract_dow AS lux_listing__window_end__extract_dow
            , nr_subq_6.window_end__extract_doy AS lux_listing__window_end__extract_doy
            , nr_subq_4.listing AS listing
            , nr_subq_4.lux_listing AS lux_listing
            , nr_subq_4.listing__lux_listing AS listing__lux_listing
          FROM (
            -- Read Elements From Semantic Model 'lux_listing_mapping'
            SELECT
              lux_listing_mapping_src_26000.listing_id AS listing
              , lux_listing_mapping_src_26000.lux_listing_id AS lux_listing
              , lux_listing_mapping_src_26000.lux_listing_id AS listing__lux_listing
            FROM ***************************.dim_lux_listing_id_mapping lux_listing_mapping_src_26000
          ) nr_subq_4
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
              nr_subq_5.window_start__day
              , nr_subq_5.window_start__week
              , nr_subq_5.window_start__month
              , nr_subq_5.window_start__quarter
              , nr_subq_5.window_start__year
              , nr_subq_5.window_start__extract_year
              , nr_subq_5.window_start__extract_quarter
              , nr_subq_5.window_start__extract_month
              , nr_subq_5.window_start__extract_day
              , nr_subq_5.window_start__extract_dow
              , nr_subq_5.window_start__extract_doy
              , nr_subq_5.window_end__day
              , nr_subq_5.window_end__week
              , nr_subq_5.window_end__month
              , nr_subq_5.window_end__quarter
              , nr_subq_5.window_end__year
              , nr_subq_5.window_end__extract_year
              , nr_subq_5.window_end__extract_quarter
              , nr_subq_5.window_end__extract_month
              , nr_subq_5.window_end__extract_day
              , nr_subq_5.window_end__extract_dow
              , nr_subq_5.window_end__extract_doy
              , nr_subq_5.lux_listing__window_start__day
              , nr_subq_5.lux_listing__window_start__week
              , nr_subq_5.lux_listing__window_start__month
              , nr_subq_5.lux_listing__window_start__quarter
              , nr_subq_5.lux_listing__window_start__year
              , nr_subq_5.lux_listing__window_start__extract_year
              , nr_subq_5.lux_listing__window_start__extract_quarter
              , nr_subq_5.lux_listing__window_start__extract_month
              , nr_subq_5.lux_listing__window_start__extract_day
              , nr_subq_5.lux_listing__window_start__extract_dow
              , nr_subq_5.lux_listing__window_start__extract_doy
              , nr_subq_5.lux_listing__window_end__day
              , nr_subq_5.lux_listing__window_end__week
              , nr_subq_5.lux_listing__window_end__month
              , nr_subq_5.lux_listing__window_end__quarter
              , nr_subq_5.lux_listing__window_end__year
              , nr_subq_5.lux_listing__window_end__extract_year
              , nr_subq_5.lux_listing__window_end__extract_quarter
              , nr_subq_5.lux_listing__window_end__extract_month
              , nr_subq_5.lux_listing__window_end__extract_day
              , nr_subq_5.lux_listing__window_end__extract_dow
              , nr_subq_5.lux_listing__window_end__extract_doy
              , nr_subq_5.lux_listing
              , nr_subq_5.is_confirmed_lux
              , nr_subq_5.lux_listing__is_confirmed_lux
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
            ) nr_subq_5
          ) nr_subq_6
          ON
            nr_subq_4.lux_listing = nr_subq_6.lux_listing
        ) nr_subq_7
      ) nr_subq_8
      ON
        (
          nr_subq_3.listing = nr_subq_8.listing
        ) AND (
          (
            nr_subq_3.metric_time__day >= nr_subq_8.lux_listing__window_start__day
          ) AND (
            (
              nr_subq_3.metric_time__day < nr_subq_8.lux_listing__window_end__day
            ) OR (
              nr_subq_8.lux_listing__window_end__day IS NULL
            )
          )
        )
    ) nr_subq_9
  ) nr_subq_10
  GROUP BY
    metric_time__day
    , listing__lux_listing__is_confirmed_lux
) nr_subq_11
