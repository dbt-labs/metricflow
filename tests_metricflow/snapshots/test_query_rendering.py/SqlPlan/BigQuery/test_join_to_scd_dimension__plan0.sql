test_name: test_join_to_scd_dimension
test_filename: test_query_rendering.py
docstring:
  Tests conversion of a plan using a dimension with a validity window inside a simple-metric input constraint.
sql_engine: BigQuery
---
-- Write to DataTable
SELECT
  subq_9.metric_time__day
  , subq_9.family_bookings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_8.metric_time__day
    , subq_8.__family_bookings AS family_bookings
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_7.metric_time__day
      , SUM(subq_7.__family_bookings) AS __family_bookings
    FROM (
      -- Pass Only Elements: ['__family_bookings', 'metric_time__day']
      SELECT
        subq_6.metric_time__day
        , subq_6.__family_bookings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_5.family_bookings AS __family_bookings
          , subq_5.listing__capacity
          , subq_5.metric_time__day
        FROM (
          -- Pass Only Elements: ['__family_bookings', 'listing__capacity', 'metric_time__day']
          SELECT
            subq_4.metric_time__day
            , subq_4.listing__capacity
            , subq_4.__family_bookings AS family_bookings
          FROM (
            -- Join Standard Outputs
            SELECT
              subq_3.capacity AS listing__capacity
              , subq_3.window_start__day AS listing__window_start__day
              , subq_3.window_end__day AS listing__window_end__day
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
              , subq_1.__bookings AS __bookings
              , subq_1.__family_bookings AS __family_bookings
              , subq_1.__potentially_lux_bookings AS __potentially_lux_bookings
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
                , subq_0.__bookings
                , subq_0.__family_bookings
                , subq_0.__potentially_lux_bookings
              FROM (
                -- Read Elements From Semantic Model 'bookings_source'
                SELECT
                  1 AS __bookings
                  , 1 AS __family_bookings
                  , 1 AS __potentially_lux_bookings
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
              -- Pass Only Elements: ['capacity', 'window_start__day', 'window_end__day', 'listing']
              SELECT
                subq_2.window_start__day
                , subq_2.window_end__day
                , subq_2.listing
                , subq_2.capacity
              FROM (
                -- Read Elements From Semantic Model 'listings'
                SELECT
                  listings_src_26000.active_from AS window_start__day
                  , DATETIME_TRUNC(listings_src_26000.active_from, isoweek) AS window_start__week
                  , DATETIME_TRUNC(listings_src_26000.active_from, month) AS window_start__month
                  , DATETIME_TRUNC(listings_src_26000.active_from, quarter) AS window_start__quarter
                  , DATETIME_TRUNC(listings_src_26000.active_from, year) AS window_start__year
                  , EXTRACT(year FROM listings_src_26000.active_from) AS window_start__extract_year
                  , EXTRACT(quarter FROM listings_src_26000.active_from) AS window_start__extract_quarter
                  , EXTRACT(month FROM listings_src_26000.active_from) AS window_start__extract_month
                  , EXTRACT(day FROM listings_src_26000.active_from) AS window_start__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_src_26000.active_from) = 1, 7, EXTRACT(dayofweek FROM listings_src_26000.active_from) - 1) AS window_start__extract_dow
                  , EXTRACT(dayofyear FROM listings_src_26000.active_from) AS window_start__extract_doy
                  , listings_src_26000.active_to AS window_end__day
                  , DATETIME_TRUNC(listings_src_26000.active_to, isoweek) AS window_end__week
                  , DATETIME_TRUNC(listings_src_26000.active_to, month) AS window_end__month
                  , DATETIME_TRUNC(listings_src_26000.active_to, quarter) AS window_end__quarter
                  , DATETIME_TRUNC(listings_src_26000.active_to, year) AS window_end__year
                  , EXTRACT(year FROM listings_src_26000.active_to) AS window_end__extract_year
                  , EXTRACT(quarter FROM listings_src_26000.active_to) AS window_end__extract_quarter
                  , EXTRACT(month FROM listings_src_26000.active_to) AS window_end__extract_month
                  , EXTRACT(day FROM listings_src_26000.active_to) AS window_end__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_src_26000.active_to) = 1, 7, EXTRACT(dayofweek FROM listings_src_26000.active_to) - 1) AS window_end__extract_dow
                  , EXTRACT(dayofyear FROM listings_src_26000.active_to) AS window_end__extract_doy
                  , listings_src_26000.country
                  , listings_src_26000.is_lux
                  , listings_src_26000.capacity
                  , listings_src_26000.active_from AS listing__window_start__day
                  , DATETIME_TRUNC(listings_src_26000.active_from, isoweek) AS listing__window_start__week
                  , DATETIME_TRUNC(listings_src_26000.active_from, month) AS listing__window_start__month
                  , DATETIME_TRUNC(listings_src_26000.active_from, quarter) AS listing__window_start__quarter
                  , DATETIME_TRUNC(listings_src_26000.active_from, year) AS listing__window_start__year
                  , EXTRACT(year FROM listings_src_26000.active_from) AS listing__window_start__extract_year
                  , EXTRACT(quarter FROM listings_src_26000.active_from) AS listing__window_start__extract_quarter
                  , EXTRACT(month FROM listings_src_26000.active_from) AS listing__window_start__extract_month
                  , EXTRACT(day FROM listings_src_26000.active_from) AS listing__window_start__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_src_26000.active_from) = 1, 7, EXTRACT(dayofweek FROM listings_src_26000.active_from) - 1) AS listing__window_start__extract_dow
                  , EXTRACT(dayofyear FROM listings_src_26000.active_from) AS listing__window_start__extract_doy
                  , listings_src_26000.active_to AS listing__window_end__day
                  , DATETIME_TRUNC(listings_src_26000.active_to, isoweek) AS listing__window_end__week
                  , DATETIME_TRUNC(listings_src_26000.active_to, month) AS listing__window_end__month
                  , DATETIME_TRUNC(listings_src_26000.active_to, quarter) AS listing__window_end__quarter
                  , DATETIME_TRUNC(listings_src_26000.active_to, year) AS listing__window_end__year
                  , EXTRACT(year FROM listings_src_26000.active_to) AS listing__window_end__extract_year
                  , EXTRACT(quarter FROM listings_src_26000.active_to) AS listing__window_end__extract_quarter
                  , EXTRACT(month FROM listings_src_26000.active_to) AS listing__window_end__extract_month
                  , EXTRACT(day FROM listings_src_26000.active_to) AS listing__window_end__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_src_26000.active_to) = 1, 7, EXTRACT(dayofweek FROM listings_src_26000.active_to) - 1) AS listing__window_end__extract_dow
                  , EXTRACT(dayofyear FROM listings_src_26000.active_to) AS listing__window_end__extract_doy
                  , listings_src_26000.country AS listing__country
                  , listings_src_26000.is_lux AS listing__is_lux
                  , listings_src_26000.capacity AS listing__capacity
                  , listings_src_26000.listing_id AS listing
                  , listings_src_26000.user_id AS user
                  , listings_src_26000.user_id AS listing__user
                FROM ***************************.dim_listings listings_src_26000
              ) subq_2
            ) subq_3
            ON
              (
                subq_1.listing = subq_3.listing
              ) AND (
                (
                  subq_1.metric_time__day >= subq_3.window_start__day
                ) AND (
                  (
                    subq_1.metric_time__day < subq_3.window_end__day
                  ) OR (
                    subq_3.window_end__day IS NULL
                  )
                )
              )
          ) subq_4
        ) subq_5
        WHERE listing__capacity > 2
      ) subq_6
    ) subq_7
    GROUP BY
      metric_time__day
  ) subq_8
) subq_9
