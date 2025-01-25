test_name: test_offset_metric_with_query_time_filters
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a query against a derived offset metric.

      TODO: support metric time filters
sql_engine: Postgres
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , listing__country_latest
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_28.metric_time__day, nr_subq_40.metric_time__day) AS metric_time__day
    , COALESCE(nr_subq_28.listing__country_latest, nr_subq_40.listing__country_latest) AS listing__country_latest
    , MAX(nr_subq_28.bookings) AS bookings
    , MAX(nr_subq_40.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , SUM(bookings) AS bookings
    FROM (
      -- Join Standard Outputs
      SELECT
        listings_latest_src_28000.country AS listing__country_latest
        , nr_subq_21.metric_time__day AS metric_time__day
        , nr_subq_21.booking__is_instant AS booking__is_instant
        , nr_subq_21.bookings AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , is_instant AS booking__is_instant
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) nr_subq_21
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        nr_subq_21.listing = listings_latest_src_28000.listing_id
    ) nr_subq_24
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
      , listing__country_latest
  ) nr_subq_28
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , listing__country_latest
      , SUM(bookings) AS bookings_2_weeks_ago
    FROM (
      -- Join Standard Outputs
      SELECT
        listings_latest_src_28000.country AS listing__country_latest
        , nr_subq_33.metric_time__day AS metric_time__day
        , nr_subq_33.booking__is_instant AS booking__is_instant
        , nr_subq_33.bookings AS bookings
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          time_spine_src_28006.ds AS metric_time__day
          , nr_subq_29.listing AS listing
          , nr_subq_29.booking__is_instant AS booking__is_instant
          , nr_subq_29.bookings AS bookings
        FROM ***************************.mf_time_spine time_spine_src_28006
        INNER JOIN (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , listing_id AS listing
            , is_instant AS booking__is_instant
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) nr_subq_29
        ON
          time_spine_src_28006.ds - MAKE_INTERVAL(days => 14) = nr_subq_29.metric_time__day
      ) nr_subq_33
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        nr_subq_33.listing = listings_latest_src_28000.listing_id
    ) nr_subq_36
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
      , listing__country_latest
  ) nr_subq_40
  ON
    (
      nr_subq_28.listing__country_latest = nr_subq_40.listing__country_latest
    ) AND (
      nr_subq_28.metric_time__day = nr_subq_40.metric_time__day
    )
  GROUP BY
    COALESCE(nr_subq_28.metric_time__day, nr_subq_40.metric_time__day)
    , COALESCE(nr_subq_28.listing__country_latest, nr_subq_40.listing__country_latest)
) nr_subq_41
