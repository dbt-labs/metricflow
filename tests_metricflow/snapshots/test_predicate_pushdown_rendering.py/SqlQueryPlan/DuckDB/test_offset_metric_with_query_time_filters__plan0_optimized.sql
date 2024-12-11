test_name: test_offset_metric_with_query_time_filters
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a query against a derived offset metric.

      TODO: support metric time filters
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , listing__country_latest
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_34.metric_time__day, subq_48.metric_time__day) AS metric_time__day
    , COALESCE(subq_34.listing__country_latest, subq_48.listing__country_latest) AS listing__country_latest
    , MAX(subq_34.bookings) AS bookings
    , MAX(subq_48.bookings_2_weeks_ago) AS bookings_2_weeks_ago
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
        , subq_26.metric_time__day AS metric_time__day
        , subq_26.booking__is_instant AS booking__is_instant
        , subq_26.bookings AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , listing_id AS listing
          , is_instant AS booking__is_instant
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_26
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_26.listing = listings_latest_src_28000.listing_id
    ) subq_30
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
      , listing__country_latest
  ) subq_34
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
        , subq_40.metric_time__day AS metric_time__day
        , subq_40.booking__is_instant AS booking__is_instant
        , subq_40.bookings AS bookings
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          time_spine_src_28006.ds AS metric_time__day
          , subq_36.listing AS listing
          , subq_36.booking__is_instant AS booking__is_instant
          , subq_36.bookings AS bookings
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
        ) subq_36
        ON
          time_spine_src_28006.ds - INTERVAL 14 day = subq_36.metric_time__day
      ) subq_40
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_28000
      ON
        subq_40.listing = listings_latest_src_28000.listing_id
    ) subq_44
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
      , listing__country_latest
  ) subq_48
  ON
    (
      subq_34.listing__country_latest = subq_48.listing__country_latest
    ) AND (
      subq_34.metric_time__day = subq_48.metric_time__day
    )
  GROUP BY
    COALESCE(subq_34.metric_time__day, subq_48.metric_time__day)
    , COALESCE(subq_34.listing__country_latest, subq_48.listing__country_latest)
) subq_49
