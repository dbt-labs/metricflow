test_name: test_offset_metric_with_query_time_filters
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a query against a derived offset metric.

      TODO: support metric time filters
sql_engine: Postgres
---
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , listing_id AS listing
    , is_instant AS booking__is_instant
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

, sma_28014_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , country AS country_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , listing__country_latest AS listing__country_latest
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_34.metric_time__day, subq_46.metric_time__day) AS metric_time__day
    , COALESCE(subq_34.listing__country_latest, subq_46.listing__country_latest) AS listing__country_latest
    , MAX(subq_34.bookings) AS bookings
    , MAX(subq_46.bookings_2_weeks_ago) AS bookings_2_weeks_ago
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
        sma_28014_cte.country_latest AS listing__country_latest
        , sma_28009_cte.metric_time__day AS metric_time__day
        , sma_28009_cte.booking__is_instant AS booking__is_instant
        , sma_28009_cte.bookings AS bookings
      FROM sma_28009_cte sma_28009_cte
      LEFT OUTER JOIN
        sma_28014_cte sma_28014_cte
      ON
        sma_28009_cte.listing = sma_28014_cte.listing
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
        sma_28014_cte.country_latest AS listing__country_latest
        , subq_39.metric_time__day AS metric_time__day
        , subq_39.booking__is_instant AS booking__is_instant
        , subq_39.bookings AS bookings
      FROM (
        -- Join to Time Spine Dataset
        SELECT
          time_spine_src_28006.ds AS metric_time__day
          , sma_28009_cte.listing AS listing
          , sma_28009_cte.booking__is_instant AS booking__is_instant
          , sma_28009_cte.bookings AS bookings
        FROM ***************************.mf_time_spine time_spine_src_28006
        INNER JOIN
          sma_28009_cte sma_28009_cte
        ON
          time_spine_src_28006.ds - MAKE_INTERVAL(days => 14) = sma_28009_cte.metric_time__day
      ) subq_39
      LEFT OUTER JOIN
        sma_28014_cte sma_28014_cte
      ON
        subq_39.listing = sma_28014_cte.listing
    ) subq_42
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
      , listing__country_latest
  ) subq_46
  ON
    (
      subq_34.listing__country_latest = subq_46.listing__country_latest
    ) AND (
      subq_34.metric_time__day = subq_46.metric_time__day
    )
  GROUP BY
    COALESCE(subq_34.metric_time__day, subq_46.metric_time__day)
    , COALESCE(subq_34.listing__country_latest, subq_46.listing__country_latest)
) subq_47
