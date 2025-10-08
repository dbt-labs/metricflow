test_name: test_offset_metric_with_query_time_filters
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a query against a derived offset metric.

      TODO: support metric time filters
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
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
    COALESCE(subq_33.metric_time__day, subq_45.metric_time__day) AS metric_time__day
    , COALESCE(subq_33.listing__country_latest, subq_45.listing__country_latest) AS listing__country_latest
    , MAX(subq_33.bookings) AS bookings
    , MAX(subq_45.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
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
      FROM sma_28009_cte
      LEFT OUTER JOIN
        sma_28014_cte
      ON
        sma_28009_cte.listing = sma_28014_cte.listing
    ) subq_29
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
      , listing__country_latest
  ) subq_33
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Compute Metrics via Expressions
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , subq_40.listing__country_latest AS listing__country_latest
      , subq_40.bookings_2_weeks_ago AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        metric_time__day
        , listing__country_latest
        , SUM(bookings) AS bookings_2_weeks_ago
      FROM (
        -- Join Standard Outputs
        SELECT
          sma_28014_cte.country_latest AS listing__country_latest
          , sma_28009_cte.metric_time__day AS metric_time__day
          , sma_28009_cte.booking__is_instant AS booking__is_instant
          , sma_28009_cte.bookings AS bookings
        FROM sma_28009_cte
        LEFT OUTER JOIN
          sma_28014_cte
        ON
          sma_28009_cte.listing = sma_28014_cte.listing
      ) subq_37
      WHERE booking__is_instant
      GROUP BY
        metric_time__day
        , listing__country_latest
    ) subq_40
    ON
      DATE_SUB(CAST(time_spine_src_28006.ds AS DATETIME), INTERVAL 14 day) = subq_40.metric_time__day
  ) subq_45
  ON
    (
      subq_33.listing__country_latest = subq_45.listing__country_latest
    ) AND (
      subq_33.metric_time__day = subq_45.metric_time__day
    )
  GROUP BY
    metric_time__day
    , listing__country_latest
) subq_46
