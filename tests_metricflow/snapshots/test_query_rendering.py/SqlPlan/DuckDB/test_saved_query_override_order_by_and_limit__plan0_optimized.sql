test_name: test_saved_query_override_order_by_and_limit
test_filename: test_query_rendering.py
sql_engine: DuckDB
---
-- Combine Aggregated Outputs
-- Order By ['bookings', 'views', 'listing__capacity_latest', 'metric_time__day'] Limit 5
-- Write to DataTable
WITH sma_28014_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , capacity AS capacity_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

SELECT
  COALESCE(subq_27.metric_time__day, subq_35.metric_time__day) AS metric_time__day
  , COALESCE(subq_27.listing__capacity_latest, subq_35.listing__capacity_latest) AS listing__capacity_latest
  , MAX(subq_27.bookings) AS bookings
  , MAX(subq_35.views) AS views
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__bookings', 'listing__capacity_latest', 'metric_time__day']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    subq_20.metric_time__day AS metric_time__day
    , sma_28014_cte.capacity_latest AS listing__capacity_latest
    , SUM(subq_20.__bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS __bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_20
  LEFT OUTER JOIN
    sma_28014_cte
  ON
    subq_20.listing = sma_28014_cte.listing
  GROUP BY
    subq_20.metric_time__day
    , sma_28014_cte.capacity_latest
) subq_27
FULL OUTER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__views', 'listing__capacity_latest', 'metric_time__day']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    subq_29.metric_time__day AS metric_time__day
    , sma_28014_cte.capacity_latest AS listing__capacity_latest
    , SUM(subq_29.__views) AS views
  FROM (
    -- Read Elements From Semantic Model 'views_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS __views
    FROM ***************************.fct_views views_source_src_28000
  ) subq_29
  LEFT OUTER JOIN
    sma_28014_cte
  ON
    subq_29.listing = sma_28014_cte.listing
  GROUP BY
    subq_29.metric_time__day
    , sma_28014_cte.capacity_latest
) subq_35
ON
  (
    subq_27.listing__capacity_latest = subq_35.listing__capacity_latest
  ) AND (
    subq_27.metric_time__day = subq_35.metric_time__day
  )
GROUP BY
  COALESCE(subq_27.metric_time__day, subq_35.metric_time__day)
  , COALESCE(subq_27.listing__capacity_latest, subq_35.listing__capacity_latest)
ORDER BY bookings, views, listing__capacity_latest, metric_time__day
LIMIT 5
