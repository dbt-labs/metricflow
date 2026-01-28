test_name: test_query_with_metric_in_where_filter_with_metric_time
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a metric in the query-level where filter using metric_time.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__listings', 'metric_time__day']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__listings', 'metric_time__day', 'listing__bookings']
  SELECT
    subq_20.metric_time__day AS metric_time__day
    , subq_27.listing__bookings AS listing__bookings
    , subq_20.__listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', created_at) AS metric_time__day
      , listing_id AS listing
      , 1 AS __listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_20
  LEFT OUTER JOIN (
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['metric_time__day', 'listing', 'listing__bookings']
    SELECT
      metric_time__day
      , listing
      , SUM(__bookings) AS listing__bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['__bookings', 'metric_time__day', 'listing']
      -- Pass Only Elements: ['__bookings', 'metric_time__day', 'listing']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_24
    GROUP BY
      metric_time__day
      , listing
  ) subq_27
  ON
    (
      subq_20.listing = subq_27.listing
    ) AND (
      subq_20.metric_time__day = subq_27.metric_time__day
    )
) subq_29
WHERE listing__bookings > 2
GROUP BY
  metric_time__day
