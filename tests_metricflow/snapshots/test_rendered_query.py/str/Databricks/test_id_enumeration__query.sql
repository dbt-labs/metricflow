test_name: test_id_enumeration
test_filename: test_rendered_query.py
sql_engine: Databricks
---
-- Combine Aggregated Outputs
-- Write to DataTable
SELECT
  COALESCE(subq_5.metric_time__day, subq_11.metric_time__day) AS metric_time__day
  , MAX(subq_5.bookings) AS bookings
  , MAX(subq_11.listings) AS listings
FROM (
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(__bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['__bookings', 'metric_time__day']
    -- Pass Only Elements: ['__bookings', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS __bookings
    FROM ***************************.fct_bookings bookings_source_src_10000
  ) subq_3
  GROUP BY
    metric_time__day
) subq_5
FULL OUTER JOIN (
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(__listings) AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['__listings', 'metric_time__day']
    -- Pass Only Elements: ['__listings', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', created_at) AS metric_time__day
      , 1 AS __listings
    FROM ***************************.dim_listings_latest listings_latest_src_10000
  ) subq_9
  GROUP BY
    metric_time__day
) subq_11
ON
  subq_5.metric_time__day = subq_11.metric_time__day
GROUP BY
  COALESCE(subq_5.metric_time__day, subq_11.metric_time__day)
