test_name: test_fnw
test_filename: test_foj.py
docstring:
  Check a soon-to-be-deprecated use case where a manifest contains a metric with the same name as a dimension.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , CAST(bookings AS DOUBLE) / CAST(NULLIF(listings, 0) AS DOUBLE) AS bookings_per_listing
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_19.metric_time__day, subq_25.metric_time__day) AS metric_time__day
    , MAX(subq_19.bookings) AS bookings
    , MAX(subq_25.listings) AS listings
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
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_17
    GROUP BY
      metric_time__day
  ) subq_19
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
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_23
    GROUP BY
      metric_time__day
  ) subq_25
  ON
    subq_19.metric_time__day = subq_25.metric_time__day
  GROUP BY
    COALESCE(subq_19.metric_time__day, subq_25.metric_time__day)
) subq_26
