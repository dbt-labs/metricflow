test_name: test_metric_with_simple_metrics_from_multiple_sources_no_dimensions
test_filename: test_query_rendering.py
sql_engine: DuckDB
---
-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  CAST(MAX(subq_19.bookings) AS DOUBLE) / CAST(NULLIF(MAX(subq_25.listings), 0) AS DOUBLE) AS bookings_per_listing
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['__bookings']
  -- Pass Only Elements: ['__bookings']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_19
CROSS JOIN (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['__listings']
  -- Pass Only Elements: ['__listings']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS listings
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) subq_25
