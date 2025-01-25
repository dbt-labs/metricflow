test_name: test_metric_with_measures_from_multiple_sources_no_dimensions
test_filename: test_query_rendering.py
sql_engine: BigQuery
---
-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  CAST(MAX(nr_subq_12.bookings) AS FLOAT64) / CAST(NULLIF(MAX(nr_subq_16.listings), 0) AS FLOAT64) AS bookings_per_listing
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookings',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) nr_subq_12
CROSS JOIN (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['listings',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS listings
  FROM ***************************.dim_listings_latest listings_latest_src_28000
) nr_subq_16
