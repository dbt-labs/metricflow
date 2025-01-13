test_name: test_metric_with_measures_from_multiple_sources_no_dimensions
test_filename: test_query_rendering.py
sql_engine: Clickhouse
---
-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  CAST(MAX(subq_15.bookings) AS DOUBLE PRECISION) / CAST(NULLIF(MAX(subq_20.listings), 0) AS DOUBLE PRECISION) AS bookings_per_listing
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookings',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_15
CROSS JOIN
(
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['listings',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS listings
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_20
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
