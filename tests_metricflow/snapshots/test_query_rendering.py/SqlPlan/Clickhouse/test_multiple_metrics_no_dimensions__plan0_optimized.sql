test_name: test_multiple_metrics_no_dimensions
test_filename: test_query_rendering.py
sql_engine: Clickhouse
---
-- Combine Aggregated Outputs
SELECT
  MAX(subq_17.bookings) AS bookings
  , MAX(subq_23.listings) AS listings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
  -- Pass Only Elements: ['bookings',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
  WHERE DATE_TRUNC('day', ds) BETWEEN '2020-01-01' AND '2020-01-01'
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_17
CROSS JOIN
(
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
  -- Pass Only Elements: ['listings',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS listings
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  WHERE DATE_TRUNC('day', created_at) BETWEEN '2020-01-01' AND '2020-01-01'
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_23
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
