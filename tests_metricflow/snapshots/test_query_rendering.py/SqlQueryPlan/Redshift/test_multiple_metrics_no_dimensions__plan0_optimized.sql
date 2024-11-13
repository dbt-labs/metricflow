test_name: test_multiple_metrics_no_dimensions
test_filename: test_query_rendering.py
sql_engine: Redshift
---
-- Combine Aggregated Outputs
WITH cm_4_cte AS (
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
)

, cm_5_cte AS (
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
)

SELECT
  MAX(cm_4_cte.bookings) AS bookings
  , MAX(cm_5_cte.listings) AS listings
FROM cm_4_cte cm_4_cte
CROSS JOIN
  cm_5_cte cm_5_cte
