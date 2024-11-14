test_name: test_metric_with_measures_from_multiple_sources_no_dimensions
test_filename: test_query_rendering.py
sql_engine: Trino
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookings',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

, cm_7_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['listings',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS listings
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

, cm_8_cte AS (
  -- Combine Aggregated Outputs
  -- Compute Metrics via Expressions
  SELECT
    CAST(MAX(cm_6_cte.bookings) AS DOUBLE) / CAST(NULLIF(MAX(cm_7_cte.listings), 0) AS DOUBLE) AS bookings_per_listing
  FROM cm_6_cte cm_6_cte
  CROSS JOIN
    cm_7_cte cm_7_cte
)

SELECT
  bookings_per_listing AS bookings_per_listing
FROM cm_8_cte cm_8_cte
