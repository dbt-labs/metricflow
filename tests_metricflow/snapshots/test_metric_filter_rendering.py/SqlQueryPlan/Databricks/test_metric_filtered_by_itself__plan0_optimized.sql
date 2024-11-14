test_name: test_metric_filtered_by_itself
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query for a metric that filters by the same metric.
sql_engine: Databricks
---
-- Read From CTE For node_id=cm_4
WITH cm_3_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookers', 'listing']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    listing_id AS listing
    , COUNT(DISTINCT guest_id) AS listing__bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    listing_id
)

, cm_4_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookers',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    COUNT(DISTINCT bookers) AS bookers
  FROM (
    -- Join Standard Outputs
    SELECT
      cm_3_cte.listing__bookers AS listing__bookers
      , bookings_source_src_28000.guest_id AS bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
    LEFT OUTER JOIN
      cm_3_cte cm_3_cte
    ON
      bookings_source_src_28000.listing_id = cm_3_cte.listing
  ) subq_20
  WHERE listing__bookers > 1.00
)

SELECT
  bookers AS bookers
FROM cm_4_cte cm_4_cte
