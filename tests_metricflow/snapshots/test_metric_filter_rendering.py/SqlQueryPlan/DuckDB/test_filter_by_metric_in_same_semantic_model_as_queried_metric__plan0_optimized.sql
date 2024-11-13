test_name: test_filter_by_metric_in_same_semantic_model_as_queried_metric
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a simple metric in the query-level where filter.
sql_engine: DuckDB
---
-- Read From CTE For node_id=cm_4
WITH cm_3_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['booking_value', 'guest']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    guest_id AS guest
    , SUM(booking_value) AS guest__booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    guest_id
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
      cm_3_cte.guest__booking_value AS guest__booking_value
      , bookings_source_src_28000.guest_id AS bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
    LEFT OUTER JOIN
      cm_3_cte cm_3_cte
    ON
      bookings_source_src_28000.guest_id = cm_3_cte.guest
  ) subq_20
  WHERE guest__booking_value > 1.00
)

SELECT
  bookers AS bookers
FROM cm_4_cte cm_4_cte
