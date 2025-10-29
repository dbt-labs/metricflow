test_name: test_filter_by_metric_in_same_semantic_model_as_queried_metric
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a simple metric in the query-level where filter.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookers']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    guest_id AS guest
    , guest_id AS bookers
    , booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  COUNT(DISTINCT bookers) AS bookers
FROM (
  -- Join Standard Outputs
  SELECT
    subq_21.guest__booking_value AS guest__booking_value
    , subq_16.bookers AS bookers
  FROM (
    -- Read From CTE For node_id=sma_28009
    SELECT
      guest
      , bookers
    FROM sma_28009_cte
  ) subq_16
  LEFT OUTER JOIN (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['booking_value', 'guest']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['guest', 'guest__booking_value']
    SELECT
      guest
      , SUM(booking_value) AS guest__booking_value
    FROM sma_28009_cte
    GROUP BY
      guest
  ) subq_21
  ON
    subq_16.guest = subq_21.guest
) subq_22
WHERE guest__booking_value > 1.00
