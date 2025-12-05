test_name: test_filter_by_metric_in_same_semantic_model_as_queried_metric
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a simple metric in the query-level where filter.
sql_engine: BigQuery
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__bookers']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    guest_id AS guest
    , booking_value AS __booking_value
    , guest_id AS __bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  COUNT(DISTINCT bookers) AS bookers
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__bookers', 'guest__booking_value']
  SELECT
    subq_25.guest__booking_value AS guest__booking_value
    , subq_19.__bookers AS bookers
  FROM (
    -- Read From CTE For node_id=sma_28009
    SELECT
      guest
      , __bookers
    FROM sma_28009_cte
  ) subq_19
  LEFT OUTER JOIN (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['__booking_value', 'guest']
    -- Pass Only Elements: ['__booking_value', 'guest']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['guest', 'guest__booking_value']
    SELECT
      guest
      , SUM(__booking_value) AS guest__booking_value
    FROM sma_28009_cte
    GROUP BY
      guest
  ) subq_25
  ON
    subq_19.guest = subq_25.guest
) subq_27
WHERE guest__booking_value > 1.00
