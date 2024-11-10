test_name: test_filter_by_metric_in_same_semantic_model_as_queried_metric
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a simple metric in the query-level where filter.
sql_engine: BigQuery
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookers',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  COUNT(DISTINCT bookers) AS bookers
FROM (
  -- Join Standard Outputs
  SELECT
    subq_19.guest__booking_value AS guest__booking_value
    , subq_13.bookers AS bookers
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      guest_id AS guest
      , guest_id AS bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_13
  LEFT OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['booking_value', 'guest']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['guest', 'guest__booking_value']
    SELECT
      guest_id AS guest
      , SUM(booking_value) AS guest__booking_value
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      guest
  ) subq_19
  ON
    subq_13.guest = subq_19.guest
) subq_20
WHERE guest__booking_value > 1.00
