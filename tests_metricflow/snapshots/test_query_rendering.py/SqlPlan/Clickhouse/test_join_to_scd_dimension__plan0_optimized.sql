test_name: test_join_to_scd_dimension
test_filename: test_query_rendering.py
docstring:
  Tests conversion of a plan using a dimension with a validity window inside a measure constraint.
sql_engine: Clickhouse
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , SUM(bookings) AS family_bookings
FROM (
  -- Join Standard Outputs
  SELECT
    listings_src_26000.capacity AS listing__capacity
    , subq_9.metric_time__day AS metric_time__day
    , subq_9.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_26000
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_9
  CROSS JOIN
    ***************************.dim_listings listings_src_26000
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_12
WHERE listing__capacity > 2
GROUP BY
  metric_time__day
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
