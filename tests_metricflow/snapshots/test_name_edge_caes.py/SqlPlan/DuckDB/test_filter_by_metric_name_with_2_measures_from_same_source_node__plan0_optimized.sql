test_name: test_filter_by_metric_name_with_2_measures_from_same_source_node
test_filename: test_name_edge_caes.py
docstring:
  Check a soon-to-be-deprecated use case of filtering by a metric name with 2 metrics from the same source node.
sql_engine: DuckDB
---
-- Combine Aggregated Outputs
-- Write to DataTable
WITH sma_32007_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , booking_value AS __homonymous_metric_and_dimension
    , booking_value AS __homonymous_metric_and_entity
  FROM ***************************.fct_bookings bookings_source_src_32000
)

SELECT
  COALESCE(subq_17.metric_time__day, subq_22.metric_time__day) AS metric_time__day
  , MAX(subq_17.homonymous_metric_and_entity) AS homonymous_metric_and_entity
  , MAX(subq_22.homonymous_metric_and_dimension) AS homonymous_metric_and_dimension
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['__homonymous_metric_and_entity', 'metric_time__day']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(homonymous_metric_and_entity) AS homonymous_metric_and_entity
  FROM (
    -- Read From CTE For node_id=sma_32007
    SELECT
      metric_time__day
      , __homonymous_metric_and_entity AS homonymous_metric_and_entity
    FROM sma_32007_cte
  ) subq_13
  WHERE homonymous_metric_and_entity IS NOT NULL
  GROUP BY
    metric_time__day
) subq_17
FULL OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['__homonymous_metric_and_dimension', 'metric_time__day']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(homonymous_metric_and_dimension) AS homonymous_metric_and_dimension
  FROM (
    -- Read From CTE For node_id=sma_32007
    SELECT
      metric_time__day
      , __homonymous_metric_and_dimension AS homonymous_metric_and_dimension
    FROM sma_32007_cte
  ) subq_18
  WHERE homonymous_metric_and_entity IS NOT NULL
  GROUP BY
    metric_time__day
) subq_22
ON
  subq_17.metric_time__day = subq_22.metric_time__day
GROUP BY
  COALESCE(subq_17.metric_time__day, subq_22.metric_time__day)
