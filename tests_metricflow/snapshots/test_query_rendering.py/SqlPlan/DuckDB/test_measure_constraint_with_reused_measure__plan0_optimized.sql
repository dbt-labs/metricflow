test_name: test_measure_constraint_with_reused_measure
test_filename: test_query_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , is_instant AS booking__is_instant
    , booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , CAST(booking_value_with_is_instant_constraint AS DOUBLE) / CAST(NULLIF(booking_value, 0) AS DOUBLE) AS instant_booking_value_ratio
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_17.metric_time__day, subq_21.metric_time__day) AS metric_time__day
    , MAX(subq_17.booking_value_with_is_instant_constraint) AS booking_value_with_is_instant_constraint
    , MAX(subq_21.booking_value) AS booking_value
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['booking_value', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(booking_value) AS booking_value_with_is_instant_constraint
    FROM (
      -- Read From CTE For node_id=sma_28009
      SELECT
        metric_time__day
        , booking__is_instant
        , booking_value
      FROM sma_28009_cte
    ) subq_13
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
  ) subq_17
  FULL OUTER JOIN (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['booking_value', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(booking_value) AS booking_value
    FROM sma_28009_cte
    GROUP BY
      metric_time__day
  ) subq_21
  ON
    subq_17.metric_time__day = subq_21.metric_time__day
  GROUP BY
    COALESCE(subq_17.metric_time__day, subq_21.metric_time__day)
) subq_22
