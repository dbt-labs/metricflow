test_name: test_measure_constraint_with_reused_measure
test_filename: test_query_rendering.py
sql_engine: Trino
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['booking_value', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(booking_value) AS booking_value_with_is_instant_constraint
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , is_instant AS booking__is_instant
      , booking_value
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_13
  WHERE booking__is_instant
  GROUP BY
    metric_time__day
)

, cm_7_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['booking_value', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , SUM(booking_value) AS booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    DATE_TRUNC('day', ds)
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , CAST(booking_value_with_is_instant_constraint AS DOUBLE) / CAST(NULLIF(booking_value, 0) AS DOUBLE) AS instant_booking_value_ratio
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_6_cte.metric_time__day, cm_7_cte.metric_time__day) AS metric_time__day
      , MAX(cm_6_cte.booking_value_with_is_instant_constraint) AS booking_value_with_is_instant_constraint
      , MAX(cm_7_cte.booking_value) AS booking_value
    FROM cm_6_cte cm_6_cte
    FULL OUTER JOIN
      cm_7_cte cm_7_cte
    ON
      cm_6_cte.metric_time__day = cm_7_cte.metric_time__day
    GROUP BY
      COALESCE(cm_6_cte.metric_time__day, cm_7_cte.metric_time__day)
  ) subq_23
)

SELECT
  metric_time__day AS metric_time__day
  , instant_booking_value_ratio AS instant_booking_value_ratio
FROM cm_8_cte cm_8_cte
