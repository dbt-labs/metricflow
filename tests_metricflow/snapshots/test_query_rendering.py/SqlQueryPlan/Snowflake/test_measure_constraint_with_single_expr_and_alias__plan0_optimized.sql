test_name: test_measure_constraint_with_single_expr_and_alias
test_filename: test_query_rendering.py
sql_engine: Snowflake
---
-- Read From CTE For node_id=cm_5
WITH cm_4_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS delayed_bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , is_instant AS booking__is_instant
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_7
  WHERE NOT booking__is_instant
  GROUP BY
    metric_time__day
)

, cm_5_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , delayed_bookings * 2 AS double_counted_delayed_bookings
  FROM (
    -- Read From CTE For node_id=cm_4
    SELECT
      metric_time__day
      , delayed_bookings
    FROM cm_4_cte cm_4_cte
  ) subq_11
)

SELECT
  metric_time__day AS metric_time__day
  , double_counted_delayed_bookings AS double_counted_delayed_bookings
FROM cm_5_cte cm_5_cte
