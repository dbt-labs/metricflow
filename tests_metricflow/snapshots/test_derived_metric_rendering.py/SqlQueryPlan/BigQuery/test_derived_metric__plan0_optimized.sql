test_name: test_derived_metric
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Read From CTE For node_id=cm_9
WITH cm_8_cte AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(referred_bookings) AS ref_bookings
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['referred_bookings', 'bookings', 'metric_time__day']
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , 1 AS bookings
      , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_13
  GROUP BY
    metric_time__day
)

, cm_9_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , (bookings - ref_bookings) * 1.0 / bookings AS non_referred_bookings_pct
  FROM (
    -- Read From CTE For node_id=cm_8
    SELECT
      metric_time__day
      , ref_bookings
      , bookings
    FROM cm_8_cte cm_8_cte
  ) subq_15
)

SELECT
  metric_time__day AS metric_time__day
  , non_referred_bookings_pct AS non_referred_bookings_pct
FROM cm_9_cte cm_9_cte
