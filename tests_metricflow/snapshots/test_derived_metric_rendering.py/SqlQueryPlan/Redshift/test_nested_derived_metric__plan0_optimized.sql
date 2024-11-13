test_name: test_nested_derived_metric
test_filename: test_derived_metric_rendering.py
sql_engine: Redshift
---
-- Read From CTE For node_id=cm_17
WITH cm_12_cte AS (
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
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
      , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_25
  GROUP BY
    metric_time__day
)

, cm_13_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , (bookings - ref_bookings) * 1.0 / bookings AS non_referred
  FROM (
    -- Read From CTE For node_id=cm_12
    SELECT
      metric_time__day
      , ref_bookings
      , bookings
    FROM cm_12_cte cm_12_cte
  ) subq_27
)

, cm_16_cte AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(instant_bookings) AS instant
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['instant_bookings', 'bookings', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
      , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_31
  GROUP BY
    metric_time__day
)

, cm_17_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , non_referred + (instant * 1.0 / bookings) AS instant_plus_non_referred_bookings_pct
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_13_cte.metric_time__day, cm_16_cte.metric_time__day) AS metric_time__day
      , MAX(cm_13_cte.non_referred) AS non_referred
      , MAX(cm_16_cte.instant) AS instant
      , MAX(cm_16_cte.bookings) AS bookings
    FROM cm_13_cte cm_13_cte
    FULL OUTER JOIN
      cm_16_cte cm_16_cte
    ON
      cm_13_cte.metric_time__day = cm_16_cte.metric_time__day
    GROUP BY
      COALESCE(cm_13_cte.metric_time__day, cm_16_cte.metric_time__day)
  ) subq_34
)

SELECT
  metric_time__day AS metric_time__day
  , instant_plus_non_referred_bookings_pct AS instant_plus_non_referred_bookings_pct
FROM cm_17_cte cm_17_cte
