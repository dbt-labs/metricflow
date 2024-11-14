test_name: test_derived_metric_with_offset_to_grain_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__week
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'metric_time__week']
    SELECT
      DATE_TRUNC('week', ds) AS metric_time__week
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_16
  GROUP BY
    metric_time__week
)

, cm_7_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__week']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATE_TRUNC('week', subq_22.ds) AS metric_time__week
    , SUM(subq_20.bookings) AS bookings_at_start_of_month
  FROM ***************************.mf_time_spine subq_22
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_20
  ON
    DATE_TRUNC('month', subq_22.ds) = subq_20.metric_time__day
  WHERE DATE_TRUNC('week', subq_22.ds) = subq_22.ds
  GROUP BY
    DATE_TRUNC('week', subq_22.ds)
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__week
    , bookings - bookings_at_start_of_month AS bookings_growth_since_start_of_month
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_6_cte.metric_time__week, cm_7_cte.metric_time__week) AS metric_time__week
      , MAX(cm_6_cte.bookings) AS bookings
      , MAX(cm_7_cte.bookings_at_start_of_month) AS bookings_at_start_of_month
    FROM cm_6_cte cm_6_cte
    FULL OUTER JOIN
      cm_7_cte cm_7_cte
    ON
      cm_6_cte.metric_time__week = cm_7_cte.metric_time__week
    GROUP BY
      COALESCE(cm_6_cte.metric_time__week, cm_7_cte.metric_time__week)
  ) subq_27
)

SELECT
  metric_time__week AS metric_time__week
  , bookings_growth_since_start_of_month AS bookings_growth_since_start_of_month
FROM cm_8_cte cm_8_cte
