test_name: test_derived_metric_with_offset_window_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: Databricks
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__quarter
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'metric_time__quarter']
    SELECT
      DATE_TRUNC('quarter', ds) AS metric_time__quarter
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_16
  GROUP BY
    metric_time__quarter
)

, cm_7_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__quarter']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATE_TRUNC('quarter', subq_22.ds) AS metric_time__quarter
    , SUM(subq_20.bookings) AS bookings_2_weeks_ago
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
    DATEADD(day, -14, subq_22.ds) = subq_20.metric_time__day
  GROUP BY
    DATE_TRUNC('quarter', subq_22.ds)
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__quarter
    , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_6_cte.metric_time__quarter, cm_7_cte.metric_time__quarter) AS metric_time__quarter
      , MAX(cm_6_cte.bookings) AS bookings
      , MAX(cm_7_cte.bookings_2_weeks_ago) AS bookings_2_weeks_ago
    FROM cm_6_cte cm_6_cte
    FULL OUTER JOIN
      cm_7_cte cm_7_cte
    ON
      cm_6_cte.metric_time__quarter = cm_7_cte.metric_time__quarter
    GROUP BY
      COALESCE(cm_6_cte.metric_time__quarter, cm_7_cte.metric_time__quarter)
  ) subq_27
)

SELECT
  metric_time__quarter AS metric_time__quarter
  , bookings_growth_2_weeks AS bookings_growth_2_weeks
FROM cm_8_cte cm_8_cte
