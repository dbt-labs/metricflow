test_name: test_derived_metric_with_offset_window_and_offset_to_grain
test_filename: test_derived_metric_rendering.py
sql_engine: Postgres
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_20.ds AS metric_time__day
    , SUM(subq_18.bookings) AS month_start_bookings
  FROM ***************************.mf_time_spine subq_20
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_18
  ON
    DATE_TRUNC('month', subq_20.ds) = subq_18.metric_time__day
  GROUP BY
    subq_20.ds
)

, cm_7_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_28.ds AS metric_time__day
    , SUM(subq_26.bookings) AS bookings_1_month_ago
  FROM ***************************.mf_time_spine subq_28
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_26
  ON
    subq_28.ds - MAKE_INTERVAL(months => 1) = subq_26.metric_time__day
  GROUP BY
    subq_28.ds
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_6_cte.metric_time__day, cm_7_cte.metric_time__day) AS metric_time__day
      , MAX(cm_6_cte.month_start_bookings) AS month_start_bookings
      , MAX(cm_7_cte.bookings_1_month_ago) AS bookings_1_month_ago
    FROM cm_6_cte cm_6_cte
    FULL OUTER JOIN
      cm_7_cte cm_7_cte
    ON
      cm_6_cte.metric_time__day = cm_7_cte.metric_time__day
    GROUP BY
      COALESCE(cm_6_cte.metric_time__day, cm_7_cte.metric_time__day)
  ) subq_33
)

SELECT
  metric_time__day AS metric_time__day
  , bookings_month_start_compared_to_1_month_prior AS bookings_month_start_compared_to_1_month_prior
FROM cm_8_cte cm_8_cte
