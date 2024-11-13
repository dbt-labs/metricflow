test_name: test_nested_offsets_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: Databricks
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_15.ds AS metric_time__day
    , SUM(subq_13.bookings) AS bookings
  FROM ***************************.mf_time_spine subq_15
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_13
  ON
    DATEADD(day, -5, subq_15.ds) = subq_13.metric_time__day
  GROUP BY
    subq_15.ds
)

, cm_7_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , 2 * bookings AS bookings_offset_once
  FROM (
    -- Read From CTE For node_id=cm_6
    SELECT
      metric_time__day
      , bookings
    FROM cm_6_cte cm_6_cte
  ) subq_19
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , 2 * bookings_offset_once AS bookings_offset_twice
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_21.metric_time__day AS metric_time__day
      , cm_7_cte.bookings_offset_once AS bookings_offset_once
    FROM (
      -- Time Spine
      SELECT
        ds AS metric_time__day
      FROM ***************************.mf_time_spine subq_22
      WHERE ds BETWEEN '2020-01-12' AND '2020-01-13'
    ) subq_21
    INNER JOIN
      cm_7_cte cm_7_cte
    ON
      DATEADD(day, -2, subq_21.metric_time__day) = cm_7_cte.metric_time__day
  ) subq_23
)

SELECT
  metric_time__day AS metric_time__day
  , bookings_offset_twice AS bookings_offset_twice
FROM cm_8_cte cm_8_cte
