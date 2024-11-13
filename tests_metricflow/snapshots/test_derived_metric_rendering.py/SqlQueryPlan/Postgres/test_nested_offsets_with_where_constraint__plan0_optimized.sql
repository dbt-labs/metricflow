test_name: test_nested_offsets_with_where_constraint
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
    subq_16.ds AS metric_time__day
    , SUM(subq_14.bookings) AS bookings
  FROM ***************************.mf_time_spine subq_16
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_14
  ON
    subq_16.ds - MAKE_INTERVAL(days => 5) = subq_14.metric_time__day
  GROUP BY
    subq_16.ds
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
  ) subq_20
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , 2 * bookings_offset_once AS bookings_offset_twice
  FROM (
    -- Constrain Output with WHERE
    SELECT
      metric_time__day
      , bookings_offset_once
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_23.ds AS metric_time__day
        , cm_7_cte.bookings_offset_once AS bookings_offset_once
      FROM ***************************.mf_time_spine subq_23
      INNER JOIN
        cm_7_cte cm_7_cte
      ON
        subq_23.ds - MAKE_INTERVAL(days => 2) = cm_7_cte.metric_time__day
    ) subq_24
    WHERE metric_time__day = '2020-01-12' or metric_time__day = '2020-01-13'
  ) subq_25
)

SELECT
  metric_time__day AS metric_time__day
  , bookings_offset_twice AS bookings_offset_twice
FROM cm_8_cte cm_8_cte
