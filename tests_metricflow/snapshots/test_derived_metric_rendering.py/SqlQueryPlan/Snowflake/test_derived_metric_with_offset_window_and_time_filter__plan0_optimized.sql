test_name: test_derived_metric_with_offset_window_and_time_filter
test_filename: test_derived_metric_rendering.py
sql_engine: Snowflake
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_17
  WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
  GROUP BY
    metric_time__day
)

, cm_7_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings_2_weeks_ago
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_25.ds AS metric_time__day
      , subq_23.bookings AS bookings
    FROM ***************************.mf_time_spine subq_25
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_23
    ON
      DATEADD(day, -14, subq_25.ds) = subq_23.metric_time__day
  ) subq_26
  WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
  GROUP BY
    metric_time__day
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_6_cte.metric_time__day, cm_7_cte.metric_time__day) AS metric_time__day
      , MAX(cm_6_cte.bookings) AS bookings
      , MAX(cm_7_cte.bookings_2_weeks_ago) AS bookings_2_weeks_ago
    FROM cm_6_cte cm_6_cte
    FULL OUTER JOIN
      cm_7_cte cm_7_cte
    ON
      cm_6_cte.metric_time__day = cm_7_cte.metric_time__day
    GROUP BY
      COALESCE(cm_6_cte.metric_time__day, cm_7_cte.metric_time__day)
  ) subq_31
)

SELECT
  metric_time__day AS metric_time__day
  , bookings_growth_2_weeks AS bookings_growth_2_weeks
FROM cm_8_cte cm_8_cte
