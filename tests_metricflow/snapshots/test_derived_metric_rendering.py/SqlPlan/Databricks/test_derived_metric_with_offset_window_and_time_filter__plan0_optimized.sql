test_name: test_derived_metric_with_offset_window_and_time_filter
test_filename: test_derived_metric_rendering.py
sql_engine: Databricks
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_27.metric_time__day, subq_39.metric_time__day) AS metric_time__day
    , MAX(subq_27.bookings) AS bookings
    , MAX(subq_39.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['__bookings', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Read From CTE For node_id=sma_28009
      -- Pass Only Elements: ['__bookings', 'metric_time__day']
      SELECT
        metric_time__day
        , __bookings AS bookings
      FROM sma_28009_cte
    ) subq_23
    WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
    GROUP BY
      metric_time__day
  ) subq_27
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Compute Metrics via Expressions
    SELECT
      subq_37.metric_time__day AS metric_time__day
      , subq_32.__bookings AS bookings_2_weeks_ago
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['metric_time__day']
      SELECT
        metric_time__day
      FROM (
        -- Read From Time Spine 'mf_time_spine'
        -- Change Column Aliases
        -- Pass Only Elements: ['metric_time__day']
        SELECT
          ds AS metric_time__day
        FROM ***************************.mf_time_spine time_spine_src_28006
      ) subq_35
      WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
    ) subq_37
    INNER JOIN (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['__bookings', 'metric_time__day']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        metric_time__day
        , SUM(bookings) AS __bookings
      FROM (
        -- Read From CTE For node_id=sma_28009
        -- Pass Only Elements: ['__bookings', 'metric_time__day']
        SELECT
          metric_time__day
          , __bookings AS bookings
        FROM sma_28009_cte
      ) subq_29
      WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
      GROUP BY
        metric_time__day
    ) subq_32
    ON
      DATEADD(day, -14, subq_37.metric_time__day) = subq_32.metric_time__day
  ) subq_39
  ON
    subq_27.metric_time__day = subq_39.metric_time__day
  GROUP BY
    COALESCE(subq_27.metric_time__day, subq_39.metric_time__day)
) subq_40
