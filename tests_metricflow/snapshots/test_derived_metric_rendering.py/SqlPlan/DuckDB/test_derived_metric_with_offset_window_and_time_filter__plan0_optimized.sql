test_name: test_derived_metric_with_offset_window_and_time_filter
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_23.metric_time__day, subq_33.metric_time__day) AS metric_time__day
    , MAX(subq_23.bookings) AS bookings
    , MAX(subq_33.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Read From CTE For node_id=sma_28009
      SELECT
        metric_time__day
        , bookings
      FROM sma_28009_cte
    ) subq_19
    WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
    GROUP BY
      metric_time__day
  ) subq_23
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Compute Metrics via Expressions
    SELECT
      subq_31.metric_time__day AS metric_time__day
      , subq_27.bookings AS bookings_2_weeks_ago
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['metric_time__day']
      SELECT
        metric_time__day
      FROM (
        -- Read From Time Spine 'mf_time_spine'
        -- Change Column Aliases
        SELECT
          ds AS metric_time__day
        FROM ***************************.mf_time_spine time_spine_src_28006
      ) subq_29
      WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
    ) subq_31
    INNER JOIN (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      -- Aggregate Measures
      SELECT
        metric_time__day
        , SUM(bookings) AS bookings
      FROM (
        -- Read From CTE For node_id=sma_28009
        SELECT
          metric_time__day
          , bookings
        FROM sma_28009_cte
      ) subq_24
      WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
      GROUP BY
        metric_time__day
    ) subq_27
    ON
      subq_31.metric_time__day - INTERVAL 14 day = subq_27.metric_time__day
  ) subq_33
  ON
    subq_23.metric_time__day = subq_33.metric_time__day
  GROUP BY
    COALESCE(subq_23.metric_time__day, subq_33.metric_time__day)
) subq_34
