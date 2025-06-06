test_name: test_derived_metric_with_offset_window_and_time_filter
test_filename: test_derived_metric_rendering.py
sql_engine: Postgres
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
    COALESCE(subq_22.metric_time__day, subq_31.metric_time__day) AS metric_time__day
    , MAX(subq_22.bookings) AS bookings
    , MAX(subq_31.bookings_2_weeks_ago) AS bookings_2_weeks_ago
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
    ) subq_18
    WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
    GROUP BY
      metric_time__day
  ) subq_22
  FULL OUTER JOIN (
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
        time_spine_src_28006.ds AS metric_time__day
        , sma_28009_cte.bookings AS bookings
      FROM ***************************.mf_time_spine time_spine_src_28006
      INNER JOIN
        sma_28009_cte
      ON
        time_spine_src_28006.ds - MAKE_INTERVAL(days => 14) = sma_28009_cte.metric_time__day
    ) subq_27
    WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
    GROUP BY
      metric_time__day
  ) subq_31
  ON
    subq_22.metric_time__day = subq_31.metric_time__day
  GROUP BY
    COALESCE(subq_22.metric_time__day, subq_31.metric_time__day)
) subq_32
