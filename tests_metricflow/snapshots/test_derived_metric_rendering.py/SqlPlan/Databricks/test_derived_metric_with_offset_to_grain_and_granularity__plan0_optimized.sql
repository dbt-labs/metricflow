test_name: test_derived_metric_with_offset_to_grain_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: Databricks
---
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , DATE_TRUNC('week', ds) AS metric_time__week
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__week AS metric_time__week
  , bookings - bookings_at_start_of_month AS bookings_growth_since_start_of_month
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_18.metric_time__week, subq_26.metric_time__week) AS metric_time__week
    , MAX(subq_18.bookings) AS bookings
    , MAX(subq_26.bookings_at_start_of_month) AS bookings_at_start_of_month
  FROM (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['bookings', 'metric_time__week']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__week
      , SUM(bookings) AS bookings
    FROM sma_28009_cte sma_28009_cte
    GROUP BY
      metric_time__week
  ) subq_18
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__week']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('week', time_spine_src_28006.ds) AS metric_time__week
      , SUM(sma_28009_cte.bookings) AS bookings_at_start_of_month
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN
      sma_28009_cte sma_28009_cte
    ON
      DATE_TRUNC('month', time_spine_src_28006.ds) = sma_28009_cte.metric_time__day
    WHERE DATE_TRUNC('week', time_spine_src_28006.ds) = time_spine_src_28006.ds
    GROUP BY
      DATE_TRUNC('week', time_spine_src_28006.ds)
  ) subq_26
  ON
    subq_18.metric_time__week = subq_26.metric_time__week
  GROUP BY
    COALESCE(subq_18.metric_time__week, subq_26.metric_time__week)
) subq_27
