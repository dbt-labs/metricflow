test_name: test_derived_metric_with_offset_window_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: Redshift
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , DATE_TRUNC('quarter', ds) AS metric_time__quarter
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__quarter AS metric_time__quarter
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_23.metric_time__quarter, subq_33.metric_time__quarter) AS metric_time__quarter
    , MAX(subq_23.bookings) AS bookings
    , MAX(subq_33.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['__bookings', 'metric_time__quarter']
    -- Pass Only Elements: ['__bookings', 'metric_time__quarter']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__quarter
      , SUM(__bookings) AS bookings
    FROM sma_28009_cte
    GROUP BY
      metric_time__quarter
  ) subq_23
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['__bookings', 'metric_time__quarter']
    -- Pass Only Elements: ['__bookings', 'metric_time__quarter']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('quarter', time_spine_src_28006.ds) AS metric_time__quarter
      , SUM(sma_28009_cte.__bookings) AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN
      sma_28009_cte
    ON
      DATEADD(day, -14, time_spine_src_28006.ds) = sma_28009_cte.metric_time__day
    GROUP BY
      DATE_TRUNC('quarter', time_spine_src_28006.ds)
  ) subq_33
  ON
    subq_23.metric_time__quarter = subq_33.metric_time__quarter
  GROUP BY
    COALESCE(subq_23.metric_time__quarter, subq_33.metric_time__quarter)
) subq_34
