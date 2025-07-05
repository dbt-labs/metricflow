test_name: test_derived_metric_with_offset_window_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    TIMESTAMP_TRUNC(ds, day) AS metric_time__day
    , TIMESTAMP_TRUNC(ds, quarter) AS metric_time__quarter
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__quarter AS metric_time__quarter
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_19.metric_time__quarter, subq_27.metric_time__quarter) AS metric_time__quarter
    , MAX(subq_19.bookings) AS bookings
    , MAX(subq_27.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['bookings', 'metric_time__quarter']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__quarter
      , SUM(bookings) AS bookings
    FROM sma_28009_cte
    GROUP BY
      metric_time__quarter
  ) subq_19
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__quarter']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      TIMESTAMP_TRUNC(time_spine_src_28006.ds, quarter) AS metric_time__quarter
      , SUM(sma_28009_cte.bookings) AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN
      sma_28009_cte
    ON
      DATE_SUB(CAST(time_spine_src_28006.ds AS DATETIME), INTERVAL 14 day) = sma_28009_cte.metric_time__day
    GROUP BY
      metric_time__quarter
  ) subq_27
  ON
    subq_19.metric_time__quarter = subq_27.metric_time__quarter
  GROUP BY
    metric_time__quarter
) subq_28
