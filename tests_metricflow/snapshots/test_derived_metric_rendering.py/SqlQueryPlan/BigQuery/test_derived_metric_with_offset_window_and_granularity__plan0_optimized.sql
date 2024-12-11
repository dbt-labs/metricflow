test_name: test_derived_metric_with_offset_window_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
    , DATETIME_TRUNC(ds, quarter) AS metric_time__quarter
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__quarter AS metric_time__quarter
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_18.metric_time__quarter, subq_25.metric_time__quarter) AS metric_time__quarter
    , MAX(subq_18.bookings) AS bookings
    , MAX(subq_25.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['bookings', 'metric_time__quarter']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__quarter
      , SUM(bookings) AS bookings
    FROM sma_28009_cte sma_28009_cte
    GROUP BY
      metric_time__quarter
  ) subq_18
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__quarter']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATETIME_TRUNC(subq_21.ds, quarter) AS metric_time__quarter
      , SUM(sma_28009_cte.bookings) AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine subq_21
    INNER JOIN
      sma_28009_cte sma_28009_cte
    ON
      DATE_SUB(CAST(subq_21.ds AS DATETIME), INTERVAL 14 day) = sma_28009_cte.metric_time__day
    GROUP BY
      metric_time__quarter
  ) subq_25
  ON
    subq_18.metric_time__quarter = subq_25.metric_time__quarter
  GROUP BY
    metric_time__quarter
) subq_26
