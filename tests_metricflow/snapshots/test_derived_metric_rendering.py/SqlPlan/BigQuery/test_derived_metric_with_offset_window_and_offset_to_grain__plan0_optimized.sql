test_name: test_derived_metric_with_offset_window_and_offset_to_grain
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_27.metric_time__day, subq_35.metric_time__day) AS metric_time__day
    , MAX(subq_27.month_start_bookings) AS month_start_bookings
    , MAX(subq_35.bookings_1_month_ago) AS bookings_1_month_ago
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , SUM(sma_28009_cte.bookings) AS month_start_bookings
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN
      sma_28009_cte sma_28009_cte
    ON
      DATETIME_TRUNC(time_spine_src_28006.ds, month) = sma_28009_cte.metric_time__day
    GROUP BY
      metric_time__day
  ) subq_27
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , SUM(sma_28009_cte.bookings) AS bookings_1_month_ago
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN
      sma_28009_cte sma_28009_cte
    ON
      DATE_SUB(CAST(time_spine_src_28006.ds AS DATETIME), INTERVAL 1 month) = sma_28009_cte.metric_time__day
    GROUP BY
      metric_time__day
  ) subq_35
  ON
    subq_27.metric_time__day = subq_35.metric_time__day
  GROUP BY
    metric_time__day
) subq_36
