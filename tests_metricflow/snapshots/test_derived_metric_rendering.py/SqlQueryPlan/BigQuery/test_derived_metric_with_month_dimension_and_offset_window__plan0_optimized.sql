test_name: test_derived_metric_with_month_dimension_and_offset_window
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Read From CTE For node_id=cm_5
WITH cm_4_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings_monthly', 'metric_time__month']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_10.metric_time__month AS metric_time__month
    , SUM(monthly_bookings_source_src_16000.bookings_monthly) AS bookings_last_month
  FROM (
    -- Time Spine
    SELECT
      DATETIME_TRUNC(ds, month) AS metric_time__month
    FROM ***************************.mf_time_spine subq_11
    GROUP BY
      metric_time__month
  ) subq_10
  INNER JOIN
    ***************************.fct_bookings_extended_monthly monthly_bookings_source_src_16000
  ON
    DATE_SUB(CAST(subq_10.metric_time__month AS DATETIME), INTERVAL 1 month) = DATETIME_TRUNC(monthly_bookings_source_src_16000.ds, month)
  GROUP BY
    metric_time__month
)

, cm_5_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__month
    , bookings_last_month AS bookings_last_month
  FROM (
    -- Read From CTE For node_id=cm_4
    SELECT
      metric_time__month
      , bookings_last_month
    FROM cm_4_cte cm_4_cte
  ) subq_15
)

SELECT
  metric_time__month AS metric_time__month
  , bookings_last_month AS bookings_last_month
FROM cm_5_cte cm_5_cte
