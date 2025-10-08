test_name: test_cumulative_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Postgres
---
-- Re-aggregate Metric via Group By
-- Write to DataTable
SELECT
  metric_time__alien_day
  , trailing_2_months_revenue
FROM (
  -- Compute Metrics via Expressions
  -- Compute Metrics via Expressions
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__alien_day
    , AVG(revenue) OVER (PARTITION BY metric_time__alien_day) AS trailing_2_months_revenue
  FROM (
    -- Join Self Over Time Range
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['revenue', 'metric_time__alien_day', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_16.alien_day AS metric_time__alien_day
      , subq_15.ds AS metric_time__day
      , SUM(revenue_src_28000.revenue) AS revenue
    FROM ***************************.mf_time_spine subq_15
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_15.ds
      ) AND (
        DATE_TRUNC('day', revenue_src_28000.created_at) > subq_15.ds - MAKE_INTERVAL(months => 2)
      )
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_16
    ON
      subq_15.ds = subq_16.ds
    GROUP BY
      subq_16.alien_day
      , subq_15.ds
  ) subq_19
) subq_22
GROUP BY
  metric_time__alien_day
  , trailing_2_months_revenue
