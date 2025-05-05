test_name: test_cumulative_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Databricks
---
-- Re-aggregate Metric via Group By
-- Write to DataTable
SELECT
  metric_time__alien_day
  , trailing_2_months_revenue
FROM (
  -- Compute Metrics via Expressions
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__alien_day
    , AVG(txn_revenue) OVER (PARTITION BY metric_time__alien_day) AS trailing_2_months_revenue
  FROM (
    -- Join Self Over Time Range
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['txn_revenue', 'metric_time__alien_day', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      subq_15.alien_day AS metric_time__alien_day
      , subq_14.ds AS metric_time__day
      , SUM(revenue_src_28000.revenue) AS txn_revenue
    FROM ***************************.mf_time_spine subq_14
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_14.ds
      ) AND (
        DATE_TRUNC('day', revenue_src_28000.created_at) > DATEADD(month, -2, subq_14.ds)
      )
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_15
    ON
      subq_14.ds = subq_15.ds
    GROUP BY
      subq_15.alien_day
      , subq_14.ds
  ) subq_18
) subq_20
GROUP BY
  metric_time__alien_day
  , trailing_2_months_revenue
