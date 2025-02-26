test_name: test_cumulative_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Trino
---
-- Re-aggregate Metric via Group By
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
      subq_14.alien_day AS metric_time__alien_day
      , subq_13.ds AS metric_time__day
      , SUM(revenue_src_28000.revenue) AS txn_revenue
    FROM ***************************.mf_time_spine subq_13
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_13.ds
      ) AND (
        DATE_TRUNC('day', revenue_src_28000.created_at) > DATE_ADD('month', -2, subq_13.ds)
      )
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_14
    ON
      subq_13.ds = subq_14.ds
    GROUP BY
      subq_14.alien_day
      , subq_13.ds
  ) subq_17
) subq_19
GROUP BY
  metric_time__alien_day
  , trailing_2_months_revenue
