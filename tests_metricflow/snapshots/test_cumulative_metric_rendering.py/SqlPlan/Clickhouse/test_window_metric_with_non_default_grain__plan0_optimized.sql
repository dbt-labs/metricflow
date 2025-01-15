test_name: test_window_metric_with_non_default_grain
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative window metric queried with non-default grain.
sql_engine: Clickhouse
---
-- Re-aggregate Metric via Group By
SELECT
  metric_time__year
  , trailing_2_months_revenue
FROM (
  -- Compute Metrics via Expressions
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__year
    , AVG(txn_revenue) OVER (PARTITION BY metric_time__year) AS trailing_2_months_revenue
  FROM (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['txn_revenue', 'metric_time__year', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      subq_12.ds AS metric_time__day
      , date_trunc('year', subq_12.ds) AS metric_time__year
      , SUM(revenue_src_28000.revenue) AS txn_revenue
    FROM ***************************.mf_time_spine subq_12
    CROSS JOIN
      ***************************.fct_revenue revenue_src_28000
    WHERE ((
      date_trunc('day', revenue_src_28000.created_at) <= subq_12.ds
    ) AND (
      date_trunc('day', revenue_src_28000.created_at) > DATEADD(month, -2, subq_12.ds)
    ))
    GROUP BY
      metric_time__day
      , metric_time__year
  ) subq_15
) subq_17
GROUP BY
  metric_time__year
  , trailing_2_months_revenue
