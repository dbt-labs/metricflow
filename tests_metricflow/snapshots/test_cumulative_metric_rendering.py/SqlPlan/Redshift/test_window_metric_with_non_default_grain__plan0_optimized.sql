test_name: test_window_metric_with_non_default_grain
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative window metric queried with non-default grain.
sql_engine: Redshift
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
      nr_subq_10.ds AS metric_time__day
      , DATE_TRUNC('year', nr_subq_10.ds) AS metric_time__year
      , SUM(revenue_src_28000.revenue) AS txn_revenue
    FROM ***************************.mf_time_spine nr_subq_10
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATE_TRUNC('day', revenue_src_28000.created_at) <= nr_subq_10.ds
      ) AND (
        DATE_TRUNC('day', revenue_src_28000.created_at) > DATEADD(month, -2, nr_subq_10.ds)
      )
    GROUP BY
      nr_subq_10.ds
      , DATE_TRUNC('year', nr_subq_10.ds)
  ) nr_subq_13
) nr_subq_15
GROUP BY
  metric_time__year
  , trailing_2_months_revenue
