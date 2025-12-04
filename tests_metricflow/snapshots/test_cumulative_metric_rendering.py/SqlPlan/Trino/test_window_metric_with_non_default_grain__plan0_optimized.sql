test_name: test_window_metric_with_non_default_grain
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative window metric queried with non-default grain.
sql_engine: Trino
---
-- Re-aggregate Metric via Group By
-- Write to DataTable
SELECT
  metric_time__year
  , trailing_2_months_revenue
FROM (
  -- Compute Metrics via Expressions
  -- Compute Metrics via Expressions
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__year
    , AVG(__revenue) OVER (PARTITION BY metric_time__year) AS trailing_2_months_revenue
  FROM (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['__revenue', 'metric_time__year', 'metric_time__day']
    -- Pass Only Elements: ['__revenue', 'metric_time__year', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_15.ds AS metric_time__day
      , DATE_TRUNC('year', subq_15.ds) AS metric_time__year
      , SUM(revenue_src_28000.revenue) AS __revenue
    FROM ***************************.mf_time_spine subq_15
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_15.ds
      ) AND (
        DATE_TRUNC('day', revenue_src_28000.created_at) > DATE_ADD('month', -2, subq_15.ds)
      )
    GROUP BY
      subq_15.ds
      , DATE_TRUNC('year', subq_15.ds)
  ) subq_19
) subq_22
GROUP BY
  metric_time__year
  , trailing_2_months_revenue
