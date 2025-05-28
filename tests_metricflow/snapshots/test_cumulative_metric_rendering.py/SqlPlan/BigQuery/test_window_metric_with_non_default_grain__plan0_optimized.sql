test_name: test_window_metric_with_non_default_grain
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative window metric queried with non-default grain.
sql_engine: BigQuery
---
-- Re-aggregate Metric via Group By
-- Write to DataTable
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
      subq_13.ds AS metric_time__day
      , TIMESTAMP_TRUNC(subq_13.ds, year) AS metric_time__year
      , SUM(revenue_src_28000.revenue) AS txn_revenue
    FROM ***************************.mf_time_spine subq_13
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        TIMESTAMP_TRUNC(revenue_src_28000.created_at, day) <= subq_13.ds
      ) AND (
        TIMESTAMP_TRUNC(revenue_src_28000.created_at, day) > DATE_SUB(CAST(subq_13.ds AS DATETIME), INTERVAL 2 month)
      )
    GROUP BY
      metric_time__day
      , metric_time__year
  ) subq_16
) subq_18
GROUP BY
  metric_time__year
  , trailing_2_months_revenue
