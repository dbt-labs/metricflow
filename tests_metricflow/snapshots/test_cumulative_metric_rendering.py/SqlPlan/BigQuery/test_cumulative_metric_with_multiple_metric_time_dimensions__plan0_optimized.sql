test_name: test_cumulative_metric_with_multiple_metric_time_dimensions
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative metric queried with multiple metric time dimensions.
sql_engine: BigQuery
---
-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'metric_time__day', 'metric_time__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_10.ds AS metric_time__day
  , DATETIME_TRUNC(subq_10.ds, month) AS metric_time__month
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM ***************************.mf_time_spine subq_10
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    DATETIME_TRUNC(revenue_src_28000.created_at, day) <= subq_10.ds
  ) AND (
    DATETIME_TRUNC(revenue_src_28000.created_at, day) > DATE_SUB(CAST(subq_10.ds AS DATETIME), INTERVAL 2 month)
  )
GROUP BY
  metric_time__day
  , metric_time__month
