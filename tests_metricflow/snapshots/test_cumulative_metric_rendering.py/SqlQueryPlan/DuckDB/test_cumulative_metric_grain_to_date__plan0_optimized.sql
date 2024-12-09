test_name: test_cumulative_metric_grain_to_date
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query against a grain_to_date cumulative metric.
sql_engine: DuckDB
---
-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_10.ds AS metric_time__day
  , SUM(revenue_src_28000.revenue) AS revenue_mtd
FROM ***************************.mf_time_spine subq_10
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  -- for each day, sum everything from the start of the month to today
  (
    DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_10.ds 
  ) AND (
    DATE_TRUNC('day', revenue_src_28000.created_at) >= DATE_TRUNC('month', subq_10.ds)
  )
GROUP BY
  subq_10.ds
