test_name: test_cumulative_metric_no_window
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query where there is a windowless cumulative metric to compute.
sql_engine: ClickHouse
---
SELECT
  toStartOfMonth(created_at) AS ds__month
  , SUM(revenue) AS revenue_all_time
FROM ***************************.fct_revenue revenue_src_28000
GROUP BY
  toStartOfMonth(created_at)
