test_name: test_cumulative_metric_grain_to_date
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query against a grain_to_date cumulative metric.
sql_engine: ClickHouse
---
SELECT
  toStartOfMonth(created_at) AS ds__month
  , SUM(revenue) AS revenue_mtd
FROM ***************************.fct_revenue revenue_src_28000
GROUP BY
  toStartOfMonth(created_at)
