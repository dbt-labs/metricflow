test_name: test_cumulative_metric
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a basic cumulative metric query.
sql_engine: ClickHouse
---
SELECT
  toStartOfDay(created_at) AS ds__day
  , SUM(revenue) AS trailing_2_months_revenue
FROM ***************************.fct_revenue revenue_src_28000
GROUP BY
  toStartOfDay(created_at)
