test_name: test_cumulative_metric_grain_to_date
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query against a grain_to_date cumulative metric.
sql_engine: Clickhouse
---
-- Read Elements From Semantic Model 'revenue'
-- Metric Time Dimension 'ds'
-- Pass Only Elements: ['txn_revenue', 'ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  date_trunc('month', created_at) AS ds__month
  , SUM(revenue) AS revenue_mtd
FROM ***************************.fct_revenue revenue_src_28000
GROUP BY
  ds__month
