test_name: test_cumulative_metric
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a basic cumulative metric query.
sql_engine: Redshift
---
-- Read Elements From Semantic Model 'revenue'
-- Metric Time Dimension 'ds'
-- Pass Only Elements: ['txn_revenue', 'ds__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  DATE_TRUNC('day', created_at) AS ds__day
  , SUM(revenue) AS trailing_2_months_revenue
FROM ***************************.fct_revenue revenue_src_28000
GROUP BY
  DATE_TRUNC('day', created_at)
