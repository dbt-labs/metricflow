-- Read Elements From Semantic Model 'revenue'
-- Metric Time Dimension 'ds'
-- Pass Only Elements: ['txn_revenue', 'ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  DATETIME_TRUNC(created_at, month) AS ds__month
  , SUM(revenue) AS revenue_all_time
FROM ***************************.fct_revenue revenue_src_28000
GROUP BY
  ds__month
