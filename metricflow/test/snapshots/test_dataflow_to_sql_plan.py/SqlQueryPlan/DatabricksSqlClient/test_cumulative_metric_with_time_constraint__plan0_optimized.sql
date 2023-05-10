-- Read Elements From Semantic Model 'revenue'
-- Metric Time Dimension 'ds'
-- Constrain Time Range to [2019-12-01T00:00:00, 2020-01-01T00:00:00]
-- Pass Only Elements:
--   ['txn_revenue', 'ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  DATE_TRUNC('month', created_at) AS ds__month
  , SUM(revenue) AS trailing_2_months_revenue
FROM ***************************.fct_revenue revenue_src_10006
WHERE created_at BETWEEN CAST('2019-12-01' AS TIMESTAMP) AND CAST('2020-01-01' AS TIMESTAMP)
GROUP BY
  DATE_TRUNC('month', created_at)
