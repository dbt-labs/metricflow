-- Read Elements From Semantic Model 'revenue'
-- Metric Time Dimension 'ds'
-- Constrain Time Range to [2000-01-01T00:00:00, 2020-01-01T00:00:00]
-- Pass Only Elements:
--   ['txn_revenue', 'ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  DATE_TRUNC(created_at, month) AS ds__month
  , SUM(revenue) AS revenue_all_time
FROM ***************************.fct_revenue revenue_src_10006
WHERE DATE_TRUNC(created_at, day) BETWEEN '2000-01-01' AND '2020-01-01'
GROUP BY
  ds__month
