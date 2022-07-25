-- Read Elements From Data Source 'revenue'
-- Metric Time Dimension 'ds'
-- Pass Only Elements:
--   ['txn_revenue', 'ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  DATE_TRUNC('month', created_at) AS ds__month
  , SUM(revenue) AS trailing_2_months_revenue
FROM (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_revenue
) revenue_src_10005
GROUP BY
  DATE_TRUNC('month', created_at)
