-- Read Elements From Data Source 'revenue'
-- Metric Time Dimension 'ds'
-- Pass Only Elements:
--   ['txn_revenue', 'ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(revenue) AS revenue_mtd
  , DATE_TRUNC(created_at, month) AS ds__month
FROM (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_revenue
) revenue_src_10005
GROUP BY
  ds__month
