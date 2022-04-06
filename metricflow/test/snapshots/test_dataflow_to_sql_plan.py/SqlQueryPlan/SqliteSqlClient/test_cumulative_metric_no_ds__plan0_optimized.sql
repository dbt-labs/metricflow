-- Read Elements From Data Source 'revenue'
-- Pass Only Elements:
--   ['txn_revenue']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(revenue) AS trailing_2_months_revenue
FROM (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_revenue
) revenue_src_10005
