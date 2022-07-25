-- Read Elements From Data Source 'revenue'
-- Metric Time Dimension 'ds'
-- Constrain Time Range to [2019-12-01T00:00:00, 2020-01-01T00:00:00]
-- Pass Only Elements:
--   ['txn_revenue', 'ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  DATE_TRUNC(created_at, month) AS ds__month
  , SUM(revenue) AS trailing_2_months_revenue
FROM (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_revenue
) revenue_src_10005
WHERE created_at BETWEEN CAST('2019-12-01' AS DATETIME) AND CAST('2020-01-01' AS DATETIME)
GROUP BY
  ds__month
