-- Join Self Over Time Range
-- Pass Only Elements:
--   ['txn_revenue']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(revenue_src_10006.revenue) AS trailing_2_months_revenue
FROM (
  -- Date Spine
  SELECT
    ds AS metric_time
  FROM ***************************.mf_time_spine subq_10
  GROUP BY
    ds
) subq_9
INNER JOIN (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_revenue
) revenue_src_10006
ON
  (
    revenue_src_10006.created_at <= subq_9.metric_time
  ) AND (
    revenue_src_10006.created_at > DATEADD(month, -2, subq_9.metric_time)
  )
