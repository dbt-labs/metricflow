-- Join Self Over Time Range
-- Pass Only Elements:
--   ['txn_revenue', 'ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  DATE_TRUNC(revenue_src_10006.created_at, month) AS ds__month
  , SUM(revenue_src_10006.revenue) AS revenue_mtd
FROM (
  -- Date Spine
  SELECT
    ds AS metric_time
  FROM ***************************.mf_time_spine subq_10
  GROUP BY
    metric_time
) subq_9
INNER JOIN (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_revenue
) revenue_src_10006
ON
  (
    revenue_src_10006.created_at <= subq_9.metric_time
  ) AND (
    revenue_src_10006.created_at >= DATE_TRUNC(subq_9.metric_time, month)
  )
GROUP BY
  ds__month
