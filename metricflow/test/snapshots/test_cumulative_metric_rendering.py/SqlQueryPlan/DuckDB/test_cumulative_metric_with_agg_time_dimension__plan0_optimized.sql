-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'revenue_instance__ds__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_10.ds AS revenue_instance__ds__day
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM ***************************.mf_time_spine subq_10
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_10.ds
  ) AND (
    DATE_TRUNC('day', revenue_src_28000.created_at) > subq_10.ds - INTERVAL 2 month
  )
GROUP BY
  subq_10.ds
