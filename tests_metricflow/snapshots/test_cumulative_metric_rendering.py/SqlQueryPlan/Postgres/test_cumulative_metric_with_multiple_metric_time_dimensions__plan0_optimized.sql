-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'metric_time__day', 'metric_time__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_10.ds AS metric_time__day
  , DATE_TRUNC('month', subq_10.ds) AS metric_time__month
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM ***************************.mf_time_spine subq_10
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_10.ds
  ) AND (
    DATE_TRUNC('day', revenue_src_28000.created_at) > subq_10.ds - MAKE_INTERVAL(months => 2)
  )
GROUP BY
  subq_10.ds
  , DATE_TRUNC('month', subq_10.ds)
