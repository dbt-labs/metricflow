-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'revenue_instance__ds__day', 'revenue_instance__ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_9.revenue_instance__ds__day AS revenue_instance__ds__day
  , subq_9.revenue_instance__ds__month AS revenue_instance__ds__month
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM (
  -- Time Spine
  SELECT
    ds AS revenue_instance__ds__day
    , DATE_TRUNC('month', ds) AS revenue_instance__ds__month
  FROM ***************************.mf_time_spine subq_10
  GROUP BY
    ds
    , DATE_TRUNC('month', ds)
) subq_9
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_9.revenue_instance__ds__day
  ) AND (
    DATE_TRUNC('day', revenue_src_28000.created_at) > DATEADD(month, -2, subq_9.revenue_instance__ds__day)
  )
GROUP BY
  subq_9.revenue_instance__ds__day
  , subq_9.revenue_instance__ds__month
