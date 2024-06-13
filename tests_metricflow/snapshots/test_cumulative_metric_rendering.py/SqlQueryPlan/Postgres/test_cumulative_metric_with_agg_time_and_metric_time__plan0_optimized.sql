-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'metric_time__day', 'revenue_instance__ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_9.revenue_instance__ds__month AS revenue_instance__ds__month
  , subq_9.metric_time__day AS metric_time__day
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM (
  -- Time Spine
  SELECT
    DATE_TRUNC('month', ds) AS revenue_instance__ds__month
    , ds AS metric_time__day
  FROM ***************************.mf_time_spine subq_10
  GROUP BY
    DATE_TRUNC('month', ds)
    , ds
) subq_9
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_9.metric_time__day
  ) AND (
    DATE_TRUNC('day', revenue_src_28000.created_at) > subq_9.metric_time__day - MAKE_INTERVAL(months => 2)
  )
GROUP BY
  subq_9.revenue_instance__ds__month
  , subq_9.metric_time__day
