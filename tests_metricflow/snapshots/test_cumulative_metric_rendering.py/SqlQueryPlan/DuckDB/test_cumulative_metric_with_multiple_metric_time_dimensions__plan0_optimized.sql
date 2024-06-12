-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'metric_time__day', 'metric_time__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_9.metric_time__day AS metric_time__day
  , subq_9.metric_time__month AS metric_time__month
  , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
FROM (
  -- Time Spine
  SELECT
    ds AS metric_time__day
    , DATE_TRUNC('month', ds) AS metric_time__month
  FROM ***************************.mf_time_spine subq_10
  GROUP BY
    ds
    , DATE_TRUNC('month', ds)
) subq_9
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_9.metric_time__day
  ) AND (
    DATE_TRUNC('day', revenue_src_28000.created_at) > subq_9.metric_time__day - INTERVAL 2 month
  )
GROUP BY
  subq_9.metric_time__day
  , subq_9.metric_time__month
