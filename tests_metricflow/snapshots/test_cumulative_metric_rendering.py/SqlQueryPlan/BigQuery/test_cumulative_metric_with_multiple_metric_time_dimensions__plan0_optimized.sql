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
    , DATETIME_TRUNC(ds, month) AS metric_time__month
  FROM ***************************.mf_time_spine subq_10
  GROUP BY
    metric_time__day
    , metric_time__month
) subq_9
INNER JOIN
  ***************************.fct_revenue revenue_src_28000
ON
  (
    DATETIME_TRUNC(revenue_src_28000.created_at, day) <= subq_9.metric_time__day
  ) AND (
    DATETIME_TRUNC(revenue_src_28000.created_at, day) > DATE_SUB(CAST(subq_9.metric_time__day AS DATETIME), INTERVAL 2 month)
  )
GROUP BY
  metric_time__day
  , metric_time__month
