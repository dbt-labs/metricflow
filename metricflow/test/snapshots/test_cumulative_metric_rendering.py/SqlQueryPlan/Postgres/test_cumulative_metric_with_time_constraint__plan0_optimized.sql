-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'metric_time__day']
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_14.metric_time__day AS metric_time__day
  , SUM(subq_13.txn_revenue) AS trailing_2_months_revenue
FROM (
  -- Time Spine
  SELECT
    ds AS metric_time__day
  FROM ***************************.mf_time_spine subq_15
  WHERE ds BETWEEN '2020-01-01' AND '2020-01-01'
) subq_14
INNER JOIN (
  -- Read Elements From Semantic Model 'revenue'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2019-11-01T00:00:00, 2020-01-01T00:00:00]
  SELECT
    DATE_TRUNC('day', created_at) AS metric_time__day
    , revenue AS txn_revenue
  FROM ***************************.fct_revenue revenue_src_28000
  WHERE DATE_TRUNC('day', created_at) BETWEEN '2019-11-01' AND '2020-01-01'
) subq_13
ON
  (
    subq_13.metric_time__day <= subq_14.metric_time__day
  ) AND (
    subq_13.metric_time__day > subq_14.metric_time__day - MAKE_INTERVAL(months => 2)
  )
WHERE subq_14.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
GROUP BY
  subq_14.metric_time__day
