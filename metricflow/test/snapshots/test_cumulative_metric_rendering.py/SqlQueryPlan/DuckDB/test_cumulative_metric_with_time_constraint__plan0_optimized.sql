-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'metric_time__day']
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_12.metric_time__day AS metric_time__day
  , SUM(subq_11.txn_revenue) AS trailing_2_months_revenue
FROM (
  -- Time Spine
  SELECT
    ds AS metric_time__day
  FROM ***************************.mf_time_spine subq_13
  WHERE ds BETWEEN '2020-01-01' AND '2020-01-01'
) subq_12
INNER JOIN (
  -- Read Elements From Semantic Model 'revenue'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2019-11-01T00:00:00, 2020-01-01T00:00:00]
  SELECT
    DATE_TRUNC('day', created_at) AS metric_time__day
    , revenue AS txn_revenue
  FROM ***************************.fct_revenue revenue_src_10007
  WHERE DATE_TRUNC('day', created_at) BETWEEN '2019-11-01' AND '2020-01-01'
) subq_11
ON
  (
    subq_11.metric_time__day <= subq_12.metric_time__day
  ) AND (
    subq_11.metric_time__day > subq_12.metric_time__day - INTERVAL 2 month
  )
WHERE subq_12.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
GROUP BY
  subq_12.metric_time__day
