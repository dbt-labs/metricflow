-- Join Self Over Time Range
-- Pass Only Elements:
--   ['txn_revenue', 'metric_time__month']
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_11.metric_time__month AS metric_time__month
  , SUM(subq_11.txn_revenue) AS trailing_2_months_revenue
FROM (
  -- Date Spine
  SELECT
    ds AS metric_time__day
  FROM ***************************.mf_time_spine subq_13
  WHERE ds BETWEEN timestamp '2020-01-01' AND timestamp '2020-01-01'
) subq_12
INNER JOIN (
  -- Read Elements From Semantic Model 'revenue'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2019-11-01T00:00:00, 2020-01-01T00:00:00]
  SELECT
    DATE_TRUNC('day', created_at) AS metric_time__day
    , DATE_TRUNC('month', created_at) AS metric_time__month
    , revenue AS txn_revenue
  FROM ***************************.fct_revenue revenue_src_10007
  WHERE DATE_TRUNC('day', created_at) BETWEEN timestamp '2019-11-01' AND timestamp '2020-01-01'
) subq_11
ON
  (
    subq_11.metric_time__day <= subq_12.metric_time__day
  ) AND (
    subq_11.metric_time__day > DATE_ADD('month', -2, subq_12.metric_time__day)
  )
WHERE subq_11.metric_time__month BETWEEN timestamp '2020-01-01' AND timestamp '2020-01-01'
GROUP BY
  subq_11.metric_time__month
