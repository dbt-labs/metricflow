-- Join Self Over Time Range
-- Pass Only Elements: ['txn_revenue', 'metric_time__day']
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_16.metric_time__day AS metric_time__day
  , SUM(subq_15.txn_revenue) AS trailing_2_months_revenue
FROM (
  -- Time Spine
  SELECT
    ds AS metric_time__day
  FROM ***************************.mf_time_spine subq_17
  WHERE ds BETWEEN '2020-01-01' AND '2020-01-01'
) subq_16
INNER JOIN (
  -- Read Elements From Semantic Model 'revenue'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2019-11-01T00:00:00, 2020-01-01T00:00:00]
  SELECT
    DATETIME_TRUNC(created_at, day) AS metric_time__day
    , revenue AS txn_revenue
  FROM ***************************.fct_revenue revenue_src_28000
  WHERE DATETIME_TRUNC(created_at, day) BETWEEN '2019-11-01' AND '2020-01-01'
) subq_15
ON
  (
    subq_15.metric_time__day <= subq_16.metric_time__day
  ) AND (
    subq_15.metric_time__day > DATE_SUB(CAST(subq_16.metric_time__day AS DATETIME), INTERVAL 2 month)
  )
WHERE subq_16.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
GROUP BY
  metric_time__day
