test_name: test_cumulative_metric_with_time_constraint
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a cumulative metric query with an adjustable time constraint.

      Not all query inputs with time constraint filters allow us to adjust the time constraint to include the full
      span of input data for a cumulative metric, but when we receive a time constraint filter expression we can
      automatically adjust it should render a query similar to this one.
sql_engine: BigQuery
---
-- Join Self Over Time Range
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
-- Pass Only Elements: ['txn_revenue', 'metric_time__day']
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
    DATETIME_TRUNC(created_at, day) AS metric_time__day
    , revenue AS txn_revenue
  FROM ***************************.fct_revenue revenue_src_28000
  WHERE DATETIME_TRUNC(created_at, day) BETWEEN '2019-11-01' AND '2020-01-01'
) subq_11
ON
  (
    subq_11.metric_time__day <= subq_12.metric_time__day
  ) AND (
    subq_11.metric_time__day > DATE_SUB(CAST(subq_12.metric_time__day AS DATETIME), INTERVAL 2 month)
  )
WHERE subq_12.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
GROUP BY
  metric_time__day
