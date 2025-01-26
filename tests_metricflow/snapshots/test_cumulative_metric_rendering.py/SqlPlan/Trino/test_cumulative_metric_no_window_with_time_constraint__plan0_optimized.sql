test_name: test_cumulative_metric_no_window_with_time_constraint
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a windowless cumulative metric query with an adjustable time constraint.
sql_engine: Trino
---
-- Join Self Over Time Range
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
-- Pass Only Elements: ['txn_revenue', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_13.metric_time__day AS metric_time__day
  , SUM(subq_12.txn_revenue) AS revenue_all_time
FROM (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS metric_time__day
  FROM ***************************.mf_time_spine subq_14
  WHERE ds BETWEEN timestamp '2020-01-01' AND timestamp '2020-01-01'
) subq_13
INNER JOIN (
  -- Read Elements From Semantic Model 'revenue'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2000-01-01T00:00:00, 2020-01-01T00:00:00]
  SELECT
    DATE_TRUNC('day', created_at) AS metric_time__day
    , revenue AS txn_revenue
  FROM ***************************.fct_revenue revenue_src_28000
  WHERE DATE_TRUNC('day', created_at) BETWEEN timestamp '2000-01-01' AND timestamp '2020-01-01'
) subq_12
ON
  (subq_12.metric_time__day <= subq_13.metric_time__day)
WHERE subq_13.metric_time__day BETWEEN timestamp '2020-01-01' AND timestamp '2020-01-01'
GROUP BY
  subq_13.metric_time__day
