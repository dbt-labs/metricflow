test_name: test_cumulative_metric_no_window_with_time_constraint
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a windowless cumulative metric query with an adjustable time constraint.
sql_engine: Clickhouse
---
-- Join Self Over Time Range
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
-- Pass Only Elements: ['txn_revenue', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_12.metric_time__day AS metric_time__day
  , SUM(subq_11.txn_revenue) AS revenue_all_time
FROM (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS metric_time__day
  FROM ***************************.mf_time_spine subq_13
  WHERE ds BETWEEN '2020-01-01' AND '2020-01-01'
) subq_12
INNER JOIN (
  -- Read Elements From Semantic Model 'revenue'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2000-01-01T00:00:00, 2020-01-01T00:00:00]
  SELECT
    date_trunc('day', created_at) AS metric_time__day
    , revenue AS txn_revenue
  FROM ***************************.fct_revenue revenue_src_28000
  WHERE date_trunc('day', created_at) BETWEEN '2000-01-01' AND '2020-01-01'
) subq_11
ON
  (subq_11.metric_time__day <= subq_12.metric_time__day)
WHERE subq_12.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
GROUP BY
  metric_time__day
