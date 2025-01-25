test_name: test_cumulative_metric_no_window_with_time_constraint
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a windowless cumulative metric query with an adjustable time constraint.
sql_engine: Redshift
---
-- Join Self Over Time Range
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
-- Pass Only Elements: ['txn_revenue', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  nr_subq_10.metric_time__day AS metric_time__day
  , SUM(nr_subq_9.txn_revenue) AS revenue_all_time
FROM (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS metric_time__day
  FROM ***************************.mf_time_spine nr_subq_11
  WHERE ds BETWEEN '2020-01-01' AND '2020-01-01'
) nr_subq_10
INNER JOIN (
  -- Read Elements From Semantic Model 'revenue'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2000-01-01T00:00:00, 2020-01-01T00:00:00]
  SELECT
    DATE_TRUNC('day', created_at) AS metric_time__day
    , revenue AS txn_revenue
  FROM ***************************.fct_revenue revenue_src_28000
  WHERE DATE_TRUNC('day', created_at) BETWEEN '2000-01-01' AND '2020-01-01'
) nr_subq_9
ON
  (nr_subq_9.metric_time__day <= nr_subq_10.metric_time__day)
WHERE nr_subq_10.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
GROUP BY
  nr_subq_10.metric_time__day
