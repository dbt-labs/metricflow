test_name: test_cumulative_metric_no_window_with_time_constraint
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a windowless cumulative metric query with an adjustable time constraint.
sql_engine: BigQuery
---
-- Join Self Over Time Range
-- Pass Only Elements: ['__revenue', 'metric_time__day']
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
-- Pass Only Elements: ['__revenue', 'metric_time__day']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_22.metric_time__day AS metric_time__day
  , SUM(subq_21.__revenue) AS revenue_all_time
FROM (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS metric_time__day
  FROM ***************************.mf_time_spine subq_23
  WHERE ds BETWEEN '2020-01-01' AND '2020-01-01'
) subq_22
INNER JOIN (
  -- Read Elements From Semantic Model 'revenue'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2000-01-01T00:00:00, 2020-01-01T00:00:00]
  SELECT
    DATETIME_TRUNC(created_at, day) AS metric_time__day
    , revenue AS __revenue
  FROM ***************************.fct_revenue revenue_src_28000
  WHERE DATETIME_TRUNC(created_at, day) BETWEEN '2000-01-01' AND '2020-01-01'
) subq_21
ON
  (subq_21.metric_time__day <= subq_22.metric_time__day)
WHERE subq_22.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
GROUP BY
  metric_time__day
