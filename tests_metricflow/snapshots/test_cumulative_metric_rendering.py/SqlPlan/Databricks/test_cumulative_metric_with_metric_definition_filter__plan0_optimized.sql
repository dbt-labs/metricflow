test_name: test_cumulative_metric_with_metric_definition_filter
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a cumulative metric that has a filter defined in the YAML metric definition.
sql_engine: Databricks
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__revenue', 'metric_time__day']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , SUM(revenue) AS trailing_2_months_revenue_with_filter
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__revenue', 'user__home_state_latest', 'metric_time__day']
  SELECT
    subq_18.metric_time__day AS metric_time__day
    , users_latest_src_28000.home_state_latest AS user__home_state_latest
    , subq_18.__revenue AS revenue
  FROM (
    -- Join Self Over Time Range
    SELECT
      subq_17.ds AS metric_time__day
      , revenue_src_28000.user_id AS user
      , revenue_src_28000.revenue AS __revenue
    FROM ***************************.mf_time_spine subq_17
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_17.ds
      ) AND (
        DATE_TRUNC('day', revenue_src_28000.created_at) > DATEADD(month, -2, subq_17.ds)
      )
  ) subq_18
  LEFT OUTER JOIN
    ***************************.dim_users_latest users_latest_src_28000
  ON
    subq_18.user = users_latest_src_28000.user_id
) subq_22
WHERE user__home_state_latest = 'CA'
GROUP BY
  metric_time__day
