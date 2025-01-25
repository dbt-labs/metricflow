test_name: test_cumulative_metric_with_non_default_grain
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative all-time metric queried with non-default grain.
sql_engine: Postgres
---
-- Re-aggregate Metric via Group By
SELECT
  metric_time__week
  , revenue_all_time
FROM (
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__week
    , LAST_VALUE(revenue_all_time) OVER (
      PARTITION BY metric_time__week
      ORDER BY metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS revenue_all_time
  FROM (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['txn_revenue', 'metric_time__week', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      nr_subq_10.ds AS metric_time__day
      , DATE_TRUNC('week', nr_subq_10.ds) AS metric_time__week
      , SUM(revenue_src_28000.revenue) AS revenue_all_time
    FROM ***************************.mf_time_spine nr_subq_10
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATE_TRUNC('day', revenue_src_28000.created_at) <= nr_subq_10.ds
      )
    GROUP BY
      nr_subq_10.ds
      , DATE_TRUNC('week', nr_subq_10.ds)
  ) nr_subq_14
) nr_subq_15
GROUP BY
  metric_time__week
  , revenue_all_time
