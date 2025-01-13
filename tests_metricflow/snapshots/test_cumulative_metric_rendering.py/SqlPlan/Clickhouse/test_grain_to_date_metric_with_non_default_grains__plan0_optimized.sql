test_name: test_grain_to_date_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative grain to date metric queried with non-default grains.

      Uses agg time dimension instead of metric_time. Excludes default grain.
sql_engine: Clickhouse
---
-- Re-aggregate Metric via Group By
SELECT
  revenue_instance__ds__quarter
  , revenue_instance__ds__year
  , revenue_mtd
FROM (
  -- Window Function for Metric Re-aggregation
  SELECT
    revenue_instance__ds__quarter
    , revenue_instance__ds__year
    , FIRST_VALUE(revenue_mtd) OVER (
      PARTITION BY
        revenue_instance__ds__quarter
        , revenue_instance__ds__year
      ORDER BY metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS revenue_mtd
  FROM (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['txn_revenue', 'revenue_instance__ds__quarter', 'revenue_instance__ds__year', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('quarter', subq_12.ds) AS revenue_instance__ds__quarter
      , DATE_TRUNC('year', subq_12.ds) AS revenue_instance__ds__year
      , subq_12.ds AS metric_time__day
      , SUM(revenue_src_28000.revenue) AS revenue_mtd
    FROM ***************************.mf_time_spine subq_12
    CROSS JOIN
      ***************************.fct_revenue revenue_src_28000
    GROUP BY
      DATE_TRUNC('quarter', subq_12.ds)
      , DATE_TRUNC('year', subq_12.ds)
      , subq_12.ds
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_16
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_17
GROUP BY
  revenue_instance__ds__quarter
  , revenue_instance__ds__year
  , revenue_mtd
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
