test_name: test_grain_to_date_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative grain to date metric queried with non-default grains.

      Uses agg time dimension instead of metric_time. Excludes default grain.
sql_engine: Redshift
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
      DATE_TRUNC('quarter', nr_subq_10.ds) AS revenue_instance__ds__quarter
      , DATE_TRUNC('year', nr_subq_10.ds) AS revenue_instance__ds__year
      , nr_subq_10.ds AS metric_time__day
      , SUM(revenue_src_28000.revenue) AS revenue_mtd
    FROM ***************************.mf_time_spine nr_subq_10
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATE_TRUNC('day', revenue_src_28000.created_at) <= nr_subq_10.ds
      ) AND (
        DATE_TRUNC('day', revenue_src_28000.created_at) >= DATE_TRUNC('month', nr_subq_10.ds)
      )
    GROUP BY
      DATE_TRUNC('quarter', nr_subq_10.ds)
      , DATE_TRUNC('year', nr_subq_10.ds)
      , nr_subq_10.ds
  ) nr_subq_14
) nr_subq_15
GROUP BY
  revenue_instance__ds__quarter
  , revenue_instance__ds__year
  , revenue_mtd
