test_name: test_grain_to_date_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative grain to date metric queried with non-default grains.

      Uses agg time dimension instead of metric_time. Excludes default grain.
sql_engine: BigQuery
---
-- Re-aggregate Metric via Group By
-- Write to DataTable
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
      TIMESTAMP_TRUNC(subq_13.ds, quarter) AS revenue_instance__ds__quarter
      , TIMESTAMP_TRUNC(subq_13.ds, year) AS revenue_instance__ds__year
      , subq_13.ds AS metric_time__day
      , SUM(revenue_src_28000.revenue) AS revenue_mtd
    FROM ***************************.mf_time_spine subq_13
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        TIMESTAMP_TRUNC(revenue_src_28000.created_at, day) <= subq_13.ds
      ) AND (
        TIMESTAMP_TRUNC(revenue_src_28000.created_at, day) >= TIMESTAMP_TRUNC(subq_13.ds, month)
      )
    GROUP BY
      revenue_instance__ds__quarter
      , revenue_instance__ds__year
      , metric_time__day
  ) subq_17
) subq_18
GROUP BY
  revenue_instance__ds__quarter
  , revenue_instance__ds__year
  , revenue_mtd
