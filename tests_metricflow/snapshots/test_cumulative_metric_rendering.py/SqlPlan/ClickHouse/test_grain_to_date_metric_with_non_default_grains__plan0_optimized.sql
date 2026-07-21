test_name: test_grain_to_date_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative grain to date metric queried with non-default grains.

      Uses agg time dimension instead of metric_time. Excludes default grain.
sql_engine: ClickHouse
---
SELECT
  revenue_instance__ds__quarter
  , revenue_instance__ds__year
  , revenue_mtd
FROM (
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
    SELECT
      toStartOfQuarter(subq_15.ds) AS revenue_instance__ds__quarter
      , toStartOfYear(subq_15.ds) AS revenue_instance__ds__year
      , subq_15.ds AS metric_time__day
      , SUM(revenue_src_28000.revenue) AS revenue_mtd
    FROM ***************************.mf_time_spine subq_15
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        toStartOfDay(revenue_src_28000.created_at) <= subq_15.ds
      ) AND (
        toStartOfDay(revenue_src_28000.created_at) >= toStartOfMonth(subq_15.ds)
      )
    GROUP BY
      toStartOfQuarter(subq_15.ds)
      , toStartOfYear(subq_15.ds)
      , subq_15.ds
  ) subq_21
) subq_22
GROUP BY
  revenue_instance__ds__quarter
  , revenue_instance__ds__year
  , revenue_mtd
