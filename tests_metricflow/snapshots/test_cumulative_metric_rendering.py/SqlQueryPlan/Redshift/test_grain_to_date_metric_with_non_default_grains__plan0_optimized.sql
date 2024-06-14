-- Re-aggregate Metrics via Window Functions
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
    -- Join Self Over Time Range
    -- Pass Only Elements: ['txn_revenue', 'revenue_instance__ds__quarter', 'revenue_instance__ds__year', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_11.revenue_instance__ds__quarter AS revenue_instance__ds__quarter
      , subq_11.revenue_instance__ds__year AS revenue_instance__ds__year
      , subq_11.metric_time__day AS metric_time__day
      , SUM(revenue_src_28000.revenue) AS revenue_mtd
    FROM (
      -- Time Spine
      SELECT
        DATE_TRUNC('quarter', ds) AS revenue_instance__ds__quarter
        , DATE_TRUNC('year', ds) AS revenue_instance__ds__year
        , ds AS metric_time__day
      FROM ***************************.mf_time_spine subq_12
      GROUP BY
        DATE_TRUNC('quarter', ds)
        , DATE_TRUNC('year', ds)
        , ds
    ) subq_11
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_11.metric_time__day
      ) AND (
        DATE_TRUNC('day', revenue_src_28000.created_at) >= DATE_TRUNC('month', subq_11.metric_time__day)
      )
    GROUP BY
      subq_11.revenue_instance__ds__quarter
      , subq_11.revenue_instance__ds__year
      , subq_11.metric_time__day
  ) subq_16
) subq_17
GROUP BY
  revenue_instance__ds__quarter
  , revenue_instance__ds__year
  , revenue_mtd
