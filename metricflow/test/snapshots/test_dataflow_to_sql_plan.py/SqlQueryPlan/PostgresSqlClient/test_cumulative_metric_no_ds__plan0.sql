-- Compute Metrics via Expressions
SELECT
  subq_4.txn_revenue AS trailing_2_months_revenue
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_3.txn_revenue) AS txn_revenue
  FROM (
    -- Pass Only Elements:
    --   ['txn_revenue']
    SELECT
      subq_1.txn_revenue
    FROM (
      -- Metric Time Dimension 'ds'
      SELECT
        subq_0.ds
        , subq_0.ds__week
        , subq_0.ds__month
        , subq_0.ds__quarter
        , subq_0.ds__year
        , subq_0.ds AS metric_time
        , subq_0.ds__week AS metric_time__week
        , subq_0.ds__month AS metric_time__month
        , subq_0.ds__quarter AS metric_time__quarter
        , subq_0.ds__year AS metric_time__year
        , subq_0.user
        , subq_0.txn_revenue
      FROM (
        -- Read Elements From Data Source 'revenue'
        SELECT
          revenue_src_10007.revenue AS txn_revenue
          , revenue_src_10007.created_at AS ds
          , DATE_TRUNC('week', revenue_src_10007.created_at) AS ds__week
          , DATE_TRUNC('month', revenue_src_10007.created_at) AS ds__month
          , DATE_TRUNC('quarter', revenue_src_10007.created_at) AS ds__quarter
          , DATE_TRUNC('year', revenue_src_10007.created_at) AS ds__year
          , revenue_src_10007.user_id AS user
        FROM (
          -- User Defined SQL Query
          SELECT * FROM ***************************.fct_revenue
        ) revenue_src_10007
      ) subq_0
    ) subq_1
  ) subq_3
) subq_4
