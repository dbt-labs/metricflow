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
        subq_0.end_of_day_revenue__ds
        , subq_0.end_of_day_revenue__ds__week
        , subq_0.end_of_day_revenue__ds__month
        , subq_0.end_of_day_revenue__ds__quarter
        , subq_0.end_of_day_revenue__ds__year
        , subq_0.end_of_day_revenue__ds AS metric_time
        , subq_0.user
        , subq_0.end_of_day_revenue__user
        , subq_0.txn_revenue
      FROM (
        -- Read Elements From Semantic Model 'revenue'
        SELECT
          revenue_src_10006.revenue AS txn_revenue
          , revenue_src_10006.created_at AS end_of_day_revenue__ds
          , DATE_TRUNC('week', revenue_src_10006.created_at) AS end_of_day_revenue__ds__week
          , DATE_TRUNC('month', revenue_src_10006.created_at) AS end_of_day_revenue__ds__month
          , DATE_TRUNC('quarter', revenue_src_10006.created_at) AS end_of_day_revenue__ds__quarter
          , DATE_TRUNC('year', revenue_src_10006.created_at) AS end_of_day_revenue__ds__year
          , revenue_src_10006.user_id AS user
          , revenue_src_10006.user_id AS end_of_day_revenue__user
        FROM ***************************.fct_revenue revenue_src_10006
      ) subq_0
    ) subq_1
  ) subq_3
) subq_4
