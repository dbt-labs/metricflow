-- Compute Metrics via Expressions
SELECT
  subq_3.txn_revenue AS trailing_2_months_revenue
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_2.txn_revenue) AS txn_revenue
  FROM (
    -- Pass Only Elements:
    --   ['txn_revenue']
    SELECT
      subq_0.txn_revenue
    FROM (
      -- Read Elements From Data Source 'revenue'
      SELECT
        revenue_src_10005.revenue AS txn_revenue
        , revenue_src_10005.created_at AS ds
        , DATE_TRUNC(revenue_src_10005.created_at, isoweek) AS ds__week
        , DATE_TRUNC(revenue_src_10005.created_at, month) AS ds__month
        , DATE_TRUNC(revenue_src_10005.created_at, quarter) AS ds__quarter
        , DATE_TRUNC(revenue_src_10005.created_at, isoyear) AS ds__year
        , revenue_src_10005.user_id AS user
      FROM (
        -- User Defined SQL Query
        SELECT * FROM ***************************.fct_revenue
      ) revenue_src_10005
    ) subq_0
  ) subq_2
) subq_3
