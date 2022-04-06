-- Compute Metrics via Expressions
SELECT
  subq_5.txn_revenue AS revenue_mtd
  , subq_5.ds__month
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_4.txn_revenue) AS txn_revenue
    , subq_4.ds__month
  FROM (
    -- Join Self Over Time Range
    SELECT
      subq_1.txn_revenue AS txn_revenue
      , subq_2.ds__month AS ds__month
    FROM (
      -- Date Spine
      SELECT
        DATE_TRUNC('month', subq_3.ds) AS ds__month
      FROM ***************************.mf_time_spine subq_3
      GROUP BY
        DATE_TRUNC('month', subq_3.ds)
    ) subq_2
    INNER JOIN (
      -- Pass Only Elements:
      --   ['txn_revenue', 'ds__month']
      SELECT
        subq_0.txn_revenue
        , subq_0.ds__month
      FROM (
        -- Read Elements From Data Source 'revenue'
        SELECT
          revenue_src_10005.revenue AS txn_revenue
          , revenue_src_10005.created_at AS ds
          , DATE_TRUNC('week', revenue_src_10005.created_at) AS ds__week
          , DATE_TRUNC('month', revenue_src_10005.created_at) AS ds__month
          , DATE_TRUNC('quarter', revenue_src_10005.created_at) AS ds__quarter
          , DATE_TRUNC('year', revenue_src_10005.created_at) AS ds__year
          , revenue_src_10005.user_id AS user
        FROM (
          -- User Defined SQL Query
          SELECT * FROM ***************************.fct_revenue
        ) revenue_src_10005
      ) subq_0
    ) subq_1
    ON
      (
        subq_1.ds__month <= subq_2.ds__month
      ) AND (
        subq_1.ds__month >= DATE_TRUNC('month', subq_2.ds__month::timestamp)
      )
  ) subq_4
  GROUP BY
    subq_4.ds__month
) subq_5
