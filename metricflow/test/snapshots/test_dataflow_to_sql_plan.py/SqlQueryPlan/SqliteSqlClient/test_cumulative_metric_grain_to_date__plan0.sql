-- Compute Metrics via Expressions
SELECT
  subq_4.txn_revenue AS revenue_mtd
  , subq_4.ds__month
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_3.txn_revenue) AS txn_revenue
    , subq_3.ds__month
  FROM (
    -- Pass Only Elements:
    --   ['txn_revenue', 'ds__month']
    SELECT
      subq_1.txn_revenue
      , subq_1.ds__month
    FROM (
      -- Metric Time Dimension 'ds'
      SELECT
        subq_0.txn_revenue
        , subq_0.ds
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
      FROM (
        -- Read Elements From Data Source 'revenue'
        SELECT
          revenue_src_10005.revenue AS txn_revenue
          , revenue_src_10005.created_at AS ds
          , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__week
          , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
          , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__quarter
          , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__year
          , revenue_src_10005.user_id AS user
        FROM (
          -- User Defined SQL Query
          SELECT * FROM ***************************.fct_revenue
        ) revenue_src_10005
      ) subq_0
    ) subq_1
  ) subq_3
  GROUP BY
    subq_3.ds__month
) subq_4
