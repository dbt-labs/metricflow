-- Compute Metrics via Expressions
SELECT
  subq_6.txn_revenue AS trailing_2_months_revenue
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_5.txn_revenue) AS txn_revenue
  FROM (
    -- Pass Only Elements:
    --   ['txn_revenue']
    SELECT
      subq_4.txn_revenue
    FROM (
      -- Join Self Over Time Range
      SELECT
        subq_2.metric_time AS metric_time
        , subq_1.ds AS ds
        , subq_1.ds__week AS ds__week
        , subq_1.ds__month AS ds__month
        , subq_1.ds__quarter AS ds__quarter
        , subq_1.ds__year AS ds__year
        , subq_1.metric_time__week AS metric_time__week
        , subq_1.metric_time__month AS metric_time__month
        , subq_1.metric_time__quarter AS metric_time__quarter
        , subq_1.metric_time__year AS metric_time__year
        , subq_1.user AS user
        , subq_1.txn_revenue AS txn_revenue
      FROM (
        -- Date Spine
        SELECT
          subq_3.ds AS metric_time
        FROM ***************************.mf_time_spine subq_3
        GROUP BY
          subq_3.ds
      ) subq_2
      INNER JOIN (
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
            revenue_src_10006.revenue AS txn_revenue
            , revenue_src_10006.created_at AS ds
            , DATE_TRUNC('week', revenue_src_10006.created_at) AS ds__week
            , DATE_TRUNC('month', revenue_src_10006.created_at) AS ds__month
            , DATE_TRUNC('quarter', revenue_src_10006.created_at) AS ds__quarter
            , DATE_TRUNC('year', revenue_src_10006.created_at) AS ds__year
            , revenue_src_10006.user_id AS user
          FROM (
            -- User Defined SQL Query
            SELECT * FROM ***************************.fct_revenue
          ) revenue_src_10006
        ) subq_0
      ) subq_1
      ON
        (
          subq_1.metric_time <= subq_2.metric_time
        ) AND (
          subq_1.metric_time > DATEADD(month, -2, subq_2.metric_time)
        )
    ) subq_4
  ) subq_5
) subq_6
