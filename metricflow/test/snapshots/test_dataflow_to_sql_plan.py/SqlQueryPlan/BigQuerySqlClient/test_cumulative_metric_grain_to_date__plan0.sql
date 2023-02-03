-- Compute Metrics via Expressions
SELECT
  subq_6.ds__month
  , subq_6.txn_revenue AS revenue_mtd
FROM (
  -- Aggregate Measures
  SELECT
    subq_5.ds__month
    , SUM(subq_5.txn_revenue) AS txn_revenue
  FROM (
    -- Pass Only Elements:
    --   ['txn_revenue', 'ds__month']
    SELECT
      subq_4.ds__month
      , subq_4.txn_revenue
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
          metric_time
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
            , DATE_TRUNC(revenue_src_10006.created_at, isoweek) AS ds__week
            , DATE_TRUNC(revenue_src_10006.created_at, month) AS ds__month
            , DATE_TRUNC(revenue_src_10006.created_at, quarter) AS ds__quarter
            , DATE_TRUNC(revenue_src_10006.created_at, isoyear) AS ds__year
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
          subq_1.metric_time >= DATE_TRUNC(subq_2.metric_time, month)
        )
    ) subq_4
  ) subq_5
  GROUP BY
    ds__month
) subq_6
