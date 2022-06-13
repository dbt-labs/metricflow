-- Compute Metrics via Expressions
SELECT
  subq_7.txn_revenue AS revenue_all_time
  , subq_7.ds__month
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_6.txn_revenue) AS txn_revenue
    , subq_6.ds__month
  FROM (
    -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
    SELECT
      subq_5.txn_revenue
      , subq_5.ds__month
    FROM (
      -- Join Self Over Time Range
      SELECT
        subq_2.txn_revenue AS txn_revenue
        , subq_3.ds__month AS ds__month
      FROM (
        -- Date Spine
        SELECT
          DATE_TRUNC('month', subq_4.ds) AS ds__month
        FROM ***************************.mf_time_spine subq_4
        WHERE (
          subq_4.ds >= CAST('2020-01-01' AS TIMESTAMP)
        ) AND (
          subq_4.ds <= CAST('2020-01-01' AS TIMESTAMP)
        )
        GROUP BY
          DATE_TRUNC('month', subq_4.ds)
      ) subq_3
      INNER JOIN (
        -- Pass Only Elements:
        --   ['txn_revenue', 'ds__month']
        SELECT
          subq_1.txn_revenue
          , subq_1.ds__month
        FROM (
          -- Constrain Time Range to [2000-01-01T00:00:00, 2020-01-01T00:00:00]
          SELECT
            subq_0.txn_revenue
            , subq_0.ds
            , subq_0.ds__week
            , subq_0.ds__month
            , subq_0.ds__quarter
            , subq_0.ds__year
            , subq_0.user
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
          WHERE (
            subq_0.ds >= CAST('2000-01-01' AS TIMESTAMP)
          ) AND (
            subq_0.ds <= CAST('2020-01-01' AS TIMESTAMP)
          )
        ) subq_1
      ) subq_2
      ON
        subq_2.ds__month <= subq_3.ds__month
    ) subq_5
    WHERE (
      subq_5.ds__month >= CAST('2020-01-01' AS TIMESTAMP)
    ) AND (
      subq_5.ds__month <= CAST('2020-01-01' AS TIMESTAMP)
    )
  ) subq_6
  GROUP BY
    subq_6.ds__month
) subq_7
