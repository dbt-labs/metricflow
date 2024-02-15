-- Compute Metrics via Expressions
SELECT
  subq_10.metric_time__day
  , subq_10.txn_revenue AS trailing_2_months_revenue
FROM (
  -- Aggregate Measures
  SELECT
    subq_9.metric_time__day
    , SUM(subq_9.txn_revenue) AS txn_revenue
  FROM (
    -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
    SELECT
      subq_8.metric_time__day
      , subq_8.txn_revenue
    FROM (
      -- Pass Only Elements: ['txn_revenue', 'metric_time__day']
      SELECT
        subq_7.metric_time__day
        , subq_7.txn_revenue
      FROM (
        -- Join Self Over Time Range
        SELECT
          subq_5.metric_time__day AS metric_time__day
          , subq_4.ds__day AS ds__day
          , subq_4.ds__week AS ds__week
          , subq_4.ds__month AS ds__month
          , subq_4.ds__quarter AS ds__quarter
          , subq_4.ds__year AS ds__year
          , subq_4.ds__extract_year AS ds__extract_year
          , subq_4.ds__extract_quarter AS ds__extract_quarter
          , subq_4.ds__extract_month AS ds__extract_month
          , subq_4.ds__extract_day AS ds__extract_day
          , subq_4.ds__extract_dow AS ds__extract_dow
          , subq_4.ds__extract_doy AS ds__extract_doy
          , subq_4.revenue_instance__ds__day AS revenue_instance__ds__day
          , subq_4.revenue_instance__ds__week AS revenue_instance__ds__week
          , subq_4.revenue_instance__ds__month AS revenue_instance__ds__month
          , subq_4.revenue_instance__ds__quarter AS revenue_instance__ds__quarter
          , subq_4.revenue_instance__ds__year AS revenue_instance__ds__year
          , subq_4.revenue_instance__ds__extract_year AS revenue_instance__ds__extract_year
          , subq_4.revenue_instance__ds__extract_quarter AS revenue_instance__ds__extract_quarter
          , subq_4.revenue_instance__ds__extract_month AS revenue_instance__ds__extract_month
          , subq_4.revenue_instance__ds__extract_day AS revenue_instance__ds__extract_day
          , subq_4.revenue_instance__ds__extract_dow AS revenue_instance__ds__extract_dow
          , subq_4.revenue_instance__ds__extract_doy AS revenue_instance__ds__extract_doy
          , subq_4.metric_time__week AS metric_time__week
          , subq_4.metric_time__month AS metric_time__month
          , subq_4.metric_time__quarter AS metric_time__quarter
          , subq_4.metric_time__year AS metric_time__year
          , subq_4.metric_time__extract_year AS metric_time__extract_year
          , subq_4.metric_time__extract_quarter AS metric_time__extract_quarter
          , subq_4.metric_time__extract_month AS metric_time__extract_month
          , subq_4.metric_time__extract_day AS metric_time__extract_day
          , subq_4.metric_time__extract_dow AS metric_time__extract_dow
          , subq_4.metric_time__extract_doy AS metric_time__extract_doy
          , subq_4.user AS user
          , subq_4.revenue_instance__user AS revenue_instance__user
          , subq_4.txn_revenue AS txn_revenue
        FROM (
          -- Time Spine
          SELECT
            subq_6.ds AS metric_time__day
          FROM ***************************.mf_time_spine subq_6
          WHERE subq_6.ds BETWEEN '2020-01-01' AND '2020-01-01'
        ) subq_5
        INNER JOIN (
          -- Constrain Time Range to [2019-11-01T00:00:00, 2020-01-01T00:00:00]
          SELECT
            subq_3.ds__day
            , subq_3.ds__week
            , subq_3.ds__month
            , subq_3.ds__quarter
            , subq_3.ds__year
            , subq_3.ds__extract_year
            , subq_3.ds__extract_quarter
            , subq_3.ds__extract_month
            , subq_3.ds__extract_day
            , subq_3.ds__extract_dow
            , subq_3.ds__extract_doy
            , subq_3.revenue_instance__ds__day
            , subq_3.revenue_instance__ds__week
            , subq_3.revenue_instance__ds__month
            , subq_3.revenue_instance__ds__quarter
            , subq_3.revenue_instance__ds__year
            , subq_3.revenue_instance__ds__extract_year
            , subq_3.revenue_instance__ds__extract_quarter
            , subq_3.revenue_instance__ds__extract_month
            , subq_3.revenue_instance__ds__extract_day
            , subq_3.revenue_instance__ds__extract_dow
            , subq_3.revenue_instance__ds__extract_doy
            , subq_3.metric_time__day
            , subq_3.metric_time__week
            , subq_3.metric_time__month
            , subq_3.metric_time__quarter
            , subq_3.metric_time__year
            , subq_3.metric_time__extract_year
            , subq_3.metric_time__extract_quarter
            , subq_3.metric_time__extract_month
            , subq_3.metric_time__extract_day
            , subq_3.metric_time__extract_dow
            , subq_3.metric_time__extract_doy
            , subq_3.user
            , subq_3.revenue_instance__user
            , subq_3.txn_revenue
          FROM (
            -- Metric Time Dimension 'ds'
            SELECT
              subq_2.ds__day
              , subq_2.ds__week
              , subq_2.ds__month
              , subq_2.ds__quarter
              , subq_2.ds__year
              , subq_2.ds__extract_year
              , subq_2.ds__extract_quarter
              , subq_2.ds__extract_month
              , subq_2.ds__extract_day
              , subq_2.ds__extract_dow
              , subq_2.ds__extract_doy
              , subq_2.revenue_instance__ds__day
              , subq_2.revenue_instance__ds__week
              , subq_2.revenue_instance__ds__month
              , subq_2.revenue_instance__ds__quarter
              , subq_2.revenue_instance__ds__year
              , subq_2.revenue_instance__ds__extract_year
              , subq_2.revenue_instance__ds__extract_quarter
              , subq_2.revenue_instance__ds__extract_month
              , subq_2.revenue_instance__ds__extract_day
              , subq_2.revenue_instance__ds__extract_dow
              , subq_2.revenue_instance__ds__extract_doy
              , subq_2.ds__day AS metric_time__day
              , subq_2.ds__week AS metric_time__week
              , subq_2.ds__month AS metric_time__month
              , subq_2.ds__quarter AS metric_time__quarter
              , subq_2.ds__year AS metric_time__year
              , subq_2.ds__extract_year AS metric_time__extract_year
              , subq_2.ds__extract_quarter AS metric_time__extract_quarter
              , subq_2.ds__extract_month AS metric_time__extract_month
              , subq_2.ds__extract_day AS metric_time__extract_day
              , subq_2.ds__extract_dow AS metric_time__extract_dow
              , subq_2.ds__extract_doy AS metric_time__extract_doy
              , subq_2.user
              , subq_2.revenue_instance__user
              , subq_2.txn_revenue
            FROM (
              -- Read Elements From Semantic Model 'revenue'
              SELECT
                revenue_src_28000.revenue AS txn_revenue
                , DATE_TRUNC('day', revenue_src_28000.created_at) AS ds__day
                , DATE_TRUNC('week', revenue_src_28000.created_at) AS ds__week
                , DATE_TRUNC('month', revenue_src_28000.created_at) AS ds__month
                , DATE_TRUNC('quarter', revenue_src_28000.created_at) AS ds__quarter
                , DATE_TRUNC('year', revenue_src_28000.created_at) AS ds__year
                , EXTRACT(year FROM revenue_src_28000.created_at) AS ds__extract_year
                , EXTRACT(quarter FROM revenue_src_28000.created_at) AS ds__extract_quarter
                , EXTRACT(month FROM revenue_src_28000.created_at) AS ds__extract_month
                , EXTRACT(day FROM revenue_src_28000.created_at) AS ds__extract_day
                , EXTRACT(isodow FROM revenue_src_28000.created_at) AS ds__extract_dow
                , EXTRACT(doy FROM revenue_src_28000.created_at) AS ds__extract_doy
                , DATE_TRUNC('day', revenue_src_28000.created_at) AS revenue_instance__ds__day
                , DATE_TRUNC('week', revenue_src_28000.created_at) AS revenue_instance__ds__week
                , DATE_TRUNC('month', revenue_src_28000.created_at) AS revenue_instance__ds__month
                , DATE_TRUNC('quarter', revenue_src_28000.created_at) AS revenue_instance__ds__quarter
                , DATE_TRUNC('year', revenue_src_28000.created_at) AS revenue_instance__ds__year
                , EXTRACT(year FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_year
                , EXTRACT(quarter FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_quarter
                , EXTRACT(month FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_month
                , EXTRACT(day FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_day
                , EXTRACT(isodow FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_dow
                , EXTRACT(doy FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_doy
                , revenue_src_28000.user_id AS user
                , revenue_src_28000.user_id AS revenue_instance__user
              FROM ***************************.fct_revenue revenue_src_28000
            ) subq_2
          ) subq_3
          WHERE subq_3.metric_time__day BETWEEN '2019-11-01' AND '2020-01-01'
        ) subq_4
        ON
          (
            subq_4.metric_time__day <= subq_5.metric_time__day
          ) AND (
            subq_4.metric_time__day > subq_5.metric_time__day - INTERVAL 2 month
          )
      ) subq_7
    ) subq_8
    WHERE subq_8.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
  ) subq_9
  GROUP BY
    subq_9.metric_time__day
) subq_10
