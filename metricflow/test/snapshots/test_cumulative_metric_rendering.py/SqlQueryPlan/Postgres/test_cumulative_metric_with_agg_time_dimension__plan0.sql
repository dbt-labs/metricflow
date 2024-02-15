-- Compute Metrics via Expressions
SELECT
  subq_6.revenue_instance__ds__day
  , subq_6.txn_revenue AS trailing_2_months_revenue
FROM (
  -- Aggregate Measures
  SELECT
    subq_5.revenue_instance__ds__day
    , SUM(subq_5.txn_revenue) AS txn_revenue
  FROM (
    -- Pass Only Elements: ['txn_revenue', 'revenue_instance__ds__day']
    SELECT
      subq_4.revenue_instance__ds__day
      , subq_4.txn_revenue
    FROM (
      -- Join Self Over Time Range
      SELECT
        subq_2.revenue_instance__ds__day AS revenue_instance__ds__day
        , subq_1.ds__day AS ds__day
        , subq_1.ds__week AS ds__week
        , subq_1.ds__month AS ds__month
        , subq_1.ds__quarter AS ds__quarter
        , subq_1.ds__year AS ds__year
        , subq_1.ds__extract_year AS ds__extract_year
        , subq_1.ds__extract_quarter AS ds__extract_quarter
        , subq_1.ds__extract_month AS ds__extract_month
        , subq_1.ds__extract_day AS ds__extract_day
        , subq_1.ds__extract_dow AS ds__extract_dow
        , subq_1.ds__extract_doy AS ds__extract_doy
        , subq_1.revenue_instance__ds__week AS revenue_instance__ds__week
        , subq_1.revenue_instance__ds__month AS revenue_instance__ds__month
        , subq_1.revenue_instance__ds__quarter AS revenue_instance__ds__quarter
        , subq_1.revenue_instance__ds__year AS revenue_instance__ds__year
        , subq_1.revenue_instance__ds__extract_year AS revenue_instance__ds__extract_year
        , subq_1.revenue_instance__ds__extract_quarter AS revenue_instance__ds__extract_quarter
        , subq_1.revenue_instance__ds__extract_month AS revenue_instance__ds__extract_month
        , subq_1.revenue_instance__ds__extract_day AS revenue_instance__ds__extract_day
        , subq_1.revenue_instance__ds__extract_dow AS revenue_instance__ds__extract_dow
        , subq_1.revenue_instance__ds__extract_doy AS revenue_instance__ds__extract_doy
        , subq_1.metric_time__day AS metric_time__day
        , subq_1.metric_time__week AS metric_time__week
        , subq_1.metric_time__month AS metric_time__month
        , subq_1.metric_time__quarter AS metric_time__quarter
        , subq_1.metric_time__year AS metric_time__year
        , subq_1.metric_time__extract_year AS metric_time__extract_year
        , subq_1.metric_time__extract_quarter AS metric_time__extract_quarter
        , subq_1.metric_time__extract_month AS metric_time__extract_month
        , subq_1.metric_time__extract_day AS metric_time__extract_day
        , subq_1.metric_time__extract_dow AS metric_time__extract_dow
        , subq_1.metric_time__extract_doy AS metric_time__extract_doy
        , subq_1.user AS user
        , subq_1.revenue_instance__user AS revenue_instance__user
        , subq_1.txn_revenue AS txn_revenue
      FROM (
        -- Time Spine
        SELECT
          subq_3.ds AS revenue_instance__ds__day
        FROM ***************************.mf_time_spine subq_3
      ) subq_2
      INNER JOIN (
        -- Metric Time Dimension 'ds'
        SELECT
          subq_0.ds__day
          , subq_0.ds__week
          , subq_0.ds__month
          , subq_0.ds__quarter
          , subq_0.ds__year
          , subq_0.ds__extract_year
          , subq_0.ds__extract_quarter
          , subq_0.ds__extract_month
          , subq_0.ds__extract_day
          , subq_0.ds__extract_dow
          , subq_0.ds__extract_doy
          , subq_0.revenue_instance__ds__day
          , subq_0.revenue_instance__ds__week
          , subq_0.revenue_instance__ds__month
          , subq_0.revenue_instance__ds__quarter
          , subq_0.revenue_instance__ds__year
          , subq_0.revenue_instance__ds__extract_year
          , subq_0.revenue_instance__ds__extract_quarter
          , subq_0.revenue_instance__ds__extract_month
          , subq_0.revenue_instance__ds__extract_day
          , subq_0.revenue_instance__ds__extract_dow
          , subq_0.revenue_instance__ds__extract_doy
          , subq_0.ds__day AS metric_time__day
          , subq_0.ds__week AS metric_time__week
          , subq_0.ds__month AS metric_time__month
          , subq_0.ds__quarter AS metric_time__quarter
          , subq_0.ds__year AS metric_time__year
          , subq_0.ds__extract_year AS metric_time__extract_year
          , subq_0.ds__extract_quarter AS metric_time__extract_quarter
          , subq_0.ds__extract_month AS metric_time__extract_month
          , subq_0.ds__extract_day AS metric_time__extract_day
          , subq_0.ds__extract_dow AS metric_time__extract_dow
          , subq_0.ds__extract_doy AS metric_time__extract_doy
          , subq_0.user
          , subq_0.revenue_instance__user
          , subq_0.txn_revenue
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
        ) subq_0
      ) subq_1
      ON
        (
          subq_1.revenue_instance__ds__day <= subq_2.revenue_instance__ds__day
        ) AND (
          subq_1.revenue_instance__ds__day > subq_2.revenue_instance__ds__day - MAKE_INTERVAL(months => 2)
        )
    ) subq_4
  ) subq_5
  GROUP BY
    subq_5.revenue_instance__ds__day
) subq_6
