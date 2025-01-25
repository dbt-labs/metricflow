test_name: test_cumulative_metric_no_window_with_time_constraint
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a windowless cumulative metric query with an adjustable time constraint.
sql_engine: Postgres
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_8.metric_time__day
  , nr_subq_8.txn_revenue AS revenue_all_time
FROM (
  -- Aggregate Measures
  SELECT
    nr_subq_7.metric_time__day
    , SUM(nr_subq_7.txn_revenue) AS txn_revenue
  FROM (
    -- Pass Only Elements: ['txn_revenue', 'metric_time__day']
    SELECT
      nr_subq_6.metric_time__day
      , nr_subq_6.txn_revenue
    FROM (
      -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
      SELECT
        nr_subq_5.ds__day
        , nr_subq_5.ds__week
        , nr_subq_5.ds__month
        , nr_subq_5.ds__quarter
        , nr_subq_5.ds__year
        , nr_subq_5.ds__extract_year
        , nr_subq_5.ds__extract_quarter
        , nr_subq_5.ds__extract_month
        , nr_subq_5.ds__extract_day
        , nr_subq_5.ds__extract_dow
        , nr_subq_5.ds__extract_doy
        , nr_subq_5.revenue_instance__ds__day
        , nr_subq_5.revenue_instance__ds__week
        , nr_subq_5.revenue_instance__ds__month
        , nr_subq_5.revenue_instance__ds__quarter
        , nr_subq_5.revenue_instance__ds__year
        , nr_subq_5.revenue_instance__ds__extract_year
        , nr_subq_5.revenue_instance__ds__extract_quarter
        , nr_subq_5.revenue_instance__ds__extract_month
        , nr_subq_5.revenue_instance__ds__extract_day
        , nr_subq_5.revenue_instance__ds__extract_dow
        , nr_subq_5.revenue_instance__ds__extract_doy
        , nr_subq_5.metric_time__day
        , nr_subq_5.metric_time__week
        , nr_subq_5.metric_time__month
        , nr_subq_5.metric_time__quarter
        , nr_subq_5.metric_time__year
        , nr_subq_5.metric_time__extract_year
        , nr_subq_5.metric_time__extract_quarter
        , nr_subq_5.metric_time__extract_month
        , nr_subq_5.metric_time__extract_day
        , nr_subq_5.metric_time__extract_dow
        , nr_subq_5.metric_time__extract_doy
        , nr_subq_5.user
        , nr_subq_5.revenue_instance__user
        , nr_subq_5.txn_revenue
      FROM (
        -- Join Self Over Time Range
        SELECT
          nr_subq_3.metric_time__day AS metric_time__day
          , nr_subq_2.ds__day AS ds__day
          , nr_subq_2.ds__week AS ds__week
          , nr_subq_2.ds__month AS ds__month
          , nr_subq_2.ds__quarter AS ds__quarter
          , nr_subq_2.ds__year AS ds__year
          , nr_subq_2.ds__extract_year AS ds__extract_year
          , nr_subq_2.ds__extract_quarter AS ds__extract_quarter
          , nr_subq_2.ds__extract_month AS ds__extract_month
          , nr_subq_2.ds__extract_day AS ds__extract_day
          , nr_subq_2.ds__extract_dow AS ds__extract_dow
          , nr_subq_2.ds__extract_doy AS ds__extract_doy
          , nr_subq_2.revenue_instance__ds__day AS revenue_instance__ds__day
          , nr_subq_2.revenue_instance__ds__week AS revenue_instance__ds__week
          , nr_subq_2.revenue_instance__ds__month AS revenue_instance__ds__month
          , nr_subq_2.revenue_instance__ds__quarter AS revenue_instance__ds__quarter
          , nr_subq_2.revenue_instance__ds__year AS revenue_instance__ds__year
          , nr_subq_2.revenue_instance__ds__extract_year AS revenue_instance__ds__extract_year
          , nr_subq_2.revenue_instance__ds__extract_quarter AS revenue_instance__ds__extract_quarter
          , nr_subq_2.revenue_instance__ds__extract_month AS revenue_instance__ds__extract_month
          , nr_subq_2.revenue_instance__ds__extract_day AS revenue_instance__ds__extract_day
          , nr_subq_2.revenue_instance__ds__extract_dow AS revenue_instance__ds__extract_dow
          , nr_subq_2.revenue_instance__ds__extract_doy AS revenue_instance__ds__extract_doy
          , nr_subq_2.metric_time__week AS metric_time__week
          , nr_subq_2.metric_time__month AS metric_time__month
          , nr_subq_2.metric_time__quarter AS metric_time__quarter
          , nr_subq_2.metric_time__year AS metric_time__year
          , nr_subq_2.metric_time__extract_year AS metric_time__extract_year
          , nr_subq_2.metric_time__extract_quarter AS metric_time__extract_quarter
          , nr_subq_2.metric_time__extract_month AS metric_time__extract_month
          , nr_subq_2.metric_time__extract_day AS metric_time__extract_day
          , nr_subq_2.metric_time__extract_dow AS metric_time__extract_dow
          , nr_subq_2.metric_time__extract_doy AS metric_time__extract_doy
          , nr_subq_2.user AS user
          , nr_subq_2.revenue_instance__user AS revenue_instance__user
          , nr_subq_2.txn_revenue AS txn_revenue
        FROM (
          -- Read From Time Spine 'mf_time_spine'
          SELECT
            nr_subq_4.ds AS metric_time__day
          FROM ***************************.mf_time_spine nr_subq_4
          WHERE nr_subq_4.ds BETWEEN '2020-01-01' AND '2020-01-01'
        ) nr_subq_3
        INNER JOIN (
          -- Constrain Time Range to [2000-01-01T00:00:00, 2020-01-01T00:00:00]
          SELECT
            nr_subq_1.ds__day
            , nr_subq_1.ds__week
            , nr_subq_1.ds__month
            , nr_subq_1.ds__quarter
            , nr_subq_1.ds__year
            , nr_subq_1.ds__extract_year
            , nr_subq_1.ds__extract_quarter
            , nr_subq_1.ds__extract_month
            , nr_subq_1.ds__extract_day
            , nr_subq_1.ds__extract_dow
            , nr_subq_1.ds__extract_doy
            , nr_subq_1.revenue_instance__ds__day
            , nr_subq_1.revenue_instance__ds__week
            , nr_subq_1.revenue_instance__ds__month
            , nr_subq_1.revenue_instance__ds__quarter
            , nr_subq_1.revenue_instance__ds__year
            , nr_subq_1.revenue_instance__ds__extract_year
            , nr_subq_1.revenue_instance__ds__extract_quarter
            , nr_subq_1.revenue_instance__ds__extract_month
            , nr_subq_1.revenue_instance__ds__extract_day
            , nr_subq_1.revenue_instance__ds__extract_dow
            , nr_subq_1.revenue_instance__ds__extract_doy
            , nr_subq_1.metric_time__day
            , nr_subq_1.metric_time__week
            , nr_subq_1.metric_time__month
            , nr_subq_1.metric_time__quarter
            , nr_subq_1.metric_time__year
            , nr_subq_1.metric_time__extract_year
            , nr_subq_1.metric_time__extract_quarter
            , nr_subq_1.metric_time__extract_month
            , nr_subq_1.metric_time__extract_day
            , nr_subq_1.metric_time__extract_dow
            , nr_subq_1.metric_time__extract_doy
            , nr_subq_1.user
            , nr_subq_1.revenue_instance__user
            , nr_subq_1.txn_revenue
          FROM (
            -- Metric Time Dimension 'ds'
            SELECT
              nr_subq_28008.ds__day
              , nr_subq_28008.ds__week
              , nr_subq_28008.ds__month
              , nr_subq_28008.ds__quarter
              , nr_subq_28008.ds__year
              , nr_subq_28008.ds__extract_year
              , nr_subq_28008.ds__extract_quarter
              , nr_subq_28008.ds__extract_month
              , nr_subq_28008.ds__extract_day
              , nr_subq_28008.ds__extract_dow
              , nr_subq_28008.ds__extract_doy
              , nr_subq_28008.revenue_instance__ds__day
              , nr_subq_28008.revenue_instance__ds__week
              , nr_subq_28008.revenue_instance__ds__month
              , nr_subq_28008.revenue_instance__ds__quarter
              , nr_subq_28008.revenue_instance__ds__year
              , nr_subq_28008.revenue_instance__ds__extract_year
              , nr_subq_28008.revenue_instance__ds__extract_quarter
              , nr_subq_28008.revenue_instance__ds__extract_month
              , nr_subq_28008.revenue_instance__ds__extract_day
              , nr_subq_28008.revenue_instance__ds__extract_dow
              , nr_subq_28008.revenue_instance__ds__extract_doy
              , nr_subq_28008.ds__day AS metric_time__day
              , nr_subq_28008.ds__week AS metric_time__week
              , nr_subq_28008.ds__month AS metric_time__month
              , nr_subq_28008.ds__quarter AS metric_time__quarter
              , nr_subq_28008.ds__year AS metric_time__year
              , nr_subq_28008.ds__extract_year AS metric_time__extract_year
              , nr_subq_28008.ds__extract_quarter AS metric_time__extract_quarter
              , nr_subq_28008.ds__extract_month AS metric_time__extract_month
              , nr_subq_28008.ds__extract_day AS metric_time__extract_day
              , nr_subq_28008.ds__extract_dow AS metric_time__extract_dow
              , nr_subq_28008.ds__extract_doy AS metric_time__extract_doy
              , nr_subq_28008.user
              , nr_subq_28008.revenue_instance__user
              , nr_subq_28008.txn_revenue
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
            ) nr_subq_28008
          ) nr_subq_1
          WHERE nr_subq_1.metric_time__day BETWEEN '2000-01-01' AND '2020-01-01'
        ) nr_subq_2
        ON
          (nr_subq_2.metric_time__day <= nr_subq_3.metric_time__day)
      ) nr_subq_5
      WHERE nr_subq_5.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
    ) nr_subq_6
  ) nr_subq_7
  GROUP BY
    nr_subq_7.metric_time__day
) nr_subq_8
