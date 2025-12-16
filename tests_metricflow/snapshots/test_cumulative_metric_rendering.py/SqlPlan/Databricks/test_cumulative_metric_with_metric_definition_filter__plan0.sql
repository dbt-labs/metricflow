test_name: test_cumulative_metric_with_metric_definition_filter
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a cumulative metric that has a filter defined in the YAML metric definition.
sql_engine: Databricks
---
-- Write to DataTable
SELECT
  subq_13.metric_time__day
  , subq_13.trailing_2_months_revenue_with_filter
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_12.metric_time__day
    , subq_12.revenue AS trailing_2_months_revenue_with_filter
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_11.metric_time__day
      , subq_11.__revenue AS revenue
    FROM (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_10.metric_time__day
        , SUM(subq_10.__revenue) AS __revenue
      FROM (
        -- Pass Only Elements: ['__revenue', 'metric_time__day']
        SELECT
          subq_9.metric_time__day
          , subq_9.__revenue
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_8.revenue AS __revenue
            , subq_8.user__home_state_latest
            , subq_8.metric_time__day
          FROM (
            -- Pass Only Elements: ['__revenue', 'user__home_state_latest', 'metric_time__day']
            SELECT
              subq_7.metric_time__day
              , subq_7.user__home_state_latest
              , subq_7.__revenue AS revenue
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_6.home_state_latest AS user__home_state_latest
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
                , subq_4.metric_time__day AS metric_time__day
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
                , subq_4.__revenue AS __revenue
              FROM (
                -- Join Self Over Time Range
                SELECT
                  subq_2.metric_time__day AS metric_time__day
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
                  , subq_1.revenue_instance__ds__day AS revenue_instance__ds__day
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
                  , subq_1.__revenue AS __revenue
                FROM (
                  -- Read From Time Spine 'mf_time_spine'
                  SELECT
                    subq_3.ds AS metric_time__day
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
                    , subq_0.__revenue
                  FROM (
                    -- Read Elements From Semantic Model 'revenue'
                    SELECT
                      revenue_src_28000.revenue AS __revenue
                      , DATE_TRUNC('day', revenue_src_28000.created_at) AS ds__day
                      , DATE_TRUNC('week', revenue_src_28000.created_at) AS ds__week
                      , DATE_TRUNC('month', revenue_src_28000.created_at) AS ds__month
                      , DATE_TRUNC('quarter', revenue_src_28000.created_at) AS ds__quarter
                      , DATE_TRUNC('year', revenue_src_28000.created_at) AS ds__year
                      , EXTRACT(year FROM revenue_src_28000.created_at) AS ds__extract_year
                      , EXTRACT(quarter FROM revenue_src_28000.created_at) AS ds__extract_quarter
                      , EXTRACT(month FROM revenue_src_28000.created_at) AS ds__extract_month
                      , EXTRACT(day FROM revenue_src_28000.created_at) AS ds__extract_day
                      , EXTRACT(DAYOFWEEK_ISO FROM revenue_src_28000.created_at) AS ds__extract_dow
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
                      , EXTRACT(DAYOFWEEK_ISO FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_dow
                      , EXTRACT(doy FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_doy
                      , revenue_src_28000.user_id AS user
                      , revenue_src_28000.user_id AS revenue_instance__user
                    FROM ***************************.fct_revenue revenue_src_28000
                  ) subq_0
                ) subq_1
                ON
                  (
                    subq_1.metric_time__day <= subq_2.metric_time__day
                  ) AND (
                    subq_1.metric_time__day > DATEADD(month, -2, subq_2.metric_time__day)
                  )
              ) subq_4
              LEFT OUTER JOIN (
                -- Pass Only Elements: ['home_state_latest', 'user']
                SELECT
                  subq_5.user
                  , subq_5.home_state_latest
                FROM (
                  -- Read Elements From Semantic Model 'users_latest'
                  SELECT
                    DATE_TRUNC('day', users_latest_src_28000.ds) AS ds_latest__day
                    , DATE_TRUNC('week', users_latest_src_28000.ds) AS ds_latest__week
                    , DATE_TRUNC('month', users_latest_src_28000.ds) AS ds_latest__month
                    , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS ds_latest__quarter
                    , DATE_TRUNC('year', users_latest_src_28000.ds) AS ds_latest__year
                    , EXTRACT(year FROM users_latest_src_28000.ds) AS ds_latest__extract_year
                    , EXTRACT(quarter FROM users_latest_src_28000.ds) AS ds_latest__extract_quarter
                    , EXTRACT(month FROM users_latest_src_28000.ds) AS ds_latest__extract_month
                    , EXTRACT(day FROM users_latest_src_28000.ds) AS ds_latest__extract_day
                    , EXTRACT(DAYOFWEEK_ISO FROM users_latest_src_28000.ds) AS ds_latest__extract_dow
                    , EXTRACT(doy FROM users_latest_src_28000.ds) AS ds_latest__extract_doy
                    , users_latest_src_28000.home_state_latest
                    , DATE_TRUNC('day', users_latest_src_28000.ds) AS user__ds_latest__day
                    , DATE_TRUNC('week', users_latest_src_28000.ds) AS user__ds_latest__week
                    , DATE_TRUNC('month', users_latest_src_28000.ds) AS user__ds_latest__month
                    , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS user__ds_latest__quarter
                    , DATE_TRUNC('year', users_latest_src_28000.ds) AS user__ds_latest__year
                    , EXTRACT(year FROM users_latest_src_28000.ds) AS user__ds_latest__extract_year
                    , EXTRACT(quarter FROM users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
                    , EXTRACT(month FROM users_latest_src_28000.ds) AS user__ds_latest__extract_month
                    , EXTRACT(day FROM users_latest_src_28000.ds) AS user__ds_latest__extract_day
                    , EXTRACT(DAYOFWEEK_ISO FROM users_latest_src_28000.ds) AS user__ds_latest__extract_dow
                    , EXTRACT(doy FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
                    , users_latest_src_28000.home_state_latest AS user__home_state_latest
                    , users_latest_src_28000.user_id AS user
                  FROM ***************************.dim_users_latest users_latest_src_28000
                ) subq_5
              ) subq_6
              ON
                subq_4.user = subq_6.user
            ) subq_7
          ) subq_8
          WHERE user__home_state_latest = 'CA'
        ) subq_9
      ) subq_10
      GROUP BY
        subq_10.metric_time__day
    ) subq_11
  ) subq_12
) subq_13
