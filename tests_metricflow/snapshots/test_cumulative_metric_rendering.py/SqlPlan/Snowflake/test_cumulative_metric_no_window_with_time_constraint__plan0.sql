test_name: test_cumulative_metric_no_window_with_time_constraint
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a windowless cumulative metric query with an adjustable time constraint.
sql_engine: Snowflake
---
-- Write to DataTable
SELECT
  subq_15.metric_time__day
  , subq_15.revenue_all_time
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_14.metric_time__day
    , subq_14.revenue AS revenue_all_time
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_13.metric_time__day
      , subq_13.__revenue AS revenue
    FROM (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_12.metric_time__day
        , SUM(subq_12.__revenue) AS __revenue
      FROM (
        -- Pass Only Elements: ['__revenue', 'metric_time__day']
        SELECT
          subq_11.metric_time__day
          , subq_11.__revenue
        FROM (
          -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
          SELECT
            subq_10.metric_time__day
            , subq_10.__revenue
          FROM (
            -- Pass Only Elements: ['__revenue', 'metric_time__day']
            SELECT
              subq_9.metric_time__day
              , subq_9.__revenue
            FROM (
              -- Join Self Over Time Range
              SELECT
                subq_7.metric_time__day AS metric_time__day
                , subq_6.ds__day AS ds__day
                , subq_6.ds__week AS ds__week
                , subq_6.ds__month AS ds__month
                , subq_6.ds__quarter AS ds__quarter
                , subq_6.ds__year AS ds__year
                , subq_6.ds__extract_year AS ds__extract_year
                , subq_6.ds__extract_quarter AS ds__extract_quarter
                , subq_6.ds__extract_month AS ds__extract_month
                , subq_6.ds__extract_day AS ds__extract_day
                , subq_6.ds__extract_dow AS ds__extract_dow
                , subq_6.ds__extract_doy AS ds__extract_doy
                , subq_6.revenue_instance__ds__day AS revenue_instance__ds__day
                , subq_6.revenue_instance__ds__week AS revenue_instance__ds__week
                , subq_6.revenue_instance__ds__month AS revenue_instance__ds__month
                , subq_6.revenue_instance__ds__quarter AS revenue_instance__ds__quarter
                , subq_6.revenue_instance__ds__year AS revenue_instance__ds__year
                , subq_6.revenue_instance__ds__extract_year AS revenue_instance__ds__extract_year
                , subq_6.revenue_instance__ds__extract_quarter AS revenue_instance__ds__extract_quarter
                , subq_6.revenue_instance__ds__extract_month AS revenue_instance__ds__extract_month
                , subq_6.revenue_instance__ds__extract_day AS revenue_instance__ds__extract_day
                , subq_6.revenue_instance__ds__extract_dow AS revenue_instance__ds__extract_dow
                , subq_6.revenue_instance__ds__extract_doy AS revenue_instance__ds__extract_doy
                , subq_6.metric_time__week AS metric_time__week
                , subq_6.metric_time__month AS metric_time__month
                , subq_6.metric_time__quarter AS metric_time__quarter
                , subq_6.metric_time__year AS metric_time__year
                , subq_6.metric_time__extract_year AS metric_time__extract_year
                , subq_6.metric_time__extract_quarter AS metric_time__extract_quarter
                , subq_6.metric_time__extract_month AS metric_time__extract_month
                , subq_6.metric_time__extract_day AS metric_time__extract_day
                , subq_6.metric_time__extract_dow AS metric_time__extract_dow
                , subq_6.metric_time__extract_doy AS metric_time__extract_doy
                , subq_6.user AS user
                , subq_6.revenue_instance__user AS revenue_instance__user
                , subq_6.__revenue AS __revenue
              FROM (
                -- Read From Time Spine 'mf_time_spine'
                SELECT
                  subq_8.ds AS metric_time__day
                FROM ***************************.mf_time_spine subq_8
                WHERE subq_8.ds BETWEEN '2020-01-01' AND '2020-01-01'
              ) subq_7
              INNER JOIN (
                -- Constrain Time Range to [2000-01-01T00:00:00, 2020-01-01T00:00:00]
                SELECT
                  subq_5.ds__day
                  , subq_5.ds__week
                  , subq_5.ds__month
                  , subq_5.ds__quarter
                  , subq_5.ds__year
                  , subq_5.ds__extract_year
                  , subq_5.ds__extract_quarter
                  , subq_5.ds__extract_month
                  , subq_5.ds__extract_day
                  , subq_5.ds__extract_dow
                  , subq_5.ds__extract_doy
                  , subq_5.revenue_instance__ds__day
                  , subq_5.revenue_instance__ds__week
                  , subq_5.revenue_instance__ds__month
                  , subq_5.revenue_instance__ds__quarter
                  , subq_5.revenue_instance__ds__year
                  , subq_5.revenue_instance__ds__extract_year
                  , subq_5.revenue_instance__ds__extract_quarter
                  , subq_5.revenue_instance__ds__extract_month
                  , subq_5.revenue_instance__ds__extract_day
                  , subq_5.revenue_instance__ds__extract_dow
                  , subq_5.revenue_instance__ds__extract_doy
                  , subq_5.metric_time__day
                  , subq_5.metric_time__week
                  , subq_5.metric_time__month
                  , subq_5.metric_time__quarter
                  , subq_5.metric_time__year
                  , subq_5.metric_time__extract_year
                  , subq_5.metric_time__extract_quarter
                  , subq_5.metric_time__extract_month
                  , subq_5.metric_time__extract_day
                  , subq_5.metric_time__extract_dow
                  , subq_5.metric_time__extract_doy
                  , subq_5.user
                  , subq_5.revenue_instance__user
                  , subq_5.__revenue
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_4.ds__day
                    , subq_4.ds__week
                    , subq_4.ds__month
                    , subq_4.ds__quarter
                    , subq_4.ds__year
                    , subq_4.ds__extract_year
                    , subq_4.ds__extract_quarter
                    , subq_4.ds__extract_month
                    , subq_4.ds__extract_day
                    , subq_4.ds__extract_dow
                    , subq_4.ds__extract_doy
                    , subq_4.revenue_instance__ds__day
                    , subq_4.revenue_instance__ds__week
                    , subq_4.revenue_instance__ds__month
                    , subq_4.revenue_instance__ds__quarter
                    , subq_4.revenue_instance__ds__year
                    , subq_4.revenue_instance__ds__extract_year
                    , subq_4.revenue_instance__ds__extract_quarter
                    , subq_4.revenue_instance__ds__extract_month
                    , subq_4.revenue_instance__ds__extract_day
                    , subq_4.revenue_instance__ds__extract_dow
                    , subq_4.revenue_instance__ds__extract_doy
                    , subq_4.ds__day AS metric_time__day
                    , subq_4.ds__week AS metric_time__week
                    , subq_4.ds__month AS metric_time__month
                    , subq_4.ds__quarter AS metric_time__quarter
                    , subq_4.ds__year AS metric_time__year
                    , subq_4.ds__extract_year AS metric_time__extract_year
                    , subq_4.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_4.ds__extract_month AS metric_time__extract_month
                    , subq_4.ds__extract_day AS metric_time__extract_day
                    , subq_4.ds__extract_dow AS metric_time__extract_dow
                    , subq_4.ds__extract_doy AS metric_time__extract_doy
                    , subq_4.user
                    , subq_4.revenue_instance__user
                    , subq_4.__revenue
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
                      , EXTRACT(dayofweekiso FROM revenue_src_28000.created_at) AS ds__extract_dow
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
                      , EXTRACT(dayofweekiso FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_dow
                      , EXTRACT(doy FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_doy
                      , revenue_src_28000.user_id AS user
                      , revenue_src_28000.user_id AS revenue_instance__user
                    FROM ***************************.fct_revenue revenue_src_28000
                  ) subq_4
                ) subq_5
                WHERE subq_5.metric_time__day BETWEEN '2000-01-01' AND '2020-01-01'
              ) subq_6
              ON
                (subq_6.metric_time__day <= subq_7.metric_time__day)
            ) subq_9
          ) subq_10
          WHERE subq_10.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
        ) subq_11
      ) subq_12
      GROUP BY
        subq_12.metric_time__day
    ) subq_13
  ) subq_14
) subq_15
