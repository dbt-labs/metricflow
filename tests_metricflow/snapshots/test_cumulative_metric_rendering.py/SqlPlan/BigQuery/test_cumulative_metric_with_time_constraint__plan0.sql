test_name: test_cumulative_metric_with_time_constraint
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a cumulative metric query with an adjustable time constraint.

      Not all query inputs with time constraint filters allow us to adjust the time constraint to include the full
      span of input data for a cumulative metric, but when we receive a time constraint filter expression we can
      automatically adjust it should render a query similar to this one.
sql_engine: BigQuery
---
-- Write to DataTable
SELECT
  subq_10.metric_time__day
  , subq_10.trailing_2_months_revenue
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_9.metric_time__day
    , subq_9.txn_revenue AS trailing_2_months_revenue
  FROM (
    -- Aggregate Measures
    SELECT
      subq_8.metric_time__day
      , SUM(subq_8.txn_revenue) AS txn_revenue
    FROM (
      -- Pass Only Elements: ['txn_revenue', 'metric_time__day']
      SELECT
        subq_7.metric_time__day
        , subq_7.txn_revenue
      FROM (
        -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-01T00:00:00]
        SELECT
          subq_6.ds__day
          , subq_6.ds__week
          , subq_6.ds__month
          , subq_6.ds__quarter
          , subq_6.ds__year
          , subq_6.ds__extract_year
          , subq_6.ds__extract_quarter
          , subq_6.ds__extract_month
          , subq_6.ds__extract_day
          , subq_6.ds__extract_dow
          , subq_6.ds__extract_doy
          , subq_6.revenue_instance__ds__day
          , subq_6.revenue_instance__ds__week
          , subq_6.revenue_instance__ds__month
          , subq_6.revenue_instance__ds__quarter
          , subq_6.revenue_instance__ds__year
          , subq_6.revenue_instance__ds__extract_year
          , subq_6.revenue_instance__ds__extract_quarter
          , subq_6.revenue_instance__ds__extract_month
          , subq_6.revenue_instance__ds__extract_day
          , subq_6.revenue_instance__ds__extract_dow
          , subq_6.revenue_instance__ds__extract_doy
          , subq_6.metric_time__day
          , subq_6.metric_time__week
          , subq_6.metric_time__month
          , subq_6.metric_time__quarter
          , subq_6.metric_time__year
          , subq_6.metric_time__extract_year
          , subq_6.metric_time__extract_quarter
          , subq_6.metric_time__extract_month
          , subq_6.metric_time__extract_day
          , subq_6.metric_time__extract_dow
          , subq_6.metric_time__extract_doy
          , subq_6.user
          , subq_6.revenue_instance__user
          , subq_6.txn_revenue
        FROM (
          -- Join Self Over Time Range
          SELECT
            subq_4.metric_time__day AS metric_time__day
            , subq_3.ds__day AS ds__day
            , subq_3.ds__week AS ds__week
            , subq_3.ds__month AS ds__month
            , subq_3.ds__quarter AS ds__quarter
            , subq_3.ds__year AS ds__year
            , subq_3.ds__extract_year AS ds__extract_year
            , subq_3.ds__extract_quarter AS ds__extract_quarter
            , subq_3.ds__extract_month AS ds__extract_month
            , subq_3.ds__extract_day AS ds__extract_day
            , subq_3.ds__extract_dow AS ds__extract_dow
            , subq_3.ds__extract_doy AS ds__extract_doy
            , subq_3.revenue_instance__ds__day AS revenue_instance__ds__day
            , subq_3.revenue_instance__ds__week AS revenue_instance__ds__week
            , subq_3.revenue_instance__ds__month AS revenue_instance__ds__month
            , subq_3.revenue_instance__ds__quarter AS revenue_instance__ds__quarter
            , subq_3.revenue_instance__ds__year AS revenue_instance__ds__year
            , subq_3.revenue_instance__ds__extract_year AS revenue_instance__ds__extract_year
            , subq_3.revenue_instance__ds__extract_quarter AS revenue_instance__ds__extract_quarter
            , subq_3.revenue_instance__ds__extract_month AS revenue_instance__ds__extract_month
            , subq_3.revenue_instance__ds__extract_day AS revenue_instance__ds__extract_day
            , subq_3.revenue_instance__ds__extract_dow AS revenue_instance__ds__extract_dow
            , subq_3.revenue_instance__ds__extract_doy AS revenue_instance__ds__extract_doy
            , subq_3.metric_time__week AS metric_time__week
            , subq_3.metric_time__month AS metric_time__month
            , subq_3.metric_time__quarter AS metric_time__quarter
            , subq_3.metric_time__year AS metric_time__year
            , subq_3.metric_time__extract_year AS metric_time__extract_year
            , subq_3.metric_time__extract_quarter AS metric_time__extract_quarter
            , subq_3.metric_time__extract_month AS metric_time__extract_month
            , subq_3.metric_time__extract_day AS metric_time__extract_day
            , subq_3.metric_time__extract_dow AS metric_time__extract_dow
            , subq_3.metric_time__extract_doy AS metric_time__extract_doy
            , subq_3.user AS user
            , subq_3.revenue_instance__user AS revenue_instance__user
            , subq_3.txn_revenue AS txn_revenue
          FROM (
            -- Read From Time Spine 'mf_time_spine'
            SELECT
              subq_5.ds AS metric_time__day
            FROM ***************************.mf_time_spine subq_5
            WHERE subq_5.ds BETWEEN '2020-01-01' AND '2020-01-01'
          ) subq_4
          INNER JOIN (
            -- Constrain Time Range to [2019-11-01T00:00:00, 2020-01-01T00:00:00]
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
              , subq_2.metric_time__day
              , subq_2.metric_time__week
              , subq_2.metric_time__month
              , subq_2.metric_time__quarter
              , subq_2.metric_time__year
              , subq_2.metric_time__extract_year
              , subq_2.metric_time__extract_quarter
              , subq_2.metric_time__extract_month
              , subq_2.metric_time__extract_day
              , subq_2.metric_time__extract_dow
              , subq_2.metric_time__extract_doy
              , subq_2.user
              , subq_2.revenue_instance__user
              , subq_2.txn_revenue
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_1.ds__day
                , subq_1.ds__week
                , subq_1.ds__month
                , subq_1.ds__quarter
                , subq_1.ds__year
                , subq_1.ds__extract_year
                , subq_1.ds__extract_quarter
                , subq_1.ds__extract_month
                , subq_1.ds__extract_day
                , subq_1.ds__extract_dow
                , subq_1.ds__extract_doy
                , subq_1.revenue_instance__ds__day
                , subq_1.revenue_instance__ds__week
                , subq_1.revenue_instance__ds__month
                , subq_1.revenue_instance__ds__quarter
                , subq_1.revenue_instance__ds__year
                , subq_1.revenue_instance__ds__extract_year
                , subq_1.revenue_instance__ds__extract_quarter
                , subq_1.revenue_instance__ds__extract_month
                , subq_1.revenue_instance__ds__extract_day
                , subq_1.revenue_instance__ds__extract_dow
                , subq_1.revenue_instance__ds__extract_doy
                , subq_1.ds__day AS metric_time__day
                , subq_1.ds__week AS metric_time__week
                , subq_1.ds__month AS metric_time__month
                , subq_1.ds__quarter AS metric_time__quarter
                , subq_1.ds__year AS metric_time__year
                , subq_1.ds__extract_year AS metric_time__extract_year
                , subq_1.ds__extract_quarter AS metric_time__extract_quarter
                , subq_1.ds__extract_month AS metric_time__extract_month
                , subq_1.ds__extract_day AS metric_time__extract_day
                , subq_1.ds__extract_dow AS metric_time__extract_dow
                , subq_1.ds__extract_doy AS metric_time__extract_doy
                , subq_1.user
                , subq_1.revenue_instance__user
                , subq_1.txn_revenue
              FROM (
                -- Read Elements From Semantic Model 'revenue'
                SELECT
                  revenue_src_28000.revenue AS txn_revenue
                  , DATETIME_TRUNC(revenue_src_28000.created_at, day) AS ds__day
                  , DATETIME_TRUNC(revenue_src_28000.created_at, isoweek) AS ds__week
                  , DATETIME_TRUNC(revenue_src_28000.created_at, month) AS ds__month
                  , DATETIME_TRUNC(revenue_src_28000.created_at, quarter) AS ds__quarter
                  , DATETIME_TRUNC(revenue_src_28000.created_at, year) AS ds__year
                  , EXTRACT(year FROM revenue_src_28000.created_at) AS ds__extract_year
                  , EXTRACT(quarter FROM revenue_src_28000.created_at) AS ds__extract_quarter
                  , EXTRACT(month FROM revenue_src_28000.created_at) AS ds__extract_month
                  , EXTRACT(day FROM revenue_src_28000.created_at) AS ds__extract_day
                  , IF(EXTRACT(dayofweek FROM revenue_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM revenue_src_28000.created_at) - 1) AS ds__extract_dow
                  , EXTRACT(dayofyear FROM revenue_src_28000.created_at) AS ds__extract_doy
                  , DATETIME_TRUNC(revenue_src_28000.created_at, day) AS revenue_instance__ds__day
                  , DATETIME_TRUNC(revenue_src_28000.created_at, isoweek) AS revenue_instance__ds__week
                  , DATETIME_TRUNC(revenue_src_28000.created_at, month) AS revenue_instance__ds__month
                  , DATETIME_TRUNC(revenue_src_28000.created_at, quarter) AS revenue_instance__ds__quarter
                  , DATETIME_TRUNC(revenue_src_28000.created_at, year) AS revenue_instance__ds__year
                  , EXTRACT(year FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_year
                  , EXTRACT(quarter FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_quarter
                  , EXTRACT(month FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_month
                  , EXTRACT(day FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_day
                  , IF(EXTRACT(dayofweek FROM revenue_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM revenue_src_28000.created_at) - 1) AS revenue_instance__ds__extract_dow
                  , EXTRACT(dayofyear FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_doy
                  , revenue_src_28000.user_id AS user
                  , revenue_src_28000.user_id AS revenue_instance__user
                FROM ***************************.fct_revenue revenue_src_28000
              ) subq_1
            ) subq_2
            WHERE subq_2.metric_time__day BETWEEN '2019-11-01' AND '2020-01-01'
          ) subq_3
          ON
            (
              subq_3.metric_time__day <= subq_4.metric_time__day
            ) AND (
              subq_3.metric_time__day > DATE_SUB(CAST(subq_4.metric_time__day AS DATETIME), INTERVAL 2 month)
            )
        ) subq_6
        WHERE subq_6.metric_time__day BETWEEN '2020-01-01' AND '2020-01-01'
      ) subq_7
    ) subq_8
    GROUP BY
      metric_time__day
  ) subq_9
) subq_10
