test_name: test_cumulative_metric
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a basic cumulative metric query.
sql_engine: BigQuery
---
-- Write to DataTable
SELECT
  subq_6.ds__day
  , subq_6.trailing_2_months_revenue
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_5.ds__day
    , subq_5.revenue AS trailing_2_months_revenue
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_4.ds__day
      , subq_4.__revenue AS revenue
    FROM (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_3.ds__day
        , SUM(subq_3.__revenue) AS __revenue
      FROM (
        -- Pass Only Elements: ['__revenue', 'ds__day']
        SELECT
          subq_2.ds__day
          , subq_2.__revenue
        FROM (
          -- Pass Only Elements: ['__revenue', 'ds__day']
          SELECT
            subq_1.ds__day
            , subq_1.__revenue
          FROM (
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
            ) subq_0
          ) subq_1
        ) subq_2
      ) subq_3
      GROUP BY
        ds__day
    ) subq_4
  ) subq_5
) subq_6
