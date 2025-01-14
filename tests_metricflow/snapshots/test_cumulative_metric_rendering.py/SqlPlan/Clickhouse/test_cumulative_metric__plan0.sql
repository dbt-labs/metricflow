test_name: test_cumulative_metric
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a basic cumulative metric query.
sql_engine: Clickhouse
---
-- Compute Metrics via Expressions
SELECT
  subq_3.ds__day
  , subq_3.txn_revenue AS trailing_2_months_revenue
FROM (
  -- Aggregate Measures
  SELECT
    subq_2.ds__day
    , SUM(subq_2.txn_revenue) AS txn_revenue
  FROM (
    -- Pass Only Elements: ['txn_revenue', 'ds__day']
    SELECT
      subq_1.ds__day
      , subq_1.txn_revenue
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
        , subq_0.txn_revenue
      FROM (
        -- Read Elements From Semantic Model 'revenue'
        SELECT
          revenue_src_28000.revenue AS txn_revenue
          , date_trunc('day', revenue_src_28000.created_at) AS ds__day
          , date_trunc('week', revenue_src_28000.created_at) AS ds__week
          , date_trunc('month', revenue_src_28000.created_at) AS ds__month
          , date_trunc('quarter', revenue_src_28000.created_at) AS ds__quarter
          , date_trunc('year', revenue_src_28000.created_at) AS ds__year
          , toYear(revenue_src_28000.created_at) AS ds__extract_year
          , toQuarter(revenue_src_28000.created_at) AS ds__extract_quarter
          , toMonth(revenue_src_28000.created_at) AS ds__extract_month
          , toDayOfMonth(revenue_src_28000.created_at) AS ds__extract_day
          , toDayOfWeek(revenue_src_28000.created_at) AS ds__extract_dow
          , toDayOfYear(revenue_src_28000.created_at) AS ds__extract_doy
          , date_trunc('day', revenue_src_28000.created_at) AS revenue_instance__ds__day
          , date_trunc('week', revenue_src_28000.created_at) AS revenue_instance__ds__week
          , date_trunc('month', revenue_src_28000.created_at) AS revenue_instance__ds__month
          , date_trunc('quarter', revenue_src_28000.created_at) AS revenue_instance__ds__quarter
          , date_trunc('year', revenue_src_28000.created_at) AS revenue_instance__ds__year
          , toYear(revenue_src_28000.created_at) AS revenue_instance__ds__extract_year
          , toQuarter(revenue_src_28000.created_at) AS revenue_instance__ds__extract_quarter
          , toMonth(revenue_src_28000.created_at) AS revenue_instance__ds__extract_month
          , toDayOfMonth(revenue_src_28000.created_at) AS revenue_instance__ds__extract_day
          , toDayOfWeek(revenue_src_28000.created_at) AS revenue_instance__ds__extract_dow
          , toDayOfYear(revenue_src_28000.created_at) AS revenue_instance__ds__extract_doy
          , revenue_src_28000.user_id AS user
          , revenue_src_28000.user_id AS revenue_instance__user
        FROM ***************************.fct_revenue revenue_src_28000
      ) subq_0
    ) subq_1
  ) subq_2
  GROUP BY
    ds__day
) subq_3
