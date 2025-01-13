test_name: test_cumulative_metric_no_window
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query where there is a windowless cumulative metric to compute.
sql_engine: Clickhouse
---
-- Compute Metrics via Expressions
SELECT
  subq_3.ds__month
  , subq_3.txn_revenue AS revenue_all_time
FROM (
  -- Aggregate Measures
  SELECT
    subq_2.ds__month
    , SUM(subq_2.txn_revenue) AS txn_revenue
  FROM (
    -- Pass Only Elements: ['txn_revenue', 'ds__month']
    SELECT
      subq_1.ds__month
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
          , DATE_TRUNC('day', revenue_src_28000.created_at) AS ds__day
          , DATE_TRUNC('week', revenue_src_28000.created_at) AS ds__week
          , DATE_TRUNC('month', revenue_src_28000.created_at) AS ds__month
          , DATE_TRUNC('quarter', revenue_src_28000.created_at) AS ds__quarter
          , DATE_TRUNC('year', revenue_src_28000.created_at) AS ds__year
          , EXTRACT(toYear FROM revenue_src_28000.created_at) AS ds__extract_year
          , EXTRACT(toQuarter FROM revenue_src_28000.created_at) AS ds__extract_quarter
          , EXTRACT(toMonth FROM revenue_src_28000.created_at) AS ds__extract_month
          , EXTRACT(toDayOfMonth FROM revenue_src_28000.created_at) AS ds__extract_day
          , EXTRACT(toDayOfWeek FROM revenue_src_28000.created_at) AS ds__extract_dow
          , EXTRACT(toDayOfYear FROM revenue_src_28000.created_at) AS ds__extract_doy
          , DATE_TRUNC('day', revenue_src_28000.created_at) AS revenue_instance__ds__day
          , DATE_TRUNC('week', revenue_src_28000.created_at) AS revenue_instance__ds__week
          , DATE_TRUNC('month', revenue_src_28000.created_at) AS revenue_instance__ds__month
          , DATE_TRUNC('quarter', revenue_src_28000.created_at) AS revenue_instance__ds__quarter
          , DATE_TRUNC('year', revenue_src_28000.created_at) AS revenue_instance__ds__year
          , EXTRACT(toYear FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_year
          , EXTRACT(toQuarter FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_quarter
          , EXTRACT(toMonth FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_month
          , EXTRACT(toDayOfMonth FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_day
          , EXTRACT(toDayOfWeek FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_dow
          , EXTRACT(toDayOfYear FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_doy
          , revenue_src_28000.user_id AS user
          , revenue_src_28000.user_id AS revenue_instance__user
        FROM ***************************.fct_revenue revenue_src_28000
        SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
      ) subq_0
      SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
    ) subq_1
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_2
  GROUP BY
    subq_2.ds__month
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_3
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
