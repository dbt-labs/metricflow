test_name: test_cumulative_metric_no_ds
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a cumulative metric with no time dimension specified.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_2.txn_revenue AS trailing_2_months_revenue
FROM (
  -- Aggregate Measures
  SELECT
    SUM(nr_subq_1.txn_revenue) AS txn_revenue
  FROM (
    -- Pass Only Elements: ['txn_revenue',]
    SELECT
      nr_subq_0.txn_revenue
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
    ) nr_subq_0
  ) nr_subq_1
) nr_subq_2
