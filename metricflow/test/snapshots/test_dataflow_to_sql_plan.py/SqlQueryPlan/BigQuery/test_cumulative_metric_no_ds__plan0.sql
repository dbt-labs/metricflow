-- Compute Metrics via Expressions
SELECT
  subq_3.txn_revenue AS trailing_2_months_revenue
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_2.txn_revenue) AS txn_revenue
  FROM (
    -- Pass Only Elements:
    --   ['txn_revenue']
    SELECT
      subq_1.txn_revenue
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
        , subq_0.ds__extract_week
        , subq_0.ds__extract_day
        , subq_0.ds__extract_dow
        , subq_0.ds__extract_doy
        , subq_0.company__ds__day
        , subq_0.company__ds__week
        , subq_0.company__ds__month
        , subq_0.company__ds__quarter
        , subq_0.company__ds__year
        , subq_0.company__ds__extract_year
        , subq_0.company__ds__extract_quarter
        , subq_0.company__ds__extract_month
        , subq_0.company__ds__extract_week
        , subq_0.company__ds__extract_day
        , subq_0.company__ds__extract_dow
        , subq_0.company__ds__extract_doy
        , subq_0.ds__day AS metric_time__day
        , subq_0.ds__week AS metric_time__week
        , subq_0.ds__month AS metric_time__month
        , subq_0.ds__quarter AS metric_time__quarter
        , subq_0.ds__year AS metric_time__year
        , subq_0.ds__extract_year AS metric_time__extract_year
        , subq_0.ds__extract_quarter AS metric_time__extract_quarter
        , subq_0.ds__extract_month AS metric_time__extract_month
        , subq_0.ds__extract_week AS metric_time__extract_week
        , subq_0.ds__extract_day AS metric_time__extract_day
        , subq_0.ds__extract_dow AS metric_time__extract_dow
        , subq_0.ds__extract_doy AS metric_time__extract_doy
        , subq_0.user
        , subq_0.company__user
        , subq_0.txn_revenue
      FROM (
        -- Read Elements From Semantic Model 'revenue'
        SELECT
          revenue_src_10006.revenue AS txn_revenue
          , DATE_TRUNC(revenue_src_10006.created_at, day) AS ds__day
          , DATE_TRUNC(revenue_src_10006.created_at, isoweek) AS ds__week
          , DATE_TRUNC(revenue_src_10006.created_at, month) AS ds__month
          , DATE_TRUNC(revenue_src_10006.created_at, quarter) AS ds__quarter
          , DATE_TRUNC(revenue_src_10006.created_at, year) AS ds__year
          , EXTRACT(year FROM revenue_src_10006.created_at) AS ds__extract_year
          , EXTRACT(quarter FROM revenue_src_10006.created_at) AS ds__extract_quarter
          , EXTRACT(month FROM revenue_src_10006.created_at) AS ds__extract_month
          , EXTRACT(isoweek FROM revenue_src_10006.created_at) AS ds__extract_week
          , EXTRACT(day FROM revenue_src_10006.created_at) AS ds__extract_day
          , EXTRACT(dayofweek FROM revenue_src_10006.created_at) AS ds__extract_dow
          , EXTRACT(dayofyear FROM revenue_src_10006.created_at) AS ds__extract_doy
          , DATE_TRUNC(revenue_src_10006.created_at, day) AS company__ds__day
          , DATE_TRUNC(revenue_src_10006.created_at, isoweek) AS company__ds__week
          , DATE_TRUNC(revenue_src_10006.created_at, month) AS company__ds__month
          , DATE_TRUNC(revenue_src_10006.created_at, quarter) AS company__ds__quarter
          , DATE_TRUNC(revenue_src_10006.created_at, year) AS company__ds__year
          , EXTRACT(year FROM revenue_src_10006.created_at) AS company__ds__extract_year
          , EXTRACT(quarter FROM revenue_src_10006.created_at) AS company__ds__extract_quarter
          , EXTRACT(month FROM revenue_src_10006.created_at) AS company__ds__extract_month
          , EXTRACT(isoweek FROM revenue_src_10006.created_at) AS company__ds__extract_week
          , EXTRACT(day FROM revenue_src_10006.created_at) AS company__ds__extract_day
          , EXTRACT(dayofweek FROM revenue_src_10006.created_at) AS company__ds__extract_dow
          , EXTRACT(dayofyear FROM revenue_src_10006.created_at) AS company__ds__extract_doy
          , revenue_src_10006.user_id AS user
          , revenue_src_10006.user_id AS company__user
        FROM ***************************.fct_revenue revenue_src_10006
      ) subq_0
    ) subq_1
  ) subq_2
) subq_3
