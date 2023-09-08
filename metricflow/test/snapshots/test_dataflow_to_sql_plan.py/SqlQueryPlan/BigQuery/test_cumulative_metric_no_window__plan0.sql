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
    -- Pass Only Elements:
    --   ['txn_revenue', 'ds__month']
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
        , subq_0.ds__extract_week
        , subq_0.ds__extract_day
        , subq_0.ds__extract_dayofweek
        , subq_0.ds__extract_dayofyear
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
        , subq_0.company__ds__extract_dayofweek
        , subq_0.company__ds__extract_dayofyear
        , subq_0.ds__day AS metric_time__day
        , subq_0.ds__week AS metric_time__week
        , subq_0.ds__month AS metric_time__month
        , subq_0.ds__quarter AS metric_time__quarter
        , subq_0.ds__year AS metric_time__year
        , subq_0.user
        , subq_0.company__user
        , subq_0.txn_revenue
      FROM (
        -- Read Elements From Semantic Model 'revenue'
        SELECT
          revenue_src_10006.revenue AS txn_revenue
          , revenue_src_10006.created_at AS ds__day
          , DATE_TRUNC(revenue_src_10006.created_at, isoweek) AS ds__week
          , DATE_TRUNC(revenue_src_10006.created_at, month) AS ds__month
          , DATE_TRUNC(revenue_src_10006.created_at, quarter) AS ds__quarter
          , DATE_TRUNC(revenue_src_10006.created_at, year) AS ds__year
          , EXTRACT(YEAR FROM revenue_src_10006.created_at) AS ds__extract_year
          , EXTRACT(QUARTER FROM revenue_src_10006.created_at) AS ds__extract_quarter
          , EXTRACT(MONTH FROM revenue_src_10006.created_at) AS ds__extract_month
          , EXTRACT(WEEK FROM revenue_src_10006.created_at) AS ds__extract_week
          , EXTRACT(DAY FROM revenue_src_10006.created_at) AS ds__extract_day
          , EXTRACT(DAYOFWEEK FROM revenue_src_10006.created_at) AS ds__extract_dayofweek
          , EXTRACT(DAYOFYEAR FROM revenue_src_10006.created_at) AS ds__extract_dayofyear
          , revenue_src_10006.created_at AS company__ds__day
          , DATE_TRUNC(revenue_src_10006.created_at, isoweek) AS company__ds__week
          , DATE_TRUNC(revenue_src_10006.created_at, month) AS company__ds__month
          , DATE_TRUNC(revenue_src_10006.created_at, quarter) AS company__ds__quarter
          , DATE_TRUNC(revenue_src_10006.created_at, year) AS company__ds__year
          , EXTRACT(YEAR FROM revenue_src_10006.created_at) AS company__ds__extract_year
          , EXTRACT(QUARTER FROM revenue_src_10006.created_at) AS company__ds__extract_quarter
          , EXTRACT(MONTH FROM revenue_src_10006.created_at) AS company__ds__extract_month
          , EXTRACT(WEEK FROM revenue_src_10006.created_at) AS company__ds__extract_week
          , EXTRACT(DAY FROM revenue_src_10006.created_at) AS company__ds__extract_day
          , EXTRACT(DAYOFWEEK FROM revenue_src_10006.created_at) AS company__ds__extract_dayofweek
          , EXTRACT(DAYOFYEAR FROM revenue_src_10006.created_at) AS company__ds__extract_dayofyear
          , revenue_src_10006.user_id AS user
          , revenue_src_10006.user_id AS company__user
        FROM ***************************.fct_revenue revenue_src_10006
      ) subq_0
    ) subq_1
  ) subq_2
  GROUP BY
    ds__month
) subq_3
