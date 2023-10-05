-- Read Elements From Semantic Model 'revenue'
SELECT
  revenue_src_10006.revenue AS txn_revenue
  , DATE_TRUNC('day', revenue_src_10006.created_at) AS ds__day
  , DATE_TRUNC('week', revenue_src_10006.created_at) AS ds__week
  , DATE_TRUNC('month', revenue_src_10006.created_at) AS ds__month
  , DATE_TRUNC('quarter', revenue_src_10006.created_at) AS ds__quarter
  , DATE_TRUNC('year', revenue_src_10006.created_at) AS ds__year
  , EXTRACT(year FROM revenue_src_10006.created_at) AS ds__extract_year
  , EXTRACT(quarter FROM revenue_src_10006.created_at) AS ds__extract_quarter
  , EXTRACT(month FROM revenue_src_10006.created_at) AS ds__extract_month
  , EXTRACT(week FROM revenue_src_10006.created_at) AS ds__extract_week
  , EXTRACT(day FROM revenue_src_10006.created_at) AS ds__extract_day
  , EXTRACT(dow FROM revenue_src_10006.created_at) AS ds__extract_dow
  , EXTRACT(doy FROM revenue_src_10006.created_at) AS ds__extract_doy
  , DATE_TRUNC('day', revenue_src_10006.created_at) AS company__ds__day
  , DATE_TRUNC('week', revenue_src_10006.created_at) AS company__ds__week
  , DATE_TRUNC('month', revenue_src_10006.created_at) AS company__ds__month
  , DATE_TRUNC('quarter', revenue_src_10006.created_at) AS company__ds__quarter
  , DATE_TRUNC('year', revenue_src_10006.created_at) AS company__ds__year
  , EXTRACT(year FROM revenue_src_10006.created_at) AS company__ds__extract_year
  , EXTRACT(quarter FROM revenue_src_10006.created_at) AS company__ds__extract_quarter
  , EXTRACT(month FROM revenue_src_10006.created_at) AS company__ds__extract_month
  , EXTRACT(week FROM revenue_src_10006.created_at) AS company__ds__extract_week
  , EXTRACT(day FROM revenue_src_10006.created_at) AS company__ds__extract_day
  , EXTRACT(dow FROM revenue_src_10006.created_at) AS company__ds__extract_dow
  , EXTRACT(doy FROM revenue_src_10006.created_at) AS company__ds__extract_doy
  , revenue_src_10006.user_id AS user
  , revenue_src_10006.user_id AS company__user
FROM ***************************.fct_revenue revenue_src_10006
