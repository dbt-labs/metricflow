-- Read Elements From Semantic Model 'revenue'
SELECT
  revenue_src_10007.revenue AS txn_revenue
  , DATE_TRUNC(revenue_src_10007.created_at, day) AS ds__day
  , DATE_TRUNC(revenue_src_10007.created_at, isoweek) AS ds__week
  , DATE_TRUNC(revenue_src_10007.created_at, month) AS ds__month
  , DATE_TRUNC(revenue_src_10007.created_at, quarter) AS ds__quarter
  , DATE_TRUNC(revenue_src_10007.created_at, year) AS ds__year
  , EXTRACT(year FROM revenue_src_10007.created_at) AS ds__extract_year
  , EXTRACT(quarter FROM revenue_src_10007.created_at) AS ds__extract_quarter
  , EXTRACT(month FROM revenue_src_10007.created_at) AS ds__extract_month
  , EXTRACT(day FROM revenue_src_10007.created_at) AS ds__extract_day
  , IF(EXTRACT(dayofweek FROM revenue_src_10007.created_at) = 1, 7, EXTRACT(dayofweek FROM revenue_src_10007.created_at) - 1) AS ds__extract_dow
  , EXTRACT(dayofyear FROM revenue_src_10007.created_at) AS ds__extract_doy
  , DATE_TRUNC(revenue_src_10007.created_at, day) AS company__ds__day
  , DATE_TRUNC(revenue_src_10007.created_at, isoweek) AS company__ds__week
  , DATE_TRUNC(revenue_src_10007.created_at, month) AS company__ds__month
  , DATE_TRUNC(revenue_src_10007.created_at, quarter) AS company__ds__quarter
  , DATE_TRUNC(revenue_src_10007.created_at, year) AS company__ds__year
  , EXTRACT(year FROM revenue_src_10007.created_at) AS company__ds__extract_year
  , EXTRACT(quarter FROM revenue_src_10007.created_at) AS company__ds__extract_quarter
  , EXTRACT(month FROM revenue_src_10007.created_at) AS company__ds__extract_month
  , EXTRACT(day FROM revenue_src_10007.created_at) AS company__ds__extract_day
  , IF(EXTRACT(dayofweek FROM revenue_src_10007.created_at) = 1, 7, EXTRACT(dayofweek FROM revenue_src_10007.created_at) - 1) AS company__ds__extract_dow
  , EXTRACT(dayofyear FROM revenue_src_10007.created_at) AS company__ds__extract_doy
  , revenue_src_10007.user_id AS user
  , revenue_src_10007.user_id AS company__user
FROM ***************************.fct_revenue revenue_src_10007
