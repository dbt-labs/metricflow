-- Read Elements From Semantic Model 'revenue'
SELECT
  revenue_src_10006.revenue AS txn_revenue
  , revenue_src_10006.created_at AS ds__day
  , DATE_TRUNC(revenue_src_10006.created_at, isoweek) AS ds__week
  , DATE_TRUNC(revenue_src_10006.created_at, month) AS ds__month
  , DATE_TRUNC(revenue_src_10006.created_at, quarter) AS ds__quarter
  , DATE_TRUNC(revenue_src_10006.created_at, isoyear) AS ds__year
  , revenue_src_10006.created_at AS company__ds__day
  , DATE_TRUNC(revenue_src_10006.created_at, isoweek) AS company__ds__week
  , DATE_TRUNC(revenue_src_10006.created_at, month) AS company__ds__month
  , DATE_TRUNC(revenue_src_10006.created_at, quarter) AS company__ds__quarter
  , DATE_TRUNC(revenue_src_10006.created_at, isoyear) AS company__ds__year
  , revenue_src_10006.user_id AS user
  , revenue_src_10006.user_id AS company__user
FROM ***************************.fct_revenue revenue_src_10006
