-- Read Elements From Semantic Model 'revenue'
SELECT
  revenue_src_10006.revenue AS txn_revenue
  , revenue_src_10006.created_at AS end_of_day_revenue__ds
  , DATE_TRUNC('week', revenue_src_10006.created_at) AS end_of_day_revenue__ds__week
  , DATE_TRUNC('month', revenue_src_10006.created_at) AS end_of_day_revenue__ds__month
  , DATE_TRUNC('quarter', revenue_src_10006.created_at) AS end_of_day_revenue__ds__quarter
  , DATE_TRUNC('year', revenue_src_10006.created_at) AS end_of_day_revenue__ds__year
  , revenue_src_10006.user_id AS user
  , revenue_src_10006.user_id AS end_of_day_revenue__user
FROM ***************************.fct_revenue revenue_src_10006
