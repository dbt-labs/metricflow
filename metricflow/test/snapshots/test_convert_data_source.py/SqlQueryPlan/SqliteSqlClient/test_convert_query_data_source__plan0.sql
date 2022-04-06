-- Read Elements From Data Source 'revenue'
SELECT
  revenue_src_10005.revenue AS txn_revenue
  , revenue_src_10005.created_at AS ds
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__week
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__quarter
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__year
  , revenue_src_10005.user_id AS user
FROM (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_revenue
) revenue_src_10005
