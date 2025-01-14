test_name: test_convert_query_semantic_model
test_filename: test_convert_semantic_model.py
sql_engine: Clickhouse
---
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
