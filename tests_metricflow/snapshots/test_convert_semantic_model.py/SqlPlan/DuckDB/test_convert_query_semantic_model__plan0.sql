test_name: test_convert_query_semantic_model
test_filename: test_convert_semantic_model.py
sql_engine: DuckDB
---
-- Read Elements From Semantic Model 'revenue'
SELECT
  revenue_src_28000.revenue AS __revenue
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
