test_name: test_convert_query_semantic_model
test_filename: test_convert_semantic_model.py
sql_engine: BigQuery
---
-- Read Elements From Semantic Model 'revenue'
SELECT
  revenue_src_28000.revenue
  , DATETIME_TRUNC(revenue_src_28000.created_at, day) AS ds__day
  , DATETIME_TRUNC(revenue_src_28000.created_at, isoweek) AS ds__week
  , DATETIME_TRUNC(revenue_src_28000.created_at, month) AS ds__month
  , DATETIME_TRUNC(revenue_src_28000.created_at, quarter) AS ds__quarter
  , DATETIME_TRUNC(revenue_src_28000.created_at, year) AS ds__year
  , EXTRACT(year FROM revenue_src_28000.created_at) AS ds__extract_year
  , EXTRACT(quarter FROM revenue_src_28000.created_at) AS ds__extract_quarter
  , EXTRACT(month FROM revenue_src_28000.created_at) AS ds__extract_month
  , EXTRACT(day FROM revenue_src_28000.created_at) AS ds__extract_day
  , IF(EXTRACT(dayofweek FROM revenue_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM revenue_src_28000.created_at) - 1) AS ds__extract_dow
  , EXTRACT(dayofyear FROM revenue_src_28000.created_at) AS ds__extract_doy
  , DATETIME_TRUNC(revenue_src_28000.created_at, day) AS revenue_instance__ds__day
  , DATETIME_TRUNC(revenue_src_28000.created_at, isoweek) AS revenue_instance__ds__week
  , DATETIME_TRUNC(revenue_src_28000.created_at, month) AS revenue_instance__ds__month
  , DATETIME_TRUNC(revenue_src_28000.created_at, quarter) AS revenue_instance__ds__quarter
  , DATETIME_TRUNC(revenue_src_28000.created_at, year) AS revenue_instance__ds__year
  , EXTRACT(year FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_year
  , EXTRACT(quarter FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_quarter
  , EXTRACT(month FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_month
  , EXTRACT(day FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_day
  , IF(EXTRACT(dayofweek FROM revenue_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM revenue_src_28000.created_at) - 1) AS revenue_instance__ds__extract_dow
  , EXTRACT(dayofyear FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_doy
  , revenue_src_28000.user_id AS user
  , revenue_src_28000.user_id AS revenue_instance__user
FROM ***************************.fct_revenue revenue_src_28000
