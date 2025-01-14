test_name: test_convert_table_semantic_model_without_measures
test_filename: test_convert_semantic_model.py
docstring:
  Simple test for converting a table semantic model. Since there are no measures, primary time is not checked.
sql_engine: Clickhouse
---
-- Read Elements From Semantic Model 'users_latest'
SELECT
  date_trunc('day', users_latest_src_28000.ds) AS ds_latest__day
  , date_trunc('week', users_latest_src_28000.ds) AS ds_latest__week
  , date_trunc('month', users_latest_src_28000.ds) AS ds_latest__month
  , date_trunc('quarter', users_latest_src_28000.ds) AS ds_latest__quarter
  , date_trunc('year', users_latest_src_28000.ds) AS ds_latest__year
  , toYear(users_latest_src_28000.ds) AS ds_latest__extract_year
  , toQuarter(users_latest_src_28000.ds) AS ds_latest__extract_quarter
  , toMonth(users_latest_src_28000.ds) AS ds_latest__extract_month
  , toDayOfMonth(users_latest_src_28000.ds) AS ds_latest__extract_day
  , toDayOfWeek(users_latest_src_28000.ds) AS ds_latest__extract_dow
  , toDayOfYear(users_latest_src_28000.ds) AS ds_latest__extract_doy
  , users_latest_src_28000.home_state_latest
  , date_trunc('day', users_latest_src_28000.ds) AS user__ds_latest__day
  , date_trunc('week', users_latest_src_28000.ds) AS user__ds_latest__week
  , date_trunc('month', users_latest_src_28000.ds) AS user__ds_latest__month
  , date_trunc('quarter', users_latest_src_28000.ds) AS user__ds_latest__quarter
  , date_trunc('year', users_latest_src_28000.ds) AS user__ds_latest__year
  , toYear(users_latest_src_28000.ds) AS user__ds_latest__extract_year
  , toQuarter(users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
  , toMonth(users_latest_src_28000.ds) AS user__ds_latest__extract_month
  , toDayOfMonth(users_latest_src_28000.ds) AS user__ds_latest__extract_day
  , toDayOfWeek(users_latest_src_28000.ds) AS user__ds_latest__extract_dow
  , toDayOfYear(users_latest_src_28000.ds) AS user__ds_latest__extract_doy
  , users_latest_src_28000.home_state_latest AS user__home_state_latest
  , users_latest_src_28000.user_id AS user
FROM ***************************.dim_users_latest users_latest_src_28000
