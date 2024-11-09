test_name: test_convert_table_semantic_model_without_measures
test_filename: test_convert_semantic_model.py
docstring:
  Simple test for converting a table semantic model. Since there are no measures, primary time is not checked.
---
-- Read Elements From Semantic Model 'users_latest'
SELECT
  DATETIME_TRUNC(users_latest_src_28000.ds, day) AS ds_latest__day
  , DATETIME_TRUNC(users_latest_src_28000.ds, isoweek) AS ds_latest__week
  , DATETIME_TRUNC(users_latest_src_28000.ds, month) AS ds_latest__month
  , DATETIME_TRUNC(users_latest_src_28000.ds, quarter) AS ds_latest__quarter
  , DATETIME_TRUNC(users_latest_src_28000.ds, year) AS ds_latest__year
  , EXTRACT(year FROM users_latest_src_28000.ds) AS ds_latest__extract_year
  , EXTRACT(quarter FROM users_latest_src_28000.ds) AS ds_latest__extract_quarter
  , EXTRACT(month FROM users_latest_src_28000.ds) AS ds_latest__extract_month
  , EXTRACT(day FROM users_latest_src_28000.ds) AS ds_latest__extract_day
  , IF(EXTRACT(dayofweek FROM users_latest_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM users_latest_src_28000.ds) - 1) AS ds_latest__extract_dow
  , EXTRACT(dayofyear FROM users_latest_src_28000.ds) AS ds_latest__extract_doy
  , users_latest_src_28000.home_state_latest
  , DATETIME_TRUNC(users_latest_src_28000.ds, day) AS user__ds_latest__day
  , DATETIME_TRUNC(users_latest_src_28000.ds, isoweek) AS user__ds_latest__week
  , DATETIME_TRUNC(users_latest_src_28000.ds, month) AS user__ds_latest__month
  , DATETIME_TRUNC(users_latest_src_28000.ds, quarter) AS user__ds_latest__quarter
  , DATETIME_TRUNC(users_latest_src_28000.ds, year) AS user__ds_latest__year
  , EXTRACT(year FROM users_latest_src_28000.ds) AS user__ds_latest__extract_year
  , EXTRACT(quarter FROM users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
  , EXTRACT(month FROM users_latest_src_28000.ds) AS user__ds_latest__extract_month
  , EXTRACT(day FROM users_latest_src_28000.ds) AS user__ds_latest__extract_day
  , IF(EXTRACT(dayofweek FROM users_latest_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM users_latest_src_28000.ds) - 1) AS user__ds_latest__extract_dow
  , EXTRACT(dayofyear FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
  , users_latest_src_28000.home_state_latest AS user__home_state_latest
  , users_latest_src_28000.user_id AS user
FROM ***************************.dim_users_latest users_latest_src_28000
