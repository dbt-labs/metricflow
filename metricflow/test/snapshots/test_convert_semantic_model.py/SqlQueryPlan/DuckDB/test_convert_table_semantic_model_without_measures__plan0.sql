-- Read Elements From Semantic Model 'users_latest'
SELECT
  DATE_TRUNC('day', users_latest_src_10008.ds) AS ds_latest__day
  , DATE_TRUNC('week', users_latest_src_10008.ds) AS ds_latest__week
  , DATE_TRUNC('month', users_latest_src_10008.ds) AS ds_latest__month
  , DATE_TRUNC('quarter', users_latest_src_10008.ds) AS ds_latest__quarter
  , DATE_TRUNC('year', users_latest_src_10008.ds) AS ds_latest__year
  , EXTRACT(year FROM users_latest_src_10008.ds) AS ds_latest__extract_year
  , EXTRACT(quarter FROM users_latest_src_10008.ds) AS ds_latest__extract_quarter
  , EXTRACT(month FROM users_latest_src_10008.ds) AS ds_latest__extract_month
  , EXTRACT(week FROM users_latest_src_10008.ds) AS ds_latest__extract_week
  , EXTRACT(day FROM users_latest_src_10008.ds) AS ds_latest__extract_day
  , EXTRACT(dow FROM users_latest_src_10008.ds) AS ds_latest__extract_dow
  , EXTRACT(doy FROM users_latest_src_10008.ds) AS ds_latest__extract_doy
  , users_latest_src_10008.home_state_latest
  , DATE_TRUNC('day', users_latest_src_10008.ds) AS user__ds_latest__day
  , DATE_TRUNC('week', users_latest_src_10008.ds) AS user__ds_latest__week
  , DATE_TRUNC('month', users_latest_src_10008.ds) AS user__ds_latest__month
  , DATE_TRUNC('quarter', users_latest_src_10008.ds) AS user__ds_latest__quarter
  , DATE_TRUNC('year', users_latest_src_10008.ds) AS user__ds_latest__year
  , EXTRACT(year FROM users_latest_src_10008.ds) AS user__ds_latest__extract_year
  , EXTRACT(quarter FROM users_latest_src_10008.ds) AS user__ds_latest__extract_quarter
  , EXTRACT(month FROM users_latest_src_10008.ds) AS user__ds_latest__extract_month
  , EXTRACT(week FROM users_latest_src_10008.ds) AS user__ds_latest__extract_week
  , EXTRACT(day FROM users_latest_src_10008.ds) AS user__ds_latest__extract_day
  , EXTRACT(dow FROM users_latest_src_10008.ds) AS user__ds_latest__extract_dow
  , EXTRACT(doy FROM users_latest_src_10008.ds) AS user__ds_latest__extract_doy
  , users_latest_src_10008.home_state_latest AS user__home_state_latest
  , users_latest_src_10008.user_id AS user
FROM ***************************.dim_users_latest users_latest_src_10008
