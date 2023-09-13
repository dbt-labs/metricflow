-- Read Elements From Semantic Model 'users_latest'
SELECT
  users_latest_src_10008.ds AS ds__day
  , DATE_TRUNC(users_latest_src_10008.ds, isoweek) AS ds__week
  , DATE_TRUNC(users_latest_src_10008.ds, month) AS ds__month
  , DATE_TRUNC(users_latest_src_10008.ds, quarter) AS ds__quarter
  , DATE_TRUNC(users_latest_src_10008.ds, year) AS ds__year
  , EXTRACT(year FROM users_latest_src_10008.ds) AS ds__extract_year
  , EXTRACT(quarter FROM users_latest_src_10008.ds) AS ds__extract_quarter
  , EXTRACT(month FROM users_latest_src_10008.ds) AS ds__extract_month
  , EXTRACT(isoweek FROM users_latest_src_10008.ds) AS ds__extract_week
  , EXTRACT(day FROM users_latest_src_10008.ds) AS ds__extract_day
  , EXTRACT(dayofweek FROM users_latest_src_10008.ds) AS ds__extract_dow
  , EXTRACT(dayofyear FROM users_latest_src_10008.ds) AS ds__extract_doy
  , users_latest_src_10008.home_state_latest
  , users_latest_src_10008.ds AS user__ds__day
  , DATE_TRUNC(users_latest_src_10008.ds, isoweek) AS user__ds__week
  , DATE_TRUNC(users_latest_src_10008.ds, month) AS user__ds__month
  , DATE_TRUNC(users_latest_src_10008.ds, quarter) AS user__ds__quarter
  , DATE_TRUNC(users_latest_src_10008.ds, year) AS user__ds__year
  , EXTRACT(year FROM users_latest_src_10008.ds) AS user__ds__extract_year
  , EXTRACT(quarter FROM users_latest_src_10008.ds) AS user__ds__extract_quarter
  , EXTRACT(month FROM users_latest_src_10008.ds) AS user__ds__extract_month
  , EXTRACT(isoweek FROM users_latest_src_10008.ds) AS user__ds__extract_week
  , EXTRACT(day FROM users_latest_src_10008.ds) AS user__ds__extract_day
  , EXTRACT(dayofweek FROM users_latest_src_10008.ds) AS user__ds__extract_dow
  , EXTRACT(dayofyear FROM users_latest_src_10008.ds) AS user__ds__extract_doy
  , users_latest_src_10008.home_state_latest AS user__home_state_latest
  , users_latest_src_10008.user_id AS user
FROM ***************************.dim_users_latest users_latest_src_10008
