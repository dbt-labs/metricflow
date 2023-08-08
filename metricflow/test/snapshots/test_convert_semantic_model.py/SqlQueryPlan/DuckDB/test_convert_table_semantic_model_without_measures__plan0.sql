-- Read Elements From Semantic Model 'users_latest'
SELECT
  users_latest_src_10008.ds AS ds_latest__day
  , DATE_TRUNC('week', users_latest_src_10008.ds) AS ds_latest__week
  , DATE_TRUNC('month', users_latest_src_10008.ds) AS ds_latest__month
  , DATE_TRUNC('quarter', users_latest_src_10008.ds) AS ds_latest__quarter
  , DATE_TRUNC('year', users_latest_src_10008.ds) AS ds_latest__year
  , users_latest_src_10008.home_state_latest
  , users_latest_src_10008.ds AS user__ds_latest__day
  , DATE_TRUNC('week', users_latest_src_10008.ds) AS user__ds_latest__week
  , DATE_TRUNC('month', users_latest_src_10008.ds) AS user__ds_latest__month
  , DATE_TRUNC('quarter', users_latest_src_10008.ds) AS user__ds_latest__quarter
  , DATE_TRUNC('year', users_latest_src_10008.ds) AS user__ds_latest__year
  , users_latest_src_10008.home_state_latest AS user__home_state_latest
  , users_latest_src_10008.user_id AS user
FROM ***************************.dim_users_latest users_latest_src_10008
