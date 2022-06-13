-- Read Elements From Data Source 'users_latest'
SELECT
  users_latest_src_10007.ds
  , DATE_TRUNC('week', users_latest_src_10007.ds) AS ds__week
  , DATE_TRUNC('month', users_latest_src_10007.ds) AS ds__month
  , DATE_TRUNC('quarter', users_latest_src_10007.ds) AS ds__quarter
  , DATE_TRUNC('year', users_latest_src_10007.ds) AS ds__year
  , users_latest_src_10007.home_state_latest
  , users_latest_src_10007.ds AS user__ds
  , DATE_TRUNC('week', users_latest_src_10007.ds) AS user__ds__week
  , DATE_TRUNC('month', users_latest_src_10007.ds) AS user__ds__month
  , DATE_TRUNC('quarter', users_latest_src_10007.ds) AS user__ds__quarter
  , DATE_TRUNC('year', users_latest_src_10007.ds) AS user__ds__year
  , users_latest_src_10007.home_state_latest AS user__home_state_latest
  , users_latest_src_10007.user_id AS user
FROM ***************************.dim_users_latest users_latest_src_10007
