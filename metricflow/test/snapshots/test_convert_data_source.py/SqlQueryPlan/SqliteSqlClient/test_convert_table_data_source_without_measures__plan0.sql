-- Read Elements From Data Source 'users_latest'
SELECT
  users_latest_src_10007.ds
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__week
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__month
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__quarter
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS ds__year
  , users_latest_src_10007.home_state_latest
  , users_latest_src_10007.ds AS user__ds
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS user__ds__week
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS user__ds__month
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS user__ds__quarter
  , '__DATE_TRUNC_NOT_SUPPORTED__' AS user__ds__year
  , users_latest_src_10007.home_state_latest AS user__home_state_latest
  , users_latest_src_10007.user_id AS user
FROM ***************************.dim_users_latest users_latest_src_10007
