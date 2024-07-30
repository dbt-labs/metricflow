-- Pass Only Elements: ['user__bio_added_ts__second',]
SELECT
  subq_0.user__bio_added_ts__second
FROM (
  -- Read Elements From Semantic Model 'users_ds_source'
  SELECT
    1 AS new_users
    , DATE_TRUNC('day', users_ds_source_src_28000.ds) AS ds__day
    , DATE_TRUNC('week', users_ds_source_src_28000.ds) AS ds__week
    , DATE_TRUNC('month', users_ds_source_src_28000.ds) AS ds__month
    , DATE_TRUNC('quarter', users_ds_source_src_28000.ds) AS ds__quarter
    , DATE_TRUNC('year', users_ds_source_src_28000.ds) AS ds__year
    , EXTRACT(year FROM users_ds_source_src_28000.ds) AS ds__extract_year
    , EXTRACT(quarter FROM users_ds_source_src_28000.ds) AS ds__extract_quarter
    , EXTRACT(month FROM users_ds_source_src_28000.ds) AS ds__extract_month
    , EXTRACT(day FROM users_ds_source_src_28000.ds) AS ds__extract_day
    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.ds) END AS ds__extract_dow
    , EXTRACT(doy FROM users_ds_source_src_28000.ds) AS ds__extract_doy
    , DATE_TRUNC('day', users_ds_source_src_28000.created_at) AS created_at__day
    , DATE_TRUNC('week', users_ds_source_src_28000.created_at) AS created_at__week
    , DATE_TRUNC('month', users_ds_source_src_28000.created_at) AS created_at__month
    , DATE_TRUNC('quarter', users_ds_source_src_28000.created_at) AS created_at__quarter
    , DATE_TRUNC('year', users_ds_source_src_28000.created_at) AS created_at__year
    , EXTRACT(year FROM users_ds_source_src_28000.created_at) AS created_at__extract_year
    , EXTRACT(quarter FROM users_ds_source_src_28000.created_at) AS created_at__extract_quarter
    , EXTRACT(month FROM users_ds_source_src_28000.created_at) AS created_at__extract_month
    , EXTRACT(day FROM users_ds_source_src_28000.created_at) AS created_at__extract_day
    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.created_at) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.created_at) END AS created_at__extract_dow
    , EXTRACT(doy FROM users_ds_source_src_28000.created_at) AS created_at__extract_doy
    , DATE_TRUNC('day', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__day
    , DATE_TRUNC('week', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__week
    , DATE_TRUNC('month', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__month
    , DATE_TRUNC('quarter', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
    , DATE_TRUNC('year', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__year
    , EXTRACT(year FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
    , EXTRACT(quarter FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
    , EXTRACT(month FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
    , EXTRACT(day FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.ds_partitioned) END AS ds_partitioned__extract_dow
    , EXTRACT(doy FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
    , users_ds_source_src_28000.home_state
    , DATE_TRUNC('millisecond', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__millisecond
    , DATE_TRUNC('second', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__second
    , DATE_TRUNC('minute', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__minute
    , DATE_TRUNC('hour', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__hour
    , DATE_TRUNC('day', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__day
    , DATE_TRUNC('week', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__week
    , DATE_TRUNC('month', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__month
    , DATE_TRUNC('quarter', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__quarter
    , DATE_TRUNC('year', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__year
    , EXTRACT(year FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_year
    , EXTRACT(quarter FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_quarter
    , EXTRACT(month FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_month
    , EXTRACT(day FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_day
    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.last_profile_edit_ts) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.last_profile_edit_ts) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.last_profile_edit_ts) END AS last_profile_edit_ts__extract_dow
    , EXTRACT(doy FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_doy
    , DATE_TRUNC('second', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__second
    , DATE_TRUNC('minute', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__minute
    , DATE_TRUNC('hour', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__hour
    , DATE_TRUNC('day', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__day
    , DATE_TRUNC('week', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__week
    , DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__month
    , DATE_TRUNC('quarter', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__quarter
    , DATE_TRUNC('year', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__year
    , EXTRACT(year FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_year
    , EXTRACT(quarter FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_quarter
    , EXTRACT(month FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_month
    , EXTRACT(day FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_day
    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.bio_added_ts) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.bio_added_ts) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.bio_added_ts) END AS bio_added_ts__extract_dow
    , EXTRACT(doy FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_doy
    , DATE_TRUNC('minute', users_ds_source_src_28000.last_login_ts) AS last_login_ts__minute
    , DATE_TRUNC('hour', users_ds_source_src_28000.last_login_ts) AS last_login_ts__hour
    , DATE_TRUNC('day', users_ds_source_src_28000.last_login_ts) AS last_login_ts__day
    , DATE_TRUNC('week', users_ds_source_src_28000.last_login_ts) AS last_login_ts__week
    , DATE_TRUNC('month', users_ds_source_src_28000.last_login_ts) AS last_login_ts__month
    , DATE_TRUNC('quarter', users_ds_source_src_28000.last_login_ts) AS last_login_ts__quarter
    , DATE_TRUNC('year', users_ds_source_src_28000.last_login_ts) AS last_login_ts__year
    , EXTRACT(year FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_year
    , EXTRACT(quarter FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_quarter
    , EXTRACT(month FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_month
    , EXTRACT(day FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_day
    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.last_login_ts) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.last_login_ts) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.last_login_ts) END AS last_login_ts__extract_dow
    , EXTRACT(doy FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_doy
    , DATE_TRUNC('hour', users_ds_source_src_28000.archived_at) AS archived_at__hour
    , DATE_TRUNC('day', users_ds_source_src_28000.archived_at) AS archived_at__day
    , DATE_TRUNC('week', users_ds_source_src_28000.archived_at) AS archived_at__week
    , DATE_TRUNC('month', users_ds_source_src_28000.archived_at) AS archived_at__month
    , DATE_TRUNC('quarter', users_ds_source_src_28000.archived_at) AS archived_at__quarter
    , DATE_TRUNC('year', users_ds_source_src_28000.archived_at) AS archived_at__year
    , EXTRACT(year FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_year
    , EXTRACT(quarter FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_quarter
    , EXTRACT(month FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_month
    , EXTRACT(day FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_day
    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.archived_at) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.archived_at) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.archived_at) END AS archived_at__extract_dow
    , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_doy
    , DATE_TRUNC('day', users_ds_source_src_28000.ds) AS user__ds__day
    , DATE_TRUNC('week', users_ds_source_src_28000.ds) AS user__ds__week
    , DATE_TRUNC('month', users_ds_source_src_28000.ds) AS user__ds__month
    , DATE_TRUNC('quarter', users_ds_source_src_28000.ds) AS user__ds__quarter
    , DATE_TRUNC('year', users_ds_source_src_28000.ds) AS user__ds__year
    , EXTRACT(year FROM users_ds_source_src_28000.ds) AS user__ds__extract_year
    , EXTRACT(quarter FROM users_ds_source_src_28000.ds) AS user__ds__extract_quarter
    , EXTRACT(month FROM users_ds_source_src_28000.ds) AS user__ds__extract_month
    , EXTRACT(day FROM users_ds_source_src_28000.ds) AS user__ds__extract_day
    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.ds) END AS user__ds__extract_dow
    , EXTRACT(doy FROM users_ds_source_src_28000.ds) AS user__ds__extract_doy
    , DATE_TRUNC('day', users_ds_source_src_28000.created_at) AS user__created_at__day
    , DATE_TRUNC('week', users_ds_source_src_28000.created_at) AS user__created_at__week
    , DATE_TRUNC('month', users_ds_source_src_28000.created_at) AS user__created_at__month
    , DATE_TRUNC('quarter', users_ds_source_src_28000.created_at) AS user__created_at__quarter
    , DATE_TRUNC('year', users_ds_source_src_28000.created_at) AS user__created_at__year
    , EXTRACT(year FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_year
    , EXTRACT(quarter FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_quarter
    , EXTRACT(month FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_month
    , EXTRACT(day FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_day
    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.created_at) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.created_at) END AS user__created_at__extract_dow
    , EXTRACT(doy FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_doy
    , DATE_TRUNC('day', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__day
    , DATE_TRUNC('week', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__week
    , DATE_TRUNC('month', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__month
    , DATE_TRUNC('quarter', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__quarter
    , DATE_TRUNC('year', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__year
    , EXTRACT(year FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_year
    , EXTRACT(quarter FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_quarter
    , EXTRACT(month FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_month
    , EXTRACT(day FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_day
    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.ds_partitioned) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.ds_partitioned) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.ds_partitioned) END AS user__ds_partitioned__extract_dow
    , EXTRACT(doy FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_doy
    , users_ds_source_src_28000.home_state AS user__home_state
    , DATE_TRUNC('millisecond', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__millisecond
    , DATE_TRUNC('second', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__second
    , DATE_TRUNC('minute', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__minute
    , DATE_TRUNC('hour', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__hour
    , DATE_TRUNC('day', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__day
    , DATE_TRUNC('week', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__week
    , DATE_TRUNC('month', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__month
    , DATE_TRUNC('quarter', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__quarter
    , DATE_TRUNC('year', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__year
    , EXTRACT(year FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_year
    , EXTRACT(quarter FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_quarter
    , EXTRACT(month FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_month
    , EXTRACT(day FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_day
    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.last_profile_edit_ts) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.last_profile_edit_ts) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.last_profile_edit_ts) END AS user__last_profile_edit_ts__extract_dow
    , EXTRACT(doy FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_doy
    , DATE_TRUNC('second', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__second
    , DATE_TRUNC('minute', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__minute
    , DATE_TRUNC('hour', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__hour
    , DATE_TRUNC('day', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__day
    , DATE_TRUNC('week', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__week
    , DATE_TRUNC('month', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__month
    , DATE_TRUNC('quarter', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__quarter
    , DATE_TRUNC('year', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__year
    , EXTRACT(year FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_year
    , EXTRACT(quarter FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_quarter
    , EXTRACT(month FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_month
    , EXTRACT(day FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_day
    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.bio_added_ts) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.bio_added_ts) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.bio_added_ts) END AS user__bio_added_ts__extract_dow
    , EXTRACT(doy FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_doy
    , DATE_TRUNC('minute', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__minute
    , DATE_TRUNC('hour', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__hour
    , DATE_TRUNC('day', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__day
    , DATE_TRUNC('week', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__week
    , DATE_TRUNC('month', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__month
    , DATE_TRUNC('quarter', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__quarter
    , DATE_TRUNC('year', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__year
    , EXTRACT(year FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_year
    , EXTRACT(quarter FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_quarter
    , EXTRACT(month FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_month
    , EXTRACT(day FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_day
    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.last_login_ts) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.last_login_ts) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.last_login_ts) END AS user__last_login_ts__extract_dow
    , EXTRACT(doy FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_doy
    , DATE_TRUNC('hour', users_ds_source_src_28000.archived_at) AS user__archived_at__hour
    , DATE_TRUNC('day', users_ds_source_src_28000.archived_at) AS user__archived_at__day
    , DATE_TRUNC('week', users_ds_source_src_28000.archived_at) AS user__archived_at__week
    , DATE_TRUNC('month', users_ds_source_src_28000.archived_at) AS user__archived_at__month
    , DATE_TRUNC('quarter', users_ds_source_src_28000.archived_at) AS user__archived_at__quarter
    , DATE_TRUNC('year', users_ds_source_src_28000.archived_at) AS user__archived_at__year
    , EXTRACT(year FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_year
    , EXTRACT(quarter FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_quarter
    , EXTRACT(month FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_month
    , EXTRACT(day FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_day
    , CASE WHEN EXTRACT(dow FROM users_ds_source_src_28000.archived_at) = 0 THEN EXTRACT(dow FROM users_ds_source_src_28000.archived_at) + 7 ELSE EXTRACT(dow FROM users_ds_source_src_28000.archived_at) END AS user__archived_at__extract_dow
    , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_doy
    , users_ds_source_src_28000.user_id AS user
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_0
GROUP BY
  subq_0.user__bio_added_ts__second
