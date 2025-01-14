test_name: test_sub_daily_dimension
test_filename: test_granularity_date_part_rendering.py
sql_engine: Clickhouse
---
-- Pass Only Elements: ['user__bio_added_ts__second',]
SELECT
  subq_0.user__bio_added_ts__second
FROM (
  -- Read Elements From Semantic Model 'users_ds_source'
  SELECT
    1 AS new_users
    , 1 AS archived_users
    , date_trunc('day', users_ds_source_src_28000.ds) AS ds__day
    , date_trunc('week', users_ds_source_src_28000.ds) AS ds__week
    , date_trunc('month', users_ds_source_src_28000.ds) AS ds__month
    , date_trunc('quarter', users_ds_source_src_28000.ds) AS ds__quarter
    , date_trunc('year', users_ds_source_src_28000.ds) AS ds__year
    , toYear(users_ds_source_src_28000.ds) AS ds__extract_year
    , toQuarter(users_ds_source_src_28000.ds) AS ds__extract_quarter
    , toMonth(users_ds_source_src_28000.ds) AS ds__extract_month
    , toDayOfMonth(users_ds_source_src_28000.ds) AS ds__extract_day
    , toDayOfWeek(users_ds_source_src_28000.ds) AS ds__extract_dow
    , toDayOfYear(users_ds_source_src_28000.ds) AS ds__extract_doy
    , date_trunc('day', users_ds_source_src_28000.created_at) AS created_at__day
    , date_trunc('week', users_ds_source_src_28000.created_at) AS created_at__week
    , date_trunc('month', users_ds_source_src_28000.created_at) AS created_at__month
    , date_trunc('quarter', users_ds_source_src_28000.created_at) AS created_at__quarter
    , date_trunc('year', users_ds_source_src_28000.created_at) AS created_at__year
    , toYear(users_ds_source_src_28000.created_at) AS created_at__extract_year
    , toQuarter(users_ds_source_src_28000.created_at) AS created_at__extract_quarter
    , toMonth(users_ds_source_src_28000.created_at) AS created_at__extract_month
    , toDayOfMonth(users_ds_source_src_28000.created_at) AS created_at__extract_day
    , toDayOfWeek(users_ds_source_src_28000.created_at) AS created_at__extract_dow
    , toDayOfYear(users_ds_source_src_28000.created_at) AS created_at__extract_doy
    , date_trunc('day', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__day
    , date_trunc('week', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__week
    , date_trunc('month', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__month
    , date_trunc('quarter', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
    , date_trunc('year', users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__year
    , toYear(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
    , toQuarter(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
    , toMonth(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
    , toDayOfMonth(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
    , toDayOfWeek(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
    , toDayOfYear(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
    , users_ds_source_src_28000.home_state
    , date_trunc('milisecond', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__millisecond
    , date_trunc('second', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__second
    , date_trunc('minute', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__minute
    , date_trunc('hour', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__hour
    , date_trunc('day', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__day
    , date_trunc('week', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__week
    , date_trunc('month', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__month
    , date_trunc('quarter', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__quarter
    , date_trunc('year', users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__year
    , toYear(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_year
    , toQuarter(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_quarter
    , toMonth(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_month
    , toDayOfMonth(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_day
    , toDayOfWeek(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_dow
    , toDayOfYear(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_doy
    , date_trunc('second', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__second
    , date_trunc('minute', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__minute
    , date_trunc('hour', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__hour
    , date_trunc('day', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__day
    , date_trunc('week', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__week
    , date_trunc('month', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__month
    , date_trunc('quarter', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__quarter
    , date_trunc('year', users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__year
    , toYear(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_year
    , toQuarter(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_quarter
    , toMonth(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_month
    , toDayOfMonth(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_day
    , toDayOfWeek(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_dow
    , toDayOfYear(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_doy
    , date_trunc('minute', users_ds_source_src_28000.last_login_ts) AS last_login_ts__minute
    , date_trunc('hour', users_ds_source_src_28000.last_login_ts) AS last_login_ts__hour
    , date_trunc('day', users_ds_source_src_28000.last_login_ts) AS last_login_ts__day
    , date_trunc('week', users_ds_source_src_28000.last_login_ts) AS last_login_ts__week
    , date_trunc('month', users_ds_source_src_28000.last_login_ts) AS last_login_ts__month
    , date_trunc('quarter', users_ds_source_src_28000.last_login_ts) AS last_login_ts__quarter
    , date_trunc('year', users_ds_source_src_28000.last_login_ts) AS last_login_ts__year
    , toYear(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_year
    , toQuarter(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_quarter
    , toMonth(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_month
    , toDayOfMonth(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_day
    , toDayOfWeek(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_dow
    , toDayOfYear(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_doy
    , date_trunc('hour', users_ds_source_src_28000.archived_at) AS archived_at__hour
    , date_trunc('day', users_ds_source_src_28000.archived_at) AS archived_at__day
    , date_trunc('week', users_ds_source_src_28000.archived_at) AS archived_at__week
    , date_trunc('month', users_ds_source_src_28000.archived_at) AS archived_at__month
    , date_trunc('quarter', users_ds_source_src_28000.archived_at) AS archived_at__quarter
    , date_trunc('year', users_ds_source_src_28000.archived_at) AS archived_at__year
    , toYear(users_ds_source_src_28000.archived_at) AS archived_at__extract_year
    , toQuarter(users_ds_source_src_28000.archived_at) AS archived_at__extract_quarter
    , toMonth(users_ds_source_src_28000.archived_at) AS archived_at__extract_month
    , toDayOfMonth(users_ds_source_src_28000.archived_at) AS archived_at__extract_day
    , toDayOfWeek(users_ds_source_src_28000.archived_at) AS archived_at__extract_dow
    , toDayOfYear(users_ds_source_src_28000.archived_at) AS archived_at__extract_doy
    , date_trunc('day', users_ds_source_src_28000.ds) AS user__ds__day
    , date_trunc('week', users_ds_source_src_28000.ds) AS user__ds__week
    , date_trunc('month', users_ds_source_src_28000.ds) AS user__ds__month
    , date_trunc('quarter', users_ds_source_src_28000.ds) AS user__ds__quarter
    , date_trunc('year', users_ds_source_src_28000.ds) AS user__ds__year
    , toYear(users_ds_source_src_28000.ds) AS user__ds__extract_year
    , toQuarter(users_ds_source_src_28000.ds) AS user__ds__extract_quarter
    , toMonth(users_ds_source_src_28000.ds) AS user__ds__extract_month
    , toDayOfMonth(users_ds_source_src_28000.ds) AS user__ds__extract_day
    , toDayOfWeek(users_ds_source_src_28000.ds) AS user__ds__extract_dow
    , toDayOfYear(users_ds_source_src_28000.ds) AS user__ds__extract_doy
    , date_trunc('day', users_ds_source_src_28000.created_at) AS user__created_at__day
    , date_trunc('week', users_ds_source_src_28000.created_at) AS user__created_at__week
    , date_trunc('month', users_ds_source_src_28000.created_at) AS user__created_at__month
    , date_trunc('quarter', users_ds_source_src_28000.created_at) AS user__created_at__quarter
    , date_trunc('year', users_ds_source_src_28000.created_at) AS user__created_at__year
    , toYear(users_ds_source_src_28000.created_at) AS user__created_at__extract_year
    , toQuarter(users_ds_source_src_28000.created_at) AS user__created_at__extract_quarter
    , toMonth(users_ds_source_src_28000.created_at) AS user__created_at__extract_month
    , toDayOfMonth(users_ds_source_src_28000.created_at) AS user__created_at__extract_day
    , toDayOfWeek(users_ds_source_src_28000.created_at) AS user__created_at__extract_dow
    , toDayOfYear(users_ds_source_src_28000.created_at) AS user__created_at__extract_doy
    , date_trunc('day', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__day
    , date_trunc('week', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__week
    , date_trunc('month', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__month
    , date_trunc('quarter', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__quarter
    , date_trunc('year', users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__year
    , toYear(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_year
    , toQuarter(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_quarter
    , toMonth(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_month
    , toDayOfMonth(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_day
    , toDayOfWeek(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_dow
    , toDayOfYear(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_doy
    , users_ds_source_src_28000.home_state AS user__home_state
    , date_trunc('milisecond', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__millisecond
    , date_trunc('second', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__second
    , date_trunc('minute', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__minute
    , date_trunc('hour', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__hour
    , date_trunc('day', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__day
    , date_trunc('week', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__week
    , date_trunc('month', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__month
    , date_trunc('quarter', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__quarter
    , date_trunc('year', users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__year
    , toYear(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_year
    , toQuarter(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_quarter
    , toMonth(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_month
    , toDayOfMonth(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_day
    , toDayOfWeek(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_dow
    , toDayOfYear(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_doy
    , date_trunc('second', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__second
    , date_trunc('minute', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__minute
    , date_trunc('hour', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__hour
    , date_trunc('day', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__day
    , date_trunc('week', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__week
    , date_trunc('month', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__month
    , date_trunc('quarter', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__quarter
    , date_trunc('year', users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__year
    , toYear(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_year
    , toQuarter(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_quarter
    , toMonth(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_month
    , toDayOfMonth(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_day
    , toDayOfWeek(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_dow
    , toDayOfYear(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_doy
    , date_trunc('minute', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__minute
    , date_trunc('hour', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__hour
    , date_trunc('day', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__day
    , date_trunc('week', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__week
    , date_trunc('month', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__month
    , date_trunc('quarter', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__quarter
    , date_trunc('year', users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__year
    , toYear(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_year
    , toQuarter(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_quarter
    , toMonth(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_month
    , toDayOfMonth(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_day
    , toDayOfWeek(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_dow
    , toDayOfYear(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_doy
    , date_trunc('hour', users_ds_source_src_28000.archived_at) AS user__archived_at__hour
    , date_trunc('day', users_ds_source_src_28000.archived_at) AS user__archived_at__day
    , date_trunc('week', users_ds_source_src_28000.archived_at) AS user__archived_at__week
    , date_trunc('month', users_ds_source_src_28000.archived_at) AS user__archived_at__month
    , date_trunc('quarter', users_ds_source_src_28000.archived_at) AS user__archived_at__quarter
    , date_trunc('year', users_ds_source_src_28000.archived_at) AS user__archived_at__year
    , toYear(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_year
    , toQuarter(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_quarter
    , toMonth(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_month
    , toDayOfMonth(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_day
    , toDayOfWeek(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_dow
    , toDayOfYear(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_doy
    , users_ds_source_src_28000.user_id AS user
  FROM ***************************.dim_users users_ds_source_src_28000
) subq_0
GROUP BY
  user__bio_added_ts__second
