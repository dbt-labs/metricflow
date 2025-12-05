test_name: test_sub_daily_dimension
test_filename: test_granularity_date_part_rendering.py
sql_engine: Trino
---
-- Write to DataTable
SELECT
  subq_2.user__bio_added_ts__second
FROM (
  -- Pass Only Elements: ['user__bio_added_ts__second']
  SELECT
    subq_1.user__bio_added_ts__second
  FROM (
    -- Pass Only Elements: ['user__bio_added_ts__second']
    SELECT
      subq_0.user__bio_added_ts__second
    FROM (
      -- Read Elements From Semantic Model 'users_ds_source'
      SELECT
        1 AS __subdaily_join_to_time_spine_metric
        , 1 AS __simple_subdaily_metric_default_day
        , 1 AS __simple_subdaily_metric_default_hour
        , 1 AS __archived_users_join_to_time_spine
        , 1 AS __archived_users
        , 1 AS __new_users
        , DATE_TRUNC('day', users_ds_source_src_28000.ds) AS ds__day
        , DATE_TRUNC('week', users_ds_source_src_28000.ds) AS ds__week
        , DATE_TRUNC('month', users_ds_source_src_28000.ds) AS ds__month
        , DATE_TRUNC('quarter', users_ds_source_src_28000.ds) AS ds__quarter
        , DATE_TRUNC('year', users_ds_source_src_28000.ds) AS ds__year
        , EXTRACT(year FROM users_ds_source_src_28000.ds) AS ds__extract_year
        , EXTRACT(quarter FROM users_ds_source_src_28000.ds) AS ds__extract_quarter
        , EXTRACT(month FROM users_ds_source_src_28000.ds) AS ds__extract_month
        , EXTRACT(day FROM users_ds_source_src_28000.ds) AS ds__extract_day
        , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.ds) AS ds__extract_dow
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
        , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.created_at) AS created_at__extract_dow
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
        , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
        , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_dow
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
        , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_dow
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
        , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_dow
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
        , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_dow
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
        , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.ds) AS user__ds__extract_dow
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
        , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_dow
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
        , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_dow
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
        , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_dow
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
        , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_dow
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
        , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_dow
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
        , EXTRACT(DAY_OF_WEEK FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_dow
        , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_doy
        , users_ds_source_src_28000.user_id AS user
      FROM ***************************.dim_users users_ds_source_src_28000
    ) subq_0
  ) subq_1
  GROUP BY
    subq_1.user__bio_added_ts__second
) subq_2
