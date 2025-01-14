test_name: test_no_metric_custom_granularity_joined_to_non_default_grain
test_filename: test_custom_granularity.py
sql_engine: Clickhouse
---
-- Pass Only Elements: ['metric_time__day', 'metric_time__martian_day', 'user__bio_added_ts__martian_day', 'user__bio_added_ts__month']
SELECT
  subq_6.user__bio_added_ts__martian_day
  , subq_6.metric_time__martian_day
  , subq_6.user__bio_added_ts__month
  , subq_6.metric_time__day
FROM (
  -- Join Standard Outputs
  -- Join to Custom Granularity Dataset
  -- Join to Custom Granularity Dataset
  SELECT
    subq_0.ds__day AS ds__day
    , subq_0.ds__week AS ds__week
    , subq_0.ds__month AS ds__month
    , subq_0.ds__quarter AS ds__quarter
    , subq_0.ds__year AS ds__year
    , subq_0.ds__extract_year AS ds__extract_year
    , subq_0.ds__extract_quarter AS ds__extract_quarter
    , subq_0.ds__extract_month AS ds__extract_month
    , subq_0.ds__extract_day AS ds__extract_day
    , subq_0.ds__extract_dow AS ds__extract_dow
    , subq_0.ds__extract_doy AS ds__extract_doy
    , subq_0.created_at__day AS created_at__day
    , subq_0.created_at__week AS created_at__week
    , subq_0.created_at__month AS created_at__month
    , subq_0.created_at__quarter AS created_at__quarter
    , subq_0.created_at__year AS created_at__year
    , subq_0.created_at__extract_year AS created_at__extract_year
    , subq_0.created_at__extract_quarter AS created_at__extract_quarter
    , subq_0.created_at__extract_month AS created_at__extract_month
    , subq_0.created_at__extract_day AS created_at__extract_day
    , subq_0.created_at__extract_dow AS created_at__extract_dow
    , subq_0.created_at__extract_doy AS created_at__extract_doy
    , subq_0.ds_partitioned__day AS ds_partitioned__day
    , subq_0.ds_partitioned__week AS ds_partitioned__week
    , subq_0.ds_partitioned__month AS ds_partitioned__month
    , subq_0.ds_partitioned__quarter AS ds_partitioned__quarter
    , subq_0.ds_partitioned__year AS ds_partitioned__year
    , subq_0.ds_partitioned__extract_year AS ds_partitioned__extract_year
    , subq_0.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
    , subq_0.ds_partitioned__extract_month AS ds_partitioned__extract_month
    , subq_0.ds_partitioned__extract_day AS ds_partitioned__extract_day
    , subq_0.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
    , subq_0.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
    , subq_0.last_profile_edit_ts__millisecond AS last_profile_edit_ts__millisecond
    , subq_0.last_profile_edit_ts__second AS last_profile_edit_ts__second
    , subq_0.last_profile_edit_ts__minute AS last_profile_edit_ts__minute
    , subq_0.last_profile_edit_ts__hour AS last_profile_edit_ts__hour
    , subq_0.last_profile_edit_ts__day AS last_profile_edit_ts__day
    , subq_0.last_profile_edit_ts__week AS last_profile_edit_ts__week
    , subq_0.last_profile_edit_ts__month AS last_profile_edit_ts__month
    , subq_0.last_profile_edit_ts__quarter AS last_profile_edit_ts__quarter
    , subq_0.last_profile_edit_ts__year AS last_profile_edit_ts__year
    , subq_0.last_profile_edit_ts__extract_year AS last_profile_edit_ts__extract_year
    , subq_0.last_profile_edit_ts__extract_quarter AS last_profile_edit_ts__extract_quarter
    , subq_0.last_profile_edit_ts__extract_month AS last_profile_edit_ts__extract_month
    , subq_0.last_profile_edit_ts__extract_day AS last_profile_edit_ts__extract_day
    , subq_0.last_profile_edit_ts__extract_dow AS last_profile_edit_ts__extract_dow
    , subq_0.last_profile_edit_ts__extract_doy AS last_profile_edit_ts__extract_doy
    , subq_0.bio_added_ts__second AS bio_added_ts__second
    , subq_0.bio_added_ts__minute AS bio_added_ts__minute
    , subq_0.bio_added_ts__hour AS bio_added_ts__hour
    , subq_0.bio_added_ts__day AS bio_added_ts__day
    , subq_0.bio_added_ts__week AS bio_added_ts__week
    , subq_0.bio_added_ts__month AS bio_added_ts__month
    , subq_0.bio_added_ts__quarter AS bio_added_ts__quarter
    , subq_0.bio_added_ts__year AS bio_added_ts__year
    , subq_0.bio_added_ts__extract_year AS bio_added_ts__extract_year
    , subq_0.bio_added_ts__extract_quarter AS bio_added_ts__extract_quarter
    , subq_0.bio_added_ts__extract_month AS bio_added_ts__extract_month
    , subq_0.bio_added_ts__extract_day AS bio_added_ts__extract_day
    , subq_0.bio_added_ts__extract_dow AS bio_added_ts__extract_dow
    , subq_0.bio_added_ts__extract_doy AS bio_added_ts__extract_doy
    , subq_0.last_login_ts__minute AS last_login_ts__minute
    , subq_0.last_login_ts__hour AS last_login_ts__hour
    , subq_0.last_login_ts__day AS last_login_ts__day
    , subq_0.last_login_ts__week AS last_login_ts__week
    , subq_0.last_login_ts__month AS last_login_ts__month
    , subq_0.last_login_ts__quarter AS last_login_ts__quarter
    , subq_0.last_login_ts__year AS last_login_ts__year
    , subq_0.last_login_ts__extract_year AS last_login_ts__extract_year
    , subq_0.last_login_ts__extract_quarter AS last_login_ts__extract_quarter
    , subq_0.last_login_ts__extract_month AS last_login_ts__extract_month
    , subq_0.last_login_ts__extract_day AS last_login_ts__extract_day
    , subq_0.last_login_ts__extract_dow AS last_login_ts__extract_dow
    , subq_0.last_login_ts__extract_doy AS last_login_ts__extract_doy
    , subq_0.archived_at__hour AS archived_at__hour
    , subq_0.archived_at__day AS archived_at__day
    , subq_0.archived_at__week AS archived_at__week
    , subq_0.archived_at__month AS archived_at__month
    , subq_0.archived_at__quarter AS archived_at__quarter
    , subq_0.archived_at__year AS archived_at__year
    , subq_0.archived_at__extract_year AS archived_at__extract_year
    , subq_0.archived_at__extract_quarter AS archived_at__extract_quarter
    , subq_0.archived_at__extract_month AS archived_at__extract_month
    , subq_0.archived_at__extract_day AS archived_at__extract_day
    , subq_0.archived_at__extract_dow AS archived_at__extract_dow
    , subq_0.archived_at__extract_doy AS archived_at__extract_doy
    , subq_0.user__ds__day AS user__ds__day
    , subq_0.user__ds__week AS user__ds__week
    , subq_0.user__ds__month AS user__ds__month
    , subq_0.user__ds__quarter AS user__ds__quarter
    , subq_0.user__ds__year AS user__ds__year
    , subq_0.user__ds__extract_year AS user__ds__extract_year
    , subq_0.user__ds__extract_quarter AS user__ds__extract_quarter
    , subq_0.user__ds__extract_month AS user__ds__extract_month
    , subq_0.user__ds__extract_day AS user__ds__extract_day
    , subq_0.user__ds__extract_dow AS user__ds__extract_dow
    , subq_0.user__ds__extract_doy AS user__ds__extract_doy
    , subq_0.user__created_at__day AS user__created_at__day
    , subq_0.user__created_at__week AS user__created_at__week
    , subq_0.user__created_at__month AS user__created_at__month
    , subq_0.user__created_at__quarter AS user__created_at__quarter
    , subq_0.user__created_at__year AS user__created_at__year
    , subq_0.user__created_at__extract_year AS user__created_at__extract_year
    , subq_0.user__created_at__extract_quarter AS user__created_at__extract_quarter
    , subq_0.user__created_at__extract_month AS user__created_at__extract_month
    , subq_0.user__created_at__extract_day AS user__created_at__extract_day
    , subq_0.user__created_at__extract_dow AS user__created_at__extract_dow
    , subq_0.user__created_at__extract_doy AS user__created_at__extract_doy
    , subq_0.user__ds_partitioned__day AS user__ds_partitioned__day
    , subq_0.user__ds_partitioned__week AS user__ds_partitioned__week
    , subq_0.user__ds_partitioned__month AS user__ds_partitioned__month
    , subq_0.user__ds_partitioned__quarter AS user__ds_partitioned__quarter
    , subq_0.user__ds_partitioned__year AS user__ds_partitioned__year
    , subq_0.user__ds_partitioned__extract_year AS user__ds_partitioned__extract_year
    , subq_0.user__ds_partitioned__extract_quarter AS user__ds_partitioned__extract_quarter
    , subq_0.user__ds_partitioned__extract_month AS user__ds_partitioned__extract_month
    , subq_0.user__ds_partitioned__extract_day AS user__ds_partitioned__extract_day
    , subq_0.user__ds_partitioned__extract_dow AS user__ds_partitioned__extract_dow
    , subq_0.user__ds_partitioned__extract_doy AS user__ds_partitioned__extract_doy
    , subq_0.user__last_profile_edit_ts__millisecond AS user__last_profile_edit_ts__millisecond
    , subq_0.user__last_profile_edit_ts__second AS user__last_profile_edit_ts__second
    , subq_0.user__last_profile_edit_ts__minute AS user__last_profile_edit_ts__minute
    , subq_0.user__last_profile_edit_ts__hour AS user__last_profile_edit_ts__hour
    , subq_0.user__last_profile_edit_ts__day AS user__last_profile_edit_ts__day
    , subq_0.user__last_profile_edit_ts__week AS user__last_profile_edit_ts__week
    , subq_0.user__last_profile_edit_ts__month AS user__last_profile_edit_ts__month
    , subq_0.user__last_profile_edit_ts__quarter AS user__last_profile_edit_ts__quarter
    , subq_0.user__last_profile_edit_ts__year AS user__last_profile_edit_ts__year
    , subq_0.user__last_profile_edit_ts__extract_year AS user__last_profile_edit_ts__extract_year
    , subq_0.user__last_profile_edit_ts__extract_quarter AS user__last_profile_edit_ts__extract_quarter
    , subq_0.user__last_profile_edit_ts__extract_month AS user__last_profile_edit_ts__extract_month
    , subq_0.user__last_profile_edit_ts__extract_day AS user__last_profile_edit_ts__extract_day
    , subq_0.user__last_profile_edit_ts__extract_dow AS user__last_profile_edit_ts__extract_dow
    , subq_0.user__last_profile_edit_ts__extract_doy AS user__last_profile_edit_ts__extract_doy
    , subq_0.user__bio_added_ts__second AS user__bio_added_ts__second
    , subq_0.user__bio_added_ts__minute AS user__bio_added_ts__minute
    , subq_0.user__bio_added_ts__hour AS user__bio_added_ts__hour
    , subq_0.user__bio_added_ts__day AS user__bio_added_ts__day
    , subq_0.user__bio_added_ts__week AS user__bio_added_ts__week
    , subq_0.user__bio_added_ts__month AS user__bio_added_ts__month
    , subq_0.user__bio_added_ts__quarter AS user__bio_added_ts__quarter
    , subq_0.user__bio_added_ts__year AS user__bio_added_ts__year
    , subq_0.user__bio_added_ts__extract_year AS user__bio_added_ts__extract_year
    , subq_0.user__bio_added_ts__extract_quarter AS user__bio_added_ts__extract_quarter
    , subq_0.user__bio_added_ts__extract_month AS user__bio_added_ts__extract_month
    , subq_0.user__bio_added_ts__extract_day AS user__bio_added_ts__extract_day
    , subq_0.user__bio_added_ts__extract_dow AS user__bio_added_ts__extract_dow
    , subq_0.user__bio_added_ts__extract_doy AS user__bio_added_ts__extract_doy
    , subq_0.user__last_login_ts__minute AS user__last_login_ts__minute
    , subq_0.user__last_login_ts__hour AS user__last_login_ts__hour
    , subq_0.user__last_login_ts__day AS user__last_login_ts__day
    , subq_0.user__last_login_ts__week AS user__last_login_ts__week
    , subq_0.user__last_login_ts__month AS user__last_login_ts__month
    , subq_0.user__last_login_ts__quarter AS user__last_login_ts__quarter
    , subq_0.user__last_login_ts__year AS user__last_login_ts__year
    , subq_0.user__last_login_ts__extract_year AS user__last_login_ts__extract_year
    , subq_0.user__last_login_ts__extract_quarter AS user__last_login_ts__extract_quarter
    , subq_0.user__last_login_ts__extract_month AS user__last_login_ts__extract_month
    , subq_0.user__last_login_ts__extract_day AS user__last_login_ts__extract_day
    , subq_0.user__last_login_ts__extract_dow AS user__last_login_ts__extract_dow
    , subq_0.user__last_login_ts__extract_doy AS user__last_login_ts__extract_doy
    , subq_0.user__archived_at__hour AS user__archived_at__hour
    , subq_0.user__archived_at__day AS user__archived_at__day
    , subq_0.user__archived_at__week AS user__archived_at__week
    , subq_0.user__archived_at__month AS user__archived_at__month
    , subq_0.user__archived_at__quarter AS user__archived_at__quarter
    , subq_0.user__archived_at__year AS user__archived_at__year
    , subq_0.user__archived_at__extract_year AS user__archived_at__extract_year
    , subq_0.user__archived_at__extract_quarter AS user__archived_at__extract_quarter
    , subq_0.user__archived_at__extract_month AS user__archived_at__extract_month
    , subq_0.user__archived_at__extract_day AS user__archived_at__extract_day
    , subq_0.user__archived_at__extract_dow AS user__archived_at__extract_dow
    , subq_0.user__archived_at__extract_doy AS user__archived_at__extract_doy
    , subq_3.metric_time__day AS metric_time__day
    , subq_0.user AS user
    , subq_0.home_state AS home_state
    , subq_0.user__home_state AS user__home_state
    , subq_0.new_users AS new_users
    , subq_0.archived_users AS archived_users
    , subq_4.martian_day AS metric_time__martian_day
    , subq_5.martian_day AS user__bio_added_ts__martian_day
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
  CROSS JOIN (
    -- Pass Only Elements: ['metric_time__day',]
    SELECT
      subq_2.metric_time__day
    FROM (
      -- Metric Time Dimension 'ds'
      SELECT
        subq_1.ds__day
        , subq_1.ds__week
        , subq_1.ds__month
        , subq_1.ds__quarter
        , subq_1.ds__year
        , subq_1.ds__extract_year
        , subq_1.ds__extract_quarter
        , subq_1.ds__extract_month
        , subq_1.ds__extract_day
        , subq_1.ds__extract_dow
        , subq_1.ds__extract_doy
        , subq_1.ds__martian_day
        , subq_1.ds__day AS metric_time__day
        , subq_1.ds__week AS metric_time__week
        , subq_1.ds__month AS metric_time__month
        , subq_1.ds__quarter AS metric_time__quarter
        , subq_1.ds__year AS metric_time__year
        , subq_1.ds__extract_year AS metric_time__extract_year
        , subq_1.ds__extract_quarter AS metric_time__extract_quarter
        , subq_1.ds__extract_month AS metric_time__extract_month
        , subq_1.ds__extract_day AS metric_time__extract_day
        , subq_1.ds__extract_dow AS metric_time__extract_dow
        , subq_1.ds__extract_doy AS metric_time__extract_doy
        , subq_1.ds__martian_day AS metric_time__martian_day
      FROM (
        -- Read From Time Spine 'mf_time_spine'
        SELECT
          time_spine_src_28006.ds AS ds__day
          , date_trunc('week', time_spine_src_28006.ds) AS ds__week
          , date_trunc('month', time_spine_src_28006.ds) AS ds__month
          , date_trunc('quarter', time_spine_src_28006.ds) AS ds__quarter
          , date_trunc('year', time_spine_src_28006.ds) AS ds__year
          , toYear(time_spine_src_28006.ds) AS ds__extract_year
          , toQuarter(time_spine_src_28006.ds) AS ds__extract_quarter
          , toMonth(time_spine_src_28006.ds) AS ds__extract_month
          , toDayOfMonth(time_spine_src_28006.ds) AS ds__extract_day
          , toDayOfWeek(time_spine_src_28006.ds) AS ds__extract_dow
          , toDayOfYear(time_spine_src_28006.ds) AS ds__extract_doy
          , time_spine_src_28006.martian_day AS ds__martian_day
        FROM ***************************.mf_time_spine time_spine_src_28006
      ) subq_1
    ) subq_2
  ) subq_3
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_4
  ON
    subq_3.metric_time__day = subq_4.ds
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_5
  ON
    subq_0.user__bio_added_ts__day = subq_5.ds
) subq_6
GROUP BY
  user__bio_added_ts__martian_day
  , metric_time__martian_day
  , user__bio_added_ts__month
  , metric_time__day
