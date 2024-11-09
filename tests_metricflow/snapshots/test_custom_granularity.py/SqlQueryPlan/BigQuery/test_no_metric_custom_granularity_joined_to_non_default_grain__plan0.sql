test_name: test_no_metric_custom_granularity_joined_to_non_default_grain
test_filename: test_custom_granularity.py
sql_engine: BigQuery
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
      , DATETIME_TRUNC(users_ds_source_src_28000.ds, day) AS ds__day
      , DATETIME_TRUNC(users_ds_source_src_28000.ds, isoweek) AS ds__week
      , DATETIME_TRUNC(users_ds_source_src_28000.ds, month) AS ds__month
      , DATETIME_TRUNC(users_ds_source_src_28000.ds, quarter) AS ds__quarter
      , DATETIME_TRUNC(users_ds_source_src_28000.ds, year) AS ds__year
      , EXTRACT(year FROM users_ds_source_src_28000.ds) AS ds__extract_year
      , EXTRACT(quarter FROM users_ds_source_src_28000.ds) AS ds__extract_quarter
      , EXTRACT(month FROM users_ds_source_src_28000.ds) AS ds__extract_month
      , EXTRACT(day FROM users_ds_source_src_28000.ds) AS ds__extract_day
      , IF(EXTRACT(dayofweek FROM users_ds_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM users_ds_source_src_28000.ds) - 1) AS ds__extract_dow
      , EXTRACT(dayofyear FROM users_ds_source_src_28000.ds) AS ds__extract_doy
      , DATETIME_TRUNC(users_ds_source_src_28000.created_at, day) AS created_at__day
      , DATETIME_TRUNC(users_ds_source_src_28000.created_at, isoweek) AS created_at__week
      , DATETIME_TRUNC(users_ds_source_src_28000.created_at, month) AS created_at__month
      , DATETIME_TRUNC(users_ds_source_src_28000.created_at, quarter) AS created_at__quarter
      , DATETIME_TRUNC(users_ds_source_src_28000.created_at, year) AS created_at__year
      , EXTRACT(year FROM users_ds_source_src_28000.created_at) AS created_at__extract_year
      , EXTRACT(quarter FROM users_ds_source_src_28000.created_at) AS created_at__extract_quarter
      , EXTRACT(month FROM users_ds_source_src_28000.created_at) AS created_at__extract_month
      , EXTRACT(day FROM users_ds_source_src_28000.created_at) AS created_at__extract_day
      , IF(EXTRACT(dayofweek FROM users_ds_source_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM users_ds_source_src_28000.created_at) - 1) AS created_at__extract_dow
      , EXTRACT(dayofyear FROM users_ds_source_src_28000.created_at) AS created_at__extract_doy
      , DATETIME_TRUNC(users_ds_source_src_28000.ds_partitioned, day) AS ds_partitioned__day
      , DATETIME_TRUNC(users_ds_source_src_28000.ds_partitioned, isoweek) AS ds_partitioned__week
      , DATETIME_TRUNC(users_ds_source_src_28000.ds_partitioned, month) AS ds_partitioned__month
      , DATETIME_TRUNC(users_ds_source_src_28000.ds_partitioned, quarter) AS ds_partitioned__quarter
      , DATETIME_TRUNC(users_ds_source_src_28000.ds_partitioned, year) AS ds_partitioned__year
      , EXTRACT(year FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
      , EXTRACT(quarter FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
      , EXTRACT(month FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
      , EXTRACT(day FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
      , IF(EXTRACT(dayofweek FROM users_ds_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM users_ds_source_src_28000.ds_partitioned) - 1) AS ds_partitioned__extract_dow
      , EXTRACT(dayofyear FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
      , users_ds_source_src_28000.home_state
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, millisecond) AS last_profile_edit_ts__millisecond
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, second) AS last_profile_edit_ts__second
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, minute) AS last_profile_edit_ts__minute
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, hour) AS last_profile_edit_ts__hour
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, day) AS last_profile_edit_ts__day
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, isoweek) AS last_profile_edit_ts__week
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, month) AS last_profile_edit_ts__month
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, quarter) AS last_profile_edit_ts__quarter
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, year) AS last_profile_edit_ts__year
      , EXTRACT(year FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_year
      , EXTRACT(quarter FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_quarter
      , EXTRACT(month FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_month
      , EXTRACT(day FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_day
      , IF(EXTRACT(dayofweek FROM users_ds_source_src_28000.last_profile_edit_ts) = 1, 7, EXTRACT(dayofweek FROM users_ds_source_src_28000.last_profile_edit_ts) - 1) AS last_profile_edit_ts__extract_dow
      , EXTRACT(dayofyear FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_doy
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, second) AS bio_added_ts__second
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, minute) AS bio_added_ts__minute
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, hour) AS bio_added_ts__hour
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, day) AS bio_added_ts__day
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, isoweek) AS bio_added_ts__week
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, month) AS bio_added_ts__month
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, quarter) AS bio_added_ts__quarter
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, year) AS bio_added_ts__year
      , EXTRACT(year FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_year
      , EXTRACT(quarter FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_quarter
      , EXTRACT(month FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_month
      , EXTRACT(day FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_day
      , IF(EXTRACT(dayofweek FROM users_ds_source_src_28000.bio_added_ts) = 1, 7, EXTRACT(dayofweek FROM users_ds_source_src_28000.bio_added_ts) - 1) AS bio_added_ts__extract_dow
      , EXTRACT(dayofyear FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_doy
      , DATETIME_TRUNC(users_ds_source_src_28000.last_login_ts, minute) AS last_login_ts__minute
      , DATETIME_TRUNC(users_ds_source_src_28000.last_login_ts, hour) AS last_login_ts__hour
      , DATETIME_TRUNC(users_ds_source_src_28000.last_login_ts, day) AS last_login_ts__day
      , DATETIME_TRUNC(users_ds_source_src_28000.last_login_ts, isoweek) AS last_login_ts__week
      , DATETIME_TRUNC(users_ds_source_src_28000.last_login_ts, month) AS last_login_ts__month
      , DATETIME_TRUNC(users_ds_source_src_28000.last_login_ts, quarter) AS last_login_ts__quarter
      , DATETIME_TRUNC(users_ds_source_src_28000.last_login_ts, year) AS last_login_ts__year
      , EXTRACT(year FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_year
      , EXTRACT(quarter FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_quarter
      , EXTRACT(month FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_month
      , EXTRACT(day FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_day
      , IF(EXTRACT(dayofweek FROM users_ds_source_src_28000.last_login_ts) = 1, 7, EXTRACT(dayofweek FROM users_ds_source_src_28000.last_login_ts) - 1) AS last_login_ts__extract_dow
      , EXTRACT(dayofyear FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_doy
      , DATETIME_TRUNC(users_ds_source_src_28000.archived_at, hour) AS archived_at__hour
      , DATETIME_TRUNC(users_ds_source_src_28000.archived_at, day) AS archived_at__day
      , DATETIME_TRUNC(users_ds_source_src_28000.archived_at, isoweek) AS archived_at__week
      , DATETIME_TRUNC(users_ds_source_src_28000.archived_at, month) AS archived_at__month
      , DATETIME_TRUNC(users_ds_source_src_28000.archived_at, quarter) AS archived_at__quarter
      , DATETIME_TRUNC(users_ds_source_src_28000.archived_at, year) AS archived_at__year
      , EXTRACT(year FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_year
      , EXTRACT(quarter FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_quarter
      , EXTRACT(month FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_month
      , EXTRACT(day FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_day
      , IF(EXTRACT(dayofweek FROM users_ds_source_src_28000.archived_at) = 1, 7, EXTRACT(dayofweek FROM users_ds_source_src_28000.archived_at) - 1) AS archived_at__extract_dow
      , EXTRACT(dayofyear FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_doy
      , DATETIME_TRUNC(users_ds_source_src_28000.ds, day) AS user__ds__day
      , DATETIME_TRUNC(users_ds_source_src_28000.ds, isoweek) AS user__ds__week
      , DATETIME_TRUNC(users_ds_source_src_28000.ds, month) AS user__ds__month
      , DATETIME_TRUNC(users_ds_source_src_28000.ds, quarter) AS user__ds__quarter
      , DATETIME_TRUNC(users_ds_source_src_28000.ds, year) AS user__ds__year
      , EXTRACT(year FROM users_ds_source_src_28000.ds) AS user__ds__extract_year
      , EXTRACT(quarter FROM users_ds_source_src_28000.ds) AS user__ds__extract_quarter
      , EXTRACT(month FROM users_ds_source_src_28000.ds) AS user__ds__extract_month
      , EXTRACT(day FROM users_ds_source_src_28000.ds) AS user__ds__extract_day
      , IF(EXTRACT(dayofweek FROM users_ds_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM users_ds_source_src_28000.ds) - 1) AS user__ds__extract_dow
      , EXTRACT(dayofyear FROM users_ds_source_src_28000.ds) AS user__ds__extract_doy
      , DATETIME_TRUNC(users_ds_source_src_28000.created_at, day) AS user__created_at__day
      , DATETIME_TRUNC(users_ds_source_src_28000.created_at, isoweek) AS user__created_at__week
      , DATETIME_TRUNC(users_ds_source_src_28000.created_at, month) AS user__created_at__month
      , DATETIME_TRUNC(users_ds_source_src_28000.created_at, quarter) AS user__created_at__quarter
      , DATETIME_TRUNC(users_ds_source_src_28000.created_at, year) AS user__created_at__year
      , EXTRACT(year FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_year
      , EXTRACT(quarter FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_quarter
      , EXTRACT(month FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_month
      , EXTRACT(day FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_day
      , IF(EXTRACT(dayofweek FROM users_ds_source_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM users_ds_source_src_28000.created_at) - 1) AS user__created_at__extract_dow
      , EXTRACT(dayofyear FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_doy
      , DATETIME_TRUNC(users_ds_source_src_28000.ds_partitioned, day) AS user__ds_partitioned__day
      , DATETIME_TRUNC(users_ds_source_src_28000.ds_partitioned, isoweek) AS user__ds_partitioned__week
      , DATETIME_TRUNC(users_ds_source_src_28000.ds_partitioned, month) AS user__ds_partitioned__month
      , DATETIME_TRUNC(users_ds_source_src_28000.ds_partitioned, quarter) AS user__ds_partitioned__quarter
      , DATETIME_TRUNC(users_ds_source_src_28000.ds_partitioned, year) AS user__ds_partitioned__year
      , EXTRACT(year FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_year
      , EXTRACT(quarter FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_quarter
      , EXTRACT(month FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_month
      , EXTRACT(day FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_day
      , IF(EXTRACT(dayofweek FROM users_ds_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM users_ds_source_src_28000.ds_partitioned) - 1) AS user__ds_partitioned__extract_dow
      , EXTRACT(dayofyear FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_doy
      , users_ds_source_src_28000.home_state AS user__home_state
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, millisecond) AS user__last_profile_edit_ts__millisecond
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, second) AS user__last_profile_edit_ts__second
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, minute) AS user__last_profile_edit_ts__minute
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, hour) AS user__last_profile_edit_ts__hour
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, day) AS user__last_profile_edit_ts__day
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, isoweek) AS user__last_profile_edit_ts__week
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, month) AS user__last_profile_edit_ts__month
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, quarter) AS user__last_profile_edit_ts__quarter
      , DATETIME_TRUNC(users_ds_source_src_28000.last_profile_edit_ts, year) AS user__last_profile_edit_ts__year
      , EXTRACT(year FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_year
      , EXTRACT(quarter FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_quarter
      , EXTRACT(month FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_month
      , EXTRACT(day FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_day
      , IF(EXTRACT(dayofweek FROM users_ds_source_src_28000.last_profile_edit_ts) = 1, 7, EXTRACT(dayofweek FROM users_ds_source_src_28000.last_profile_edit_ts) - 1) AS user__last_profile_edit_ts__extract_dow
      , EXTRACT(dayofyear FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_doy
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, second) AS user__bio_added_ts__second
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, minute) AS user__bio_added_ts__minute
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, hour) AS user__bio_added_ts__hour
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, day) AS user__bio_added_ts__day
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, isoweek) AS user__bio_added_ts__week
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, month) AS user__bio_added_ts__month
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, quarter) AS user__bio_added_ts__quarter
      , DATETIME_TRUNC(users_ds_source_src_28000.bio_added_ts, year) AS user__bio_added_ts__year
      , EXTRACT(year FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_year
      , EXTRACT(quarter FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_quarter
      , EXTRACT(month FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_month
      , EXTRACT(day FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_day
      , IF(EXTRACT(dayofweek FROM users_ds_source_src_28000.bio_added_ts) = 1, 7, EXTRACT(dayofweek FROM users_ds_source_src_28000.bio_added_ts) - 1) AS user__bio_added_ts__extract_dow
      , EXTRACT(dayofyear FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_doy
      , DATETIME_TRUNC(users_ds_source_src_28000.last_login_ts, minute) AS user__last_login_ts__minute
      , DATETIME_TRUNC(users_ds_source_src_28000.last_login_ts, hour) AS user__last_login_ts__hour
      , DATETIME_TRUNC(users_ds_source_src_28000.last_login_ts, day) AS user__last_login_ts__day
      , DATETIME_TRUNC(users_ds_source_src_28000.last_login_ts, isoweek) AS user__last_login_ts__week
      , DATETIME_TRUNC(users_ds_source_src_28000.last_login_ts, month) AS user__last_login_ts__month
      , DATETIME_TRUNC(users_ds_source_src_28000.last_login_ts, quarter) AS user__last_login_ts__quarter
      , DATETIME_TRUNC(users_ds_source_src_28000.last_login_ts, year) AS user__last_login_ts__year
      , EXTRACT(year FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_year
      , EXTRACT(quarter FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_quarter
      , EXTRACT(month FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_month
      , EXTRACT(day FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_day
      , IF(EXTRACT(dayofweek FROM users_ds_source_src_28000.last_login_ts) = 1, 7, EXTRACT(dayofweek FROM users_ds_source_src_28000.last_login_ts) - 1) AS user__last_login_ts__extract_dow
      , EXTRACT(dayofyear FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_doy
      , DATETIME_TRUNC(users_ds_source_src_28000.archived_at, hour) AS user__archived_at__hour
      , DATETIME_TRUNC(users_ds_source_src_28000.archived_at, day) AS user__archived_at__day
      , DATETIME_TRUNC(users_ds_source_src_28000.archived_at, isoweek) AS user__archived_at__week
      , DATETIME_TRUNC(users_ds_source_src_28000.archived_at, month) AS user__archived_at__month
      , DATETIME_TRUNC(users_ds_source_src_28000.archived_at, quarter) AS user__archived_at__quarter
      , DATETIME_TRUNC(users_ds_source_src_28000.archived_at, year) AS user__archived_at__year
      , EXTRACT(year FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_year
      , EXTRACT(quarter FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_quarter
      , EXTRACT(month FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_month
      , EXTRACT(day FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_day
      , IF(EXTRACT(dayofweek FROM users_ds_source_src_28000.archived_at) = 1, 7, EXTRACT(dayofweek FROM users_ds_source_src_28000.archived_at) - 1) AS user__archived_at__extract_dow
      , EXTRACT(dayofyear FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_doy
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
        -- Time Spine
        SELECT
          DATETIME_TRUNC(time_spine_src_28006.ds, day) AS ds__day
          , DATETIME_TRUNC(time_spine_src_28006.ds, isoweek) AS ds__week
          , DATETIME_TRUNC(time_spine_src_28006.ds, month) AS ds__month
          , DATETIME_TRUNC(time_spine_src_28006.ds, quarter) AS ds__quarter
          , DATETIME_TRUNC(time_spine_src_28006.ds, year) AS ds__year
          , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
          , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
          , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
          , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
          , IF(EXTRACT(dayofweek FROM time_spine_src_28006.ds) = 1, 7, EXTRACT(dayofweek FROM time_spine_src_28006.ds) - 1) AS ds__extract_dow
          , EXTRACT(dayofyear FROM time_spine_src_28006.ds) AS ds__extract_doy
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
