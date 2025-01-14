test_name: test_subdaily_granularity_overrides_metric_default_granularity
test_filename: test_granularity_date_part_rendering.py
sql_engine: Clickhouse
---
-- Compute Metrics via Expressions
SELECT
  subq_7.metric_time__hour
  , subq_7.archived_users AS subdaily_join_to_time_spine_metric
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_6.metric_time__hour AS metric_time__hour
    , subq_3.archived_users AS archived_users
  FROM (
    -- Pass Only Elements: ['metric_time__hour',]
    SELECT
      subq_5.metric_time__hour
    FROM (
      -- Change Column Aliases
      SELECT
        subq_4.ts__hour AS metric_time__hour
        , subq_4.ts__day
        , subq_4.ts__week
        , subq_4.ts__month
        , subq_4.ts__quarter
        , subq_4.ts__year
        , subq_4.ts__extract_year
        , subq_4.ts__extract_quarter
        , subq_4.ts__extract_month
        , subq_4.ts__extract_day
        , subq_4.ts__extract_dow
        , subq_4.ts__extract_doy
      FROM (
        -- Read From Time Spine 'mf_time_spine_hour'
        SELECT
          time_spine_src_28005.ts AS ts__hour
          , date_trunc('day', time_spine_src_28005.ts) AS ts__day
          , date_trunc('week', time_spine_src_28005.ts) AS ts__week
          , date_trunc('month', time_spine_src_28005.ts) AS ts__month
          , date_trunc('quarter', time_spine_src_28005.ts) AS ts__quarter
          , date_trunc('year', time_spine_src_28005.ts) AS ts__year
          , toYear(time_spine_src_28005.ts) AS ts__extract_year
          , toQuarter(time_spine_src_28005.ts) AS ts__extract_quarter
          , toMonth(time_spine_src_28005.ts) AS ts__extract_month
          , toDayOfMonth(time_spine_src_28005.ts) AS ts__extract_day
          , toDayOfWeek(time_spine_src_28005.ts) AS ts__extract_dow
          , toDayOfYear(time_spine_src_28005.ts) AS ts__extract_doy
        FROM ***************************.mf_time_spine_hour time_spine_src_28005
      ) subq_4
    ) subq_5
  ) subq_6
  LEFT OUTER JOIN (
    -- Aggregate Measures
    SELECT
      subq_2.metric_time__hour
      , SUM(subq_2.archived_users) AS archived_users
    FROM (
      -- Pass Only Elements: ['archived_users', 'metric_time__hour']
      SELECT
        subq_1.metric_time__hour
        , subq_1.archived_users
      FROM (
        -- Metric Time Dimension 'archived_at'
        SELECT
          subq_0.ds__day
          , subq_0.ds__week
          , subq_0.ds__month
          , subq_0.ds__quarter
          , subq_0.ds__year
          , subq_0.ds__extract_year
          , subq_0.ds__extract_quarter
          , subq_0.ds__extract_month
          , subq_0.ds__extract_day
          , subq_0.ds__extract_dow
          , subq_0.ds__extract_doy
          , subq_0.created_at__day
          , subq_0.created_at__week
          , subq_0.created_at__month
          , subq_0.created_at__quarter
          , subq_0.created_at__year
          , subq_0.created_at__extract_year
          , subq_0.created_at__extract_quarter
          , subq_0.created_at__extract_month
          , subq_0.created_at__extract_day
          , subq_0.created_at__extract_dow
          , subq_0.created_at__extract_doy
          , subq_0.ds_partitioned__day
          , subq_0.ds_partitioned__week
          , subq_0.ds_partitioned__month
          , subq_0.ds_partitioned__quarter
          , subq_0.ds_partitioned__year
          , subq_0.ds_partitioned__extract_year
          , subq_0.ds_partitioned__extract_quarter
          , subq_0.ds_partitioned__extract_month
          , subq_0.ds_partitioned__extract_day
          , subq_0.ds_partitioned__extract_dow
          , subq_0.ds_partitioned__extract_doy
          , subq_0.last_profile_edit_ts__millisecond
          , subq_0.last_profile_edit_ts__second
          , subq_0.last_profile_edit_ts__minute
          , subq_0.last_profile_edit_ts__hour
          , subq_0.last_profile_edit_ts__day
          , subq_0.last_profile_edit_ts__week
          , subq_0.last_profile_edit_ts__month
          , subq_0.last_profile_edit_ts__quarter
          , subq_0.last_profile_edit_ts__year
          , subq_0.last_profile_edit_ts__extract_year
          , subq_0.last_profile_edit_ts__extract_quarter
          , subq_0.last_profile_edit_ts__extract_month
          , subq_0.last_profile_edit_ts__extract_day
          , subq_0.last_profile_edit_ts__extract_dow
          , subq_0.last_profile_edit_ts__extract_doy
          , subq_0.bio_added_ts__second
          , subq_0.bio_added_ts__minute
          , subq_0.bio_added_ts__hour
          , subq_0.bio_added_ts__day
          , subq_0.bio_added_ts__week
          , subq_0.bio_added_ts__month
          , subq_0.bio_added_ts__quarter
          , subq_0.bio_added_ts__year
          , subq_0.bio_added_ts__extract_year
          , subq_0.bio_added_ts__extract_quarter
          , subq_0.bio_added_ts__extract_month
          , subq_0.bio_added_ts__extract_day
          , subq_0.bio_added_ts__extract_dow
          , subq_0.bio_added_ts__extract_doy
          , subq_0.last_login_ts__minute
          , subq_0.last_login_ts__hour
          , subq_0.last_login_ts__day
          , subq_0.last_login_ts__week
          , subq_0.last_login_ts__month
          , subq_0.last_login_ts__quarter
          , subq_0.last_login_ts__year
          , subq_0.last_login_ts__extract_year
          , subq_0.last_login_ts__extract_quarter
          , subq_0.last_login_ts__extract_month
          , subq_0.last_login_ts__extract_day
          , subq_0.last_login_ts__extract_dow
          , subq_0.last_login_ts__extract_doy
          , subq_0.archived_at__hour
          , subq_0.archived_at__day
          , subq_0.archived_at__week
          , subq_0.archived_at__month
          , subq_0.archived_at__quarter
          , subq_0.archived_at__year
          , subq_0.archived_at__extract_year
          , subq_0.archived_at__extract_quarter
          , subq_0.archived_at__extract_month
          , subq_0.archived_at__extract_day
          , subq_0.archived_at__extract_dow
          , subq_0.archived_at__extract_doy
          , subq_0.user__ds__day
          , subq_0.user__ds__week
          , subq_0.user__ds__month
          , subq_0.user__ds__quarter
          , subq_0.user__ds__year
          , subq_0.user__ds__extract_year
          , subq_0.user__ds__extract_quarter
          , subq_0.user__ds__extract_month
          , subq_0.user__ds__extract_day
          , subq_0.user__ds__extract_dow
          , subq_0.user__ds__extract_doy
          , subq_0.user__created_at__day
          , subq_0.user__created_at__week
          , subq_0.user__created_at__month
          , subq_0.user__created_at__quarter
          , subq_0.user__created_at__year
          , subq_0.user__created_at__extract_year
          , subq_0.user__created_at__extract_quarter
          , subq_0.user__created_at__extract_month
          , subq_0.user__created_at__extract_day
          , subq_0.user__created_at__extract_dow
          , subq_0.user__created_at__extract_doy
          , subq_0.user__ds_partitioned__day
          , subq_0.user__ds_partitioned__week
          , subq_0.user__ds_partitioned__month
          , subq_0.user__ds_partitioned__quarter
          , subq_0.user__ds_partitioned__year
          , subq_0.user__ds_partitioned__extract_year
          , subq_0.user__ds_partitioned__extract_quarter
          , subq_0.user__ds_partitioned__extract_month
          , subq_0.user__ds_partitioned__extract_day
          , subq_0.user__ds_partitioned__extract_dow
          , subq_0.user__ds_partitioned__extract_doy
          , subq_0.user__last_profile_edit_ts__millisecond
          , subq_0.user__last_profile_edit_ts__second
          , subq_0.user__last_profile_edit_ts__minute
          , subq_0.user__last_profile_edit_ts__hour
          , subq_0.user__last_profile_edit_ts__day
          , subq_0.user__last_profile_edit_ts__week
          , subq_0.user__last_profile_edit_ts__month
          , subq_0.user__last_profile_edit_ts__quarter
          , subq_0.user__last_profile_edit_ts__year
          , subq_0.user__last_profile_edit_ts__extract_year
          , subq_0.user__last_profile_edit_ts__extract_quarter
          , subq_0.user__last_profile_edit_ts__extract_month
          , subq_0.user__last_profile_edit_ts__extract_day
          , subq_0.user__last_profile_edit_ts__extract_dow
          , subq_0.user__last_profile_edit_ts__extract_doy
          , subq_0.user__bio_added_ts__second
          , subq_0.user__bio_added_ts__minute
          , subq_0.user__bio_added_ts__hour
          , subq_0.user__bio_added_ts__day
          , subq_0.user__bio_added_ts__week
          , subq_0.user__bio_added_ts__month
          , subq_0.user__bio_added_ts__quarter
          , subq_0.user__bio_added_ts__year
          , subq_0.user__bio_added_ts__extract_year
          , subq_0.user__bio_added_ts__extract_quarter
          , subq_0.user__bio_added_ts__extract_month
          , subq_0.user__bio_added_ts__extract_day
          , subq_0.user__bio_added_ts__extract_dow
          , subq_0.user__bio_added_ts__extract_doy
          , subq_0.user__last_login_ts__minute
          , subq_0.user__last_login_ts__hour
          , subq_0.user__last_login_ts__day
          , subq_0.user__last_login_ts__week
          , subq_0.user__last_login_ts__month
          , subq_0.user__last_login_ts__quarter
          , subq_0.user__last_login_ts__year
          , subq_0.user__last_login_ts__extract_year
          , subq_0.user__last_login_ts__extract_quarter
          , subq_0.user__last_login_ts__extract_month
          , subq_0.user__last_login_ts__extract_day
          , subq_0.user__last_login_ts__extract_dow
          , subq_0.user__last_login_ts__extract_doy
          , subq_0.user__archived_at__hour
          , subq_0.user__archived_at__day
          , subq_0.user__archived_at__week
          , subq_0.user__archived_at__month
          , subq_0.user__archived_at__quarter
          , subq_0.user__archived_at__year
          , subq_0.user__archived_at__extract_year
          , subq_0.user__archived_at__extract_quarter
          , subq_0.user__archived_at__extract_month
          , subq_0.user__archived_at__extract_day
          , subq_0.user__archived_at__extract_dow
          , subq_0.user__archived_at__extract_doy
          , subq_0.archived_at__hour AS metric_time__hour
          , subq_0.archived_at__day AS metric_time__day
          , subq_0.archived_at__week AS metric_time__week
          , subq_0.archived_at__month AS metric_time__month
          , subq_0.archived_at__quarter AS metric_time__quarter
          , subq_0.archived_at__year AS metric_time__year
          , subq_0.archived_at__extract_year AS metric_time__extract_year
          , subq_0.archived_at__extract_quarter AS metric_time__extract_quarter
          , subq_0.archived_at__extract_month AS metric_time__extract_month
          , subq_0.archived_at__extract_day AS metric_time__extract_day
          , subq_0.archived_at__extract_dow AS metric_time__extract_dow
          , subq_0.archived_at__extract_doy AS metric_time__extract_doy
          , subq_0.user
          , subq_0.home_state
          , subq_0.user__home_state
          , subq_0.archived_users
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
      ) subq_1
    ) subq_2
    GROUP BY
      metric_time__hour
  ) subq_3
  ON
    subq_6.metric_time__hour = subq_3.metric_time__hour
) subq_7
