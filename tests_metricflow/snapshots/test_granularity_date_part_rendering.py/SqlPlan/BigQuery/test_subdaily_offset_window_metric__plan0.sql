test_name: test_subdaily_offset_window_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: BigQuery
---
-- Write to DataTable
SELECT
  subq_9.metric_time__hour
  , subq_9.subdaily_offset_window_metric
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_8.metric_time__hour
    , archived_users AS subdaily_offset_window_metric
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_7.metric_time__hour
      , subq_7.archived_users
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_6.metric_time__hour AS metric_time__hour
        , subq_3.archived_users AS archived_users
      FROM (
        -- Pass Only Elements: ['metric_time__hour']
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
              , DATETIME_TRUNC(time_spine_src_28005.ts, day) AS ts__day
              , DATETIME_TRUNC(time_spine_src_28005.ts, isoweek) AS ts__week
              , DATETIME_TRUNC(time_spine_src_28005.ts, month) AS ts__month
              , DATETIME_TRUNC(time_spine_src_28005.ts, quarter) AS ts__quarter
              , DATETIME_TRUNC(time_spine_src_28005.ts, year) AS ts__year
              , EXTRACT(year FROM time_spine_src_28005.ts) AS ts__extract_year
              , EXTRACT(quarter FROM time_spine_src_28005.ts) AS ts__extract_quarter
              , EXTRACT(month FROM time_spine_src_28005.ts) AS ts__extract_month
              , EXTRACT(day FROM time_spine_src_28005.ts) AS ts__extract_day
              , IF(EXTRACT(dayofweek FROM time_spine_src_28005.ts) = 1, 7, EXTRACT(dayofweek FROM time_spine_src_28005.ts) - 1) AS ts__extract_dow
              , EXTRACT(dayofyear FROM time_spine_src_28005.ts) AS ts__extract_doy
            FROM ***************************.mf_time_spine_hour time_spine_src_28005
          ) subq_4
        ) subq_5
      ) subq_6
      INNER JOIN (
        -- Aggregate Inputs for Simple Metrics
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
              , subq_0.subdaily_join_to_time_spine_metric
              , subq_0.simple_subdaily_metric_default_day
              , subq_0.simple_subdaily_metric_default_hour
              , subq_0.archived_users_join_to_time_spine
              , subq_0.archived_users
            FROM (
              -- Read Elements From Semantic Model 'users_ds_source'
              SELECT
                1 AS subdaily_join_to_time_spine_metric
                , 1 AS simple_subdaily_metric_default_day
                , 1 AS simple_subdaily_metric_default_hour
                , 1 AS archived_users_join_to_time_spine
                , 1 AS archived_users
                , 1 AS new_users
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
          ) subq_1
        ) subq_2
        GROUP BY
          metric_time__hour
      ) subq_3
      ON
        DATE_SUB(CAST(subq_6.metric_time__hour AS DATETIME), INTERVAL 1 hour) = subq_3.metric_time__hour
    ) subq_7
  ) subq_8
) subq_9
