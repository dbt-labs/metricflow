test_name: test_subdaily_cumulative_grain_to_date_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: DuckDB
---
-- Write to DataTable
SELECT
  subq_9.metric_time__hour
  , subq_9.subdaily_cumulative_grain_to_date_metric
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_8.metric_time__hour
    , subq_8.simple_subdaily_metric_default_day AS subdaily_cumulative_grain_to_date_metric
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      subq_7.metric_time__hour
      , subq_7.__simple_subdaily_metric_default_day AS simple_subdaily_metric_default_day
    FROM (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_6.metric_time__hour
        , SUM(subq_6.__simple_subdaily_metric_default_day) AS __simple_subdaily_metric_default_day
      FROM (
        -- Pass Only Elements: ['__simple_subdaily_metric_default_day', 'metric_time__hour']
        SELECT
          subq_5.metric_time__hour
          , subq_5.__simple_subdaily_metric_default_day
        FROM (
          -- Pass Only Elements: ['__simple_subdaily_metric_default_day', 'metric_time__hour']
          SELECT
            subq_4.metric_time__hour
            , subq_4.__simple_subdaily_metric_default_day
          FROM (
            -- Join Self Over Time Range
            SELECT
              subq_2.metric_time__hour AS metric_time__hour
              , subq_1.ds__day AS ds__day
              , subq_1.ds__week AS ds__week
              , subq_1.ds__month AS ds__month
              , subq_1.ds__quarter AS ds__quarter
              , subq_1.ds__year AS ds__year
              , subq_1.ds__extract_year AS ds__extract_year
              , subq_1.ds__extract_quarter AS ds__extract_quarter
              , subq_1.ds__extract_month AS ds__extract_month
              , subq_1.ds__extract_day AS ds__extract_day
              , subq_1.ds__extract_dow AS ds__extract_dow
              , subq_1.ds__extract_doy AS ds__extract_doy
              , subq_1.created_at__day AS created_at__day
              , subq_1.created_at__week AS created_at__week
              , subq_1.created_at__month AS created_at__month
              , subq_1.created_at__quarter AS created_at__quarter
              , subq_1.created_at__year AS created_at__year
              , subq_1.created_at__extract_year AS created_at__extract_year
              , subq_1.created_at__extract_quarter AS created_at__extract_quarter
              , subq_1.created_at__extract_month AS created_at__extract_month
              , subq_1.created_at__extract_day AS created_at__extract_day
              , subq_1.created_at__extract_dow AS created_at__extract_dow
              , subq_1.created_at__extract_doy AS created_at__extract_doy
              , subq_1.ds_partitioned__day AS ds_partitioned__day
              , subq_1.ds_partitioned__week AS ds_partitioned__week
              , subq_1.ds_partitioned__month AS ds_partitioned__month
              , subq_1.ds_partitioned__quarter AS ds_partitioned__quarter
              , subq_1.ds_partitioned__year AS ds_partitioned__year
              , subq_1.ds_partitioned__extract_year AS ds_partitioned__extract_year
              , subq_1.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
              , subq_1.ds_partitioned__extract_month AS ds_partitioned__extract_month
              , subq_1.ds_partitioned__extract_day AS ds_partitioned__extract_day
              , subq_1.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
              , subq_1.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
              , subq_1.last_profile_edit_ts__millisecond AS last_profile_edit_ts__millisecond
              , subq_1.last_profile_edit_ts__second AS last_profile_edit_ts__second
              , subq_1.last_profile_edit_ts__minute AS last_profile_edit_ts__minute
              , subq_1.last_profile_edit_ts__hour AS last_profile_edit_ts__hour
              , subq_1.last_profile_edit_ts__day AS last_profile_edit_ts__day
              , subq_1.last_profile_edit_ts__week AS last_profile_edit_ts__week
              , subq_1.last_profile_edit_ts__month AS last_profile_edit_ts__month
              , subq_1.last_profile_edit_ts__quarter AS last_profile_edit_ts__quarter
              , subq_1.last_profile_edit_ts__year AS last_profile_edit_ts__year
              , subq_1.last_profile_edit_ts__extract_year AS last_profile_edit_ts__extract_year
              , subq_1.last_profile_edit_ts__extract_quarter AS last_profile_edit_ts__extract_quarter
              , subq_1.last_profile_edit_ts__extract_month AS last_profile_edit_ts__extract_month
              , subq_1.last_profile_edit_ts__extract_day AS last_profile_edit_ts__extract_day
              , subq_1.last_profile_edit_ts__extract_dow AS last_profile_edit_ts__extract_dow
              , subq_1.last_profile_edit_ts__extract_doy AS last_profile_edit_ts__extract_doy
              , subq_1.bio_added_ts__second AS bio_added_ts__second
              , subq_1.bio_added_ts__minute AS bio_added_ts__minute
              , subq_1.bio_added_ts__hour AS bio_added_ts__hour
              , subq_1.bio_added_ts__day AS bio_added_ts__day
              , subq_1.bio_added_ts__week AS bio_added_ts__week
              , subq_1.bio_added_ts__month AS bio_added_ts__month
              , subq_1.bio_added_ts__quarter AS bio_added_ts__quarter
              , subq_1.bio_added_ts__year AS bio_added_ts__year
              , subq_1.bio_added_ts__extract_year AS bio_added_ts__extract_year
              , subq_1.bio_added_ts__extract_quarter AS bio_added_ts__extract_quarter
              , subq_1.bio_added_ts__extract_month AS bio_added_ts__extract_month
              , subq_1.bio_added_ts__extract_day AS bio_added_ts__extract_day
              , subq_1.bio_added_ts__extract_dow AS bio_added_ts__extract_dow
              , subq_1.bio_added_ts__extract_doy AS bio_added_ts__extract_doy
              , subq_1.last_login_ts__minute AS last_login_ts__minute
              , subq_1.last_login_ts__hour AS last_login_ts__hour
              , subq_1.last_login_ts__day AS last_login_ts__day
              , subq_1.last_login_ts__week AS last_login_ts__week
              , subq_1.last_login_ts__month AS last_login_ts__month
              , subq_1.last_login_ts__quarter AS last_login_ts__quarter
              , subq_1.last_login_ts__year AS last_login_ts__year
              , subq_1.last_login_ts__extract_year AS last_login_ts__extract_year
              , subq_1.last_login_ts__extract_quarter AS last_login_ts__extract_quarter
              , subq_1.last_login_ts__extract_month AS last_login_ts__extract_month
              , subq_1.last_login_ts__extract_day AS last_login_ts__extract_day
              , subq_1.last_login_ts__extract_dow AS last_login_ts__extract_dow
              , subq_1.last_login_ts__extract_doy AS last_login_ts__extract_doy
              , subq_1.archived_at__hour AS archived_at__hour
              , subq_1.archived_at__day AS archived_at__day
              , subq_1.archived_at__week AS archived_at__week
              , subq_1.archived_at__month AS archived_at__month
              , subq_1.archived_at__quarter AS archived_at__quarter
              , subq_1.archived_at__year AS archived_at__year
              , subq_1.archived_at__extract_year AS archived_at__extract_year
              , subq_1.archived_at__extract_quarter AS archived_at__extract_quarter
              , subq_1.archived_at__extract_month AS archived_at__extract_month
              , subq_1.archived_at__extract_day AS archived_at__extract_day
              , subq_1.archived_at__extract_dow AS archived_at__extract_dow
              , subq_1.archived_at__extract_doy AS archived_at__extract_doy
              , subq_1.user__ds__day AS user__ds__day
              , subq_1.user__ds__week AS user__ds__week
              , subq_1.user__ds__month AS user__ds__month
              , subq_1.user__ds__quarter AS user__ds__quarter
              , subq_1.user__ds__year AS user__ds__year
              , subq_1.user__ds__extract_year AS user__ds__extract_year
              , subq_1.user__ds__extract_quarter AS user__ds__extract_quarter
              , subq_1.user__ds__extract_month AS user__ds__extract_month
              , subq_1.user__ds__extract_day AS user__ds__extract_day
              , subq_1.user__ds__extract_dow AS user__ds__extract_dow
              , subq_1.user__ds__extract_doy AS user__ds__extract_doy
              , subq_1.user__created_at__day AS user__created_at__day
              , subq_1.user__created_at__week AS user__created_at__week
              , subq_1.user__created_at__month AS user__created_at__month
              , subq_1.user__created_at__quarter AS user__created_at__quarter
              , subq_1.user__created_at__year AS user__created_at__year
              , subq_1.user__created_at__extract_year AS user__created_at__extract_year
              , subq_1.user__created_at__extract_quarter AS user__created_at__extract_quarter
              , subq_1.user__created_at__extract_month AS user__created_at__extract_month
              , subq_1.user__created_at__extract_day AS user__created_at__extract_day
              , subq_1.user__created_at__extract_dow AS user__created_at__extract_dow
              , subq_1.user__created_at__extract_doy AS user__created_at__extract_doy
              , subq_1.user__ds_partitioned__day AS user__ds_partitioned__day
              , subq_1.user__ds_partitioned__week AS user__ds_partitioned__week
              , subq_1.user__ds_partitioned__month AS user__ds_partitioned__month
              , subq_1.user__ds_partitioned__quarter AS user__ds_partitioned__quarter
              , subq_1.user__ds_partitioned__year AS user__ds_partitioned__year
              , subq_1.user__ds_partitioned__extract_year AS user__ds_partitioned__extract_year
              , subq_1.user__ds_partitioned__extract_quarter AS user__ds_partitioned__extract_quarter
              , subq_1.user__ds_partitioned__extract_month AS user__ds_partitioned__extract_month
              , subq_1.user__ds_partitioned__extract_day AS user__ds_partitioned__extract_day
              , subq_1.user__ds_partitioned__extract_dow AS user__ds_partitioned__extract_dow
              , subq_1.user__ds_partitioned__extract_doy AS user__ds_partitioned__extract_doy
              , subq_1.user__last_profile_edit_ts__millisecond AS user__last_profile_edit_ts__millisecond
              , subq_1.user__last_profile_edit_ts__second AS user__last_profile_edit_ts__second
              , subq_1.user__last_profile_edit_ts__minute AS user__last_profile_edit_ts__minute
              , subq_1.user__last_profile_edit_ts__hour AS user__last_profile_edit_ts__hour
              , subq_1.user__last_profile_edit_ts__day AS user__last_profile_edit_ts__day
              , subq_1.user__last_profile_edit_ts__week AS user__last_profile_edit_ts__week
              , subq_1.user__last_profile_edit_ts__month AS user__last_profile_edit_ts__month
              , subq_1.user__last_profile_edit_ts__quarter AS user__last_profile_edit_ts__quarter
              , subq_1.user__last_profile_edit_ts__year AS user__last_profile_edit_ts__year
              , subq_1.user__last_profile_edit_ts__extract_year AS user__last_profile_edit_ts__extract_year
              , subq_1.user__last_profile_edit_ts__extract_quarter AS user__last_profile_edit_ts__extract_quarter
              , subq_1.user__last_profile_edit_ts__extract_month AS user__last_profile_edit_ts__extract_month
              , subq_1.user__last_profile_edit_ts__extract_day AS user__last_profile_edit_ts__extract_day
              , subq_1.user__last_profile_edit_ts__extract_dow AS user__last_profile_edit_ts__extract_dow
              , subq_1.user__last_profile_edit_ts__extract_doy AS user__last_profile_edit_ts__extract_doy
              , subq_1.user__bio_added_ts__second AS user__bio_added_ts__second
              , subq_1.user__bio_added_ts__minute AS user__bio_added_ts__minute
              , subq_1.user__bio_added_ts__hour AS user__bio_added_ts__hour
              , subq_1.user__bio_added_ts__day AS user__bio_added_ts__day
              , subq_1.user__bio_added_ts__week AS user__bio_added_ts__week
              , subq_1.user__bio_added_ts__month AS user__bio_added_ts__month
              , subq_1.user__bio_added_ts__quarter AS user__bio_added_ts__quarter
              , subq_1.user__bio_added_ts__year AS user__bio_added_ts__year
              , subq_1.user__bio_added_ts__extract_year AS user__bio_added_ts__extract_year
              , subq_1.user__bio_added_ts__extract_quarter AS user__bio_added_ts__extract_quarter
              , subq_1.user__bio_added_ts__extract_month AS user__bio_added_ts__extract_month
              , subq_1.user__bio_added_ts__extract_day AS user__bio_added_ts__extract_day
              , subq_1.user__bio_added_ts__extract_dow AS user__bio_added_ts__extract_dow
              , subq_1.user__bio_added_ts__extract_doy AS user__bio_added_ts__extract_doy
              , subq_1.user__last_login_ts__minute AS user__last_login_ts__minute
              , subq_1.user__last_login_ts__hour AS user__last_login_ts__hour
              , subq_1.user__last_login_ts__day AS user__last_login_ts__day
              , subq_1.user__last_login_ts__week AS user__last_login_ts__week
              , subq_1.user__last_login_ts__month AS user__last_login_ts__month
              , subq_1.user__last_login_ts__quarter AS user__last_login_ts__quarter
              , subq_1.user__last_login_ts__year AS user__last_login_ts__year
              , subq_1.user__last_login_ts__extract_year AS user__last_login_ts__extract_year
              , subq_1.user__last_login_ts__extract_quarter AS user__last_login_ts__extract_quarter
              , subq_1.user__last_login_ts__extract_month AS user__last_login_ts__extract_month
              , subq_1.user__last_login_ts__extract_day AS user__last_login_ts__extract_day
              , subq_1.user__last_login_ts__extract_dow AS user__last_login_ts__extract_dow
              , subq_1.user__last_login_ts__extract_doy AS user__last_login_ts__extract_doy
              , subq_1.user__archived_at__hour AS user__archived_at__hour
              , subq_1.user__archived_at__day AS user__archived_at__day
              , subq_1.user__archived_at__week AS user__archived_at__week
              , subq_1.user__archived_at__month AS user__archived_at__month
              , subq_1.user__archived_at__quarter AS user__archived_at__quarter
              , subq_1.user__archived_at__year AS user__archived_at__year
              , subq_1.user__archived_at__extract_year AS user__archived_at__extract_year
              , subq_1.user__archived_at__extract_quarter AS user__archived_at__extract_quarter
              , subq_1.user__archived_at__extract_month AS user__archived_at__extract_month
              , subq_1.user__archived_at__extract_day AS user__archived_at__extract_day
              , subq_1.user__archived_at__extract_dow AS user__archived_at__extract_dow
              , subq_1.user__archived_at__extract_doy AS user__archived_at__extract_doy
              , subq_1.metric_time__day AS metric_time__day
              , subq_1.metric_time__week AS metric_time__week
              , subq_1.metric_time__month AS metric_time__month
              , subq_1.metric_time__quarter AS metric_time__quarter
              , subq_1.metric_time__year AS metric_time__year
              , subq_1.metric_time__extract_year AS metric_time__extract_year
              , subq_1.metric_time__extract_quarter AS metric_time__extract_quarter
              , subq_1.metric_time__extract_month AS metric_time__extract_month
              , subq_1.metric_time__extract_day AS metric_time__extract_day
              , subq_1.metric_time__extract_dow AS metric_time__extract_dow
              , subq_1.metric_time__extract_doy AS metric_time__extract_doy
              , subq_1.user AS user
              , subq_1.home_state AS home_state
              , subq_1.user__home_state AS user__home_state
              , subq_1.__subdaily_join_to_time_spine_metric AS __subdaily_join_to_time_spine_metric
              , subq_1.__simple_subdaily_metric_default_day AS __simple_subdaily_metric_default_day
              , subq_1.__simple_subdaily_metric_default_hour AS __simple_subdaily_metric_default_hour
              , subq_1.__archived_users_join_to_time_spine AS __archived_users_join_to_time_spine
              , subq_1.__archived_users AS __archived_users
            FROM (
              -- Read From Time Spine 'mf_time_spine_hour'
              SELECT
                subq_3.ts AS metric_time__hour
              FROM ***************************.mf_time_spine_hour subq_3
            ) subq_2
            INNER JOIN (
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
                , subq_0.__subdaily_join_to_time_spine_metric
                , subq_0.__simple_subdaily_metric_default_day
                , subq_0.__simple_subdaily_metric_default_hour
                , subq_0.__archived_users_join_to_time_spine
                , subq_0.__archived_users
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
                  , EXTRACT(isodow FROM users_ds_source_src_28000.ds) AS ds__extract_dow
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
                  , EXTRACT(isodow FROM users_ds_source_src_28000.created_at) AS created_at__extract_dow
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
                  , EXTRACT(isodow FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
                  , EXTRACT(isodow FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_dow
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
                  , EXTRACT(isodow FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_dow
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
                  , EXTRACT(isodow FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_dow
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
                  , EXTRACT(isodow FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_dow
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
                  , EXTRACT(isodow FROM users_ds_source_src_28000.ds) AS user__ds__extract_dow
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
                  , EXTRACT(isodow FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_dow
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
                  , EXTRACT(isodow FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_dow
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
                  , EXTRACT(isodow FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_dow
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
                  , EXTRACT(isodow FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_dow
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
                  , EXTRACT(isodow FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_dow
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
                  , EXTRACT(isodow FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_dow
                  , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_doy
                  , users_ds_source_src_28000.user_id AS user
                FROM ***************************.dim_users users_ds_source_src_28000
              ) subq_0
            ) subq_1
            ON
              (
                subq_1.metric_time__hour <= subq_2.metric_time__hour
              ) AND (
                subq_1.metric_time__hour >= DATE_TRUNC('hour', subq_2.metric_time__hour)
              )
          ) subq_4
        ) subq_5
      ) subq_6
      GROUP BY
        subq_6.metric_time__hour
    ) subq_7
  ) subq_8
) subq_9
