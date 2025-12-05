test_name: test_multiple_time_spines_in_query_for_join_to_time_spine_metric
test_filename: test_custom_granularity.py
sql_engine: Snowflake
---
-- Write to DataTable
SELECT
  subq_12.metric_time__alien_day
  , subq_12.metric_time__hour
  , subq_12.subdaily_join_to_time_spine_metric
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_11.metric_time__alien_day
    , subq_11.metric_time__hour
    , subq_11.__subdaily_join_to_time_spine_metric AS subdaily_join_to_time_spine_metric
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_10.metric_time__alien_day AS metric_time__alien_day
      , subq_10.metric_time__hour AS metric_time__hour
      , subq_5.__subdaily_join_to_time_spine_metric AS __subdaily_join_to_time_spine_metric
    FROM (
      -- Pass Only Elements: ['metric_time__alien_day', 'metric_time__hour']
      SELECT
        subq_9.metric_time__alien_day
        , subq_9.metric_time__hour
      FROM (
        -- Pass Only Elements: ['metric_time__alien_day', 'metric_time__hour']
        SELECT
          subq_8.metric_time__alien_day
          , subq_8.metric_time__hour
        FROM (
          -- Change Column Aliases
          -- Join to Custom Granularity Dataset
          SELECT
            subq_6.ts__hour AS metric_time__hour
            , subq_6.ts__day AS metric_time__day
            , subq_6.ts__week AS ts__week
            , subq_6.ts__month AS ts__month
            , subq_6.ts__quarter AS ts__quarter
            , subq_6.ts__year AS ts__year
            , subq_6.ts__extract_year AS ts__extract_year
            , subq_6.ts__extract_quarter AS ts__extract_quarter
            , subq_6.ts__extract_month AS ts__extract_month
            , subq_6.ts__extract_day AS ts__extract_day
            , subq_6.ts__extract_dow AS ts__extract_dow
            , subq_6.ts__extract_doy AS ts__extract_doy
            , subq_7.alien_day AS metric_time__alien_day
          FROM (
            -- Read From Time Spine 'mf_time_spine_hour'
            SELECT
              time_spine_src_28005.ts AS ts__hour
              , DATE_TRUNC('day', time_spine_src_28005.ts) AS ts__day
              , DATE_TRUNC('week', time_spine_src_28005.ts) AS ts__week
              , DATE_TRUNC('month', time_spine_src_28005.ts) AS ts__month
              , DATE_TRUNC('quarter', time_spine_src_28005.ts) AS ts__quarter
              , DATE_TRUNC('year', time_spine_src_28005.ts) AS ts__year
              , EXTRACT(year FROM time_spine_src_28005.ts) AS ts__extract_year
              , EXTRACT(quarter FROM time_spine_src_28005.ts) AS ts__extract_quarter
              , EXTRACT(month FROM time_spine_src_28005.ts) AS ts__extract_month
              , EXTRACT(day FROM time_spine_src_28005.ts) AS ts__extract_day
              , EXTRACT(dayofweekiso FROM time_spine_src_28005.ts) AS ts__extract_dow
              , EXTRACT(doy FROM time_spine_src_28005.ts) AS ts__extract_doy
            FROM ***************************.mf_time_spine_hour time_spine_src_28005
          ) subq_6
          LEFT OUTER JOIN
            ***************************.mf_time_spine subq_7
          ON
            subq_6.ts__day = subq_7.ds
        ) subq_8
      ) subq_9
    ) subq_10
    LEFT OUTER JOIN (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_4.metric_time__alien_day
        , subq_4.metric_time__hour
        , SUM(subq_4.__subdaily_join_to_time_spine_metric) AS __subdaily_join_to_time_spine_metric
      FROM (
        -- Pass Only Elements: ['__subdaily_join_to_time_spine_metric', 'metric_time__alien_day', 'metric_time__hour']
        SELECT
          subq_3.metric_time__alien_day
          , subq_3.metric_time__hour
          , subq_3.__subdaily_join_to_time_spine_metric
        FROM (
          -- Pass Only Elements: ['__subdaily_join_to_time_spine_metric', 'metric_time__alien_day', 'metric_time__hour']
          SELECT
            subq_2.metric_time__alien_day
            , subq_2.metric_time__hour
            , subq_2.__subdaily_join_to_time_spine_metric
          FROM (
            -- Metric Time Dimension 'archived_at'
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
              , subq_0.user AS user
              , subq_0.home_state AS home_state
              , subq_0.user__home_state AS user__home_state
              , subq_0.__subdaily_join_to_time_spine_metric AS __subdaily_join_to_time_spine_metric
              , subq_0.__simple_subdaily_metric_default_day AS __simple_subdaily_metric_default_day
              , subq_0.__simple_subdaily_metric_default_hour AS __simple_subdaily_metric_default_hour
              , subq_0.__archived_users_join_to_time_spine AS __archived_users_join_to_time_spine
              , subq_0.__archived_users AS __archived_users
              , subq_1.alien_day AS metric_time__alien_day
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
                , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.ds) AS ds__extract_dow
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
                , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.created_at) AS created_at__extract_dow
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
                , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
                , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_dow
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
                , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_dow
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
                , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_dow
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
                , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.archived_at) AS archived_at__extract_dow
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
                , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.ds) AS user__ds__extract_dow
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
                , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.created_at) AS user__created_at__extract_dow
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
                , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_dow
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
                , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_dow
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
                , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_dow
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
                , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_dow
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
                , EXTRACT(dayofweekiso FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_dow
                , EXTRACT(doy FROM users_ds_source_src_28000.archived_at) AS user__archived_at__extract_doy
                , users_ds_source_src_28000.user_id AS user
              FROM ***************************.dim_users users_ds_source_src_28000
            ) subq_0
            LEFT OUTER JOIN
              ***************************.mf_time_spine subq_1
            ON
              subq_0.archived_at__day = subq_1.ds
          ) subq_2
        ) subq_3
      ) subq_4
      GROUP BY
        subq_4.metric_time__alien_day
        , subq_4.metric_time__hour
    ) subq_5
    ON
      subq_10.metric_time__hour = subq_5.metric_time__hour
  ) subq_11
) subq_12
