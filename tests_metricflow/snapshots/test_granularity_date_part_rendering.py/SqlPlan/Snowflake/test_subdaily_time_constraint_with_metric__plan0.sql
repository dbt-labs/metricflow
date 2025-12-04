test_name: test_subdaily_time_constraint_with_metric
test_filename: test_granularity_date_part_rendering.py
sql_engine: Snowflake
---
-- Write to DataTable
SELECT
  subq_15.metric_time__hour
  , subq_15.subdaily_join_to_time_spine_metric
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_14.metric_time__hour
    , subq_14.__subdaily_join_to_time_spine_metric AS subdaily_join_to_time_spine_metric
  FROM (
    -- Constrain Time Range to [2020-01-01T02:00:00, 2020-01-01T05:00:00]
    SELECT
      subq_13.metric_time__hour
      , subq_13.__subdaily_join_to_time_spine_metric
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_12.metric_time__hour AS metric_time__hour
        , subq_7.__subdaily_join_to_time_spine_metric AS __subdaily_join_to_time_spine_metric
      FROM (
        -- Pass Only Elements: ['metric_time__hour']
        SELECT
          subq_11.metric_time__hour
        FROM (
          -- Constrain Time Range to [2020-01-01T02:00:00, 2020-01-01T05:00:00]
          SELECT
            subq_10.metric_time__hour
          FROM (
            -- Pass Only Elements: ['metric_time__hour']
            SELECT
              subq_9.metric_time__hour
            FROM (
              -- Change Column Aliases
              SELECT
                subq_8.ts__hour AS metric_time__hour
                , subq_8.ts__day
                , subq_8.ts__week
                , subq_8.ts__month
                , subq_8.ts__quarter
                , subq_8.ts__year
                , subq_8.ts__extract_year
                , subq_8.ts__extract_quarter
                , subq_8.ts__extract_month
                , subq_8.ts__extract_day
                , subq_8.ts__extract_dow
                , subq_8.ts__extract_doy
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
              ) subq_8
            ) subq_9
          ) subq_10
          WHERE subq_10.metric_time__hour BETWEEN '2020-01-01 02:00:00' AND '2020-01-01 05:00:00'
        ) subq_11
      ) subq_12
      LEFT OUTER JOIN (
        -- Aggregate Inputs for Simple Metrics
        SELECT
          subq_6.metric_time__hour
          , SUM(subq_6.__subdaily_join_to_time_spine_metric) AS __subdaily_join_to_time_spine_metric
        FROM (
          -- Pass Only Elements: ['__subdaily_join_to_time_spine_metric', 'metric_time__hour']
          SELECT
            subq_5.metric_time__hour
            , subq_5.__subdaily_join_to_time_spine_metric
          FROM (
            -- Pass Only Elements: ['__subdaily_join_to_time_spine_metric', 'metric_time__hour']
            SELECT
              subq_4.metric_time__hour
              , subq_4.__subdaily_join_to_time_spine_metric
            FROM (
              -- Constrain Time Range to [2020-01-01T02:00:00, 2020-01-01T05:00:00]
              SELECT
                subq_3.ds__day
                , subq_3.ds__week
                , subq_3.ds__month
                , subq_3.ds__quarter
                , subq_3.ds__year
                , subq_3.ds__extract_year
                , subq_3.ds__extract_quarter
                , subq_3.ds__extract_month
                , subq_3.ds__extract_day
                , subq_3.ds__extract_dow
                , subq_3.ds__extract_doy
                , subq_3.created_at__day
                , subq_3.created_at__week
                , subq_3.created_at__month
                , subq_3.created_at__quarter
                , subq_3.created_at__year
                , subq_3.created_at__extract_year
                , subq_3.created_at__extract_quarter
                , subq_3.created_at__extract_month
                , subq_3.created_at__extract_day
                , subq_3.created_at__extract_dow
                , subq_3.created_at__extract_doy
                , subq_3.ds_partitioned__day
                , subq_3.ds_partitioned__week
                , subq_3.ds_partitioned__month
                , subq_3.ds_partitioned__quarter
                , subq_3.ds_partitioned__year
                , subq_3.ds_partitioned__extract_year
                , subq_3.ds_partitioned__extract_quarter
                , subq_3.ds_partitioned__extract_month
                , subq_3.ds_partitioned__extract_day
                , subq_3.ds_partitioned__extract_dow
                , subq_3.ds_partitioned__extract_doy
                , subq_3.last_profile_edit_ts__millisecond
                , subq_3.last_profile_edit_ts__second
                , subq_3.last_profile_edit_ts__minute
                , subq_3.last_profile_edit_ts__hour
                , subq_3.last_profile_edit_ts__day
                , subq_3.last_profile_edit_ts__week
                , subq_3.last_profile_edit_ts__month
                , subq_3.last_profile_edit_ts__quarter
                , subq_3.last_profile_edit_ts__year
                , subq_3.last_profile_edit_ts__extract_year
                , subq_3.last_profile_edit_ts__extract_quarter
                , subq_3.last_profile_edit_ts__extract_month
                , subq_3.last_profile_edit_ts__extract_day
                , subq_3.last_profile_edit_ts__extract_dow
                , subq_3.last_profile_edit_ts__extract_doy
                , subq_3.bio_added_ts__second
                , subq_3.bio_added_ts__minute
                , subq_3.bio_added_ts__hour
                , subq_3.bio_added_ts__day
                , subq_3.bio_added_ts__week
                , subq_3.bio_added_ts__month
                , subq_3.bio_added_ts__quarter
                , subq_3.bio_added_ts__year
                , subq_3.bio_added_ts__extract_year
                , subq_3.bio_added_ts__extract_quarter
                , subq_3.bio_added_ts__extract_month
                , subq_3.bio_added_ts__extract_day
                , subq_3.bio_added_ts__extract_dow
                , subq_3.bio_added_ts__extract_doy
                , subq_3.last_login_ts__minute
                , subq_3.last_login_ts__hour
                , subq_3.last_login_ts__day
                , subq_3.last_login_ts__week
                , subq_3.last_login_ts__month
                , subq_3.last_login_ts__quarter
                , subq_3.last_login_ts__year
                , subq_3.last_login_ts__extract_year
                , subq_3.last_login_ts__extract_quarter
                , subq_3.last_login_ts__extract_month
                , subq_3.last_login_ts__extract_day
                , subq_3.last_login_ts__extract_dow
                , subq_3.last_login_ts__extract_doy
                , subq_3.archived_at__hour
                , subq_3.archived_at__day
                , subq_3.archived_at__week
                , subq_3.archived_at__month
                , subq_3.archived_at__quarter
                , subq_3.archived_at__year
                , subq_3.archived_at__extract_year
                , subq_3.archived_at__extract_quarter
                , subq_3.archived_at__extract_month
                , subq_3.archived_at__extract_day
                , subq_3.archived_at__extract_dow
                , subq_3.archived_at__extract_doy
                , subq_3.user__ds__day
                , subq_3.user__ds__week
                , subq_3.user__ds__month
                , subq_3.user__ds__quarter
                , subq_3.user__ds__year
                , subq_3.user__ds__extract_year
                , subq_3.user__ds__extract_quarter
                , subq_3.user__ds__extract_month
                , subq_3.user__ds__extract_day
                , subq_3.user__ds__extract_dow
                , subq_3.user__ds__extract_doy
                , subq_3.user__created_at__day
                , subq_3.user__created_at__week
                , subq_3.user__created_at__month
                , subq_3.user__created_at__quarter
                , subq_3.user__created_at__year
                , subq_3.user__created_at__extract_year
                , subq_3.user__created_at__extract_quarter
                , subq_3.user__created_at__extract_month
                , subq_3.user__created_at__extract_day
                , subq_3.user__created_at__extract_dow
                , subq_3.user__created_at__extract_doy
                , subq_3.user__ds_partitioned__day
                , subq_3.user__ds_partitioned__week
                , subq_3.user__ds_partitioned__month
                , subq_3.user__ds_partitioned__quarter
                , subq_3.user__ds_partitioned__year
                , subq_3.user__ds_partitioned__extract_year
                , subq_3.user__ds_partitioned__extract_quarter
                , subq_3.user__ds_partitioned__extract_month
                , subq_3.user__ds_partitioned__extract_day
                , subq_3.user__ds_partitioned__extract_dow
                , subq_3.user__ds_partitioned__extract_doy
                , subq_3.user__last_profile_edit_ts__millisecond
                , subq_3.user__last_profile_edit_ts__second
                , subq_3.user__last_profile_edit_ts__minute
                , subq_3.user__last_profile_edit_ts__hour
                , subq_3.user__last_profile_edit_ts__day
                , subq_3.user__last_profile_edit_ts__week
                , subq_3.user__last_profile_edit_ts__month
                , subq_3.user__last_profile_edit_ts__quarter
                , subq_3.user__last_profile_edit_ts__year
                , subq_3.user__last_profile_edit_ts__extract_year
                , subq_3.user__last_profile_edit_ts__extract_quarter
                , subq_3.user__last_profile_edit_ts__extract_month
                , subq_3.user__last_profile_edit_ts__extract_day
                , subq_3.user__last_profile_edit_ts__extract_dow
                , subq_3.user__last_profile_edit_ts__extract_doy
                , subq_3.user__bio_added_ts__second
                , subq_3.user__bio_added_ts__minute
                , subq_3.user__bio_added_ts__hour
                , subq_3.user__bio_added_ts__day
                , subq_3.user__bio_added_ts__week
                , subq_3.user__bio_added_ts__month
                , subq_3.user__bio_added_ts__quarter
                , subq_3.user__bio_added_ts__year
                , subq_3.user__bio_added_ts__extract_year
                , subq_3.user__bio_added_ts__extract_quarter
                , subq_3.user__bio_added_ts__extract_month
                , subq_3.user__bio_added_ts__extract_day
                , subq_3.user__bio_added_ts__extract_dow
                , subq_3.user__bio_added_ts__extract_doy
                , subq_3.user__last_login_ts__minute
                , subq_3.user__last_login_ts__hour
                , subq_3.user__last_login_ts__day
                , subq_3.user__last_login_ts__week
                , subq_3.user__last_login_ts__month
                , subq_3.user__last_login_ts__quarter
                , subq_3.user__last_login_ts__year
                , subq_3.user__last_login_ts__extract_year
                , subq_3.user__last_login_ts__extract_quarter
                , subq_3.user__last_login_ts__extract_month
                , subq_3.user__last_login_ts__extract_day
                , subq_3.user__last_login_ts__extract_dow
                , subq_3.user__last_login_ts__extract_doy
                , subq_3.user__archived_at__hour
                , subq_3.user__archived_at__day
                , subq_3.user__archived_at__week
                , subq_3.user__archived_at__month
                , subq_3.user__archived_at__quarter
                , subq_3.user__archived_at__year
                , subq_3.user__archived_at__extract_year
                , subq_3.user__archived_at__extract_quarter
                , subq_3.user__archived_at__extract_month
                , subq_3.user__archived_at__extract_day
                , subq_3.user__archived_at__extract_dow
                , subq_3.user__archived_at__extract_doy
                , subq_3.metric_time__hour
                , subq_3.metric_time__day
                , subq_3.metric_time__week
                , subq_3.metric_time__month
                , subq_3.metric_time__quarter
                , subq_3.metric_time__year
                , subq_3.metric_time__extract_year
                , subq_3.metric_time__extract_quarter
                , subq_3.metric_time__extract_month
                , subq_3.metric_time__extract_day
                , subq_3.metric_time__extract_dow
                , subq_3.metric_time__extract_doy
                , subq_3.user
                , subq_3.home_state
                , subq_3.user__home_state
                , subq_3.__subdaily_join_to_time_spine_metric
                , subq_3.__simple_subdaily_metric_default_day
                , subq_3.__simple_subdaily_metric_default_hour
                , subq_3.__archived_users_join_to_time_spine
                , subq_3.__archived_users
              FROM (
                -- Metric Time Dimension 'archived_at'
                SELECT
                  subq_2.ds__day
                  , subq_2.ds__week
                  , subq_2.ds__month
                  , subq_2.ds__quarter
                  , subq_2.ds__year
                  , subq_2.ds__extract_year
                  , subq_2.ds__extract_quarter
                  , subq_2.ds__extract_month
                  , subq_2.ds__extract_day
                  , subq_2.ds__extract_dow
                  , subq_2.ds__extract_doy
                  , subq_2.created_at__day
                  , subq_2.created_at__week
                  , subq_2.created_at__month
                  , subq_2.created_at__quarter
                  , subq_2.created_at__year
                  , subq_2.created_at__extract_year
                  , subq_2.created_at__extract_quarter
                  , subq_2.created_at__extract_month
                  , subq_2.created_at__extract_day
                  , subq_2.created_at__extract_dow
                  , subq_2.created_at__extract_doy
                  , subq_2.ds_partitioned__day
                  , subq_2.ds_partitioned__week
                  , subq_2.ds_partitioned__month
                  , subq_2.ds_partitioned__quarter
                  , subq_2.ds_partitioned__year
                  , subq_2.ds_partitioned__extract_year
                  , subq_2.ds_partitioned__extract_quarter
                  , subq_2.ds_partitioned__extract_month
                  , subq_2.ds_partitioned__extract_day
                  , subq_2.ds_partitioned__extract_dow
                  , subq_2.ds_partitioned__extract_doy
                  , subq_2.last_profile_edit_ts__millisecond
                  , subq_2.last_profile_edit_ts__second
                  , subq_2.last_profile_edit_ts__minute
                  , subq_2.last_profile_edit_ts__hour
                  , subq_2.last_profile_edit_ts__day
                  , subq_2.last_profile_edit_ts__week
                  , subq_2.last_profile_edit_ts__month
                  , subq_2.last_profile_edit_ts__quarter
                  , subq_2.last_profile_edit_ts__year
                  , subq_2.last_profile_edit_ts__extract_year
                  , subq_2.last_profile_edit_ts__extract_quarter
                  , subq_2.last_profile_edit_ts__extract_month
                  , subq_2.last_profile_edit_ts__extract_day
                  , subq_2.last_profile_edit_ts__extract_dow
                  , subq_2.last_profile_edit_ts__extract_doy
                  , subq_2.bio_added_ts__second
                  , subq_2.bio_added_ts__minute
                  , subq_2.bio_added_ts__hour
                  , subq_2.bio_added_ts__day
                  , subq_2.bio_added_ts__week
                  , subq_2.bio_added_ts__month
                  , subq_2.bio_added_ts__quarter
                  , subq_2.bio_added_ts__year
                  , subq_2.bio_added_ts__extract_year
                  , subq_2.bio_added_ts__extract_quarter
                  , subq_2.bio_added_ts__extract_month
                  , subq_2.bio_added_ts__extract_day
                  , subq_2.bio_added_ts__extract_dow
                  , subq_2.bio_added_ts__extract_doy
                  , subq_2.last_login_ts__minute
                  , subq_2.last_login_ts__hour
                  , subq_2.last_login_ts__day
                  , subq_2.last_login_ts__week
                  , subq_2.last_login_ts__month
                  , subq_2.last_login_ts__quarter
                  , subq_2.last_login_ts__year
                  , subq_2.last_login_ts__extract_year
                  , subq_2.last_login_ts__extract_quarter
                  , subq_2.last_login_ts__extract_month
                  , subq_2.last_login_ts__extract_day
                  , subq_2.last_login_ts__extract_dow
                  , subq_2.last_login_ts__extract_doy
                  , subq_2.archived_at__hour
                  , subq_2.archived_at__day
                  , subq_2.archived_at__week
                  , subq_2.archived_at__month
                  , subq_2.archived_at__quarter
                  , subq_2.archived_at__year
                  , subq_2.archived_at__extract_year
                  , subq_2.archived_at__extract_quarter
                  , subq_2.archived_at__extract_month
                  , subq_2.archived_at__extract_day
                  , subq_2.archived_at__extract_dow
                  , subq_2.archived_at__extract_doy
                  , subq_2.user__ds__day
                  , subq_2.user__ds__week
                  , subq_2.user__ds__month
                  , subq_2.user__ds__quarter
                  , subq_2.user__ds__year
                  , subq_2.user__ds__extract_year
                  , subq_2.user__ds__extract_quarter
                  , subq_2.user__ds__extract_month
                  , subq_2.user__ds__extract_day
                  , subq_2.user__ds__extract_dow
                  , subq_2.user__ds__extract_doy
                  , subq_2.user__created_at__day
                  , subq_2.user__created_at__week
                  , subq_2.user__created_at__month
                  , subq_2.user__created_at__quarter
                  , subq_2.user__created_at__year
                  , subq_2.user__created_at__extract_year
                  , subq_2.user__created_at__extract_quarter
                  , subq_2.user__created_at__extract_month
                  , subq_2.user__created_at__extract_day
                  , subq_2.user__created_at__extract_dow
                  , subq_2.user__created_at__extract_doy
                  , subq_2.user__ds_partitioned__day
                  , subq_2.user__ds_partitioned__week
                  , subq_2.user__ds_partitioned__month
                  , subq_2.user__ds_partitioned__quarter
                  , subq_2.user__ds_partitioned__year
                  , subq_2.user__ds_partitioned__extract_year
                  , subq_2.user__ds_partitioned__extract_quarter
                  , subq_2.user__ds_partitioned__extract_month
                  , subq_2.user__ds_partitioned__extract_day
                  , subq_2.user__ds_partitioned__extract_dow
                  , subq_2.user__ds_partitioned__extract_doy
                  , subq_2.user__last_profile_edit_ts__millisecond
                  , subq_2.user__last_profile_edit_ts__second
                  , subq_2.user__last_profile_edit_ts__minute
                  , subq_2.user__last_profile_edit_ts__hour
                  , subq_2.user__last_profile_edit_ts__day
                  , subq_2.user__last_profile_edit_ts__week
                  , subq_2.user__last_profile_edit_ts__month
                  , subq_2.user__last_profile_edit_ts__quarter
                  , subq_2.user__last_profile_edit_ts__year
                  , subq_2.user__last_profile_edit_ts__extract_year
                  , subq_2.user__last_profile_edit_ts__extract_quarter
                  , subq_2.user__last_profile_edit_ts__extract_month
                  , subq_2.user__last_profile_edit_ts__extract_day
                  , subq_2.user__last_profile_edit_ts__extract_dow
                  , subq_2.user__last_profile_edit_ts__extract_doy
                  , subq_2.user__bio_added_ts__second
                  , subq_2.user__bio_added_ts__minute
                  , subq_2.user__bio_added_ts__hour
                  , subq_2.user__bio_added_ts__day
                  , subq_2.user__bio_added_ts__week
                  , subq_2.user__bio_added_ts__month
                  , subq_2.user__bio_added_ts__quarter
                  , subq_2.user__bio_added_ts__year
                  , subq_2.user__bio_added_ts__extract_year
                  , subq_2.user__bio_added_ts__extract_quarter
                  , subq_2.user__bio_added_ts__extract_month
                  , subq_2.user__bio_added_ts__extract_day
                  , subq_2.user__bio_added_ts__extract_dow
                  , subq_2.user__bio_added_ts__extract_doy
                  , subq_2.user__last_login_ts__minute
                  , subq_2.user__last_login_ts__hour
                  , subq_2.user__last_login_ts__day
                  , subq_2.user__last_login_ts__week
                  , subq_2.user__last_login_ts__month
                  , subq_2.user__last_login_ts__quarter
                  , subq_2.user__last_login_ts__year
                  , subq_2.user__last_login_ts__extract_year
                  , subq_2.user__last_login_ts__extract_quarter
                  , subq_2.user__last_login_ts__extract_month
                  , subq_2.user__last_login_ts__extract_day
                  , subq_2.user__last_login_ts__extract_dow
                  , subq_2.user__last_login_ts__extract_doy
                  , subq_2.user__archived_at__hour
                  , subq_2.user__archived_at__day
                  , subq_2.user__archived_at__week
                  , subq_2.user__archived_at__month
                  , subq_2.user__archived_at__quarter
                  , subq_2.user__archived_at__year
                  , subq_2.user__archived_at__extract_year
                  , subq_2.user__archived_at__extract_quarter
                  , subq_2.user__archived_at__extract_month
                  , subq_2.user__archived_at__extract_day
                  , subq_2.user__archived_at__extract_dow
                  , subq_2.user__archived_at__extract_doy
                  , subq_2.archived_at__hour AS metric_time__hour
                  , subq_2.archived_at__day AS metric_time__day
                  , subq_2.archived_at__week AS metric_time__week
                  , subq_2.archived_at__month AS metric_time__month
                  , subq_2.archived_at__quarter AS metric_time__quarter
                  , subq_2.archived_at__year AS metric_time__year
                  , subq_2.archived_at__extract_year AS metric_time__extract_year
                  , subq_2.archived_at__extract_quarter AS metric_time__extract_quarter
                  , subq_2.archived_at__extract_month AS metric_time__extract_month
                  , subq_2.archived_at__extract_day AS metric_time__extract_day
                  , subq_2.archived_at__extract_dow AS metric_time__extract_dow
                  , subq_2.archived_at__extract_doy AS metric_time__extract_doy
                  , subq_2.user
                  , subq_2.home_state
                  , subq_2.user__home_state
                  , subq_2.__subdaily_join_to_time_spine_metric
                  , subq_2.__simple_subdaily_metric_default_day
                  , subq_2.__simple_subdaily_metric_default_hour
                  , subq_2.__archived_users_join_to_time_spine
                  , subq_2.__archived_users
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
                ) subq_2
              ) subq_3
              WHERE subq_3.metric_time__hour BETWEEN '2020-01-01 02:00:00' AND '2020-01-01 05:00:00'
            ) subq_4
          ) subq_5
        ) subq_6
        GROUP BY
          subq_6.metric_time__hour
      ) subq_7
      ON
        subq_12.metric_time__hour = subq_7.metric_time__hour
    ) subq_13
    WHERE subq_13.metric_time__hour BETWEEN '2020-01-01 02:00:00' AND '2020-01-01 05:00:00'
  ) subq_14
) subq_15
