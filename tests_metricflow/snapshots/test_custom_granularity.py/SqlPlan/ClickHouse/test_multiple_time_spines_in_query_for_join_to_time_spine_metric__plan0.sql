test_name: test_multiple_time_spines_in_query_for_join_to_time_spine_metric
test_filename: test_custom_granularity.py
sql_engine: ClickHouse
---
SELECT
  subq_12.metric_time__alien_day
  , subq_12.metric_time__hour
  , subq_12.subdaily_join_to_time_spine_metric
FROM (
  SELECT
    subq_11.metric_time__alien_day
    , subq_11.metric_time__hour
    , subq_11.__subdaily_join_to_time_spine_metric AS subdaily_join_to_time_spine_metric
  FROM (
    SELECT
      subq_10.metric_time__alien_day AS metric_time__alien_day
      , subq_10.metric_time__hour AS metric_time__hour
      , subq_5.__subdaily_join_to_time_spine_metric AS __subdaily_join_to_time_spine_metric
    FROM (
      SELECT
        subq_9.metric_time__alien_day
        , subq_9.metric_time__hour
      FROM (
        SELECT
          subq_8.metric_time__alien_day
          , subq_8.metric_time__hour
        FROM (
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
            SELECT
              time_spine_src_28005.ts AS ts__hour
              , toStartOfDay(time_spine_src_28005.ts) AS ts__day
              , toStartOfWeek(time_spine_src_28005.ts, 1) AS ts__week
              , toStartOfMonth(time_spine_src_28005.ts) AS ts__month
              , toStartOfQuarter(time_spine_src_28005.ts) AS ts__quarter
              , toStartOfYear(time_spine_src_28005.ts) AS ts__year
              , toYear(time_spine_src_28005.ts) AS ts__extract_year
              , toQuarter(time_spine_src_28005.ts) AS ts__extract_quarter
              , toMonth(time_spine_src_28005.ts) AS ts__extract_month
              , toDayOfMonth(time_spine_src_28005.ts) AS ts__extract_day
              , toDayOfWeek(time_spine_src_28005.ts) AS ts__extract_dow
              , toDayOfYear(time_spine_src_28005.ts) AS ts__extract_doy
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
      SELECT
        subq_4.metric_time__alien_day
        , subq_4.metric_time__hour
        , SUM(subq_4.__subdaily_join_to_time_spine_metric) AS __subdaily_join_to_time_spine_metric
      FROM (
        SELECT
          subq_3.metric_time__alien_day
          , subq_3.metric_time__hour
          , subq_3.__subdaily_join_to_time_spine_metric
        FROM (
          SELECT
            subq_2.metric_time__alien_day
            , subq_2.metric_time__hour
            , subq_2.__subdaily_join_to_time_spine_metric
          FROM (
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
              SELECT
                1 AS __subdaily_join_to_time_spine_metric
                , 1 AS __simple_subdaily_metric_default_day
                , 1 AS __simple_subdaily_metric_default_hour
                , 1 AS __archived_users_join_to_time_spine
                , 1 AS __archived_users
                , 1 AS __new_users
                , toStartOfDay(users_ds_source_src_28000.ds) AS ds__day
                , toStartOfWeek(users_ds_source_src_28000.ds, 1) AS ds__week
                , toStartOfMonth(users_ds_source_src_28000.ds) AS ds__month
                , toStartOfQuarter(users_ds_source_src_28000.ds) AS ds__quarter
                , toStartOfYear(users_ds_source_src_28000.ds) AS ds__year
                , toYear(users_ds_source_src_28000.ds) AS ds__extract_year
                , toQuarter(users_ds_source_src_28000.ds) AS ds__extract_quarter
                , toMonth(users_ds_source_src_28000.ds) AS ds__extract_month
                , toDayOfMonth(users_ds_source_src_28000.ds) AS ds__extract_day
                , toDayOfWeek(users_ds_source_src_28000.ds) AS ds__extract_dow
                , toDayOfYear(users_ds_source_src_28000.ds) AS ds__extract_doy
                , toStartOfDay(users_ds_source_src_28000.created_at) AS created_at__day
                , toStartOfWeek(users_ds_source_src_28000.created_at, 1) AS created_at__week
                , toStartOfMonth(users_ds_source_src_28000.created_at) AS created_at__month
                , toStartOfQuarter(users_ds_source_src_28000.created_at) AS created_at__quarter
                , toStartOfYear(users_ds_source_src_28000.created_at) AS created_at__year
                , toYear(users_ds_source_src_28000.created_at) AS created_at__extract_year
                , toQuarter(users_ds_source_src_28000.created_at) AS created_at__extract_quarter
                , toMonth(users_ds_source_src_28000.created_at) AS created_at__extract_month
                , toDayOfMonth(users_ds_source_src_28000.created_at) AS created_at__extract_day
                , toDayOfWeek(users_ds_source_src_28000.created_at) AS created_at__extract_dow
                , toDayOfYear(users_ds_source_src_28000.created_at) AS created_at__extract_doy
                , toStartOfDay(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__day
                , toStartOfWeek(users_ds_source_src_28000.ds_partitioned, 1) AS ds_partitioned__week
                , toStartOfMonth(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__month
                , toStartOfQuarter(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__quarter
                , toStartOfYear(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__year
                , toYear(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                , toQuarter(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                , toMonth(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                , toDayOfMonth(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                , toDayOfWeek(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
                , toDayOfYear(users_ds_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                , users_ds_source_src_28000.home_state
                , toStartOfMillisecond(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__millisecond
                , toStartOfSecond(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__second
                , toStartOfMinute(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__minute
                , toStartOfHour(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__hour
                , toStartOfDay(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__day
                , toStartOfWeek(users_ds_source_src_28000.last_profile_edit_ts, 1) AS last_profile_edit_ts__week
                , toStartOfMonth(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__month
                , toStartOfQuarter(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__quarter
                , toStartOfYear(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__year
                , toYear(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_year
                , toQuarter(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_quarter
                , toMonth(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_month
                , toDayOfMonth(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_day
                , toDayOfWeek(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_dow
                , toDayOfYear(users_ds_source_src_28000.last_profile_edit_ts) AS last_profile_edit_ts__extract_doy
                , toStartOfSecond(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__second
                , toStartOfMinute(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__minute
                , toStartOfHour(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__hour
                , toStartOfDay(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__day
                , toStartOfWeek(users_ds_source_src_28000.bio_added_ts, 1) AS bio_added_ts__week
                , toStartOfMonth(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__month
                , toStartOfQuarter(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__quarter
                , toStartOfYear(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__year
                , toYear(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_year
                , toQuarter(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_quarter
                , toMonth(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_month
                , toDayOfMonth(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_day
                , toDayOfWeek(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_dow
                , toDayOfYear(users_ds_source_src_28000.bio_added_ts) AS bio_added_ts__extract_doy
                , toStartOfMinute(users_ds_source_src_28000.last_login_ts) AS last_login_ts__minute
                , toStartOfHour(users_ds_source_src_28000.last_login_ts) AS last_login_ts__hour
                , toStartOfDay(users_ds_source_src_28000.last_login_ts) AS last_login_ts__day
                , toStartOfWeek(users_ds_source_src_28000.last_login_ts, 1) AS last_login_ts__week
                , toStartOfMonth(users_ds_source_src_28000.last_login_ts) AS last_login_ts__month
                , toStartOfQuarter(users_ds_source_src_28000.last_login_ts) AS last_login_ts__quarter
                , toStartOfYear(users_ds_source_src_28000.last_login_ts) AS last_login_ts__year
                , toYear(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_year
                , toQuarter(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_quarter
                , toMonth(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_month
                , toDayOfMonth(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_day
                , toDayOfWeek(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_dow
                , toDayOfYear(users_ds_source_src_28000.last_login_ts) AS last_login_ts__extract_doy
                , toStartOfHour(users_ds_source_src_28000.archived_at) AS archived_at__hour
                , toStartOfDay(users_ds_source_src_28000.archived_at) AS archived_at__day
                , toStartOfWeek(users_ds_source_src_28000.archived_at, 1) AS archived_at__week
                , toStartOfMonth(users_ds_source_src_28000.archived_at) AS archived_at__month
                , toStartOfQuarter(users_ds_source_src_28000.archived_at) AS archived_at__quarter
                , toStartOfYear(users_ds_source_src_28000.archived_at) AS archived_at__year
                , toYear(users_ds_source_src_28000.archived_at) AS archived_at__extract_year
                , toQuarter(users_ds_source_src_28000.archived_at) AS archived_at__extract_quarter
                , toMonth(users_ds_source_src_28000.archived_at) AS archived_at__extract_month
                , toDayOfMonth(users_ds_source_src_28000.archived_at) AS archived_at__extract_day
                , toDayOfWeek(users_ds_source_src_28000.archived_at) AS archived_at__extract_dow
                , toDayOfYear(users_ds_source_src_28000.archived_at) AS archived_at__extract_doy
                , toStartOfDay(users_ds_source_src_28000.ds) AS user__ds__day
                , toStartOfWeek(users_ds_source_src_28000.ds, 1) AS user__ds__week
                , toStartOfMonth(users_ds_source_src_28000.ds) AS user__ds__month
                , toStartOfQuarter(users_ds_source_src_28000.ds) AS user__ds__quarter
                , toStartOfYear(users_ds_source_src_28000.ds) AS user__ds__year
                , toYear(users_ds_source_src_28000.ds) AS user__ds__extract_year
                , toQuarter(users_ds_source_src_28000.ds) AS user__ds__extract_quarter
                , toMonth(users_ds_source_src_28000.ds) AS user__ds__extract_month
                , toDayOfMonth(users_ds_source_src_28000.ds) AS user__ds__extract_day
                , toDayOfWeek(users_ds_source_src_28000.ds) AS user__ds__extract_dow
                , toDayOfYear(users_ds_source_src_28000.ds) AS user__ds__extract_doy
                , toStartOfDay(users_ds_source_src_28000.created_at) AS user__created_at__day
                , toStartOfWeek(users_ds_source_src_28000.created_at, 1) AS user__created_at__week
                , toStartOfMonth(users_ds_source_src_28000.created_at) AS user__created_at__month
                , toStartOfQuarter(users_ds_source_src_28000.created_at) AS user__created_at__quarter
                , toStartOfYear(users_ds_source_src_28000.created_at) AS user__created_at__year
                , toYear(users_ds_source_src_28000.created_at) AS user__created_at__extract_year
                , toQuarter(users_ds_source_src_28000.created_at) AS user__created_at__extract_quarter
                , toMonth(users_ds_source_src_28000.created_at) AS user__created_at__extract_month
                , toDayOfMonth(users_ds_source_src_28000.created_at) AS user__created_at__extract_day
                , toDayOfWeek(users_ds_source_src_28000.created_at) AS user__created_at__extract_dow
                , toDayOfYear(users_ds_source_src_28000.created_at) AS user__created_at__extract_doy
                , toStartOfDay(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__day
                , toStartOfWeek(users_ds_source_src_28000.ds_partitioned, 1) AS user__ds_partitioned__week
                , toStartOfMonth(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__month
                , toStartOfQuarter(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__quarter
                , toStartOfYear(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__year
                , toYear(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_year
                , toQuarter(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_quarter
                , toMonth(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_month
                , toDayOfMonth(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_day
                , toDayOfWeek(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_dow
                , toDayOfYear(users_ds_source_src_28000.ds_partitioned) AS user__ds_partitioned__extract_doy
                , users_ds_source_src_28000.home_state AS user__home_state
                , toStartOfMillisecond(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__millisecond
                , toStartOfSecond(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__second
                , toStartOfMinute(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__minute
                , toStartOfHour(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__hour
                , toStartOfDay(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__day
                , toStartOfWeek(users_ds_source_src_28000.last_profile_edit_ts, 1) AS user__last_profile_edit_ts__week
                , toStartOfMonth(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__month
                , toStartOfQuarter(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__quarter
                , toStartOfYear(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__year
                , toYear(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_year
                , toQuarter(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_quarter
                , toMonth(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_month
                , toDayOfMonth(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_day
                , toDayOfWeek(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_dow
                , toDayOfYear(users_ds_source_src_28000.last_profile_edit_ts) AS user__last_profile_edit_ts__extract_doy
                , toStartOfSecond(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__second
                , toStartOfMinute(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__minute
                , toStartOfHour(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__hour
                , toStartOfDay(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__day
                , toStartOfWeek(users_ds_source_src_28000.bio_added_ts, 1) AS user__bio_added_ts__week
                , toStartOfMonth(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__month
                , toStartOfQuarter(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__quarter
                , toStartOfYear(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__year
                , toYear(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_year
                , toQuarter(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_quarter
                , toMonth(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_month
                , toDayOfMonth(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_day
                , toDayOfWeek(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_dow
                , toDayOfYear(users_ds_source_src_28000.bio_added_ts) AS user__bio_added_ts__extract_doy
                , toStartOfMinute(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__minute
                , toStartOfHour(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__hour
                , toStartOfDay(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__day
                , toStartOfWeek(users_ds_source_src_28000.last_login_ts, 1) AS user__last_login_ts__week
                , toStartOfMonth(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__month
                , toStartOfQuarter(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__quarter
                , toStartOfYear(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__year
                , toYear(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_year
                , toQuarter(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_quarter
                , toMonth(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_month
                , toDayOfMonth(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_day
                , toDayOfWeek(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_dow
                , toDayOfYear(users_ds_source_src_28000.last_login_ts) AS user__last_login_ts__extract_doy
                , toStartOfHour(users_ds_source_src_28000.archived_at) AS user__archived_at__hour
                , toStartOfDay(users_ds_source_src_28000.archived_at) AS user__archived_at__day
                , toStartOfWeek(users_ds_source_src_28000.archived_at, 1) AS user__archived_at__week
                , toStartOfMonth(users_ds_source_src_28000.archived_at) AS user__archived_at__month
                , toStartOfQuarter(users_ds_source_src_28000.archived_at) AS user__archived_at__quarter
                , toStartOfYear(users_ds_source_src_28000.archived_at) AS user__archived_at__year
                , toYear(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_year
                , toQuarter(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_quarter
                , toMonth(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_month
                , toDayOfMonth(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_day
                , toDayOfWeek(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_dow
                , toDayOfYear(users_ds_source_src_28000.archived_at) AS user__archived_at__extract_doy
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
