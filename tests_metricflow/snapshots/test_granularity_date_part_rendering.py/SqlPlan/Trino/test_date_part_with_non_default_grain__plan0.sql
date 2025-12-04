test_name: test_date_part_with_non_default_grain
test_filename: test_granularity_date_part_rendering.py
sql_engine: Trino
---
-- Write to DataTable
SELECT
  subq_6.metric_time__extract_year
  , subq_6.archived_users
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_5.metric_time__extract_year
    , subq_5.__archived_users AS archived_users
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_4.metric_time__extract_year
      , SUM(subq_4.__archived_users) AS __archived_users
    FROM (
      -- Pass Only Elements: ['__archived_users', 'metric_time__extract_year']
      SELECT
        subq_3.metric_time__extract_year
        , subq_3.__archived_users
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_2.archived_users AS __archived_users
          , subq_2.metric_time__extract_year
          , subq_2.metric_time__extract_day
        FROM (
          -- Pass Only Elements: ['__archived_users', 'metric_time__extract_year', 'metric_time__extract_day']
          SELECT
            subq_1.metric_time__extract_year
            , subq_1.metric_time__extract_day
            , subq_1.__archived_users AS archived_users
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
        ) subq_2
        WHERE metric_time__extract_day = '2020-01-01'
      ) subq_3
    ) subq_4
    GROUP BY
      subq_4.metric_time__extract_year
  ) subq_5
) subq_6
