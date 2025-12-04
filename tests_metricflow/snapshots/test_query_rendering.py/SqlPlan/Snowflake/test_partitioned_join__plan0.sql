test_name: test_partitioned_join
test_filename: test_query_rendering.py
docstring:
  Tests converting a dataflow plan where there's a join on a partitioned dimension.
sql_engine: Snowflake
---
-- Write to DataTable
SELECT
  subq_9.user__home_state
  , subq_9.identity_verifications
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_8.user__home_state
    , subq_8.__identity_verifications AS identity_verifications
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_7.user__home_state
      , SUM(subq_7.__identity_verifications) AS __identity_verifications
    FROM (
      -- Pass Only Elements: ['__identity_verifications', 'user__home_state']
      SELECT
        subq_6.user__home_state
        , subq_6.__identity_verifications
      FROM (
        -- Pass Only Elements: ['__identity_verifications', 'user__home_state']
        SELECT
          subq_5.user__home_state
          , subq_5.__identity_verifications
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_4.home_state AS user__home_state
            , subq_4.ds_partitioned__day AS user__ds_partitioned__day
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
            , subq_1.verification__ds__day AS verification__ds__day
            , subq_1.verification__ds__week AS verification__ds__week
            , subq_1.verification__ds__month AS verification__ds__month
            , subq_1.verification__ds__quarter AS verification__ds__quarter
            , subq_1.verification__ds__year AS verification__ds__year
            , subq_1.verification__ds__extract_year AS verification__ds__extract_year
            , subq_1.verification__ds__extract_quarter AS verification__ds__extract_quarter
            , subq_1.verification__ds__extract_month AS verification__ds__extract_month
            , subq_1.verification__ds__extract_day AS verification__ds__extract_day
            , subq_1.verification__ds__extract_dow AS verification__ds__extract_dow
            , subq_1.verification__ds__extract_doy AS verification__ds__extract_doy
            , subq_1.verification__ds_partitioned__day AS verification__ds_partitioned__day
            , subq_1.verification__ds_partitioned__week AS verification__ds_partitioned__week
            , subq_1.verification__ds_partitioned__month AS verification__ds_partitioned__month
            , subq_1.verification__ds_partitioned__quarter AS verification__ds_partitioned__quarter
            , subq_1.verification__ds_partitioned__year AS verification__ds_partitioned__year
            , subq_1.verification__ds_partitioned__extract_year AS verification__ds_partitioned__extract_year
            , subq_1.verification__ds_partitioned__extract_quarter AS verification__ds_partitioned__extract_quarter
            , subq_1.verification__ds_partitioned__extract_month AS verification__ds_partitioned__extract_month
            , subq_1.verification__ds_partitioned__extract_day AS verification__ds_partitioned__extract_day
            , subq_1.verification__ds_partitioned__extract_dow AS verification__ds_partitioned__extract_dow
            , subq_1.verification__ds_partitioned__extract_doy AS verification__ds_partitioned__extract_doy
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
            , subq_1.verification AS verification
            , subq_1.user AS user
            , subq_1.verification__user AS verification__user
            , subq_1.verification_type AS verification_type
            , subq_1.verification__verification_type AS verification__verification_type
            , subq_1.__identity_verifications AS __identity_verifications
          FROM (
            -- Metric Time Dimension 'ds'
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
              , subq_0.verification__ds__day
              , subq_0.verification__ds__week
              , subq_0.verification__ds__month
              , subq_0.verification__ds__quarter
              , subq_0.verification__ds__year
              , subq_0.verification__ds__extract_year
              , subq_0.verification__ds__extract_quarter
              , subq_0.verification__ds__extract_month
              , subq_0.verification__ds__extract_day
              , subq_0.verification__ds__extract_dow
              , subq_0.verification__ds__extract_doy
              , subq_0.verification__ds_partitioned__day
              , subq_0.verification__ds_partitioned__week
              , subq_0.verification__ds_partitioned__month
              , subq_0.verification__ds_partitioned__quarter
              , subq_0.verification__ds_partitioned__year
              , subq_0.verification__ds_partitioned__extract_year
              , subq_0.verification__ds_partitioned__extract_quarter
              , subq_0.verification__ds_partitioned__extract_month
              , subq_0.verification__ds_partitioned__extract_day
              , subq_0.verification__ds_partitioned__extract_dow
              , subq_0.verification__ds_partitioned__extract_doy
              , subq_0.ds__day AS metric_time__day
              , subq_0.ds__week AS metric_time__week
              , subq_0.ds__month AS metric_time__month
              , subq_0.ds__quarter AS metric_time__quarter
              , subq_0.ds__year AS metric_time__year
              , subq_0.ds__extract_year AS metric_time__extract_year
              , subq_0.ds__extract_quarter AS metric_time__extract_quarter
              , subq_0.ds__extract_month AS metric_time__extract_month
              , subq_0.ds__extract_day AS metric_time__extract_day
              , subq_0.ds__extract_dow AS metric_time__extract_dow
              , subq_0.ds__extract_doy AS metric_time__extract_doy
              , subq_0.verification
              , subq_0.user
              , subq_0.verification__user
              , subq_0.verification_type
              , subq_0.verification__verification_type
              , subq_0.__identity_verifications
            FROM (
              -- Read Elements From Semantic Model 'id_verifications'
              SELECT
                1 AS __identity_verifications
                , DATE_TRUNC('day', id_verifications_src_28000.ds) AS ds__day
                , DATE_TRUNC('week', id_verifications_src_28000.ds) AS ds__week
                , DATE_TRUNC('month', id_verifications_src_28000.ds) AS ds__month
                , DATE_TRUNC('quarter', id_verifications_src_28000.ds) AS ds__quarter
                , DATE_TRUNC('year', id_verifications_src_28000.ds) AS ds__year
                , EXTRACT(year FROM id_verifications_src_28000.ds) AS ds__extract_year
                , EXTRACT(quarter FROM id_verifications_src_28000.ds) AS ds__extract_quarter
                , EXTRACT(month FROM id_verifications_src_28000.ds) AS ds__extract_month
                , EXTRACT(day FROM id_verifications_src_28000.ds) AS ds__extract_day
                , EXTRACT(dayofweekiso FROM id_verifications_src_28000.ds) AS ds__extract_dow
                , EXTRACT(doy FROM id_verifications_src_28000.ds) AS ds__extract_doy
                , DATE_TRUNC('day', id_verifications_src_28000.ds_partitioned) AS ds_partitioned__day
                , DATE_TRUNC('week', id_verifications_src_28000.ds_partitioned) AS ds_partitioned__week
                , DATE_TRUNC('month', id_verifications_src_28000.ds_partitioned) AS ds_partitioned__month
                , DATE_TRUNC('quarter', id_verifications_src_28000.ds_partitioned) AS ds_partitioned__quarter
                , DATE_TRUNC('year', id_verifications_src_28000.ds_partitioned) AS ds_partitioned__year
                , EXTRACT(year FROM id_verifications_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                , EXTRACT(quarter FROM id_verifications_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                , EXTRACT(month FROM id_verifications_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                , EXTRACT(day FROM id_verifications_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                , EXTRACT(dayofweekiso FROM id_verifications_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
                , EXTRACT(doy FROM id_verifications_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                , id_verifications_src_28000.verification_type
                , DATE_TRUNC('day', id_verifications_src_28000.ds) AS verification__ds__day
                , DATE_TRUNC('week', id_verifications_src_28000.ds) AS verification__ds__week
                , DATE_TRUNC('month', id_verifications_src_28000.ds) AS verification__ds__month
                , DATE_TRUNC('quarter', id_verifications_src_28000.ds) AS verification__ds__quarter
                , DATE_TRUNC('year', id_verifications_src_28000.ds) AS verification__ds__year
                , EXTRACT(year FROM id_verifications_src_28000.ds) AS verification__ds__extract_year
                , EXTRACT(quarter FROM id_verifications_src_28000.ds) AS verification__ds__extract_quarter
                , EXTRACT(month FROM id_verifications_src_28000.ds) AS verification__ds__extract_month
                , EXTRACT(day FROM id_verifications_src_28000.ds) AS verification__ds__extract_day
                , EXTRACT(dayofweekiso FROM id_verifications_src_28000.ds) AS verification__ds__extract_dow
                , EXTRACT(doy FROM id_verifications_src_28000.ds) AS verification__ds__extract_doy
                , DATE_TRUNC('day', id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__day
                , DATE_TRUNC('week', id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__week
                , DATE_TRUNC('month', id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__month
                , DATE_TRUNC('quarter', id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__quarter
                , DATE_TRUNC('year', id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__year
                , EXTRACT(year FROM id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_year
                , EXTRACT(quarter FROM id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_quarter
                , EXTRACT(month FROM id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_month
                , EXTRACT(day FROM id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_day
                , EXTRACT(dayofweekiso FROM id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_dow
                , EXTRACT(doy FROM id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_doy
                , id_verifications_src_28000.verification_type AS verification__verification_type
                , id_verifications_src_28000.verification_id AS verification
                , id_verifications_src_28000.user_id AS user
                , id_verifications_src_28000.user_id AS verification__user
              FROM ***************************.fct_id_verifications id_verifications_src_28000
            ) subq_0
          ) subq_1
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['home_state', 'ds_partitioned__day', 'user']
            SELECT
              subq_3.ds_partitioned__day
              , subq_3.user
              , subq_3.home_state
            FROM (
              -- Metric Time Dimension 'created_at'
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
                , subq_2.created_at__day AS metric_time__day
                , subq_2.created_at__week AS metric_time__week
                , subq_2.created_at__month AS metric_time__month
                , subq_2.created_at__quarter AS metric_time__quarter
                , subq_2.created_at__year AS metric_time__year
                , subq_2.created_at__extract_year AS metric_time__extract_year
                , subq_2.created_at__extract_quarter AS metric_time__extract_quarter
                , subq_2.created_at__extract_month AS metric_time__extract_month
                , subq_2.created_at__extract_day AS metric_time__extract_day
                , subq_2.created_at__extract_dow AS metric_time__extract_dow
                , subq_2.created_at__extract_doy AS metric_time__extract_doy
                , subq_2.user
                , subq_2.home_state
                , subq_2.user__home_state
                , subq_2.__new_users
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
          ) subq_4
          ON
            (
              subq_1.user = subq_4.user
            ) AND (
              subq_1.ds_partitioned__day = subq_4.ds_partitioned__day
            )
        ) subq_5
      ) subq_6
    ) subq_7
    GROUP BY
      subq_7.user__home_state
  ) subq_8
) subq_9
