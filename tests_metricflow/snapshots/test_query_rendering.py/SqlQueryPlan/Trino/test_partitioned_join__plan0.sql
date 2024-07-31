-- Compute Metrics via Expressions
SELECT
  subq_8.user__home_state
  , subq_8.identity_verifications
FROM (
  -- Aggregate Measures
  SELECT
    subq_7.user__home_state
    , SUM(subq_7.identity_verifications) AS identity_verifications
  FROM (
    -- Pass Only Elements: ['identity_verifications', 'user__home_state']
    SELECT
      subq_6.user__home_state
      , subq_6.identity_verifications
    FROM (
      -- Join Standard Outputs
      SELECT
        subq_2.ds_partitioned__day AS ds_partitioned__day
        , subq_5.ds_partitioned__day AS user__ds_partitioned__day
        , subq_2.user AS user
        , subq_5.home_state AS user__home_state
        , subq_2.identity_verifications AS identity_verifications
      FROM (
        -- Pass Only Elements: ['identity_verifications', 'ds_partitioned__day', 'user']
        SELECT
          subq_1.ds_partitioned__day
          , subq_1.user
          , subq_1.identity_verifications
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
            , subq_0.identity_verifications
          FROM (
            -- Read Elements From Semantic Model 'id_verifications'
            SELECT
              1 AS identity_verifications
              , DATE_TRUNC('day', id_verifications_src_28000.ds) AS ds__day
              , DATE_TRUNC('week', id_verifications_src_28000.ds) AS ds__week
              , DATE_TRUNC('month', id_verifications_src_28000.ds) AS ds__month
              , DATE_TRUNC('quarter', id_verifications_src_28000.ds) AS ds__quarter
              , DATE_TRUNC('year', id_verifications_src_28000.ds) AS ds__year
              , EXTRACT(year FROM id_verifications_src_28000.ds) AS ds__extract_year
              , EXTRACT(quarter FROM id_verifications_src_28000.ds) AS ds__extract_quarter
              , EXTRACT(month FROM id_verifications_src_28000.ds) AS ds__extract_month
              , EXTRACT(day FROM id_verifications_src_28000.ds) AS ds__extract_day
              , EXTRACT(DAY_OF_WEEK FROM id_verifications_src_28000.ds) AS ds__extract_dow
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
              , EXTRACT(DAY_OF_WEEK FROM id_verifications_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
              , EXTRACT(DAY_OF_WEEK FROM id_verifications_src_28000.ds) AS verification__ds__extract_dow
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
              , EXTRACT(DAY_OF_WEEK FROM id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_dow
              , EXTRACT(doy FROM id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_doy
              , id_verifications_src_28000.verification_type AS verification__verification_type
              , id_verifications_src_28000.verification_id AS verification
              , id_verifications_src_28000.user_id AS user
              , id_verifications_src_28000.user_id AS verification__user
            FROM ***************************.fct_id_verifications id_verifications_src_28000
          ) subq_0
        ) subq_1
      ) subq_2
      LEFT OUTER JOIN (
        -- Pass Only Elements: ['home_state', 'ds_partitioned__day', 'user']
        SELECT
          subq_4.ds_partitioned__day
          , subq_4.user
          , subq_4.home_state
        FROM (
          -- Metric Time Dimension 'created_at'
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
            , subq_3.created_at__day AS metric_time__day
            , subq_3.created_at__week AS metric_time__week
            , subq_3.created_at__month AS metric_time__month
            , subq_3.created_at__quarter AS metric_time__quarter
            , subq_3.created_at__year AS metric_time__year
            , subq_3.created_at__extract_year AS metric_time__extract_year
            , subq_3.created_at__extract_quarter AS metric_time__extract_quarter
            , subq_3.created_at__extract_month AS metric_time__extract_month
            , subq_3.created_at__extract_day AS metric_time__extract_day
            , subq_3.created_at__extract_dow AS metric_time__extract_dow
            , subq_3.created_at__extract_doy AS metric_time__extract_doy
            , subq_3.user
            , subq_3.home_state
            , subq_3.user__home_state
            , subq_3.new_users
          FROM (
            -- Read Elements From Semantic Model 'users_ds_source'
            SELECT
              1 AS new_users
              , 1 AS archived_users
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
          ) subq_3
        ) subq_4
      ) subq_5
      ON
        (
          subq_2.user = subq_5.user
        ) AND (
          subq_2.ds_partitioned__day = subq_5.ds_partitioned__day
        )
    ) subq_6
  ) subq_7
  GROUP BY
    subq_7.user__home_state
) subq_8
