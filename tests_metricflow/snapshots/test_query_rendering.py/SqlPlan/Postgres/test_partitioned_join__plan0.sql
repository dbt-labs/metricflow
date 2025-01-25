test_name: test_partitioned_join
test_filename: test_query_rendering.py
docstring:
  Tests converting a dataflow plan where there's a join on a partitioned dimension.
sql_engine: Postgres
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_5.user__home_state
  , nr_subq_5.identity_verifications
FROM (
  -- Aggregate Measures
  SELECT
    nr_subq_4.user__home_state
    , SUM(nr_subq_4.identity_verifications) AS identity_verifications
  FROM (
    -- Pass Only Elements: ['identity_verifications', 'user__home_state']
    SELECT
      nr_subq_3.user__home_state
      , nr_subq_3.identity_verifications
    FROM (
      -- Join Standard Outputs
      SELECT
        nr_subq_2.home_state AS user__home_state
        , nr_subq_2.ds_partitioned__day AS user__ds_partitioned__day
        , nr_subq_0.ds__day AS ds__day
        , nr_subq_0.ds__week AS ds__week
        , nr_subq_0.ds__month AS ds__month
        , nr_subq_0.ds__quarter AS ds__quarter
        , nr_subq_0.ds__year AS ds__year
        , nr_subq_0.ds__extract_year AS ds__extract_year
        , nr_subq_0.ds__extract_quarter AS ds__extract_quarter
        , nr_subq_0.ds__extract_month AS ds__extract_month
        , nr_subq_0.ds__extract_day AS ds__extract_day
        , nr_subq_0.ds__extract_dow AS ds__extract_dow
        , nr_subq_0.ds__extract_doy AS ds__extract_doy
        , nr_subq_0.ds_partitioned__day AS ds_partitioned__day
        , nr_subq_0.ds_partitioned__week AS ds_partitioned__week
        , nr_subq_0.ds_partitioned__month AS ds_partitioned__month
        , nr_subq_0.ds_partitioned__quarter AS ds_partitioned__quarter
        , nr_subq_0.ds_partitioned__year AS ds_partitioned__year
        , nr_subq_0.ds_partitioned__extract_year AS ds_partitioned__extract_year
        , nr_subq_0.ds_partitioned__extract_quarter AS ds_partitioned__extract_quarter
        , nr_subq_0.ds_partitioned__extract_month AS ds_partitioned__extract_month
        , nr_subq_0.ds_partitioned__extract_day AS ds_partitioned__extract_day
        , nr_subq_0.ds_partitioned__extract_dow AS ds_partitioned__extract_dow
        , nr_subq_0.ds_partitioned__extract_doy AS ds_partitioned__extract_doy
        , nr_subq_0.verification__ds__day AS verification__ds__day
        , nr_subq_0.verification__ds__week AS verification__ds__week
        , nr_subq_0.verification__ds__month AS verification__ds__month
        , nr_subq_0.verification__ds__quarter AS verification__ds__quarter
        , nr_subq_0.verification__ds__year AS verification__ds__year
        , nr_subq_0.verification__ds__extract_year AS verification__ds__extract_year
        , nr_subq_0.verification__ds__extract_quarter AS verification__ds__extract_quarter
        , nr_subq_0.verification__ds__extract_month AS verification__ds__extract_month
        , nr_subq_0.verification__ds__extract_day AS verification__ds__extract_day
        , nr_subq_0.verification__ds__extract_dow AS verification__ds__extract_dow
        , nr_subq_0.verification__ds__extract_doy AS verification__ds__extract_doy
        , nr_subq_0.verification__ds_partitioned__day AS verification__ds_partitioned__day
        , nr_subq_0.verification__ds_partitioned__week AS verification__ds_partitioned__week
        , nr_subq_0.verification__ds_partitioned__month AS verification__ds_partitioned__month
        , nr_subq_0.verification__ds_partitioned__quarter AS verification__ds_partitioned__quarter
        , nr_subq_0.verification__ds_partitioned__year AS verification__ds_partitioned__year
        , nr_subq_0.verification__ds_partitioned__extract_year AS verification__ds_partitioned__extract_year
        , nr_subq_0.verification__ds_partitioned__extract_quarter AS verification__ds_partitioned__extract_quarter
        , nr_subq_0.verification__ds_partitioned__extract_month AS verification__ds_partitioned__extract_month
        , nr_subq_0.verification__ds_partitioned__extract_day AS verification__ds_partitioned__extract_day
        , nr_subq_0.verification__ds_partitioned__extract_dow AS verification__ds_partitioned__extract_dow
        , nr_subq_0.verification__ds_partitioned__extract_doy AS verification__ds_partitioned__extract_doy
        , nr_subq_0.metric_time__day AS metric_time__day
        , nr_subq_0.metric_time__week AS metric_time__week
        , nr_subq_0.metric_time__month AS metric_time__month
        , nr_subq_0.metric_time__quarter AS metric_time__quarter
        , nr_subq_0.metric_time__year AS metric_time__year
        , nr_subq_0.metric_time__extract_year AS metric_time__extract_year
        , nr_subq_0.metric_time__extract_quarter AS metric_time__extract_quarter
        , nr_subq_0.metric_time__extract_month AS metric_time__extract_month
        , nr_subq_0.metric_time__extract_day AS metric_time__extract_day
        , nr_subq_0.metric_time__extract_dow AS metric_time__extract_dow
        , nr_subq_0.metric_time__extract_doy AS metric_time__extract_doy
        , nr_subq_0.verification AS verification
        , nr_subq_0.user AS user
        , nr_subq_0.verification__user AS verification__user
        , nr_subq_0.verification_type AS verification_type
        , nr_subq_0.verification__verification_type AS verification__verification_type
        , nr_subq_0.identity_verifications AS identity_verifications
      FROM (
        -- Metric Time Dimension 'ds'
        SELECT
          nr_subq_28006.ds__day
          , nr_subq_28006.ds__week
          , nr_subq_28006.ds__month
          , nr_subq_28006.ds__quarter
          , nr_subq_28006.ds__year
          , nr_subq_28006.ds__extract_year
          , nr_subq_28006.ds__extract_quarter
          , nr_subq_28006.ds__extract_month
          , nr_subq_28006.ds__extract_day
          , nr_subq_28006.ds__extract_dow
          , nr_subq_28006.ds__extract_doy
          , nr_subq_28006.ds_partitioned__day
          , nr_subq_28006.ds_partitioned__week
          , nr_subq_28006.ds_partitioned__month
          , nr_subq_28006.ds_partitioned__quarter
          , nr_subq_28006.ds_partitioned__year
          , nr_subq_28006.ds_partitioned__extract_year
          , nr_subq_28006.ds_partitioned__extract_quarter
          , nr_subq_28006.ds_partitioned__extract_month
          , nr_subq_28006.ds_partitioned__extract_day
          , nr_subq_28006.ds_partitioned__extract_dow
          , nr_subq_28006.ds_partitioned__extract_doy
          , nr_subq_28006.verification__ds__day
          , nr_subq_28006.verification__ds__week
          , nr_subq_28006.verification__ds__month
          , nr_subq_28006.verification__ds__quarter
          , nr_subq_28006.verification__ds__year
          , nr_subq_28006.verification__ds__extract_year
          , nr_subq_28006.verification__ds__extract_quarter
          , nr_subq_28006.verification__ds__extract_month
          , nr_subq_28006.verification__ds__extract_day
          , nr_subq_28006.verification__ds__extract_dow
          , nr_subq_28006.verification__ds__extract_doy
          , nr_subq_28006.verification__ds_partitioned__day
          , nr_subq_28006.verification__ds_partitioned__week
          , nr_subq_28006.verification__ds_partitioned__month
          , nr_subq_28006.verification__ds_partitioned__quarter
          , nr_subq_28006.verification__ds_partitioned__year
          , nr_subq_28006.verification__ds_partitioned__extract_year
          , nr_subq_28006.verification__ds_partitioned__extract_quarter
          , nr_subq_28006.verification__ds_partitioned__extract_month
          , nr_subq_28006.verification__ds_partitioned__extract_day
          , nr_subq_28006.verification__ds_partitioned__extract_dow
          , nr_subq_28006.verification__ds_partitioned__extract_doy
          , nr_subq_28006.ds__day AS metric_time__day
          , nr_subq_28006.ds__week AS metric_time__week
          , nr_subq_28006.ds__month AS metric_time__month
          , nr_subq_28006.ds__quarter AS metric_time__quarter
          , nr_subq_28006.ds__year AS metric_time__year
          , nr_subq_28006.ds__extract_year AS metric_time__extract_year
          , nr_subq_28006.ds__extract_quarter AS metric_time__extract_quarter
          , nr_subq_28006.ds__extract_month AS metric_time__extract_month
          , nr_subq_28006.ds__extract_day AS metric_time__extract_day
          , nr_subq_28006.ds__extract_dow AS metric_time__extract_dow
          , nr_subq_28006.ds__extract_doy AS metric_time__extract_doy
          , nr_subq_28006.verification
          , nr_subq_28006.user
          , nr_subq_28006.verification__user
          , nr_subq_28006.verification_type
          , nr_subq_28006.verification__verification_type
          , nr_subq_28006.identity_verifications
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
            , EXTRACT(isodow FROM id_verifications_src_28000.ds) AS ds__extract_dow
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
            , EXTRACT(isodow FROM id_verifications_src_28000.ds_partitioned) AS ds_partitioned__extract_dow
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
            , EXTRACT(isodow FROM id_verifications_src_28000.ds) AS verification__ds__extract_dow
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
            , EXTRACT(isodow FROM id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_dow
            , EXTRACT(doy FROM id_verifications_src_28000.ds_partitioned) AS verification__ds_partitioned__extract_doy
            , id_verifications_src_28000.verification_type AS verification__verification_type
            , id_verifications_src_28000.verification_id AS verification
            , id_verifications_src_28000.user_id AS user
            , id_verifications_src_28000.user_id AS verification__user
          FROM ***************************.fct_id_verifications id_verifications_src_28000
        ) nr_subq_28006
      ) nr_subq_0
      LEFT OUTER JOIN (
        -- Pass Only Elements: ['home_state', 'ds_partitioned__day', 'user']
        SELECT
          nr_subq_1.ds_partitioned__day
          , nr_subq_1.user
          , nr_subq_1.home_state
        FROM (
          -- Metric Time Dimension 'created_at'
          SELECT
            nr_subq_28009.ds__day
            , nr_subq_28009.ds__week
            , nr_subq_28009.ds__month
            , nr_subq_28009.ds__quarter
            , nr_subq_28009.ds__year
            , nr_subq_28009.ds__extract_year
            , nr_subq_28009.ds__extract_quarter
            , nr_subq_28009.ds__extract_month
            , nr_subq_28009.ds__extract_day
            , nr_subq_28009.ds__extract_dow
            , nr_subq_28009.ds__extract_doy
            , nr_subq_28009.created_at__day
            , nr_subq_28009.created_at__week
            , nr_subq_28009.created_at__month
            , nr_subq_28009.created_at__quarter
            , nr_subq_28009.created_at__year
            , nr_subq_28009.created_at__extract_year
            , nr_subq_28009.created_at__extract_quarter
            , nr_subq_28009.created_at__extract_month
            , nr_subq_28009.created_at__extract_day
            , nr_subq_28009.created_at__extract_dow
            , nr_subq_28009.created_at__extract_doy
            , nr_subq_28009.ds_partitioned__day
            , nr_subq_28009.ds_partitioned__week
            , nr_subq_28009.ds_partitioned__month
            , nr_subq_28009.ds_partitioned__quarter
            , nr_subq_28009.ds_partitioned__year
            , nr_subq_28009.ds_partitioned__extract_year
            , nr_subq_28009.ds_partitioned__extract_quarter
            , nr_subq_28009.ds_partitioned__extract_month
            , nr_subq_28009.ds_partitioned__extract_day
            , nr_subq_28009.ds_partitioned__extract_dow
            , nr_subq_28009.ds_partitioned__extract_doy
            , nr_subq_28009.last_profile_edit_ts__millisecond
            , nr_subq_28009.last_profile_edit_ts__second
            , nr_subq_28009.last_profile_edit_ts__minute
            , nr_subq_28009.last_profile_edit_ts__hour
            , nr_subq_28009.last_profile_edit_ts__day
            , nr_subq_28009.last_profile_edit_ts__week
            , nr_subq_28009.last_profile_edit_ts__month
            , nr_subq_28009.last_profile_edit_ts__quarter
            , nr_subq_28009.last_profile_edit_ts__year
            , nr_subq_28009.last_profile_edit_ts__extract_year
            , nr_subq_28009.last_profile_edit_ts__extract_quarter
            , nr_subq_28009.last_profile_edit_ts__extract_month
            , nr_subq_28009.last_profile_edit_ts__extract_day
            , nr_subq_28009.last_profile_edit_ts__extract_dow
            , nr_subq_28009.last_profile_edit_ts__extract_doy
            , nr_subq_28009.bio_added_ts__second
            , nr_subq_28009.bio_added_ts__minute
            , nr_subq_28009.bio_added_ts__hour
            , nr_subq_28009.bio_added_ts__day
            , nr_subq_28009.bio_added_ts__week
            , nr_subq_28009.bio_added_ts__month
            , nr_subq_28009.bio_added_ts__quarter
            , nr_subq_28009.bio_added_ts__year
            , nr_subq_28009.bio_added_ts__extract_year
            , nr_subq_28009.bio_added_ts__extract_quarter
            , nr_subq_28009.bio_added_ts__extract_month
            , nr_subq_28009.bio_added_ts__extract_day
            , nr_subq_28009.bio_added_ts__extract_dow
            , nr_subq_28009.bio_added_ts__extract_doy
            , nr_subq_28009.last_login_ts__minute
            , nr_subq_28009.last_login_ts__hour
            , nr_subq_28009.last_login_ts__day
            , nr_subq_28009.last_login_ts__week
            , nr_subq_28009.last_login_ts__month
            , nr_subq_28009.last_login_ts__quarter
            , nr_subq_28009.last_login_ts__year
            , nr_subq_28009.last_login_ts__extract_year
            , nr_subq_28009.last_login_ts__extract_quarter
            , nr_subq_28009.last_login_ts__extract_month
            , nr_subq_28009.last_login_ts__extract_day
            , nr_subq_28009.last_login_ts__extract_dow
            , nr_subq_28009.last_login_ts__extract_doy
            , nr_subq_28009.archived_at__hour
            , nr_subq_28009.archived_at__day
            , nr_subq_28009.archived_at__week
            , nr_subq_28009.archived_at__month
            , nr_subq_28009.archived_at__quarter
            , nr_subq_28009.archived_at__year
            , nr_subq_28009.archived_at__extract_year
            , nr_subq_28009.archived_at__extract_quarter
            , nr_subq_28009.archived_at__extract_month
            , nr_subq_28009.archived_at__extract_day
            , nr_subq_28009.archived_at__extract_dow
            , nr_subq_28009.archived_at__extract_doy
            , nr_subq_28009.user__ds__day
            , nr_subq_28009.user__ds__week
            , nr_subq_28009.user__ds__month
            , nr_subq_28009.user__ds__quarter
            , nr_subq_28009.user__ds__year
            , nr_subq_28009.user__ds__extract_year
            , nr_subq_28009.user__ds__extract_quarter
            , nr_subq_28009.user__ds__extract_month
            , nr_subq_28009.user__ds__extract_day
            , nr_subq_28009.user__ds__extract_dow
            , nr_subq_28009.user__ds__extract_doy
            , nr_subq_28009.user__created_at__day
            , nr_subq_28009.user__created_at__week
            , nr_subq_28009.user__created_at__month
            , nr_subq_28009.user__created_at__quarter
            , nr_subq_28009.user__created_at__year
            , nr_subq_28009.user__created_at__extract_year
            , nr_subq_28009.user__created_at__extract_quarter
            , nr_subq_28009.user__created_at__extract_month
            , nr_subq_28009.user__created_at__extract_day
            , nr_subq_28009.user__created_at__extract_dow
            , nr_subq_28009.user__created_at__extract_doy
            , nr_subq_28009.user__ds_partitioned__day
            , nr_subq_28009.user__ds_partitioned__week
            , nr_subq_28009.user__ds_partitioned__month
            , nr_subq_28009.user__ds_partitioned__quarter
            , nr_subq_28009.user__ds_partitioned__year
            , nr_subq_28009.user__ds_partitioned__extract_year
            , nr_subq_28009.user__ds_partitioned__extract_quarter
            , nr_subq_28009.user__ds_partitioned__extract_month
            , nr_subq_28009.user__ds_partitioned__extract_day
            , nr_subq_28009.user__ds_partitioned__extract_dow
            , nr_subq_28009.user__ds_partitioned__extract_doy
            , nr_subq_28009.user__last_profile_edit_ts__millisecond
            , nr_subq_28009.user__last_profile_edit_ts__second
            , nr_subq_28009.user__last_profile_edit_ts__minute
            , nr_subq_28009.user__last_profile_edit_ts__hour
            , nr_subq_28009.user__last_profile_edit_ts__day
            , nr_subq_28009.user__last_profile_edit_ts__week
            , nr_subq_28009.user__last_profile_edit_ts__month
            , nr_subq_28009.user__last_profile_edit_ts__quarter
            , nr_subq_28009.user__last_profile_edit_ts__year
            , nr_subq_28009.user__last_profile_edit_ts__extract_year
            , nr_subq_28009.user__last_profile_edit_ts__extract_quarter
            , nr_subq_28009.user__last_profile_edit_ts__extract_month
            , nr_subq_28009.user__last_profile_edit_ts__extract_day
            , nr_subq_28009.user__last_profile_edit_ts__extract_dow
            , nr_subq_28009.user__last_profile_edit_ts__extract_doy
            , nr_subq_28009.user__bio_added_ts__second
            , nr_subq_28009.user__bio_added_ts__minute
            , nr_subq_28009.user__bio_added_ts__hour
            , nr_subq_28009.user__bio_added_ts__day
            , nr_subq_28009.user__bio_added_ts__week
            , nr_subq_28009.user__bio_added_ts__month
            , nr_subq_28009.user__bio_added_ts__quarter
            , nr_subq_28009.user__bio_added_ts__year
            , nr_subq_28009.user__bio_added_ts__extract_year
            , nr_subq_28009.user__bio_added_ts__extract_quarter
            , nr_subq_28009.user__bio_added_ts__extract_month
            , nr_subq_28009.user__bio_added_ts__extract_day
            , nr_subq_28009.user__bio_added_ts__extract_dow
            , nr_subq_28009.user__bio_added_ts__extract_doy
            , nr_subq_28009.user__last_login_ts__minute
            , nr_subq_28009.user__last_login_ts__hour
            , nr_subq_28009.user__last_login_ts__day
            , nr_subq_28009.user__last_login_ts__week
            , nr_subq_28009.user__last_login_ts__month
            , nr_subq_28009.user__last_login_ts__quarter
            , nr_subq_28009.user__last_login_ts__year
            , nr_subq_28009.user__last_login_ts__extract_year
            , nr_subq_28009.user__last_login_ts__extract_quarter
            , nr_subq_28009.user__last_login_ts__extract_month
            , nr_subq_28009.user__last_login_ts__extract_day
            , nr_subq_28009.user__last_login_ts__extract_dow
            , nr_subq_28009.user__last_login_ts__extract_doy
            , nr_subq_28009.user__archived_at__hour
            , nr_subq_28009.user__archived_at__day
            , nr_subq_28009.user__archived_at__week
            , nr_subq_28009.user__archived_at__month
            , nr_subq_28009.user__archived_at__quarter
            , nr_subq_28009.user__archived_at__year
            , nr_subq_28009.user__archived_at__extract_year
            , nr_subq_28009.user__archived_at__extract_quarter
            , nr_subq_28009.user__archived_at__extract_month
            , nr_subq_28009.user__archived_at__extract_day
            , nr_subq_28009.user__archived_at__extract_dow
            , nr_subq_28009.user__archived_at__extract_doy
            , nr_subq_28009.created_at__day AS metric_time__day
            , nr_subq_28009.created_at__week AS metric_time__week
            , nr_subq_28009.created_at__month AS metric_time__month
            , nr_subq_28009.created_at__quarter AS metric_time__quarter
            , nr_subq_28009.created_at__year AS metric_time__year
            , nr_subq_28009.created_at__extract_year AS metric_time__extract_year
            , nr_subq_28009.created_at__extract_quarter AS metric_time__extract_quarter
            , nr_subq_28009.created_at__extract_month AS metric_time__extract_month
            , nr_subq_28009.created_at__extract_day AS metric_time__extract_day
            , nr_subq_28009.created_at__extract_dow AS metric_time__extract_dow
            , nr_subq_28009.created_at__extract_doy AS metric_time__extract_doy
            , nr_subq_28009.user
            , nr_subq_28009.home_state
            , nr_subq_28009.user__home_state
            , nr_subq_28009.new_users
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
          ) nr_subq_28009
        ) nr_subq_1
      ) nr_subq_2
      ON
        (
          nr_subq_0.user = nr_subq_2.user
        ) AND (
          nr_subq_0.ds_partitioned__day = nr_subq_2.ds_partitioned__day
        )
    ) nr_subq_3
  ) nr_subq_4
  GROUP BY
    nr_subq_4.user__home_state
) nr_subq_5
