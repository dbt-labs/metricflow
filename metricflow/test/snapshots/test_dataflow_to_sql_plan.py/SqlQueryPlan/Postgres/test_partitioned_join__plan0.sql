-- Compute Metrics via Expressions
SELECT
  subq_7.user__home_state
  , subq_7.identity_verifications
FROM (
  -- Aggregate Measures
  SELECT
    subq_6.user__home_state
    , SUM(subq_6.identity_verifications) AS identity_verifications
  FROM (
    -- Pass Only Elements:
    --   ['identity_verifications', 'user__home_state']
    SELECT
      subq_5.user__home_state
      , subq_5.identity_verifications
    FROM (
      -- Join Standard Outputs
      SELECT
        subq_2.ds_partitioned__day AS ds_partitioned__day
        , subq_4.ds_partitioned__day AS user__ds_partitioned__day
        , subq_2.user AS user
        , subq_4.home_state AS user__home_state
        , subq_2.identity_verifications AS identity_verifications
      FROM (
        -- Pass Only Elements:
        --   ['identity_verifications', 'ds_partitioned__day', 'user']
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
            , subq_0.ds__extract_week
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
            , subq_0.ds_partitioned__extract_week
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
            , subq_0.verification__ds__extract_week
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
            , subq_0.verification__ds_partitioned__extract_week
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
            , subq_0.ds__extract_week AS metric_time__extract_week
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
              , DATE_TRUNC('day', id_verifications_src_10003.ds) AS ds__day
              , DATE_TRUNC('week', id_verifications_src_10003.ds) AS ds__week
              , DATE_TRUNC('month', id_verifications_src_10003.ds) AS ds__month
              , DATE_TRUNC('quarter', id_verifications_src_10003.ds) AS ds__quarter
              , DATE_TRUNC('year', id_verifications_src_10003.ds) AS ds__year
              , EXTRACT(year FROM id_verifications_src_10003.ds) AS ds__extract_year
              , EXTRACT(quarter FROM id_verifications_src_10003.ds) AS ds__extract_quarter
              , EXTRACT(month FROM id_verifications_src_10003.ds) AS ds__extract_month
              , EXTRACT(week FROM id_verifications_src_10003.ds) AS ds__extract_week
              , EXTRACT(day FROM id_verifications_src_10003.ds) AS ds__extract_day
              , EXTRACT(dow FROM id_verifications_src_10003.ds) AS ds__extract_dow
              , EXTRACT(doy FROM id_verifications_src_10003.ds) AS ds__extract_doy
              , DATE_TRUNC('day', id_verifications_src_10003.ds_partitioned) AS ds_partitioned__day
              , DATE_TRUNC('week', id_verifications_src_10003.ds_partitioned) AS ds_partitioned__week
              , DATE_TRUNC('month', id_verifications_src_10003.ds_partitioned) AS ds_partitioned__month
              , DATE_TRUNC('quarter', id_verifications_src_10003.ds_partitioned) AS ds_partitioned__quarter
              , DATE_TRUNC('year', id_verifications_src_10003.ds_partitioned) AS ds_partitioned__year
              , EXTRACT(year FROM id_verifications_src_10003.ds_partitioned) AS ds_partitioned__extract_year
              , EXTRACT(quarter FROM id_verifications_src_10003.ds_partitioned) AS ds_partitioned__extract_quarter
              , EXTRACT(month FROM id_verifications_src_10003.ds_partitioned) AS ds_partitioned__extract_month
              , EXTRACT(week FROM id_verifications_src_10003.ds_partitioned) AS ds_partitioned__extract_week
              , EXTRACT(day FROM id_verifications_src_10003.ds_partitioned) AS ds_partitioned__extract_day
              , EXTRACT(dow FROM id_verifications_src_10003.ds_partitioned) AS ds_partitioned__extract_dow
              , EXTRACT(doy FROM id_verifications_src_10003.ds_partitioned) AS ds_partitioned__extract_doy
              , id_verifications_src_10003.verification_type
              , DATE_TRUNC('day', id_verifications_src_10003.ds) AS verification__ds__day
              , DATE_TRUNC('week', id_verifications_src_10003.ds) AS verification__ds__week
              , DATE_TRUNC('month', id_verifications_src_10003.ds) AS verification__ds__month
              , DATE_TRUNC('quarter', id_verifications_src_10003.ds) AS verification__ds__quarter
              , DATE_TRUNC('year', id_verifications_src_10003.ds) AS verification__ds__year
              , EXTRACT(year FROM id_verifications_src_10003.ds) AS verification__ds__extract_year
              , EXTRACT(quarter FROM id_verifications_src_10003.ds) AS verification__ds__extract_quarter
              , EXTRACT(month FROM id_verifications_src_10003.ds) AS verification__ds__extract_month
              , EXTRACT(week FROM id_verifications_src_10003.ds) AS verification__ds__extract_week
              , EXTRACT(day FROM id_verifications_src_10003.ds) AS verification__ds__extract_day
              , EXTRACT(dow FROM id_verifications_src_10003.ds) AS verification__ds__extract_dow
              , EXTRACT(doy FROM id_verifications_src_10003.ds) AS verification__ds__extract_doy
              , DATE_TRUNC('day', id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__day
              , DATE_TRUNC('week', id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__week
              , DATE_TRUNC('month', id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__month
              , DATE_TRUNC('quarter', id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__quarter
              , DATE_TRUNC('year', id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__year
              , EXTRACT(year FROM id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__extract_year
              , EXTRACT(quarter FROM id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__extract_quarter
              , EXTRACT(month FROM id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__extract_month
              , EXTRACT(week FROM id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__extract_week
              , EXTRACT(day FROM id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__extract_day
              , EXTRACT(dow FROM id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__extract_dow
              , EXTRACT(doy FROM id_verifications_src_10003.ds_partitioned) AS verification__ds_partitioned__extract_doy
              , id_verifications_src_10003.verification_type AS verification__verification_type
              , id_verifications_src_10003.verification_id AS verification
              , id_verifications_src_10003.user_id AS user
              , id_verifications_src_10003.user_id AS verification__user
            FROM ***************************.fct_id_verifications id_verifications_src_10003
          ) subq_0
        ) subq_1
      ) subq_2
      LEFT OUTER JOIN (
        -- Pass Only Elements:
        --   ['home_state', 'ds_partitioned__day', 'user']
        SELECT
          subq_3.ds_partitioned__day
          , subq_3.user
          , subq_3.home_state
        FROM (
          -- Read Elements From Semantic Model 'users_ds_source'
          SELECT
            DATE_TRUNC('day', users_ds_source_src_10007.ds) AS ds__day
            , DATE_TRUNC('week', users_ds_source_src_10007.ds) AS ds__week
            , DATE_TRUNC('month', users_ds_source_src_10007.ds) AS ds__month
            , DATE_TRUNC('quarter', users_ds_source_src_10007.ds) AS ds__quarter
            , DATE_TRUNC('year', users_ds_source_src_10007.ds) AS ds__year
            , EXTRACT(year FROM users_ds_source_src_10007.ds) AS ds__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_10007.ds) AS ds__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_10007.ds) AS ds__extract_month
            , EXTRACT(week FROM users_ds_source_src_10007.ds) AS ds__extract_week
            , EXTRACT(day FROM users_ds_source_src_10007.ds) AS ds__extract_day
            , EXTRACT(dow FROM users_ds_source_src_10007.ds) AS ds__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_10007.ds) AS ds__extract_doy
            , DATE_TRUNC('day', users_ds_source_src_10007.created_at) AS created_at__day
            , DATE_TRUNC('week', users_ds_source_src_10007.created_at) AS created_at__week
            , DATE_TRUNC('month', users_ds_source_src_10007.created_at) AS created_at__month
            , DATE_TRUNC('quarter', users_ds_source_src_10007.created_at) AS created_at__quarter
            , DATE_TRUNC('year', users_ds_source_src_10007.created_at) AS created_at__year
            , EXTRACT(year FROM users_ds_source_src_10007.created_at) AS created_at__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_10007.created_at) AS created_at__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_10007.created_at) AS created_at__extract_month
            , EXTRACT(week FROM users_ds_source_src_10007.created_at) AS created_at__extract_week
            , EXTRACT(day FROM users_ds_source_src_10007.created_at) AS created_at__extract_day
            , EXTRACT(dow FROM users_ds_source_src_10007.created_at) AS created_at__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_10007.created_at) AS created_at__extract_doy
            , DATE_TRUNC('day', users_ds_source_src_10007.ds_partitioned) AS ds_partitioned__day
            , DATE_TRUNC('week', users_ds_source_src_10007.ds_partitioned) AS ds_partitioned__week
            , DATE_TRUNC('month', users_ds_source_src_10007.ds_partitioned) AS ds_partitioned__month
            , DATE_TRUNC('quarter', users_ds_source_src_10007.ds_partitioned) AS ds_partitioned__quarter
            , DATE_TRUNC('year', users_ds_source_src_10007.ds_partitioned) AS ds_partitioned__year
            , EXTRACT(year FROM users_ds_source_src_10007.ds_partitioned) AS ds_partitioned__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_10007.ds_partitioned) AS ds_partitioned__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_10007.ds_partitioned) AS ds_partitioned__extract_month
            , EXTRACT(week FROM users_ds_source_src_10007.ds_partitioned) AS ds_partitioned__extract_week
            , EXTRACT(day FROM users_ds_source_src_10007.ds_partitioned) AS ds_partitioned__extract_day
            , EXTRACT(dow FROM users_ds_source_src_10007.ds_partitioned) AS ds_partitioned__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_10007.ds_partitioned) AS ds_partitioned__extract_doy
            , users_ds_source_src_10007.home_state
            , DATE_TRUNC('day', users_ds_source_src_10007.ds) AS user__ds__day
            , DATE_TRUNC('week', users_ds_source_src_10007.ds) AS user__ds__week
            , DATE_TRUNC('month', users_ds_source_src_10007.ds) AS user__ds__month
            , DATE_TRUNC('quarter', users_ds_source_src_10007.ds) AS user__ds__quarter
            , DATE_TRUNC('year', users_ds_source_src_10007.ds) AS user__ds__year
            , EXTRACT(year FROM users_ds_source_src_10007.ds) AS user__ds__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_10007.ds) AS user__ds__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_10007.ds) AS user__ds__extract_month
            , EXTRACT(week FROM users_ds_source_src_10007.ds) AS user__ds__extract_week
            , EXTRACT(day FROM users_ds_source_src_10007.ds) AS user__ds__extract_day
            , EXTRACT(dow FROM users_ds_source_src_10007.ds) AS user__ds__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_10007.ds) AS user__ds__extract_doy
            , DATE_TRUNC('day', users_ds_source_src_10007.created_at) AS user__created_at__day
            , DATE_TRUNC('week', users_ds_source_src_10007.created_at) AS user__created_at__week
            , DATE_TRUNC('month', users_ds_source_src_10007.created_at) AS user__created_at__month
            , DATE_TRUNC('quarter', users_ds_source_src_10007.created_at) AS user__created_at__quarter
            , DATE_TRUNC('year', users_ds_source_src_10007.created_at) AS user__created_at__year
            , EXTRACT(year FROM users_ds_source_src_10007.created_at) AS user__created_at__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_10007.created_at) AS user__created_at__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_10007.created_at) AS user__created_at__extract_month
            , EXTRACT(week FROM users_ds_source_src_10007.created_at) AS user__created_at__extract_week
            , EXTRACT(day FROM users_ds_source_src_10007.created_at) AS user__created_at__extract_day
            , EXTRACT(dow FROM users_ds_source_src_10007.created_at) AS user__created_at__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_10007.created_at) AS user__created_at__extract_doy
            , DATE_TRUNC('day', users_ds_source_src_10007.ds_partitioned) AS user__ds_partitioned__day
            , DATE_TRUNC('week', users_ds_source_src_10007.ds_partitioned) AS user__ds_partitioned__week
            , DATE_TRUNC('month', users_ds_source_src_10007.ds_partitioned) AS user__ds_partitioned__month
            , DATE_TRUNC('quarter', users_ds_source_src_10007.ds_partitioned) AS user__ds_partitioned__quarter
            , DATE_TRUNC('year', users_ds_source_src_10007.ds_partitioned) AS user__ds_partitioned__year
            , EXTRACT(year FROM users_ds_source_src_10007.ds_partitioned) AS user__ds_partitioned__extract_year
            , EXTRACT(quarter FROM users_ds_source_src_10007.ds_partitioned) AS user__ds_partitioned__extract_quarter
            , EXTRACT(month FROM users_ds_source_src_10007.ds_partitioned) AS user__ds_partitioned__extract_month
            , EXTRACT(week FROM users_ds_source_src_10007.ds_partitioned) AS user__ds_partitioned__extract_week
            , EXTRACT(day FROM users_ds_source_src_10007.ds_partitioned) AS user__ds_partitioned__extract_day
            , EXTRACT(dow FROM users_ds_source_src_10007.ds_partitioned) AS user__ds_partitioned__extract_dow
            , EXTRACT(doy FROM users_ds_source_src_10007.ds_partitioned) AS user__ds_partitioned__extract_doy
            , users_ds_source_src_10007.home_state AS user__home_state
            , users_ds_source_src_10007.user_id AS user
          FROM ***************************.dim_users users_ds_source_src_10007
        ) subq_3
      ) subq_4
      ON
        (
          subq_2.user = subq_4.user
        ) AND (
          subq_2.ds_partitioned__day = subq_4.ds_partitioned__day
        )
    ) subq_5
  ) subq_6
  GROUP BY
    subq_6.user__home_state
) subq_7
