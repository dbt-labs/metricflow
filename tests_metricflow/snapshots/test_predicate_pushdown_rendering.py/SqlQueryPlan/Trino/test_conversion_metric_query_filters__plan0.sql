-- Compute Metrics via Expressions
SELECT
  subq_27.metric_time__day
  , subq_27.user__home_state_latest
  , CAST(subq_27.buys AS DOUBLE) / CAST(NULLIF(subq_27.visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_9.metric_time__day, subq_26.metric_time__day) AS metric_time__day
    , COALESCE(subq_9.user__home_state_latest, subq_26.user__home_state_latest) AS user__home_state_latest
    , MAX(subq_9.visits) AS visits
    , MAX(subq_26.buys) AS buys
  FROM (
    -- Aggregate Measures
    SELECT
      subq_8.metric_time__day
      , subq_8.user__home_state_latest
      , SUM(subq_8.visits) AS visits
    FROM (
      -- Pass Only Elements: ['visits', 'user__home_state_latest', 'metric_time__day']
      SELECT
        subq_7.metric_time__day
        , subq_7.user__home_state_latest
        , subq_7.visits
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_6.metric_time__day
          , subq_6.visit__referrer_id
          , subq_6.user__home_state_latest
          , subq_6.visits
        FROM (
          -- Pass Only Elements: ['visits', 'user__home_state_latest', 'visit__referrer_id', 'metric_time__day']
          SELECT
            subq_5.metric_time__day
            , subq_5.visit__referrer_id
            , subq_5.user__home_state_latest
            , subq_5.visits
          FROM (
            -- Join Standard Outputs
            SELECT
              subq_2.metric_time__day AS metric_time__day
              , subq_2.user AS user
              , subq_2.visit__referrer_id AS visit__referrer_id
              , subq_4.home_state_latest AS user__home_state_latest
              , subq_2.visits AS visits
            FROM (
              -- Pass Only Elements: ['visits', 'visit__referrer_id', 'metric_time__day', 'user']
              SELECT
                subq_1.metric_time__day
                , subq_1.user
                , subq_1.visit__referrer_id
                , subq_1.visits
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
                  , subq_0.visit__ds__day
                  , subq_0.visit__ds__week
                  , subq_0.visit__ds__month
                  , subq_0.visit__ds__quarter
                  , subq_0.visit__ds__year
                  , subq_0.visit__ds__extract_year
                  , subq_0.visit__ds__extract_quarter
                  , subq_0.visit__ds__extract_month
                  , subq_0.visit__ds__extract_day
                  , subq_0.visit__ds__extract_dow
                  , subq_0.visit__ds__extract_doy
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
                  , subq_0.user
                  , subq_0.session
                  , subq_0.visit__user
                  , subq_0.visit__session
                  , subq_0.referrer_id
                  , subq_0.visit__referrer_id
                  , subq_0.visits
                  , subq_0.visitors
                FROM (
                  -- Read Elements From Semantic Model 'visits_source'
                  SELECT
                    1 AS visits
                    , visits_source_src_28000.user_id AS visitors
                    , DATE_TRUNC('day', visits_source_src_28000.ds) AS ds__day
                    , DATE_TRUNC('week', visits_source_src_28000.ds) AS ds__week
                    , DATE_TRUNC('month', visits_source_src_28000.ds) AS ds__month
                    , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS ds__quarter
                    , DATE_TRUNC('year', visits_source_src_28000.ds) AS ds__year
                    , EXTRACT(year FROM visits_source_src_28000.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM visits_source_src_28000.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM visits_source_src_28000.ds) AS ds__extract_month
                    , EXTRACT(day FROM visits_source_src_28000.ds) AS ds__extract_day
                    , EXTRACT(DAY_OF_WEEK FROM visits_source_src_28000.ds) AS ds__extract_dow
                    , EXTRACT(doy FROM visits_source_src_28000.ds) AS ds__extract_doy
                    , visits_source_src_28000.referrer_id
                    , DATE_TRUNC('day', visits_source_src_28000.ds) AS visit__ds__day
                    , DATE_TRUNC('week', visits_source_src_28000.ds) AS visit__ds__week
                    , DATE_TRUNC('month', visits_source_src_28000.ds) AS visit__ds__month
                    , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS visit__ds__quarter
                    , DATE_TRUNC('year', visits_source_src_28000.ds) AS visit__ds__year
                    , EXTRACT(year FROM visits_source_src_28000.ds) AS visit__ds__extract_year
                    , EXTRACT(quarter FROM visits_source_src_28000.ds) AS visit__ds__extract_quarter
                    , EXTRACT(month FROM visits_source_src_28000.ds) AS visit__ds__extract_month
                    , EXTRACT(day FROM visits_source_src_28000.ds) AS visit__ds__extract_day
                    , EXTRACT(DAY_OF_WEEK FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                    , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                    , visits_source_src_28000.referrer_id AS visit__referrer_id
                    , visits_source_src_28000.user_id AS user
                    , visits_source_src_28000.session_id AS session
                    , visits_source_src_28000.user_id AS visit__user
                    , visits_source_src_28000.session_id AS visit__session
                  FROM ***************************.fct_visits visits_source_src_28000
                ) subq_0
              ) subq_1
            ) subq_2
            LEFT OUTER JOIN (
              -- Pass Only Elements: ['home_state_latest', 'user']
              SELECT
                subq_3.user
                , subq_3.home_state_latest
              FROM (
                -- Read Elements From Semantic Model 'users_latest'
                SELECT
                  DATE_TRUNC('day', users_latest_src_28000.ds) AS ds_latest__day
                  , DATE_TRUNC('week', users_latest_src_28000.ds) AS ds_latest__week
                  , DATE_TRUNC('month', users_latest_src_28000.ds) AS ds_latest__month
                  , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS ds_latest__quarter
                  , DATE_TRUNC('year', users_latest_src_28000.ds) AS ds_latest__year
                  , EXTRACT(year FROM users_latest_src_28000.ds) AS ds_latest__extract_year
                  , EXTRACT(quarter FROM users_latest_src_28000.ds) AS ds_latest__extract_quarter
                  , EXTRACT(month FROM users_latest_src_28000.ds) AS ds_latest__extract_month
                  , EXTRACT(day FROM users_latest_src_28000.ds) AS ds_latest__extract_day
                  , EXTRACT(DAY_OF_WEEK FROM users_latest_src_28000.ds) AS ds_latest__extract_dow
                  , EXTRACT(doy FROM users_latest_src_28000.ds) AS ds_latest__extract_doy
                  , users_latest_src_28000.home_state_latest
                  , DATE_TRUNC('day', users_latest_src_28000.ds) AS user__ds_latest__day
                  , DATE_TRUNC('week', users_latest_src_28000.ds) AS user__ds_latest__week
                  , DATE_TRUNC('month', users_latest_src_28000.ds) AS user__ds_latest__month
                  , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS user__ds_latest__quarter
                  , DATE_TRUNC('year', users_latest_src_28000.ds) AS user__ds_latest__year
                  , EXTRACT(year FROM users_latest_src_28000.ds) AS user__ds_latest__extract_year
                  , EXTRACT(quarter FROM users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
                  , EXTRACT(month FROM users_latest_src_28000.ds) AS user__ds_latest__extract_month
                  , EXTRACT(day FROM users_latest_src_28000.ds) AS user__ds_latest__extract_day
                  , EXTRACT(DAY_OF_WEEK FROM users_latest_src_28000.ds) AS user__ds_latest__extract_dow
                  , EXTRACT(doy FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
                  , users_latest_src_28000.home_state_latest AS user__home_state_latest
                  , users_latest_src_28000.user_id AS user
                FROM ***************************.dim_users_latest users_latest_src_28000
              ) subq_3
            ) subq_4
            ON
              subq_2.user = subq_4.user
          ) subq_5
        ) subq_6
        WHERE visit__referrer_id = '123456'
      ) subq_7
    ) subq_8
    GROUP BY
      subq_8.metric_time__day
      , subq_8.user__home_state_latest
  ) subq_9
  FULL OUTER JOIN (
    -- Aggregate Measures
    SELECT
      subq_25.metric_time__day
      , subq_25.user__home_state_latest
      , SUM(subq_25.buys) AS buys
    FROM (
      -- Pass Only Elements: ['buys', 'user__home_state_latest', 'metric_time__day']
      SELECT
        subq_24.metric_time__day
        , subq_24.user__home_state_latest
        , subq_24.buys
      FROM (
        -- Join Standard Outputs
        SELECT
          subq_21.metric_time__day AS metric_time__day
          , subq_21.user AS user
          , subq_21.visit__referrer_id AS visit__referrer_id
          , subq_23.home_state_latest AS user__home_state_latest
          , subq_21.buys AS buys
        FROM (
          -- Pass Only Elements: ['buys', 'visit__referrer_id', 'metric_time__day', 'user']
          SELECT
            subq_20.metric_time__day
            , subq_20.user
            , subq_20.visit__referrer_id
            , subq_20.buys
          FROM (
            -- Find conversions for user within the range of 7 day
            SELECT
              subq_19.ds__day
              , subq_19.metric_time__day
              , subq_19.user
              , subq_19.visit__referrer_id
              , subq_19.user__home_state_latest
              , subq_19.buys
              , subq_19.visits
            FROM (
              -- Dedupe the fanout with mf_internal_uuid in the conversion data set
              SELECT DISTINCT
                FIRST_VALUE(subq_15.visits) OVER (
                  PARTITION BY
                    subq_18.user
                    , subq_18.ds__day
                    , subq_18.mf_internal_uuid
                  ORDER BY subq_15.ds__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS visits
                , FIRST_VALUE(subq_15.visit__referrer_id) OVER (
                  PARTITION BY
                    subq_18.user
                    , subq_18.ds__day
                    , subq_18.mf_internal_uuid
                  ORDER BY subq_15.ds__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS visit__referrer_id
                , FIRST_VALUE(subq_15.user__home_state_latest) OVER (
                  PARTITION BY
                    subq_18.user
                    , subq_18.ds__day
                    , subq_18.mf_internal_uuid
                  ORDER BY subq_15.ds__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS user__home_state_latest
                , FIRST_VALUE(subq_15.ds__day) OVER (
                  PARTITION BY
                    subq_18.user
                    , subq_18.ds__day
                    , subq_18.mf_internal_uuid
                  ORDER BY subq_15.ds__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS ds__day
                , FIRST_VALUE(subq_15.metric_time__day) OVER (
                  PARTITION BY
                    subq_18.user
                    , subq_18.ds__day
                    , subq_18.mf_internal_uuid
                  ORDER BY subq_15.ds__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS metric_time__day
                , FIRST_VALUE(subq_15.user) OVER (
                  PARTITION BY
                    subq_18.user
                    , subq_18.ds__day
                    , subq_18.mf_internal_uuid
                  ORDER BY subq_15.ds__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS user
                , subq_18.mf_internal_uuid AS mf_internal_uuid
                , subq_18.buys AS buys
              FROM (
                -- Pass Only Elements: ['visits', 'visit__referrer_id', 'user__home_state_latest', 'ds__day', 'metric_time__day', 'user']
                SELECT
                  subq_14.ds__day
                  , subq_14.metric_time__day
                  , subq_14.user
                  , subq_14.visit__referrer_id
                  , subq_14.user__home_state_latest
                  , subq_14.visits
                FROM (
                  -- Join Standard Outputs
                  SELECT
                    subq_11.ds__day AS ds__day
                    , subq_11.ds__week AS ds__week
                    , subq_11.ds__month AS ds__month
                    , subq_11.ds__quarter AS ds__quarter
                    , subq_11.ds__year AS ds__year
                    , subq_11.ds__extract_year AS ds__extract_year
                    , subq_11.ds__extract_quarter AS ds__extract_quarter
                    , subq_11.ds__extract_month AS ds__extract_month
                    , subq_11.ds__extract_day AS ds__extract_day
                    , subq_11.ds__extract_dow AS ds__extract_dow
                    , subq_11.ds__extract_doy AS ds__extract_doy
                    , subq_11.visit__ds__day AS visit__ds__day
                    , subq_11.visit__ds__week AS visit__ds__week
                    , subq_11.visit__ds__month AS visit__ds__month
                    , subq_11.visit__ds__quarter AS visit__ds__quarter
                    , subq_11.visit__ds__year AS visit__ds__year
                    , subq_11.visit__ds__extract_year AS visit__ds__extract_year
                    , subq_11.visit__ds__extract_quarter AS visit__ds__extract_quarter
                    , subq_11.visit__ds__extract_month AS visit__ds__extract_month
                    , subq_11.visit__ds__extract_day AS visit__ds__extract_day
                    , subq_11.visit__ds__extract_dow AS visit__ds__extract_dow
                    , subq_11.visit__ds__extract_doy AS visit__ds__extract_doy
                    , subq_11.metric_time__day AS metric_time__day
                    , subq_11.metric_time__week AS metric_time__week
                    , subq_11.metric_time__month AS metric_time__month
                    , subq_11.metric_time__quarter AS metric_time__quarter
                    , subq_11.metric_time__year AS metric_time__year
                    , subq_11.metric_time__extract_year AS metric_time__extract_year
                    , subq_11.metric_time__extract_quarter AS metric_time__extract_quarter
                    , subq_11.metric_time__extract_month AS metric_time__extract_month
                    , subq_11.metric_time__extract_day AS metric_time__extract_day
                    , subq_11.metric_time__extract_dow AS metric_time__extract_dow
                    , subq_11.metric_time__extract_doy AS metric_time__extract_doy
                    , subq_11.user AS user
                    , subq_11.session AS session
                    , subq_11.visit__user AS visit__user
                    , subq_11.visit__session AS visit__session
                    , subq_11.referrer_id AS referrer_id
                    , subq_11.visit__referrer_id AS visit__referrer_id
                    , subq_13.home_state_latest AS user__home_state_latest
                    , subq_11.visits AS visits
                    , subq_11.visitors AS visitors
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      subq_10.ds__day
                      , subq_10.ds__week
                      , subq_10.ds__month
                      , subq_10.ds__quarter
                      , subq_10.ds__year
                      , subq_10.ds__extract_year
                      , subq_10.ds__extract_quarter
                      , subq_10.ds__extract_month
                      , subq_10.ds__extract_day
                      , subq_10.ds__extract_dow
                      , subq_10.ds__extract_doy
                      , subq_10.visit__ds__day
                      , subq_10.visit__ds__week
                      , subq_10.visit__ds__month
                      , subq_10.visit__ds__quarter
                      , subq_10.visit__ds__year
                      , subq_10.visit__ds__extract_year
                      , subq_10.visit__ds__extract_quarter
                      , subq_10.visit__ds__extract_month
                      , subq_10.visit__ds__extract_day
                      , subq_10.visit__ds__extract_dow
                      , subq_10.visit__ds__extract_doy
                      , subq_10.ds__day AS metric_time__day
                      , subq_10.ds__week AS metric_time__week
                      , subq_10.ds__month AS metric_time__month
                      , subq_10.ds__quarter AS metric_time__quarter
                      , subq_10.ds__year AS metric_time__year
                      , subq_10.ds__extract_year AS metric_time__extract_year
                      , subq_10.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_10.ds__extract_month AS metric_time__extract_month
                      , subq_10.ds__extract_day AS metric_time__extract_day
                      , subq_10.ds__extract_dow AS metric_time__extract_dow
                      , subq_10.ds__extract_doy AS metric_time__extract_doy
                      , subq_10.user
                      , subq_10.session
                      , subq_10.visit__user
                      , subq_10.visit__session
                      , subq_10.referrer_id
                      , subq_10.visit__referrer_id
                      , subq_10.visits
                      , subq_10.visitors
                    FROM (
                      -- Read Elements From Semantic Model 'visits_source'
                      SELECT
                        1 AS visits
                        , visits_source_src_28000.user_id AS visitors
                        , DATE_TRUNC('day', visits_source_src_28000.ds) AS ds__day
                        , DATE_TRUNC('week', visits_source_src_28000.ds) AS ds__week
                        , DATE_TRUNC('month', visits_source_src_28000.ds) AS ds__month
                        , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS ds__quarter
                        , DATE_TRUNC('year', visits_source_src_28000.ds) AS ds__year
                        , EXTRACT(year FROM visits_source_src_28000.ds) AS ds__extract_year
                        , EXTRACT(quarter FROM visits_source_src_28000.ds) AS ds__extract_quarter
                        , EXTRACT(month FROM visits_source_src_28000.ds) AS ds__extract_month
                        , EXTRACT(day FROM visits_source_src_28000.ds) AS ds__extract_day
                        , EXTRACT(DAY_OF_WEEK FROM visits_source_src_28000.ds) AS ds__extract_dow
                        , EXTRACT(doy FROM visits_source_src_28000.ds) AS ds__extract_doy
                        , visits_source_src_28000.referrer_id
                        , DATE_TRUNC('day', visits_source_src_28000.ds) AS visit__ds__day
                        , DATE_TRUNC('week', visits_source_src_28000.ds) AS visit__ds__week
                        , DATE_TRUNC('month', visits_source_src_28000.ds) AS visit__ds__month
                        , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS visit__ds__quarter
                        , DATE_TRUNC('year', visits_source_src_28000.ds) AS visit__ds__year
                        , EXTRACT(year FROM visits_source_src_28000.ds) AS visit__ds__extract_year
                        , EXTRACT(quarter FROM visits_source_src_28000.ds) AS visit__ds__extract_quarter
                        , EXTRACT(month FROM visits_source_src_28000.ds) AS visit__ds__extract_month
                        , EXTRACT(day FROM visits_source_src_28000.ds) AS visit__ds__extract_day
                        , EXTRACT(DAY_OF_WEEK FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                        , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                        , visits_source_src_28000.referrer_id AS visit__referrer_id
                        , visits_source_src_28000.user_id AS user
                        , visits_source_src_28000.session_id AS session
                        , visits_source_src_28000.user_id AS visit__user
                        , visits_source_src_28000.session_id AS visit__session
                      FROM ***************************.fct_visits visits_source_src_28000
                    ) subq_10
                  ) subq_11
                  LEFT OUTER JOIN (
                    -- Pass Only Elements: ['home_state_latest', 'user']
                    SELECT
                      subq_12.user
                      , subq_12.home_state_latest
                    FROM (
                      -- Read Elements From Semantic Model 'users_latest'
                      SELECT
                        DATE_TRUNC('day', users_latest_src_28000.ds) AS ds_latest__day
                        , DATE_TRUNC('week', users_latest_src_28000.ds) AS ds_latest__week
                        , DATE_TRUNC('month', users_latest_src_28000.ds) AS ds_latest__month
                        , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS ds_latest__quarter
                        , DATE_TRUNC('year', users_latest_src_28000.ds) AS ds_latest__year
                        , EXTRACT(year FROM users_latest_src_28000.ds) AS ds_latest__extract_year
                        , EXTRACT(quarter FROM users_latest_src_28000.ds) AS ds_latest__extract_quarter
                        , EXTRACT(month FROM users_latest_src_28000.ds) AS ds_latest__extract_month
                        , EXTRACT(day FROM users_latest_src_28000.ds) AS ds_latest__extract_day
                        , EXTRACT(DAY_OF_WEEK FROM users_latest_src_28000.ds) AS ds_latest__extract_dow
                        , EXTRACT(doy FROM users_latest_src_28000.ds) AS ds_latest__extract_doy
                        , users_latest_src_28000.home_state_latest
                        , DATE_TRUNC('day', users_latest_src_28000.ds) AS user__ds_latest__day
                        , DATE_TRUNC('week', users_latest_src_28000.ds) AS user__ds_latest__week
                        , DATE_TRUNC('month', users_latest_src_28000.ds) AS user__ds_latest__month
                        , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS user__ds_latest__quarter
                        , DATE_TRUNC('year', users_latest_src_28000.ds) AS user__ds_latest__year
                        , EXTRACT(year FROM users_latest_src_28000.ds) AS user__ds_latest__extract_year
                        , EXTRACT(quarter FROM users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
                        , EXTRACT(month FROM users_latest_src_28000.ds) AS user__ds_latest__extract_month
                        , EXTRACT(day FROM users_latest_src_28000.ds) AS user__ds_latest__extract_day
                        , EXTRACT(DAY_OF_WEEK FROM users_latest_src_28000.ds) AS user__ds_latest__extract_dow
                        , EXTRACT(doy FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
                        , users_latest_src_28000.home_state_latest AS user__home_state_latest
                        , users_latest_src_28000.user_id AS user
                      FROM ***************************.dim_users_latest users_latest_src_28000
                    ) subq_12
                  ) subq_13
                  ON
                    subq_11.user = subq_13.user
                ) subq_14
              ) subq_15
              INNER JOIN (
                -- Add column with generated UUID
                SELECT
                  subq_17.ds__day
                  , subq_17.ds__week
                  , subq_17.ds__month
                  , subq_17.ds__quarter
                  , subq_17.ds__year
                  , subq_17.ds__extract_year
                  , subq_17.ds__extract_quarter
                  , subq_17.ds__extract_month
                  , subq_17.ds__extract_day
                  , subq_17.ds__extract_dow
                  , subq_17.ds__extract_doy
                  , subq_17.buy__ds__day
                  , subq_17.buy__ds__week
                  , subq_17.buy__ds__month
                  , subq_17.buy__ds__quarter
                  , subq_17.buy__ds__year
                  , subq_17.buy__ds__extract_year
                  , subq_17.buy__ds__extract_quarter
                  , subq_17.buy__ds__extract_month
                  , subq_17.buy__ds__extract_day
                  , subq_17.buy__ds__extract_dow
                  , subq_17.buy__ds__extract_doy
                  , subq_17.metric_time__day
                  , subq_17.metric_time__week
                  , subq_17.metric_time__month
                  , subq_17.metric_time__quarter
                  , subq_17.metric_time__year
                  , subq_17.metric_time__extract_year
                  , subq_17.metric_time__extract_quarter
                  , subq_17.metric_time__extract_month
                  , subq_17.metric_time__extract_day
                  , subq_17.metric_time__extract_dow
                  , subq_17.metric_time__extract_doy
                  , subq_17.user
                  , subq_17.session_id
                  , subq_17.buy__user
                  , subq_17.buy__session_id
                  , subq_17.buys
                  , subq_17.buyers
                  , uuid() AS mf_internal_uuid
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_16.ds__day
                    , subq_16.ds__week
                    , subq_16.ds__month
                    , subq_16.ds__quarter
                    , subq_16.ds__year
                    , subq_16.ds__extract_year
                    , subq_16.ds__extract_quarter
                    , subq_16.ds__extract_month
                    , subq_16.ds__extract_day
                    , subq_16.ds__extract_dow
                    , subq_16.ds__extract_doy
                    , subq_16.buy__ds__day
                    , subq_16.buy__ds__week
                    , subq_16.buy__ds__month
                    , subq_16.buy__ds__quarter
                    , subq_16.buy__ds__year
                    , subq_16.buy__ds__extract_year
                    , subq_16.buy__ds__extract_quarter
                    , subq_16.buy__ds__extract_month
                    , subq_16.buy__ds__extract_day
                    , subq_16.buy__ds__extract_dow
                    , subq_16.buy__ds__extract_doy
                    , subq_16.ds__day AS metric_time__day
                    , subq_16.ds__week AS metric_time__week
                    , subq_16.ds__month AS metric_time__month
                    , subq_16.ds__quarter AS metric_time__quarter
                    , subq_16.ds__year AS metric_time__year
                    , subq_16.ds__extract_year AS metric_time__extract_year
                    , subq_16.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_16.ds__extract_month AS metric_time__extract_month
                    , subq_16.ds__extract_day AS metric_time__extract_day
                    , subq_16.ds__extract_dow AS metric_time__extract_dow
                    , subq_16.ds__extract_doy AS metric_time__extract_doy
                    , subq_16.user
                    , subq_16.session_id
                    , subq_16.buy__user
                    , subq_16.buy__session_id
                    , subq_16.buys
                    , subq_16.buyers
                  FROM (
                    -- Read Elements From Semantic Model 'buys_source'
                    SELECT
                      1 AS buys
                      , buys_source_src_28000.user_id AS buyers
                      , DATE_TRUNC('day', buys_source_src_28000.ds) AS ds__day
                      , DATE_TRUNC('week', buys_source_src_28000.ds) AS ds__week
                      , DATE_TRUNC('month', buys_source_src_28000.ds) AS ds__month
                      , DATE_TRUNC('quarter', buys_source_src_28000.ds) AS ds__quarter
                      , DATE_TRUNC('year', buys_source_src_28000.ds) AS ds__year
                      , EXTRACT(year FROM buys_source_src_28000.ds) AS ds__extract_year
                      , EXTRACT(quarter FROM buys_source_src_28000.ds) AS ds__extract_quarter
                      , EXTRACT(month FROM buys_source_src_28000.ds) AS ds__extract_month
                      , EXTRACT(day FROM buys_source_src_28000.ds) AS ds__extract_day
                      , EXTRACT(DAY_OF_WEEK FROM buys_source_src_28000.ds) AS ds__extract_dow
                      , EXTRACT(doy FROM buys_source_src_28000.ds) AS ds__extract_doy
                      , DATE_TRUNC('day', buys_source_src_28000.ds) AS buy__ds__day
                      , DATE_TRUNC('week', buys_source_src_28000.ds) AS buy__ds__week
                      , DATE_TRUNC('month', buys_source_src_28000.ds) AS buy__ds__month
                      , DATE_TRUNC('quarter', buys_source_src_28000.ds) AS buy__ds__quarter
                      , DATE_TRUNC('year', buys_source_src_28000.ds) AS buy__ds__year
                      , EXTRACT(year FROM buys_source_src_28000.ds) AS buy__ds__extract_year
                      , EXTRACT(quarter FROM buys_source_src_28000.ds) AS buy__ds__extract_quarter
                      , EXTRACT(month FROM buys_source_src_28000.ds) AS buy__ds__extract_month
                      , EXTRACT(day FROM buys_source_src_28000.ds) AS buy__ds__extract_day
                      , EXTRACT(DAY_OF_WEEK FROM buys_source_src_28000.ds) AS buy__ds__extract_dow
                      , EXTRACT(doy FROM buys_source_src_28000.ds) AS buy__ds__extract_doy
                      , buys_source_src_28000.user_id AS user
                      , buys_source_src_28000.session_id
                      , buys_source_src_28000.user_id AS buy__user
                      , buys_source_src_28000.session_id AS buy__session_id
                    FROM ***************************.fct_buys buys_source_src_28000
                  ) subq_16
                ) subq_17
              ) subq_18
              ON
                (
                  subq_15.user = subq_18.user
                ) AND (
                  (
                    subq_15.ds__day <= subq_18.ds__day
                  ) AND (
                    subq_15.ds__day > DATE_ADD('day', -7, subq_18.ds__day)
                  )
                )
            ) subq_19
          ) subq_20
        ) subq_21
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['home_state_latest', 'user']
          SELECT
            subq_22.user
            , subq_22.home_state_latest
          FROM (
            -- Read Elements From Semantic Model 'users_latest'
            SELECT
              DATE_TRUNC('day', users_latest_src_28000.ds) AS ds_latest__day
              , DATE_TRUNC('week', users_latest_src_28000.ds) AS ds_latest__week
              , DATE_TRUNC('month', users_latest_src_28000.ds) AS ds_latest__month
              , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS ds_latest__quarter
              , DATE_TRUNC('year', users_latest_src_28000.ds) AS ds_latest__year
              , EXTRACT(year FROM users_latest_src_28000.ds) AS ds_latest__extract_year
              , EXTRACT(quarter FROM users_latest_src_28000.ds) AS ds_latest__extract_quarter
              , EXTRACT(month FROM users_latest_src_28000.ds) AS ds_latest__extract_month
              , EXTRACT(day FROM users_latest_src_28000.ds) AS ds_latest__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_latest_src_28000.ds) AS ds_latest__extract_dow
              , EXTRACT(doy FROM users_latest_src_28000.ds) AS ds_latest__extract_doy
              , users_latest_src_28000.home_state_latest
              , DATE_TRUNC('day', users_latest_src_28000.ds) AS user__ds_latest__day
              , DATE_TRUNC('week', users_latest_src_28000.ds) AS user__ds_latest__week
              , DATE_TRUNC('month', users_latest_src_28000.ds) AS user__ds_latest__month
              , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS user__ds_latest__quarter
              , DATE_TRUNC('year', users_latest_src_28000.ds) AS user__ds_latest__year
              , EXTRACT(year FROM users_latest_src_28000.ds) AS user__ds_latest__extract_year
              , EXTRACT(quarter FROM users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
              , EXTRACT(month FROM users_latest_src_28000.ds) AS user__ds_latest__extract_month
              , EXTRACT(day FROM users_latest_src_28000.ds) AS user__ds_latest__extract_day
              , EXTRACT(DAY_OF_WEEK FROM users_latest_src_28000.ds) AS user__ds_latest__extract_dow
              , EXTRACT(doy FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
              , users_latest_src_28000.home_state_latest AS user__home_state_latest
              , users_latest_src_28000.user_id AS user
            FROM ***************************.dim_users_latest users_latest_src_28000
          ) subq_22
        ) subq_23
        ON
          subq_21.user = subq_23.user
      ) subq_24
    ) subq_25
    GROUP BY
      subq_25.metric_time__day
      , subq_25.user__home_state_latest
  ) subq_26
  ON
    (
      subq_9.user__home_state_latest = subq_26.user__home_state_latest
    ) AND (
      subq_9.metric_time__day = subq_26.metric_time__day
    )
  GROUP BY
    COALESCE(subq_9.metric_time__day, subq_26.metric_time__day)
    , COALESCE(subq_9.user__home_state_latest, subq_26.user__home_state_latest)
) subq_27
