test_name: test_conversion_metric_query_filters
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a simple predicate on a conversion metric.
sql_engine: Postgres
---
-- Write to DataTable
SELECT
  subq_25.metric_time__day
  , subq_25.user__home_state_latest
  , subq_25.visit_buy_conversion_rate_7days
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_24.metric_time__day
    , subq_24.user__home_state_latest
    , CAST(subq_24.__buys AS DOUBLE PRECISION) / CAST(NULLIF(subq_24.__visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate_7days
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_8.metric_time__day, subq_23.metric_time__day) AS metric_time__day
      , COALESCE(subq_8.user__home_state_latest, subq_23.user__home_state_latest) AS user__home_state_latest
      , MAX(subq_8.__visits) AS __visits
      , MAX(subq_23.__buys) AS __buys
    FROM (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_7.metric_time__day
        , subq_7.user__home_state_latest
        , SUM(subq_7.__visits) AS __visits
      FROM (
        -- Pass Only Elements: ['__visits', 'user__home_state_latest', 'metric_time__day']
        SELECT
          subq_6.metric_time__day
          , subq_6.user__home_state_latest
          , subq_6.__visits
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_5.visits AS __visits
            , subq_5.visit__referrer_id
            , subq_5.user__home_state_latest
            , subq_5.metric_time__day
          FROM (
            -- Pass Only Elements: ['__visits', 'user__home_state_latest', 'visit__referrer_id', 'metric_time__day']
            SELECT
              subq_4.metric_time__day
              , subq_4.visit__referrer_id
              , subq_4.user__home_state_latest
              , subq_4.__visits AS visits
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_3.home_state_latest AS user__home_state_latest
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
                , subq_1.visit__ds__day AS visit__ds__day
                , subq_1.visit__ds__week AS visit__ds__week
                , subq_1.visit__ds__month AS visit__ds__month
                , subq_1.visit__ds__quarter AS visit__ds__quarter
                , subq_1.visit__ds__year AS visit__ds__year
                , subq_1.visit__ds__extract_year AS visit__ds__extract_year
                , subq_1.visit__ds__extract_quarter AS visit__ds__extract_quarter
                , subq_1.visit__ds__extract_month AS visit__ds__extract_month
                , subq_1.visit__ds__extract_day AS visit__ds__extract_day
                , subq_1.visit__ds__extract_dow AS visit__ds__extract_dow
                , subq_1.visit__ds__extract_doy AS visit__ds__extract_doy
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
                , subq_1.session AS session
                , subq_1.visit__user AS visit__user
                , subq_1.visit__session AS visit__session
                , subq_1.referrer_id AS referrer_id
                , subq_1.visit__referrer_id AS visit__referrer_id
                , subq_1.__visits AS __visits
                , subq_1.__visits_fill_nulls_with_0_join_to_timespine AS __visits_fill_nulls_with_0_join_to_timespine
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
                  , subq_0.__visits
                  , subq_0.__visits_fill_nulls_with_0_join_to_timespine
                FROM (
                  -- Read Elements From Semantic Model 'visits_source'
                  SELECT
                    1 AS __visits
                    , 1 AS __visits_fill_nulls_with_0_join_to_timespine
                    , DATE_TRUNC('day', visits_source_src_28000.ds) AS ds__day
                    , DATE_TRUNC('week', visits_source_src_28000.ds) AS ds__week
                    , DATE_TRUNC('month', visits_source_src_28000.ds) AS ds__month
                    , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS ds__quarter
                    , DATE_TRUNC('year', visits_source_src_28000.ds) AS ds__year
                    , EXTRACT(year FROM visits_source_src_28000.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM visits_source_src_28000.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM visits_source_src_28000.ds) AS ds__extract_month
                    , EXTRACT(day FROM visits_source_src_28000.ds) AS ds__extract_day
                    , EXTRACT(isodow FROM visits_source_src_28000.ds) AS ds__extract_dow
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
                    , EXTRACT(isodow FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                    , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                    , visits_source_src_28000.referrer_id AS visit__referrer_id
                    , visits_source_src_28000.user_id AS user
                    , visits_source_src_28000.session_id AS session
                    , visits_source_src_28000.user_id AS visit__user
                    , visits_source_src_28000.session_id AS visit__session
                  FROM ***************************.fct_visits visits_source_src_28000
                ) subq_0
              ) subq_1
              LEFT OUTER JOIN (
                -- Pass Only Elements: ['home_state_latest', 'user']
                SELECT
                  subq_2.user
                  , subq_2.home_state_latest
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
                    , EXTRACT(isodow FROM users_latest_src_28000.ds) AS ds_latest__extract_dow
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
                    , EXTRACT(isodow FROM users_latest_src_28000.ds) AS user__ds_latest__extract_dow
                    , EXTRACT(doy FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
                    , users_latest_src_28000.home_state_latest AS user__home_state_latest
                    , users_latest_src_28000.user_id AS user
                  FROM ***************************.dim_users_latest users_latest_src_28000
                ) subq_2
              ) subq_3
              ON
                subq_1.user = subq_3.user
            ) subq_4
          ) subq_5
          WHERE visit__referrer_id = '123456'
        ) subq_6
      ) subq_7
      GROUP BY
        subq_7.metric_time__day
        , subq_7.user__home_state_latest
    ) subq_8
    FULL OUTER JOIN (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_22.metric_time__day
        , subq_22.user__home_state_latest
        , SUM(subq_22.__buys) AS __buys
      FROM (
        -- Pass Only Elements: ['__buys', 'user__home_state_latest', 'metric_time__day']
        SELECT
          subq_21.metric_time__day
          , subq_21.user__home_state_latest
          , subq_21.__buys
        FROM (
          -- Pass Only Elements: ['__buys', 'user__home_state_latest', 'metric_time__day']
          SELECT
            subq_20.metric_time__day
            , subq_20.user__home_state_latest
            , subq_20.__buys
          FROM (
            -- Find conversions for user within the range of 7 day
            SELECT
              subq_19.metric_time__day
              , subq_19.user
              , subq_19.visit__referrer_id
              , subq_19.user__home_state_latest
              , subq_19.__buys
              , subq_19.__visits
            FROM (
              -- Dedupe the fanout with mf_internal_uuid in the conversion data set
              SELECT DISTINCT
                FIRST_VALUE(subq_15.__visits) OVER (
                  PARTITION BY
                    subq_18.user
                    , subq_18.metric_time__day
                    , subq_18.mf_internal_uuid
                  ORDER BY subq_15.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS __visits
                , FIRST_VALUE(subq_15.visit__referrer_id) OVER (
                  PARTITION BY
                    subq_18.user
                    , subq_18.metric_time__day
                    , subq_18.mf_internal_uuid
                  ORDER BY subq_15.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS visit__referrer_id
                , FIRST_VALUE(subq_15.user__home_state_latest) OVER (
                  PARTITION BY
                    subq_18.user
                    , subq_18.metric_time__day
                    , subq_18.mf_internal_uuid
                  ORDER BY subq_15.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS user__home_state_latest
                , FIRST_VALUE(subq_15.metric_time__day) OVER (
                  PARTITION BY
                    subq_18.user
                    , subq_18.metric_time__day
                    , subq_18.mf_internal_uuid
                  ORDER BY subq_15.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS metric_time__day
                , FIRST_VALUE(subq_15.user) OVER (
                  PARTITION BY
                    subq_18.user
                    , subq_18.metric_time__day
                    , subq_18.mf_internal_uuid
                  ORDER BY subq_15.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS user
                , subq_18.mf_internal_uuid AS mf_internal_uuid
                , subq_18.__buys AS __buys
              FROM (
                -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'user__home_state_latest', 'metric_time__day', 'user']
                SELECT
                  subq_14.metric_time__day
                  , subq_14.user
                  , subq_14.visit__referrer_id
                  , subq_14.user__home_state_latest
                  , subq_14.__visits
                FROM (
                  -- Constrain Output with WHERE
                  SELECT
                    subq_13.visits AS __visits
                    , subq_13.visit__referrer_id
                    , subq_13.user__home_state_latest
                    , subq_13.metric_time__day
                    , subq_13.user
                  FROM (
                    -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'user__home_state_latest', 'metric_time__day', 'user']
                    SELECT
                      subq_12.metric_time__day
                      , subq_12.user
                      , subq_12.visit__referrer_id
                      , subq_12.user__home_state_latest
                      , subq_12.__visits AS visits
                    FROM (
                      -- Join Standard Outputs
                      SELECT
                        subq_11.home_state_latest AS user__home_state_latest
                        , subq_9.ds__day AS ds__day
                        , subq_9.ds__week AS ds__week
                        , subq_9.ds__month AS ds__month
                        , subq_9.ds__quarter AS ds__quarter
                        , subq_9.ds__year AS ds__year
                        , subq_9.ds__extract_year AS ds__extract_year
                        , subq_9.ds__extract_quarter AS ds__extract_quarter
                        , subq_9.ds__extract_month AS ds__extract_month
                        , subq_9.ds__extract_day AS ds__extract_day
                        , subq_9.ds__extract_dow AS ds__extract_dow
                        , subq_9.ds__extract_doy AS ds__extract_doy
                        , subq_9.visit__ds__day AS visit__ds__day
                        , subq_9.visit__ds__week AS visit__ds__week
                        , subq_9.visit__ds__month AS visit__ds__month
                        , subq_9.visit__ds__quarter AS visit__ds__quarter
                        , subq_9.visit__ds__year AS visit__ds__year
                        , subq_9.visit__ds__extract_year AS visit__ds__extract_year
                        , subq_9.visit__ds__extract_quarter AS visit__ds__extract_quarter
                        , subq_9.visit__ds__extract_month AS visit__ds__extract_month
                        , subq_9.visit__ds__extract_day AS visit__ds__extract_day
                        , subq_9.visit__ds__extract_dow AS visit__ds__extract_dow
                        , subq_9.visit__ds__extract_doy AS visit__ds__extract_doy
                        , subq_9.metric_time__day AS metric_time__day
                        , subq_9.metric_time__week AS metric_time__week
                        , subq_9.metric_time__month AS metric_time__month
                        , subq_9.metric_time__quarter AS metric_time__quarter
                        , subq_9.metric_time__year AS metric_time__year
                        , subq_9.metric_time__extract_year AS metric_time__extract_year
                        , subq_9.metric_time__extract_quarter AS metric_time__extract_quarter
                        , subq_9.metric_time__extract_month AS metric_time__extract_month
                        , subq_9.metric_time__extract_day AS metric_time__extract_day
                        , subq_9.metric_time__extract_dow AS metric_time__extract_dow
                        , subq_9.metric_time__extract_doy AS metric_time__extract_doy
                        , subq_9.user AS user
                        , subq_9.session AS session
                        , subq_9.visit__user AS visit__user
                        , subq_9.visit__session AS visit__session
                        , subq_9.referrer_id AS referrer_id
                        , subq_9.visit__referrer_id AS visit__referrer_id
                        , subq_9.__visits AS __visits
                        , subq_9.__visits_fill_nulls_with_0_join_to_timespine AS __visits_fill_nulls_with_0_join_to_timespine
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
                          , subq_0.__visits
                          , subq_0.__visits_fill_nulls_with_0_join_to_timespine
                        FROM (
                          -- Read Elements From Semantic Model 'visits_source'
                          SELECT
                            1 AS __visits
                            , 1 AS __visits_fill_nulls_with_0_join_to_timespine
                            , DATE_TRUNC('day', visits_source_src_28000.ds) AS ds__day
                            , DATE_TRUNC('week', visits_source_src_28000.ds) AS ds__week
                            , DATE_TRUNC('month', visits_source_src_28000.ds) AS ds__month
                            , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS ds__quarter
                            , DATE_TRUNC('year', visits_source_src_28000.ds) AS ds__year
                            , EXTRACT(year FROM visits_source_src_28000.ds) AS ds__extract_year
                            , EXTRACT(quarter FROM visits_source_src_28000.ds) AS ds__extract_quarter
                            , EXTRACT(month FROM visits_source_src_28000.ds) AS ds__extract_month
                            , EXTRACT(day FROM visits_source_src_28000.ds) AS ds__extract_day
                            , EXTRACT(isodow FROM visits_source_src_28000.ds) AS ds__extract_dow
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
                            , EXTRACT(isodow FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                            , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                            , visits_source_src_28000.referrer_id AS visit__referrer_id
                            , visits_source_src_28000.user_id AS user
                            , visits_source_src_28000.session_id AS session
                            , visits_source_src_28000.user_id AS visit__user
                            , visits_source_src_28000.session_id AS visit__session
                          FROM ***************************.fct_visits visits_source_src_28000
                        ) subq_0
                      ) subq_9
                      LEFT OUTER JOIN (
                        -- Pass Only Elements: ['home_state_latest', 'user']
                        SELECT
                          subq_10.user
                          , subq_10.home_state_latest
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
                            , EXTRACT(isodow FROM users_latest_src_28000.ds) AS ds_latest__extract_dow
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
                            , EXTRACT(isodow FROM users_latest_src_28000.ds) AS user__ds_latest__extract_dow
                            , EXTRACT(doy FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
                            , users_latest_src_28000.home_state_latest AS user__home_state_latest
                            , users_latest_src_28000.user_id AS user
                          FROM ***************************.dim_users_latest users_latest_src_28000
                        ) subq_10
                      ) subq_11
                      ON
                        subq_9.user = subq_11.user
                    ) subq_12
                  ) subq_13
                  WHERE visit__referrer_id = '123456'
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
                  , subq_17.ds_month__month
                  , subq_17.ds_month__quarter
                  , subq_17.ds_month__year
                  , subq_17.ds_month__extract_year
                  , subq_17.ds_month__extract_quarter
                  , subq_17.ds_month__extract_month
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
                  , subq_17.buy__ds_month__month
                  , subq_17.buy__ds_month__quarter
                  , subq_17.buy__ds_month__year
                  , subq_17.buy__ds_month__extract_year
                  , subq_17.buy__ds_month__extract_quarter
                  , subq_17.buy__ds_month__extract_month
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
                  , subq_17.__buys
                  , subq_17.__buys_fill_nulls_with_0
                  , subq_17.__buys_fill_nulls_with_0_join_to_timespine
                  , GEN_RANDOM_UUID() AS mf_internal_uuid
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
                    , subq_16.ds_month__month
                    , subq_16.ds_month__quarter
                    , subq_16.ds_month__year
                    , subq_16.ds_month__extract_year
                    , subq_16.ds_month__extract_quarter
                    , subq_16.ds_month__extract_month
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
                    , subq_16.buy__ds_month__month
                    , subq_16.buy__ds_month__quarter
                    , subq_16.buy__ds_month__year
                    , subq_16.buy__ds_month__extract_year
                    , subq_16.buy__ds_month__extract_quarter
                    , subq_16.buy__ds_month__extract_month
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
                    , subq_16.__buys
                    , subq_16.__buys_fill_nulls_with_0
                    , subq_16.__buys_fill_nulls_with_0_join_to_timespine
                  FROM (
                    -- Read Elements From Semantic Model 'buys_source'
                    SELECT
                      1 AS __buys
                      , 1 AS __buys_fill_nulls_with_0
                      , 1 AS __buys_fill_nulls_with_0_join_to_timespine
                      , 1 AS __buys_month
                      , DATE_TRUNC('day', buys_source_src_28000.ds) AS ds__day
                      , DATE_TRUNC('week', buys_source_src_28000.ds) AS ds__week
                      , DATE_TRUNC('month', buys_source_src_28000.ds) AS ds__month
                      , DATE_TRUNC('quarter', buys_source_src_28000.ds) AS ds__quarter
                      , DATE_TRUNC('year', buys_source_src_28000.ds) AS ds__year
                      , EXTRACT(year FROM buys_source_src_28000.ds) AS ds__extract_year
                      , EXTRACT(quarter FROM buys_source_src_28000.ds) AS ds__extract_quarter
                      , EXTRACT(month FROM buys_source_src_28000.ds) AS ds__extract_month
                      , EXTRACT(day FROM buys_source_src_28000.ds) AS ds__extract_day
                      , EXTRACT(isodow FROM buys_source_src_28000.ds) AS ds__extract_dow
                      , EXTRACT(doy FROM buys_source_src_28000.ds) AS ds__extract_doy
                      , DATE_TRUNC('month', buys_source_src_28000.ds_month) AS ds_month__month
                      , DATE_TRUNC('quarter', buys_source_src_28000.ds_month) AS ds_month__quarter
                      , DATE_TRUNC('year', buys_source_src_28000.ds_month) AS ds_month__year
                      , EXTRACT(year FROM buys_source_src_28000.ds_month) AS ds_month__extract_year
                      , EXTRACT(quarter FROM buys_source_src_28000.ds_month) AS ds_month__extract_quarter
                      , EXTRACT(month FROM buys_source_src_28000.ds_month) AS ds_month__extract_month
                      , DATE_TRUNC('day', buys_source_src_28000.ds) AS buy__ds__day
                      , DATE_TRUNC('week', buys_source_src_28000.ds) AS buy__ds__week
                      , DATE_TRUNC('month', buys_source_src_28000.ds) AS buy__ds__month
                      , DATE_TRUNC('quarter', buys_source_src_28000.ds) AS buy__ds__quarter
                      , DATE_TRUNC('year', buys_source_src_28000.ds) AS buy__ds__year
                      , EXTRACT(year FROM buys_source_src_28000.ds) AS buy__ds__extract_year
                      , EXTRACT(quarter FROM buys_source_src_28000.ds) AS buy__ds__extract_quarter
                      , EXTRACT(month FROM buys_source_src_28000.ds) AS buy__ds__extract_month
                      , EXTRACT(day FROM buys_source_src_28000.ds) AS buy__ds__extract_day
                      , EXTRACT(isodow FROM buys_source_src_28000.ds) AS buy__ds__extract_dow
                      , EXTRACT(doy FROM buys_source_src_28000.ds) AS buy__ds__extract_doy
                      , DATE_TRUNC('month', buys_source_src_28000.ds_month) AS buy__ds_month__month
                      , DATE_TRUNC('quarter', buys_source_src_28000.ds_month) AS buy__ds_month__quarter
                      , DATE_TRUNC('year', buys_source_src_28000.ds_month) AS buy__ds_month__year
                      , EXTRACT(year FROM buys_source_src_28000.ds_month) AS buy__ds_month__extract_year
                      , EXTRACT(quarter FROM buys_source_src_28000.ds_month) AS buy__ds_month__extract_quarter
                      , EXTRACT(month FROM buys_source_src_28000.ds_month) AS buy__ds_month__extract_month
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
                    subq_15.metric_time__day <= subq_18.metric_time__day
                  ) AND (
                    subq_15.metric_time__day > subq_18.metric_time__day - MAKE_INTERVAL(days => 7)
                  )
                )
            ) subq_19
          ) subq_20
        ) subq_21
      ) subq_22
      GROUP BY
        subq_22.metric_time__day
        , subq_22.user__home_state_latest
    ) subq_23
    ON
      (
        subq_8.user__home_state_latest = subq_23.user__home_state_latest
      ) AND (
        subq_8.metric_time__day = subq_23.metric_time__day
      )
    GROUP BY
      COALESCE(subq_8.metric_time__day, subq_23.metric_time__day)
      , COALESCE(subq_8.user__home_state_latest, subq_23.user__home_state_latest)
  ) subq_24
) subq_25
