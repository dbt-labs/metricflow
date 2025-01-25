test_name: test_conversion_metric_query_filters
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a simple predicate on a conversion metric.
sql_engine: Databricks
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_19.metric_time__day
  , nr_subq_19.user__home_state_latest
  , CAST(nr_subq_19.buys AS DOUBLE) / CAST(NULLIF(nr_subq_19.visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_6.metric_time__day, nr_subq_18.metric_time__day) AS metric_time__day
    , COALESCE(nr_subq_6.user__home_state_latest, nr_subq_18.user__home_state_latest) AS user__home_state_latest
    , MAX(nr_subq_6.visits) AS visits
    , MAX(nr_subq_18.buys) AS buys
  FROM (
    -- Aggregate Measures
    SELECT
      nr_subq_5.metric_time__day
      , nr_subq_5.user__home_state_latest
      , SUM(nr_subq_5.visits) AS visits
    FROM (
      -- Pass Only Elements: ['visits', 'user__home_state_latest', 'metric_time__day']
      SELECT
        nr_subq_4.metric_time__day
        , nr_subq_4.user__home_state_latest
        , nr_subq_4.visits
      FROM (
        -- Constrain Output with WHERE
        SELECT
          nr_subq_3.ds__day
          , nr_subq_3.ds__week
          , nr_subq_3.ds__month
          , nr_subq_3.ds__quarter
          , nr_subq_3.ds__year
          , nr_subq_3.ds__extract_year
          , nr_subq_3.ds__extract_quarter
          , nr_subq_3.ds__extract_month
          , nr_subq_3.ds__extract_day
          , nr_subq_3.ds__extract_dow
          , nr_subq_3.ds__extract_doy
          , nr_subq_3.visit__ds__day
          , nr_subq_3.visit__ds__week
          , nr_subq_3.visit__ds__month
          , nr_subq_3.visit__ds__quarter
          , nr_subq_3.visit__ds__year
          , nr_subq_3.visit__ds__extract_year
          , nr_subq_3.visit__ds__extract_quarter
          , nr_subq_3.visit__ds__extract_month
          , nr_subq_3.visit__ds__extract_day
          , nr_subq_3.visit__ds__extract_dow
          , nr_subq_3.visit__ds__extract_doy
          , nr_subq_3.metric_time__day
          , nr_subq_3.metric_time__week
          , nr_subq_3.metric_time__month
          , nr_subq_3.metric_time__quarter
          , nr_subq_3.metric_time__year
          , nr_subq_3.metric_time__extract_year
          , nr_subq_3.metric_time__extract_quarter
          , nr_subq_3.metric_time__extract_month
          , nr_subq_3.metric_time__extract_day
          , nr_subq_3.metric_time__extract_dow
          , nr_subq_3.metric_time__extract_doy
          , nr_subq_3.user
          , nr_subq_3.session
          , nr_subq_3.visit__user
          , nr_subq_3.visit__session
          , nr_subq_3.referrer_id
          , nr_subq_3.visit__referrer_id
          , nr_subq_3.user__home_state_latest
          , nr_subq_3.visits
          , nr_subq_3.visitors
        FROM (
          -- Join Standard Outputs
          SELECT
            nr_subq_2.home_state_latest AS user__home_state_latest
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
            , nr_subq_0.visit__ds__day AS visit__ds__day
            , nr_subq_0.visit__ds__week AS visit__ds__week
            , nr_subq_0.visit__ds__month AS visit__ds__month
            , nr_subq_0.visit__ds__quarter AS visit__ds__quarter
            , nr_subq_0.visit__ds__year AS visit__ds__year
            , nr_subq_0.visit__ds__extract_year AS visit__ds__extract_year
            , nr_subq_0.visit__ds__extract_quarter AS visit__ds__extract_quarter
            , nr_subq_0.visit__ds__extract_month AS visit__ds__extract_month
            , nr_subq_0.visit__ds__extract_day AS visit__ds__extract_day
            , nr_subq_0.visit__ds__extract_dow AS visit__ds__extract_dow
            , nr_subq_0.visit__ds__extract_doy AS visit__ds__extract_doy
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
            , nr_subq_0.user AS user
            , nr_subq_0.session AS session
            , nr_subq_0.visit__user AS visit__user
            , nr_subq_0.visit__session AS visit__session
            , nr_subq_0.referrer_id AS referrer_id
            , nr_subq_0.visit__referrer_id AS visit__referrer_id
            , nr_subq_0.visits AS visits
            , nr_subq_0.visitors AS visitors
          FROM (
            -- Metric Time Dimension 'ds'
            SELECT
              nr_subq_28012.ds__day
              , nr_subq_28012.ds__week
              , nr_subq_28012.ds__month
              , nr_subq_28012.ds__quarter
              , nr_subq_28012.ds__year
              , nr_subq_28012.ds__extract_year
              , nr_subq_28012.ds__extract_quarter
              , nr_subq_28012.ds__extract_month
              , nr_subq_28012.ds__extract_day
              , nr_subq_28012.ds__extract_dow
              , nr_subq_28012.ds__extract_doy
              , nr_subq_28012.visit__ds__day
              , nr_subq_28012.visit__ds__week
              , nr_subq_28012.visit__ds__month
              , nr_subq_28012.visit__ds__quarter
              , nr_subq_28012.visit__ds__year
              , nr_subq_28012.visit__ds__extract_year
              , nr_subq_28012.visit__ds__extract_quarter
              , nr_subq_28012.visit__ds__extract_month
              , nr_subq_28012.visit__ds__extract_day
              , nr_subq_28012.visit__ds__extract_dow
              , nr_subq_28012.visit__ds__extract_doy
              , nr_subq_28012.ds__day AS metric_time__day
              , nr_subq_28012.ds__week AS metric_time__week
              , nr_subq_28012.ds__month AS metric_time__month
              , nr_subq_28012.ds__quarter AS metric_time__quarter
              , nr_subq_28012.ds__year AS metric_time__year
              , nr_subq_28012.ds__extract_year AS metric_time__extract_year
              , nr_subq_28012.ds__extract_quarter AS metric_time__extract_quarter
              , nr_subq_28012.ds__extract_month AS metric_time__extract_month
              , nr_subq_28012.ds__extract_day AS metric_time__extract_day
              , nr_subq_28012.ds__extract_dow AS metric_time__extract_dow
              , nr_subq_28012.ds__extract_doy AS metric_time__extract_doy
              , nr_subq_28012.user
              , nr_subq_28012.session
              , nr_subq_28012.visit__user
              , nr_subq_28012.visit__session
              , nr_subq_28012.referrer_id
              , nr_subq_28012.visit__referrer_id
              , nr_subq_28012.visits
              , nr_subq_28012.visitors
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
                , EXTRACT(DAYOFWEEK_ISO FROM visits_source_src_28000.ds) AS ds__extract_dow
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
                , EXTRACT(DAYOFWEEK_ISO FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                , visits_source_src_28000.referrer_id AS visit__referrer_id
                , visits_source_src_28000.user_id AS user
                , visits_source_src_28000.session_id AS session
                , visits_source_src_28000.user_id AS visit__user
                , visits_source_src_28000.session_id AS visit__session
              FROM ***************************.fct_visits visits_source_src_28000
            ) nr_subq_28012
          ) nr_subq_0
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['home_state_latest', 'user']
            SELECT
              nr_subq_1.user
              , nr_subq_1.home_state_latest
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
                , EXTRACT(DAYOFWEEK_ISO FROM users_latest_src_28000.ds) AS ds_latest__extract_dow
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
                , EXTRACT(DAYOFWEEK_ISO FROM users_latest_src_28000.ds) AS user__ds_latest__extract_dow
                , EXTRACT(doy FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
                , users_latest_src_28000.home_state_latest AS user__home_state_latest
                , users_latest_src_28000.user_id AS user
              FROM ***************************.dim_users_latest users_latest_src_28000
            ) nr_subq_1
          ) nr_subq_2
          ON
            nr_subq_0.user = nr_subq_2.user
        ) nr_subq_3
        WHERE visit__referrer_id = '123456'
      ) nr_subq_4
    ) nr_subq_5
    GROUP BY
      nr_subq_5.metric_time__day
      , nr_subq_5.user__home_state_latest
  ) nr_subq_6
  FULL OUTER JOIN (
    -- Aggregate Measures
    SELECT
      nr_subq_17.metric_time__day
      , nr_subq_17.user__home_state_latest
      , SUM(nr_subq_17.buys) AS buys
    FROM (
      -- Pass Only Elements: ['buys', 'user__home_state_latest', 'metric_time__day']
      SELECT
        nr_subq_16.metric_time__day
        , nr_subq_16.user__home_state_latest
        , nr_subq_16.buys
      FROM (
        -- Find conversions for user within the range of 7 day
        SELECT
          nr_subq_15.metric_time__day
          , nr_subq_15.user
          , nr_subq_15.visit__referrer_id
          , nr_subq_15.user__home_state_latest
          , nr_subq_15.buys
          , nr_subq_15.visits
        FROM (
          -- Dedupe the fanout with mf_internal_uuid in the conversion data set
          SELECT DISTINCT
            FIRST_VALUE(nr_subq_12.visits) OVER (
              PARTITION BY
                nr_subq_14.user
                , nr_subq_14.metric_time__day
                , nr_subq_14.mf_internal_uuid
              ORDER BY nr_subq_12.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visits
            , FIRST_VALUE(nr_subq_12.visit__referrer_id) OVER (
              PARTITION BY
                nr_subq_14.user
                , nr_subq_14.metric_time__day
                , nr_subq_14.mf_internal_uuid
              ORDER BY nr_subq_12.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visit__referrer_id
            , FIRST_VALUE(nr_subq_12.user__home_state_latest) OVER (
              PARTITION BY
                nr_subq_14.user
                , nr_subq_14.metric_time__day
                , nr_subq_14.mf_internal_uuid
              ORDER BY nr_subq_12.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS user__home_state_latest
            , FIRST_VALUE(nr_subq_12.metric_time__day) OVER (
              PARTITION BY
                nr_subq_14.user
                , nr_subq_14.metric_time__day
                , nr_subq_14.mf_internal_uuid
              ORDER BY nr_subq_12.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS metric_time__day
            , FIRST_VALUE(nr_subq_12.user) OVER (
              PARTITION BY
                nr_subq_14.user
                , nr_subq_14.metric_time__day
                , nr_subq_14.mf_internal_uuid
              ORDER BY nr_subq_12.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS user
            , nr_subq_14.mf_internal_uuid AS mf_internal_uuid
            , nr_subq_14.buys AS buys
          FROM (
            -- Pass Only Elements: ['visits', 'visit__referrer_id', 'user__home_state_latest', 'metric_time__day', 'user']
            SELECT
              nr_subq_11.metric_time__day
              , nr_subq_11.user
              , nr_subq_11.visit__referrer_id
              , nr_subq_11.user__home_state_latest
              , nr_subq_11.visits
            FROM (
              -- Constrain Output with WHERE
              SELECT
                nr_subq_10.ds__day
                , nr_subq_10.ds__week
                , nr_subq_10.ds__month
                , nr_subq_10.ds__quarter
                , nr_subq_10.ds__year
                , nr_subq_10.ds__extract_year
                , nr_subq_10.ds__extract_quarter
                , nr_subq_10.ds__extract_month
                , nr_subq_10.ds__extract_day
                , nr_subq_10.ds__extract_dow
                , nr_subq_10.ds__extract_doy
                , nr_subq_10.visit__ds__day
                , nr_subq_10.visit__ds__week
                , nr_subq_10.visit__ds__month
                , nr_subq_10.visit__ds__quarter
                , nr_subq_10.visit__ds__year
                , nr_subq_10.visit__ds__extract_year
                , nr_subq_10.visit__ds__extract_quarter
                , nr_subq_10.visit__ds__extract_month
                , nr_subq_10.visit__ds__extract_day
                , nr_subq_10.visit__ds__extract_dow
                , nr_subq_10.visit__ds__extract_doy
                , nr_subq_10.metric_time__day
                , nr_subq_10.metric_time__week
                , nr_subq_10.metric_time__month
                , nr_subq_10.metric_time__quarter
                , nr_subq_10.metric_time__year
                , nr_subq_10.metric_time__extract_year
                , nr_subq_10.metric_time__extract_quarter
                , nr_subq_10.metric_time__extract_month
                , nr_subq_10.metric_time__extract_day
                , nr_subq_10.metric_time__extract_dow
                , nr_subq_10.metric_time__extract_doy
                , nr_subq_10.user
                , nr_subq_10.session
                , nr_subq_10.visit__user
                , nr_subq_10.visit__session
                , nr_subq_10.referrer_id
                , nr_subq_10.visit__referrer_id
                , nr_subq_10.user__home_state_latest
                , nr_subq_10.visits
                , nr_subq_10.visitors
              FROM (
                -- Join Standard Outputs
                SELECT
                  nr_subq_9.home_state_latest AS user__home_state_latest
                  , nr_subq_7.ds__day AS ds__day
                  , nr_subq_7.ds__week AS ds__week
                  , nr_subq_7.ds__month AS ds__month
                  , nr_subq_7.ds__quarter AS ds__quarter
                  , nr_subq_7.ds__year AS ds__year
                  , nr_subq_7.ds__extract_year AS ds__extract_year
                  , nr_subq_7.ds__extract_quarter AS ds__extract_quarter
                  , nr_subq_7.ds__extract_month AS ds__extract_month
                  , nr_subq_7.ds__extract_day AS ds__extract_day
                  , nr_subq_7.ds__extract_dow AS ds__extract_dow
                  , nr_subq_7.ds__extract_doy AS ds__extract_doy
                  , nr_subq_7.visit__ds__day AS visit__ds__day
                  , nr_subq_7.visit__ds__week AS visit__ds__week
                  , nr_subq_7.visit__ds__month AS visit__ds__month
                  , nr_subq_7.visit__ds__quarter AS visit__ds__quarter
                  , nr_subq_7.visit__ds__year AS visit__ds__year
                  , nr_subq_7.visit__ds__extract_year AS visit__ds__extract_year
                  , nr_subq_7.visit__ds__extract_quarter AS visit__ds__extract_quarter
                  , nr_subq_7.visit__ds__extract_month AS visit__ds__extract_month
                  , nr_subq_7.visit__ds__extract_day AS visit__ds__extract_day
                  , nr_subq_7.visit__ds__extract_dow AS visit__ds__extract_dow
                  , nr_subq_7.visit__ds__extract_doy AS visit__ds__extract_doy
                  , nr_subq_7.metric_time__day AS metric_time__day
                  , nr_subq_7.metric_time__week AS metric_time__week
                  , nr_subq_7.metric_time__month AS metric_time__month
                  , nr_subq_7.metric_time__quarter AS metric_time__quarter
                  , nr_subq_7.metric_time__year AS metric_time__year
                  , nr_subq_7.metric_time__extract_year AS metric_time__extract_year
                  , nr_subq_7.metric_time__extract_quarter AS metric_time__extract_quarter
                  , nr_subq_7.metric_time__extract_month AS metric_time__extract_month
                  , nr_subq_7.metric_time__extract_day AS metric_time__extract_day
                  , nr_subq_7.metric_time__extract_dow AS metric_time__extract_dow
                  , nr_subq_7.metric_time__extract_doy AS metric_time__extract_doy
                  , nr_subq_7.user AS user
                  , nr_subq_7.session AS session
                  , nr_subq_7.visit__user AS visit__user
                  , nr_subq_7.visit__session AS visit__session
                  , nr_subq_7.referrer_id AS referrer_id
                  , nr_subq_7.visit__referrer_id AS visit__referrer_id
                  , nr_subq_7.visits AS visits
                  , nr_subq_7.visitors AS visitors
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    nr_subq_28012.ds__day
                    , nr_subq_28012.ds__week
                    , nr_subq_28012.ds__month
                    , nr_subq_28012.ds__quarter
                    , nr_subq_28012.ds__year
                    , nr_subq_28012.ds__extract_year
                    , nr_subq_28012.ds__extract_quarter
                    , nr_subq_28012.ds__extract_month
                    , nr_subq_28012.ds__extract_day
                    , nr_subq_28012.ds__extract_dow
                    , nr_subq_28012.ds__extract_doy
                    , nr_subq_28012.visit__ds__day
                    , nr_subq_28012.visit__ds__week
                    , nr_subq_28012.visit__ds__month
                    , nr_subq_28012.visit__ds__quarter
                    , nr_subq_28012.visit__ds__year
                    , nr_subq_28012.visit__ds__extract_year
                    , nr_subq_28012.visit__ds__extract_quarter
                    , nr_subq_28012.visit__ds__extract_month
                    , nr_subq_28012.visit__ds__extract_day
                    , nr_subq_28012.visit__ds__extract_dow
                    , nr_subq_28012.visit__ds__extract_doy
                    , nr_subq_28012.ds__day AS metric_time__day
                    , nr_subq_28012.ds__week AS metric_time__week
                    , nr_subq_28012.ds__month AS metric_time__month
                    , nr_subq_28012.ds__quarter AS metric_time__quarter
                    , nr_subq_28012.ds__year AS metric_time__year
                    , nr_subq_28012.ds__extract_year AS metric_time__extract_year
                    , nr_subq_28012.ds__extract_quarter AS metric_time__extract_quarter
                    , nr_subq_28012.ds__extract_month AS metric_time__extract_month
                    , nr_subq_28012.ds__extract_day AS metric_time__extract_day
                    , nr_subq_28012.ds__extract_dow AS metric_time__extract_dow
                    , nr_subq_28012.ds__extract_doy AS metric_time__extract_doy
                    , nr_subq_28012.user
                    , nr_subq_28012.session
                    , nr_subq_28012.visit__user
                    , nr_subq_28012.visit__session
                    , nr_subq_28012.referrer_id
                    , nr_subq_28012.visit__referrer_id
                    , nr_subq_28012.visits
                    , nr_subq_28012.visitors
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
                      , EXTRACT(DAYOFWEEK_ISO FROM visits_source_src_28000.ds) AS ds__extract_dow
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
                      , EXTRACT(DAYOFWEEK_ISO FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                      , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                      , visits_source_src_28000.referrer_id AS visit__referrer_id
                      , visits_source_src_28000.user_id AS user
                      , visits_source_src_28000.session_id AS session
                      , visits_source_src_28000.user_id AS visit__user
                      , visits_source_src_28000.session_id AS visit__session
                    FROM ***************************.fct_visits visits_source_src_28000
                  ) nr_subq_28012
                ) nr_subq_7
                LEFT OUTER JOIN (
                  -- Pass Only Elements: ['home_state_latest', 'user']
                  SELECT
                    nr_subq_8.user
                    , nr_subq_8.home_state_latest
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
                      , EXTRACT(DAYOFWEEK_ISO FROM users_latest_src_28000.ds) AS ds_latest__extract_dow
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
                      , EXTRACT(DAYOFWEEK_ISO FROM users_latest_src_28000.ds) AS user__ds_latest__extract_dow
                      , EXTRACT(doy FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
                      , users_latest_src_28000.home_state_latest AS user__home_state_latest
                      , users_latest_src_28000.user_id AS user
                    FROM ***************************.dim_users_latest users_latest_src_28000
                  ) nr_subq_8
                ) nr_subq_9
                ON
                  nr_subq_7.user = nr_subq_9.user
              ) nr_subq_10
              WHERE visit__referrer_id = '123456'
            ) nr_subq_11
          ) nr_subq_12
          INNER JOIN (
            -- Add column with generated UUID
            SELECT
              nr_subq_13.ds__day
              , nr_subq_13.ds__week
              , nr_subq_13.ds__month
              , nr_subq_13.ds__quarter
              , nr_subq_13.ds__year
              , nr_subq_13.ds__extract_year
              , nr_subq_13.ds__extract_quarter
              , nr_subq_13.ds__extract_month
              , nr_subq_13.ds__extract_day
              , nr_subq_13.ds__extract_dow
              , nr_subq_13.ds__extract_doy
              , nr_subq_13.ds_month__month
              , nr_subq_13.ds_month__quarter
              , nr_subq_13.ds_month__year
              , nr_subq_13.ds_month__extract_year
              , nr_subq_13.ds_month__extract_quarter
              , nr_subq_13.ds_month__extract_month
              , nr_subq_13.buy__ds__day
              , nr_subq_13.buy__ds__week
              , nr_subq_13.buy__ds__month
              , nr_subq_13.buy__ds__quarter
              , nr_subq_13.buy__ds__year
              , nr_subq_13.buy__ds__extract_year
              , nr_subq_13.buy__ds__extract_quarter
              , nr_subq_13.buy__ds__extract_month
              , nr_subq_13.buy__ds__extract_day
              , nr_subq_13.buy__ds__extract_dow
              , nr_subq_13.buy__ds__extract_doy
              , nr_subq_13.buy__ds_month__month
              , nr_subq_13.buy__ds_month__quarter
              , nr_subq_13.buy__ds_month__year
              , nr_subq_13.buy__ds_month__extract_year
              , nr_subq_13.buy__ds_month__extract_quarter
              , nr_subq_13.buy__ds_month__extract_month
              , nr_subq_13.metric_time__day
              , nr_subq_13.metric_time__week
              , nr_subq_13.metric_time__month
              , nr_subq_13.metric_time__quarter
              , nr_subq_13.metric_time__year
              , nr_subq_13.metric_time__extract_year
              , nr_subq_13.metric_time__extract_quarter
              , nr_subq_13.metric_time__extract_month
              , nr_subq_13.metric_time__extract_day
              , nr_subq_13.metric_time__extract_dow
              , nr_subq_13.metric_time__extract_doy
              , nr_subq_13.user
              , nr_subq_13.session_id
              , nr_subq_13.buy__user
              , nr_subq_13.buy__session_id
              , nr_subq_13.buys
              , nr_subq_13.buyers
              , UUID() AS mf_internal_uuid
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                nr_subq_28004.ds__day
                , nr_subq_28004.ds__week
                , nr_subq_28004.ds__month
                , nr_subq_28004.ds__quarter
                , nr_subq_28004.ds__year
                , nr_subq_28004.ds__extract_year
                , nr_subq_28004.ds__extract_quarter
                , nr_subq_28004.ds__extract_month
                , nr_subq_28004.ds__extract_day
                , nr_subq_28004.ds__extract_dow
                , nr_subq_28004.ds__extract_doy
                , nr_subq_28004.ds_month__month
                , nr_subq_28004.ds_month__quarter
                , nr_subq_28004.ds_month__year
                , nr_subq_28004.ds_month__extract_year
                , nr_subq_28004.ds_month__extract_quarter
                , nr_subq_28004.ds_month__extract_month
                , nr_subq_28004.buy__ds__day
                , nr_subq_28004.buy__ds__week
                , nr_subq_28004.buy__ds__month
                , nr_subq_28004.buy__ds__quarter
                , nr_subq_28004.buy__ds__year
                , nr_subq_28004.buy__ds__extract_year
                , nr_subq_28004.buy__ds__extract_quarter
                , nr_subq_28004.buy__ds__extract_month
                , nr_subq_28004.buy__ds__extract_day
                , nr_subq_28004.buy__ds__extract_dow
                , nr_subq_28004.buy__ds__extract_doy
                , nr_subq_28004.buy__ds_month__month
                , nr_subq_28004.buy__ds_month__quarter
                , nr_subq_28004.buy__ds_month__year
                , nr_subq_28004.buy__ds_month__extract_year
                , nr_subq_28004.buy__ds_month__extract_quarter
                , nr_subq_28004.buy__ds_month__extract_month
                , nr_subq_28004.ds__day AS metric_time__day
                , nr_subq_28004.ds__week AS metric_time__week
                , nr_subq_28004.ds__month AS metric_time__month
                , nr_subq_28004.ds__quarter AS metric_time__quarter
                , nr_subq_28004.ds__year AS metric_time__year
                , nr_subq_28004.ds__extract_year AS metric_time__extract_year
                , nr_subq_28004.ds__extract_quarter AS metric_time__extract_quarter
                , nr_subq_28004.ds__extract_month AS metric_time__extract_month
                , nr_subq_28004.ds__extract_day AS metric_time__extract_day
                , nr_subq_28004.ds__extract_dow AS metric_time__extract_dow
                , nr_subq_28004.ds__extract_doy AS metric_time__extract_doy
                , nr_subq_28004.user
                , nr_subq_28004.session_id
                , nr_subq_28004.buy__user
                , nr_subq_28004.buy__session_id
                , nr_subq_28004.buys
                , nr_subq_28004.buyers
              FROM (
                -- Read Elements From Semantic Model 'buys_source'
                SELECT
                  1 AS buys
                  , 1 AS buys_month
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
                  , EXTRACT(DAYOFWEEK_ISO FROM buys_source_src_28000.ds) AS ds__extract_dow
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
                  , EXTRACT(DAYOFWEEK_ISO FROM buys_source_src_28000.ds) AS buy__ds__extract_dow
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
              ) nr_subq_28004
            ) nr_subq_13
          ) nr_subq_14
          ON
            (
              nr_subq_12.user = nr_subq_14.user
            ) AND (
              (
                nr_subq_12.metric_time__day <= nr_subq_14.metric_time__day
              ) AND (
                nr_subq_12.metric_time__day > DATEADD(day, -7, nr_subq_14.metric_time__day)
              )
            )
        ) nr_subq_15
      ) nr_subq_16
    ) nr_subq_17
    GROUP BY
      nr_subq_17.metric_time__day
      , nr_subq_17.user__home_state_latest
  ) nr_subq_18
  ON
    (
      nr_subq_6.user__home_state_latest = nr_subq_18.user__home_state_latest
    ) AND (
      nr_subq_6.metric_time__day = nr_subq_18.metric_time__day
    )
  GROUP BY
    COALESCE(nr_subq_6.metric_time__day, nr_subq_18.metric_time__day)
    , COALESCE(nr_subq_6.user__home_state_latest, nr_subq_18.user__home_state_latest)
) nr_subq_19
