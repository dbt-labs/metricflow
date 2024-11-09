test_name: test_conversion_metric_with_time_constraint
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric with a time constraint and categorical filter.
---
-- Compute Metrics via Expressions
SELECT
  subq_18.visit__referrer_id
  , CAST(subq_18.buys AS DOUBLE) / CAST(NULLIF(subq_18.visits, 0) AS DOUBLE) AS visit_buy_conversion_rate
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_5.visit__referrer_id, subq_17.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_5.visits) AS visits
    , MAX(subq_17.buys) AS buys
  FROM (
    -- Aggregate Measures
    SELECT
      subq_4.visit__referrer_id
      , SUM(subq_4.visits) AS visits
    FROM (
      -- Pass Only Elements: ['visits', 'visit__referrer_id']
      SELECT
        subq_3.visit__referrer_id
        , subq_3.visits
      FROM (
        -- Constrain Output with WHERE
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
          , subq_2.visit__ds__day
          , subq_2.visit__ds__week
          , subq_2.visit__ds__month
          , subq_2.visit__ds__quarter
          , subq_2.visit__ds__year
          , subq_2.visit__ds__extract_year
          , subq_2.visit__ds__extract_quarter
          , subq_2.visit__ds__extract_month
          , subq_2.visit__ds__extract_day
          , subq_2.visit__ds__extract_dow
          , subq_2.visit__ds__extract_doy
          , subq_2.metric_time__day
          , subq_2.metric_time__week
          , subq_2.metric_time__month
          , subq_2.metric_time__quarter
          , subq_2.metric_time__year
          , subq_2.metric_time__extract_year
          , subq_2.metric_time__extract_quarter
          , subq_2.metric_time__extract_month
          , subq_2.metric_time__extract_day
          , subq_2.metric_time__extract_dow
          , subq_2.metric_time__extract_doy
          , subq_2.user
          , subq_2.session
          , subq_2.visit__user
          , subq_2.visit__session
          , subq_2.referrer_id
          , subq_2.visit__referrer_id
          , subq_2.visits
          , subq_2.visitors
        FROM (
          -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
          SELECT
            subq_1.ds__day
            , subq_1.ds__week
            , subq_1.ds__month
            , subq_1.ds__quarter
            , subq_1.ds__year
            , subq_1.ds__extract_year
            , subq_1.ds__extract_quarter
            , subq_1.ds__extract_month
            , subq_1.ds__extract_day
            , subq_1.ds__extract_dow
            , subq_1.ds__extract_doy
            , subq_1.visit__ds__day
            , subq_1.visit__ds__week
            , subq_1.visit__ds__month
            , subq_1.visit__ds__quarter
            , subq_1.visit__ds__year
            , subq_1.visit__ds__extract_year
            , subq_1.visit__ds__extract_quarter
            , subq_1.visit__ds__extract_month
            , subq_1.visit__ds__extract_day
            , subq_1.visit__ds__extract_dow
            , subq_1.visit__ds__extract_doy
            , subq_1.metric_time__day
            , subq_1.metric_time__week
            , subq_1.metric_time__month
            , subq_1.metric_time__quarter
            , subq_1.metric_time__year
            , subq_1.metric_time__extract_year
            , subq_1.metric_time__extract_quarter
            , subq_1.metric_time__extract_month
            , subq_1.metric_time__extract_day
            , subq_1.metric_time__extract_dow
            , subq_1.metric_time__extract_doy
            , subq_1.user
            , subq_1.session
            , subq_1.visit__user
            , subq_1.visit__session
            , subq_1.referrer_id
            , subq_1.visit__referrer_id
            , subq_1.visits
            , subq_1.visitors
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
            ) subq_0
          ) subq_1
          WHERE subq_1.metric_time__day BETWEEN '2020-01-01' AND '2020-01-02'
        ) subq_2
        WHERE visit__referrer_id = 'ref_id_01'
      ) subq_3
    ) subq_4
    GROUP BY
      subq_4.visit__referrer_id
  ) subq_5
  FULL OUTER JOIN (
    -- Aggregate Measures
    SELECT
      subq_16.visit__referrer_id
      , SUM(subq_16.buys) AS buys
    FROM (
      -- Pass Only Elements: ['buys', 'visit__referrer_id']
      SELECT
        subq_15.visit__referrer_id
        , subq_15.buys
      FROM (
        -- Find conversions for user within the range of INF
        SELECT
          subq_14.metric_time__day
          , subq_14.user
          , subq_14.visit__referrer_id
          , subq_14.buys
          , subq_14.visits
        FROM (
          -- Dedupe the fanout with mf_internal_uuid in the conversion data set
          SELECT DISTINCT
            FIRST_VALUE(subq_10.visits) OVER (
              PARTITION BY
                subq_13.user
                , subq_13.metric_time__day
                , subq_13.mf_internal_uuid
              ORDER BY subq_10.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visits
            , FIRST_VALUE(subq_10.visit__referrer_id) OVER (
              PARTITION BY
                subq_13.user
                , subq_13.metric_time__day
                , subq_13.mf_internal_uuid
              ORDER BY subq_10.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visit__referrer_id
            , FIRST_VALUE(subq_10.metric_time__day) OVER (
              PARTITION BY
                subq_13.user
                , subq_13.metric_time__day
                , subq_13.mf_internal_uuid
              ORDER BY subq_10.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS metric_time__day
            , FIRST_VALUE(subq_10.user) OVER (
              PARTITION BY
                subq_13.user
                , subq_13.metric_time__day
                , subq_13.mf_internal_uuid
              ORDER BY subq_10.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS user
            , subq_13.mf_internal_uuid AS mf_internal_uuid
            , subq_13.buys AS buys
          FROM (
            -- Pass Only Elements: ['visits', 'visit__referrer_id', 'metric_time__day', 'user']
            SELECT
              subq_9.metric_time__day
              , subq_9.user
              , subq_9.visit__referrer_id
              , subq_9.visits
            FROM (
              -- Constrain Output with WHERE
              SELECT
                subq_8.ds__day
                , subq_8.ds__week
                , subq_8.ds__month
                , subq_8.ds__quarter
                , subq_8.ds__year
                , subq_8.ds__extract_year
                , subq_8.ds__extract_quarter
                , subq_8.ds__extract_month
                , subq_8.ds__extract_day
                , subq_8.ds__extract_dow
                , subq_8.ds__extract_doy
                , subq_8.visit__ds__day
                , subq_8.visit__ds__week
                , subq_8.visit__ds__month
                , subq_8.visit__ds__quarter
                , subq_8.visit__ds__year
                , subq_8.visit__ds__extract_year
                , subq_8.visit__ds__extract_quarter
                , subq_8.visit__ds__extract_month
                , subq_8.visit__ds__extract_day
                , subq_8.visit__ds__extract_dow
                , subq_8.visit__ds__extract_doy
                , subq_8.metric_time__day
                , subq_8.metric_time__week
                , subq_8.metric_time__month
                , subq_8.metric_time__quarter
                , subq_8.metric_time__year
                , subq_8.metric_time__extract_year
                , subq_8.metric_time__extract_quarter
                , subq_8.metric_time__extract_month
                , subq_8.metric_time__extract_day
                , subq_8.metric_time__extract_dow
                , subq_8.metric_time__extract_doy
                , subq_8.user
                , subq_8.session
                , subq_8.visit__user
                , subq_8.visit__session
                , subq_8.referrer_id
                , subq_8.visit__referrer_id
                , subq_8.visits
                , subq_8.visitors
              FROM (
                -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
                SELECT
                  subq_7.ds__day
                  , subq_7.ds__week
                  , subq_7.ds__month
                  , subq_7.ds__quarter
                  , subq_7.ds__year
                  , subq_7.ds__extract_year
                  , subq_7.ds__extract_quarter
                  , subq_7.ds__extract_month
                  , subq_7.ds__extract_day
                  , subq_7.ds__extract_dow
                  , subq_7.ds__extract_doy
                  , subq_7.visit__ds__day
                  , subq_7.visit__ds__week
                  , subq_7.visit__ds__month
                  , subq_7.visit__ds__quarter
                  , subq_7.visit__ds__year
                  , subq_7.visit__ds__extract_year
                  , subq_7.visit__ds__extract_quarter
                  , subq_7.visit__ds__extract_month
                  , subq_7.visit__ds__extract_day
                  , subq_7.visit__ds__extract_dow
                  , subq_7.visit__ds__extract_doy
                  , subq_7.metric_time__day
                  , subq_7.metric_time__week
                  , subq_7.metric_time__month
                  , subq_7.metric_time__quarter
                  , subq_7.metric_time__year
                  , subq_7.metric_time__extract_year
                  , subq_7.metric_time__extract_quarter
                  , subq_7.metric_time__extract_month
                  , subq_7.metric_time__extract_day
                  , subq_7.metric_time__extract_dow
                  , subq_7.metric_time__extract_doy
                  , subq_7.user
                  , subq_7.session
                  , subq_7.visit__user
                  , subq_7.visit__session
                  , subq_7.referrer_id
                  , subq_7.visit__referrer_id
                  , subq_7.visits
                  , subq_7.visitors
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_6.ds__day
                    , subq_6.ds__week
                    , subq_6.ds__month
                    , subq_6.ds__quarter
                    , subq_6.ds__year
                    , subq_6.ds__extract_year
                    , subq_6.ds__extract_quarter
                    , subq_6.ds__extract_month
                    , subq_6.ds__extract_day
                    , subq_6.ds__extract_dow
                    , subq_6.ds__extract_doy
                    , subq_6.visit__ds__day
                    , subq_6.visit__ds__week
                    , subq_6.visit__ds__month
                    , subq_6.visit__ds__quarter
                    , subq_6.visit__ds__year
                    , subq_6.visit__ds__extract_year
                    , subq_6.visit__ds__extract_quarter
                    , subq_6.visit__ds__extract_month
                    , subq_6.visit__ds__extract_day
                    , subq_6.visit__ds__extract_dow
                    , subq_6.visit__ds__extract_doy
                    , subq_6.ds__day AS metric_time__day
                    , subq_6.ds__week AS metric_time__week
                    , subq_6.ds__month AS metric_time__month
                    , subq_6.ds__quarter AS metric_time__quarter
                    , subq_6.ds__year AS metric_time__year
                    , subq_6.ds__extract_year AS metric_time__extract_year
                    , subq_6.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_6.ds__extract_month AS metric_time__extract_month
                    , subq_6.ds__extract_day AS metric_time__extract_day
                    , subq_6.ds__extract_dow AS metric_time__extract_dow
                    , subq_6.ds__extract_doy AS metric_time__extract_doy
                    , subq_6.user
                    , subq_6.session
                    , subq_6.visit__user
                    , subq_6.visit__session
                    , subq_6.referrer_id
                    , subq_6.visit__referrer_id
                    , subq_6.visits
                    , subq_6.visitors
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
                  ) subq_6
                ) subq_7
                WHERE subq_7.metric_time__day BETWEEN '2020-01-01' AND '2020-01-02'
              ) subq_8
              WHERE visit__referrer_id = 'ref_id_01'
            ) subq_9
          ) subq_10
          INNER JOIN (
            -- Add column with generated UUID
            SELECT
              subq_12.ds__day
              , subq_12.ds__week
              , subq_12.ds__month
              , subq_12.ds__quarter
              , subq_12.ds__year
              , subq_12.ds__extract_year
              , subq_12.ds__extract_quarter
              , subq_12.ds__extract_month
              , subq_12.ds__extract_day
              , subq_12.ds__extract_dow
              , subq_12.ds__extract_doy
              , subq_12.ds_month__month
              , subq_12.ds_month__quarter
              , subq_12.ds_month__year
              , subq_12.ds_month__extract_year
              , subq_12.ds_month__extract_quarter
              , subq_12.ds_month__extract_month
              , subq_12.buy__ds__day
              , subq_12.buy__ds__week
              , subq_12.buy__ds__month
              , subq_12.buy__ds__quarter
              , subq_12.buy__ds__year
              , subq_12.buy__ds__extract_year
              , subq_12.buy__ds__extract_quarter
              , subq_12.buy__ds__extract_month
              , subq_12.buy__ds__extract_day
              , subq_12.buy__ds__extract_dow
              , subq_12.buy__ds__extract_doy
              , subq_12.buy__ds_month__month
              , subq_12.buy__ds_month__quarter
              , subq_12.buy__ds_month__year
              , subq_12.buy__ds_month__extract_year
              , subq_12.buy__ds_month__extract_quarter
              , subq_12.buy__ds_month__extract_month
              , subq_12.metric_time__day
              , subq_12.metric_time__week
              , subq_12.metric_time__month
              , subq_12.metric_time__quarter
              , subq_12.metric_time__year
              , subq_12.metric_time__extract_year
              , subq_12.metric_time__extract_quarter
              , subq_12.metric_time__extract_month
              , subq_12.metric_time__extract_day
              , subq_12.metric_time__extract_dow
              , subq_12.metric_time__extract_doy
              , subq_12.user
              , subq_12.session_id
              , subq_12.buy__user
              , subq_12.buy__session_id
              , subq_12.buys
              , subq_12.buyers
              , UUID() AS mf_internal_uuid
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_11.ds__day
                , subq_11.ds__week
                , subq_11.ds__month
                , subq_11.ds__quarter
                , subq_11.ds__year
                , subq_11.ds__extract_year
                , subq_11.ds__extract_quarter
                , subq_11.ds__extract_month
                , subq_11.ds__extract_day
                , subq_11.ds__extract_dow
                , subq_11.ds__extract_doy
                , subq_11.ds_month__month
                , subq_11.ds_month__quarter
                , subq_11.ds_month__year
                , subq_11.ds_month__extract_year
                , subq_11.ds_month__extract_quarter
                , subq_11.ds_month__extract_month
                , subq_11.buy__ds__day
                , subq_11.buy__ds__week
                , subq_11.buy__ds__month
                , subq_11.buy__ds__quarter
                , subq_11.buy__ds__year
                , subq_11.buy__ds__extract_year
                , subq_11.buy__ds__extract_quarter
                , subq_11.buy__ds__extract_month
                , subq_11.buy__ds__extract_day
                , subq_11.buy__ds__extract_dow
                , subq_11.buy__ds__extract_doy
                , subq_11.buy__ds_month__month
                , subq_11.buy__ds_month__quarter
                , subq_11.buy__ds_month__year
                , subq_11.buy__ds_month__extract_year
                , subq_11.buy__ds_month__extract_quarter
                , subq_11.buy__ds_month__extract_month
                , subq_11.ds__day AS metric_time__day
                , subq_11.ds__week AS metric_time__week
                , subq_11.ds__month AS metric_time__month
                , subq_11.ds__quarter AS metric_time__quarter
                , subq_11.ds__year AS metric_time__year
                , subq_11.ds__extract_year AS metric_time__extract_year
                , subq_11.ds__extract_quarter AS metric_time__extract_quarter
                , subq_11.ds__extract_month AS metric_time__extract_month
                , subq_11.ds__extract_day AS metric_time__extract_day
                , subq_11.ds__extract_dow AS metric_time__extract_dow
                , subq_11.ds__extract_doy AS metric_time__extract_doy
                , subq_11.user
                , subq_11.session_id
                , subq_11.buy__user
                , subq_11.buy__session_id
                , subq_11.buys
                , subq_11.buyers
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
              ) subq_11
            ) subq_12
          ) subq_13
          ON
            (
              subq_10.user = subq_13.user
            ) AND (
              (subq_10.metric_time__day <= subq_13.metric_time__day)
            )
        ) subq_14
      ) subq_15
    ) subq_16
    GROUP BY
      subq_16.visit__referrer_id
  ) subq_17
  ON
    subq_5.visit__referrer_id = subq_17.visit__referrer_id
  GROUP BY
    COALESCE(subq_5.visit__referrer_id, subq_17.visit__referrer_id)
) subq_18
