test_name: test_conversion_metric_with_filter_not_in_group_by
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_13.buys AS visit_buy_conversions
FROM (
  -- Combine Aggregated Outputs
  SELECT
    MAX(nr_subq_3.visits) AS visits
    , COALESCE(MAX(nr_subq_12.buys), 0) AS buys
  FROM (
    -- Aggregate Measures
    SELECT
      SUM(nr_subq_2.visits) AS visits
    FROM (
      -- Pass Only Elements: ['visits',]
      SELECT
        nr_subq_1.visits
      FROM (
        -- Constrain Output with WHERE
        SELECT
          nr_subq_0.ds__day
          , nr_subq_0.ds__week
          , nr_subq_0.ds__month
          , nr_subq_0.ds__quarter
          , nr_subq_0.ds__year
          , nr_subq_0.ds__extract_year
          , nr_subq_0.ds__extract_quarter
          , nr_subq_0.ds__extract_month
          , nr_subq_0.ds__extract_day
          , nr_subq_0.ds__extract_dow
          , nr_subq_0.ds__extract_doy
          , nr_subq_0.visit__ds__day
          , nr_subq_0.visit__ds__week
          , nr_subq_0.visit__ds__month
          , nr_subq_0.visit__ds__quarter
          , nr_subq_0.visit__ds__year
          , nr_subq_0.visit__ds__extract_year
          , nr_subq_0.visit__ds__extract_quarter
          , nr_subq_0.visit__ds__extract_month
          , nr_subq_0.visit__ds__extract_day
          , nr_subq_0.visit__ds__extract_dow
          , nr_subq_0.visit__ds__extract_doy
          , nr_subq_0.metric_time__day
          , nr_subq_0.metric_time__week
          , nr_subq_0.metric_time__month
          , nr_subq_0.metric_time__quarter
          , nr_subq_0.metric_time__year
          , nr_subq_0.metric_time__extract_year
          , nr_subq_0.metric_time__extract_quarter
          , nr_subq_0.metric_time__extract_month
          , nr_subq_0.metric_time__extract_day
          , nr_subq_0.metric_time__extract_dow
          , nr_subq_0.metric_time__extract_doy
          , nr_subq_0.user
          , nr_subq_0.session
          , nr_subq_0.visit__user
          , nr_subq_0.visit__session
          , nr_subq_0.referrer_id
          , nr_subq_0.visit__referrer_id
          , nr_subq_0.visits
          , nr_subq_0.visitors
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
              , EXTRACT(dayofweekiso FROM visits_source_src_28000.ds) AS ds__extract_dow
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
              , EXTRACT(dayofweekiso FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
              , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
              , visits_source_src_28000.referrer_id AS visit__referrer_id
              , visits_source_src_28000.user_id AS user
              , visits_source_src_28000.session_id AS session
              , visits_source_src_28000.user_id AS visit__user
              , visits_source_src_28000.session_id AS visit__session
            FROM ***************************.fct_visits visits_source_src_28000
          ) nr_subq_28012
        ) nr_subq_0
        WHERE visit__referrer_id = 'ref_id_01'
      ) nr_subq_1
    ) nr_subq_2
  ) nr_subq_3
  CROSS JOIN (
    -- Aggregate Measures
    SELECT
      SUM(nr_subq_11.buys) AS buys
    FROM (
      -- Pass Only Elements: ['buys',]
      SELECT
        nr_subq_10.buys
      FROM (
        -- Find conversions for user within the range of 7 day
        SELECT
          nr_subq_9.metric_time__day
          , nr_subq_9.user
          , nr_subq_9.visit__referrer_id
          , nr_subq_9.buys
          , nr_subq_9.visits
        FROM (
          -- Dedupe the fanout with mf_internal_uuid in the conversion data set
          SELECT DISTINCT
            FIRST_VALUE(nr_subq_6.visits) OVER (
              PARTITION BY
                nr_subq_8.user
                , nr_subq_8.metric_time__day
                , nr_subq_8.mf_internal_uuid
              ORDER BY nr_subq_6.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visits
            , FIRST_VALUE(nr_subq_6.visit__referrer_id) OVER (
              PARTITION BY
                nr_subq_8.user
                , nr_subq_8.metric_time__day
                , nr_subq_8.mf_internal_uuid
              ORDER BY nr_subq_6.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visit__referrer_id
            , FIRST_VALUE(nr_subq_6.metric_time__day) OVER (
              PARTITION BY
                nr_subq_8.user
                , nr_subq_8.metric_time__day
                , nr_subq_8.mf_internal_uuid
              ORDER BY nr_subq_6.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS metric_time__day
            , FIRST_VALUE(nr_subq_6.user) OVER (
              PARTITION BY
                nr_subq_8.user
                , nr_subq_8.metric_time__day
                , nr_subq_8.mf_internal_uuid
              ORDER BY nr_subq_6.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS user
            , nr_subq_8.mf_internal_uuid AS mf_internal_uuid
            , nr_subq_8.buys AS buys
          FROM (
            -- Pass Only Elements: ['visits', 'visit__referrer_id', 'metric_time__day', 'user']
            SELECT
              nr_subq_5.metric_time__day
              , nr_subq_5.user
              , nr_subq_5.visit__referrer_id
              , nr_subq_5.visits
            FROM (
              -- Constrain Output with WHERE
              SELECT
                nr_subq_4.ds__day
                , nr_subq_4.ds__week
                , nr_subq_4.ds__month
                , nr_subq_4.ds__quarter
                , nr_subq_4.ds__year
                , nr_subq_4.ds__extract_year
                , nr_subq_4.ds__extract_quarter
                , nr_subq_4.ds__extract_month
                , nr_subq_4.ds__extract_day
                , nr_subq_4.ds__extract_dow
                , nr_subq_4.ds__extract_doy
                , nr_subq_4.visit__ds__day
                , nr_subq_4.visit__ds__week
                , nr_subq_4.visit__ds__month
                , nr_subq_4.visit__ds__quarter
                , nr_subq_4.visit__ds__year
                , nr_subq_4.visit__ds__extract_year
                , nr_subq_4.visit__ds__extract_quarter
                , nr_subq_4.visit__ds__extract_month
                , nr_subq_4.visit__ds__extract_day
                , nr_subq_4.visit__ds__extract_dow
                , nr_subq_4.visit__ds__extract_doy
                , nr_subq_4.metric_time__day
                , nr_subq_4.metric_time__week
                , nr_subq_4.metric_time__month
                , nr_subq_4.metric_time__quarter
                , nr_subq_4.metric_time__year
                , nr_subq_4.metric_time__extract_year
                , nr_subq_4.metric_time__extract_quarter
                , nr_subq_4.metric_time__extract_month
                , nr_subq_4.metric_time__extract_day
                , nr_subq_4.metric_time__extract_dow
                , nr_subq_4.metric_time__extract_doy
                , nr_subq_4.user
                , nr_subq_4.session
                , nr_subq_4.visit__user
                , nr_subq_4.visit__session
                , nr_subq_4.referrer_id
                , nr_subq_4.visit__referrer_id
                , nr_subq_4.visits
                , nr_subq_4.visitors
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
                    , EXTRACT(dayofweekiso FROM visits_source_src_28000.ds) AS ds__extract_dow
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
                    , EXTRACT(dayofweekiso FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                    , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                    , visits_source_src_28000.referrer_id AS visit__referrer_id
                    , visits_source_src_28000.user_id AS user
                    , visits_source_src_28000.session_id AS session
                    , visits_source_src_28000.user_id AS visit__user
                    , visits_source_src_28000.session_id AS visit__session
                  FROM ***************************.fct_visits visits_source_src_28000
                ) nr_subq_28012
              ) nr_subq_4
              WHERE visit__referrer_id = 'ref_id_01'
            ) nr_subq_5
          ) nr_subq_6
          INNER JOIN (
            -- Add column with generated UUID
            SELECT
              nr_subq_7.ds__day
              , nr_subq_7.ds__week
              , nr_subq_7.ds__month
              , nr_subq_7.ds__quarter
              , nr_subq_7.ds__year
              , nr_subq_7.ds__extract_year
              , nr_subq_7.ds__extract_quarter
              , nr_subq_7.ds__extract_month
              , nr_subq_7.ds__extract_day
              , nr_subq_7.ds__extract_dow
              , nr_subq_7.ds__extract_doy
              , nr_subq_7.ds_month__month
              , nr_subq_7.ds_month__quarter
              , nr_subq_7.ds_month__year
              , nr_subq_7.ds_month__extract_year
              , nr_subq_7.ds_month__extract_quarter
              , nr_subq_7.ds_month__extract_month
              , nr_subq_7.buy__ds__day
              , nr_subq_7.buy__ds__week
              , nr_subq_7.buy__ds__month
              , nr_subq_7.buy__ds__quarter
              , nr_subq_7.buy__ds__year
              , nr_subq_7.buy__ds__extract_year
              , nr_subq_7.buy__ds__extract_quarter
              , nr_subq_7.buy__ds__extract_month
              , nr_subq_7.buy__ds__extract_day
              , nr_subq_7.buy__ds__extract_dow
              , nr_subq_7.buy__ds__extract_doy
              , nr_subq_7.buy__ds_month__month
              , nr_subq_7.buy__ds_month__quarter
              , nr_subq_7.buy__ds_month__year
              , nr_subq_7.buy__ds_month__extract_year
              , nr_subq_7.buy__ds_month__extract_quarter
              , nr_subq_7.buy__ds_month__extract_month
              , nr_subq_7.metric_time__day
              , nr_subq_7.metric_time__week
              , nr_subq_7.metric_time__month
              , nr_subq_7.metric_time__quarter
              , nr_subq_7.metric_time__year
              , nr_subq_7.metric_time__extract_year
              , nr_subq_7.metric_time__extract_quarter
              , nr_subq_7.metric_time__extract_month
              , nr_subq_7.metric_time__extract_day
              , nr_subq_7.metric_time__extract_dow
              , nr_subq_7.metric_time__extract_doy
              , nr_subq_7.user
              , nr_subq_7.session_id
              , nr_subq_7.buy__user
              , nr_subq_7.buy__session_id
              , nr_subq_7.buys
              , nr_subq_7.buyers
              , UUID_STRING() AS mf_internal_uuid
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
                  , EXTRACT(dayofweekiso FROM buys_source_src_28000.ds) AS ds__extract_dow
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
                  , EXTRACT(dayofweekiso FROM buys_source_src_28000.ds) AS buy__ds__extract_dow
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
            ) nr_subq_7
          ) nr_subq_8
          ON
            (
              nr_subq_6.user = nr_subq_8.user
            ) AND (
              (
                nr_subq_6.metric_time__day <= nr_subq_8.metric_time__day
              ) AND (
                nr_subq_6.metric_time__day > DATEADD(day, -7, nr_subq_8.metric_time__day)
              )
            )
        ) nr_subq_9
      ) nr_subq_10
    ) nr_subq_11
  ) nr_subq_12
) nr_subq_13
