test_name: test_conversion_metric_with_filter_not_in_group_by
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  buys AS visit_buy_conversions
FROM (
  -- Combine Aggregated Outputs
  SELECT
    MAX(subq_88.visits) AS visits
    , COALESCE(MAX(subq_99.buys), 0) AS buys
  FROM (
    -- Aggregate Measures
    SELECT
      SUM(visits) AS visits
    FROM (
      -- Pass Only Elements: ['visits',]
      SELECT
        visits
      FROM (
        -- Constrain Output with WHERE
        SELECT
          ds__day
          , ds__week
          , ds__month
          , ds__quarter
          , ds__year
          , ds__extract_year
          , ds__extract_quarter
          , ds__extract_month
          , ds__extract_day
          , ds__extract_dow
          , ds__extract_doy
          , visit__ds__day
          , visit__ds__week
          , visit__ds__month
          , visit__ds__quarter
          , visit__ds__year
          , visit__ds__extract_year
          , visit__ds__extract_quarter
          , visit__ds__extract_month
          , visit__ds__extract_day
          , visit__ds__extract_dow
          , visit__ds__extract_doy
          , metric_time__day
          , metric_time__week
          , metric_time__month
          , metric_time__quarter
          , metric_time__year
          , metric_time__extract_year
          , metric_time__extract_quarter
          , metric_time__extract_month
          , metric_time__extract_day
          , metric_time__extract_dow
          , metric_time__extract_doy
          , subq_85.user
          , session
          , visit__user
          , visit__session
          , referrer_id
          , visit__referrer_id
          , visits
          , visitors
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            ds__day
            , ds__week
            , ds__month
            , ds__quarter
            , ds__year
            , ds__extract_year
            , ds__extract_quarter
            , ds__extract_month
            , ds__extract_day
            , ds__extract_dow
            , ds__extract_doy
            , visit__ds__day
            , visit__ds__week
            , visit__ds__month
            , visit__ds__quarter
            , visit__ds__year
            , visit__ds__extract_year
            , visit__ds__extract_quarter
            , visit__ds__extract_month
            , visit__ds__extract_day
            , visit__ds__extract_dow
            , visit__ds__extract_doy
            , ds__day AS metric_time__day
            , ds__week AS metric_time__week
            , ds__month AS metric_time__month
            , ds__quarter AS metric_time__quarter
            , ds__year AS metric_time__year
            , ds__extract_year AS metric_time__extract_year
            , ds__extract_quarter AS metric_time__extract_quarter
            , ds__extract_month AS metric_time__extract_month
            , ds__extract_day AS metric_time__extract_day
            , ds__extract_dow AS metric_time__extract_dow
            , ds__extract_doy AS metric_time__extract_doy
            , subq_84.user
            , session
            , visit__user
            , visit__session
            , referrer_id
            , visit__referrer_id
            , visits
            , visitors
          FROM (
            -- Read Elements From Semantic Model 'visits_source'
            SELECT
              1 AS visits
              , user_id AS visitors
              , DATE_TRUNC('day', ds) AS ds__day
              , DATE_TRUNC('week', ds) AS ds__week
              , DATE_TRUNC('month', ds) AS ds__month
              , DATE_TRUNC('quarter', ds) AS ds__quarter
              , DATE_TRUNC('year', ds) AS ds__year
              , EXTRACT(year FROM ds) AS ds__extract_year
              , EXTRACT(quarter FROM ds) AS ds__extract_quarter
              , EXTRACT(month FROM ds) AS ds__extract_month
              , EXTRACT(day FROM ds) AS ds__extract_day
              , EXTRACT(isodow FROM ds) AS ds__extract_dow
              , EXTRACT(doy FROM ds) AS ds__extract_doy
              , referrer_id
              , DATE_TRUNC('day', ds) AS visit__ds__day
              , DATE_TRUNC('week', ds) AS visit__ds__week
              , DATE_TRUNC('month', ds) AS visit__ds__month
              , DATE_TRUNC('quarter', ds) AS visit__ds__quarter
              , DATE_TRUNC('year', ds) AS visit__ds__year
              , EXTRACT(year FROM ds) AS visit__ds__extract_year
              , EXTRACT(quarter FROM ds) AS visit__ds__extract_quarter
              , EXTRACT(month FROM ds) AS visit__ds__extract_month
              , EXTRACT(day FROM ds) AS visit__ds__extract_day
              , EXTRACT(isodow FROM ds) AS visit__ds__extract_dow
              , EXTRACT(doy FROM ds) AS visit__ds__extract_doy
              , referrer_id AS visit__referrer_id
              , user_id AS user
              , session_id AS session
              , user_id AS visit__user
              , session_id AS visit__session
            FROM ***************************.fct_visits visits_source_src_28000
          ) subq_84
        ) subq_85
        WHERE visit__referrer_id = 'ref_id_01'
      ) subq_86
    ) subq_87
  ) subq_88
  CROSS JOIN (
    -- Aggregate Measures
    SELECT
      SUM(buys) AS buys
    FROM (
      -- Pass Only Elements: ['buys',]
      SELECT
        buys
      FROM (
        -- Find conversions for user within the range of 7 day
        SELECT
          metric_time__day
          , subq_96.user
          , visit__referrer_id
          , buys
          , visits
        FROM (
          -- Dedupe the fanout with mf_internal_uuid in the conversion data set
          SELECT DISTINCT
            FIRST_VALUE(subq_92.visits) OVER (
              PARTITION BY
                subq_95.user
                , subq_95.metric_time__day
                , subq_95.mf_internal_uuid
              ORDER BY subq_92.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visits
            , FIRST_VALUE(subq_92.visit__referrer_id) OVER (
              PARTITION BY
                subq_95.user
                , subq_95.metric_time__day
                , subq_95.mf_internal_uuid
              ORDER BY subq_92.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visit__referrer_id
            , FIRST_VALUE(subq_92.metric_time__day) OVER (
              PARTITION BY
                subq_95.user
                , subq_95.metric_time__day
                , subq_95.mf_internal_uuid
              ORDER BY subq_92.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS metric_time__day
            , FIRST_VALUE(subq_92.user) OVER (
              PARTITION BY
                subq_95.user
                , subq_95.metric_time__day
                , subq_95.mf_internal_uuid
              ORDER BY subq_92.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS user
            , subq_95.mf_internal_uuid AS mf_internal_uuid
            , subq_95.buys AS buys
          FROM (
            -- Pass Only Elements: ['visits', 'visit__referrer_id', 'metric_time__day', 'user']
            SELECT
              metric_time__day
              , subq_91.user
              , visit__referrer_id
              , visits
            FROM (
              -- Constrain Output with WHERE
              SELECT
                ds__day
                , ds__week
                , ds__month
                , ds__quarter
                , ds__year
                , ds__extract_year
                , ds__extract_quarter
                , ds__extract_month
                , ds__extract_day
                , ds__extract_dow
                , ds__extract_doy
                , visit__ds__day
                , visit__ds__week
                , visit__ds__month
                , visit__ds__quarter
                , visit__ds__year
                , visit__ds__extract_year
                , visit__ds__extract_quarter
                , visit__ds__extract_month
                , visit__ds__extract_day
                , visit__ds__extract_dow
                , visit__ds__extract_doy
                , metric_time__day
                , metric_time__week
                , metric_time__month
                , metric_time__quarter
                , metric_time__year
                , metric_time__extract_year
                , metric_time__extract_quarter
                , metric_time__extract_month
                , metric_time__extract_day
                , metric_time__extract_dow
                , metric_time__extract_doy
                , subq_90.user
                , session
                , visit__user
                , visit__session
                , referrer_id
                , visit__referrer_id
                , visits
                , visitors
              FROM (
                -- Metric Time Dimension 'ds'
                SELECT
                  ds__day
                  , ds__week
                  , ds__month
                  , ds__quarter
                  , ds__year
                  , ds__extract_year
                  , ds__extract_quarter
                  , ds__extract_month
                  , ds__extract_day
                  , ds__extract_dow
                  , ds__extract_doy
                  , visit__ds__day
                  , visit__ds__week
                  , visit__ds__month
                  , visit__ds__quarter
                  , visit__ds__year
                  , visit__ds__extract_year
                  , visit__ds__extract_quarter
                  , visit__ds__extract_month
                  , visit__ds__extract_day
                  , visit__ds__extract_dow
                  , visit__ds__extract_doy
                  , ds__day AS metric_time__day
                  , ds__week AS metric_time__week
                  , ds__month AS metric_time__month
                  , ds__quarter AS metric_time__quarter
                  , ds__year AS metric_time__year
                  , ds__extract_year AS metric_time__extract_year
                  , ds__extract_quarter AS metric_time__extract_quarter
                  , ds__extract_month AS metric_time__extract_month
                  , ds__extract_day AS metric_time__extract_day
                  , ds__extract_dow AS metric_time__extract_dow
                  , ds__extract_doy AS metric_time__extract_doy
                  , subq_89.user
                  , session
                  , visit__user
                  , visit__session
                  , referrer_id
                  , visit__referrer_id
                  , visits
                  , visitors
                FROM (
                  -- Read Elements From Semantic Model 'visits_source'
                  SELECT
                    1 AS visits
                    , user_id AS visitors
                    , DATE_TRUNC('day', ds) AS ds__day
                    , DATE_TRUNC('week', ds) AS ds__week
                    , DATE_TRUNC('month', ds) AS ds__month
                    , DATE_TRUNC('quarter', ds) AS ds__quarter
                    , DATE_TRUNC('year', ds) AS ds__year
                    , EXTRACT(year FROM ds) AS ds__extract_year
                    , EXTRACT(quarter FROM ds) AS ds__extract_quarter
                    , EXTRACT(month FROM ds) AS ds__extract_month
                    , EXTRACT(day FROM ds) AS ds__extract_day
                    , EXTRACT(isodow FROM ds) AS ds__extract_dow
                    , EXTRACT(doy FROM ds) AS ds__extract_doy
                    , referrer_id
                    , DATE_TRUNC('day', ds) AS visit__ds__day
                    , DATE_TRUNC('week', ds) AS visit__ds__week
                    , DATE_TRUNC('month', ds) AS visit__ds__month
                    , DATE_TRUNC('quarter', ds) AS visit__ds__quarter
                    , DATE_TRUNC('year', ds) AS visit__ds__year
                    , EXTRACT(year FROM ds) AS visit__ds__extract_year
                    , EXTRACT(quarter FROM ds) AS visit__ds__extract_quarter
                    , EXTRACT(month FROM ds) AS visit__ds__extract_month
                    , EXTRACT(day FROM ds) AS visit__ds__extract_day
                    , EXTRACT(isodow FROM ds) AS visit__ds__extract_dow
                    , EXTRACT(doy FROM ds) AS visit__ds__extract_doy
                    , referrer_id AS visit__referrer_id
                    , user_id AS user
                    , session_id AS session
                    , user_id AS visit__user
                    , session_id AS visit__session
                  FROM ***************************.fct_visits visits_source_src_28000
                ) subq_89
              ) subq_90
              WHERE visit__referrer_id = 'ref_id_01'
            ) subq_91
          ) subq_92
          INNER JOIN (
            -- Add column with generated UUID
            SELECT
              ds__day
              , ds__week
              , ds__month
              , ds__quarter
              , ds__year
              , ds__extract_year
              , ds__extract_quarter
              , ds__extract_month
              , ds__extract_day
              , ds__extract_dow
              , ds__extract_doy
              , ds_month__month
              , ds_month__quarter
              , ds_month__year
              , ds_month__extract_year
              , ds_month__extract_quarter
              , ds_month__extract_month
              , buy__ds__day
              , buy__ds__week
              , buy__ds__month
              , buy__ds__quarter
              , buy__ds__year
              , buy__ds__extract_year
              , buy__ds__extract_quarter
              , buy__ds__extract_month
              , buy__ds__extract_day
              , buy__ds__extract_dow
              , buy__ds__extract_doy
              , buy__ds_month__month
              , buy__ds_month__quarter
              , buy__ds_month__year
              , buy__ds_month__extract_year
              , buy__ds_month__extract_quarter
              , buy__ds_month__extract_month
              , metric_time__day
              , metric_time__week
              , metric_time__month
              , metric_time__quarter
              , metric_time__year
              , metric_time__extract_year
              , metric_time__extract_quarter
              , metric_time__extract_month
              , metric_time__extract_day
              , metric_time__extract_dow
              , metric_time__extract_doy
              , subq_94.user
              , session_id
              , buy__user
              , buy__session_id
              , buys
              , buyers
              , GEN_RANDOM_UUID() AS mf_internal_uuid
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                ds__day
                , ds__week
                , ds__month
                , ds__quarter
                , ds__year
                , ds__extract_year
                , ds__extract_quarter
                , ds__extract_month
                , ds__extract_day
                , ds__extract_dow
                , ds__extract_doy
                , ds_month__month
                , ds_month__quarter
                , ds_month__year
                , ds_month__extract_year
                , ds_month__extract_quarter
                , ds_month__extract_month
                , buy__ds__day
                , buy__ds__week
                , buy__ds__month
                , buy__ds__quarter
                , buy__ds__year
                , buy__ds__extract_year
                , buy__ds__extract_quarter
                , buy__ds__extract_month
                , buy__ds__extract_day
                , buy__ds__extract_dow
                , buy__ds__extract_doy
                , buy__ds_month__month
                , buy__ds_month__quarter
                , buy__ds_month__year
                , buy__ds_month__extract_year
                , buy__ds_month__extract_quarter
                , buy__ds_month__extract_month
                , ds__day AS metric_time__day
                , ds__week AS metric_time__week
                , ds__month AS metric_time__month
                , ds__quarter AS metric_time__quarter
                , ds__year AS metric_time__year
                , ds__extract_year AS metric_time__extract_year
                , ds__extract_quarter AS metric_time__extract_quarter
                , ds__extract_month AS metric_time__extract_month
                , ds__extract_day AS metric_time__extract_day
                , ds__extract_dow AS metric_time__extract_dow
                , ds__extract_doy AS metric_time__extract_doy
                , subq_93.user
                , session_id
                , buy__user
                , buy__session_id
                , buys
                , buyers
              FROM (
                -- Read Elements From Semantic Model 'buys_source'
                SELECT
                  1 AS buys
                  , 1 AS buys_month
                  , user_id AS buyers
                  , DATE_TRUNC('day', ds) AS ds__day
                  , DATE_TRUNC('week', ds) AS ds__week
                  , DATE_TRUNC('month', ds) AS ds__month
                  , DATE_TRUNC('quarter', ds) AS ds__quarter
                  , DATE_TRUNC('year', ds) AS ds__year
                  , EXTRACT(year FROM ds) AS ds__extract_year
                  , EXTRACT(quarter FROM ds) AS ds__extract_quarter
                  , EXTRACT(month FROM ds) AS ds__extract_month
                  , EXTRACT(day FROM ds) AS ds__extract_day
                  , EXTRACT(isodow FROM ds) AS ds__extract_dow
                  , EXTRACT(doy FROM ds) AS ds__extract_doy
                  , DATE_TRUNC('month', ds_month) AS ds_month__month
                  , DATE_TRUNC('quarter', ds_month) AS ds_month__quarter
                  , DATE_TRUNC('year', ds_month) AS ds_month__year
                  , EXTRACT(year FROM ds_month) AS ds_month__extract_year
                  , EXTRACT(quarter FROM ds_month) AS ds_month__extract_quarter
                  , EXTRACT(month FROM ds_month) AS ds_month__extract_month
                  , DATE_TRUNC('day', ds) AS buy__ds__day
                  , DATE_TRUNC('week', ds) AS buy__ds__week
                  , DATE_TRUNC('month', ds) AS buy__ds__month
                  , DATE_TRUNC('quarter', ds) AS buy__ds__quarter
                  , DATE_TRUNC('year', ds) AS buy__ds__year
                  , EXTRACT(year FROM ds) AS buy__ds__extract_year
                  , EXTRACT(quarter FROM ds) AS buy__ds__extract_quarter
                  , EXTRACT(month FROM ds) AS buy__ds__extract_month
                  , EXTRACT(day FROM ds) AS buy__ds__extract_day
                  , EXTRACT(isodow FROM ds) AS buy__ds__extract_dow
                  , EXTRACT(doy FROM ds) AS buy__ds__extract_doy
                  , DATE_TRUNC('month', ds_month) AS buy__ds_month__month
                  , DATE_TRUNC('quarter', ds_month) AS buy__ds_month__quarter
                  , DATE_TRUNC('year', ds_month) AS buy__ds_month__year
                  , EXTRACT(year FROM ds_month) AS buy__ds_month__extract_year
                  , EXTRACT(quarter FROM ds_month) AS buy__ds_month__extract_quarter
                  , EXTRACT(month FROM ds_month) AS buy__ds_month__extract_month
                  , user_id AS user
                  , session_id
                  , user_id AS buy__user
                  , session_id AS buy__session_id
                FROM ***************************.fct_buys buys_source_src_28000
              ) subq_93
            ) subq_94
          ) subq_95
          ON
            (
              subq_92.user = subq_95.user
            ) AND (
              (
                subq_92.metric_time__day <= subq_95.metric_time__day
              ) AND (
                subq_92.metric_time__day > subq_95.metric_time__day - INTERVAL 7 day
              )
            )
        ) subq_96
      ) subq_97
    ) subq_98
  ) subq_99
) subq_100
