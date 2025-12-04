test_name: test_conversion_metric_with_filter_not_in_group_by
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
sql_engine: Redshift
---
-- Write to DataTable
SELECT
  subq_19.visit_buy_conversions
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_18.__buys_fill_nulls_with_0 AS visit_buy_conversions
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      MAX(subq_5.__visits) AS __visits
      , COALESCE(MAX(subq_17.__buys_fill_nulls_with_0), 0) AS __buys_fill_nulls_with_0
    FROM (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        SUM(subq_4.__visits) AS __visits
      FROM (
        -- Pass Only Elements: ['__visits']
        SELECT
          subq_3.__visits
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_2.visits AS __visits
            , subq_2.visit__referrer_id
          FROM (
            -- Pass Only Elements: ['__visits', 'visit__referrer_id']
            SELECT
              subq_1.visit__referrer_id
              , subq_1.__visits AS visits
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
                  , CASE WHEN EXTRACT(dow FROM visits_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM visits_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM visits_source_src_28000.ds) END AS ds__extract_dow
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
                  , CASE WHEN EXTRACT(dow FROM visits_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM visits_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM visits_source_src_28000.ds) END AS visit__ds__extract_dow
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
          WHERE visit__referrer_id = 'ref_id_01'
        ) subq_3
      ) subq_4
    ) subq_5
    CROSS JOIN (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        SUM(subq_16.__buys_fill_nulls_with_0) AS __buys_fill_nulls_with_0
      FROM (
        -- Pass Only Elements: ['__buys_fill_nulls_with_0']
        SELECT
          subq_15.__buys_fill_nulls_with_0
        FROM (
          -- Pass Only Elements: ['__buys_fill_nulls_with_0']
          SELECT
            subq_14.__buys_fill_nulls_with_0
          FROM (
            -- Find conversions for user within the range of 7 day
            SELECT
              subq_13.metric_time__day
              , subq_13.user
              , subq_13.visit__referrer_id
              , subq_13.__buys_fill_nulls_with_0
              , subq_13.__visits
            FROM (
              -- Dedupe the fanout with mf_internal_uuid in the conversion data set
              SELECT DISTINCT
                FIRST_VALUE(subq_9.__visits) OVER (
                  PARTITION BY
                    subq_12.user
                    , subq_12.metric_time__day
                    , subq_12.mf_internal_uuid
                  ORDER BY subq_9.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS __visits
                , FIRST_VALUE(subq_9.visit__referrer_id) OVER (
                  PARTITION BY
                    subq_12.user
                    , subq_12.metric_time__day
                    , subq_12.mf_internal_uuid
                  ORDER BY subq_9.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS visit__referrer_id
                , FIRST_VALUE(subq_9.metric_time__day) OVER (
                  PARTITION BY
                    subq_12.user
                    , subq_12.metric_time__day
                    , subq_12.mf_internal_uuid
                  ORDER BY subq_9.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS metric_time__day
                , FIRST_VALUE(subq_9.user) OVER (
                  PARTITION BY
                    subq_12.user
                    , subq_12.metric_time__day
                    , subq_12.mf_internal_uuid
                  ORDER BY subq_9.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS user
                , subq_12.mf_internal_uuid AS mf_internal_uuid
                , subq_12.__buys_fill_nulls_with_0 AS __buys_fill_nulls_with_0
              FROM (
                -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'metric_time__day', 'user']
                SELECT
                  subq_8.metric_time__day
                  , subq_8.user
                  , subq_8.visit__referrer_id
                  , subq_8.__visits
                FROM (
                  -- Constrain Output with WHERE
                  SELECT
                    subq_7.visits AS __visits
                    , subq_7.visit__referrer_id
                    , subq_7.metric_time__day
                    , subq_7.user
                  FROM (
                    -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'metric_time__day', 'user']
                    SELECT
                      subq_6.metric_time__day
                      , subq_6.user
                      , subq_6.visit__referrer_id
                      , subq_6.__visits AS visits
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
                          , CASE WHEN EXTRACT(dow FROM visits_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM visits_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM visits_source_src_28000.ds) END AS ds__extract_dow
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
                          , CASE WHEN EXTRACT(dow FROM visits_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM visits_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM visits_source_src_28000.ds) END AS visit__ds__extract_dow
                          , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                          , visits_source_src_28000.referrer_id AS visit__referrer_id
                          , visits_source_src_28000.user_id AS user
                          , visits_source_src_28000.session_id AS session
                          , visits_source_src_28000.user_id AS visit__user
                          , visits_source_src_28000.session_id AS visit__session
                        FROM ***************************.fct_visits visits_source_src_28000
                      ) subq_0
                    ) subq_6
                  ) subq_7
                  WHERE visit__referrer_id = 'ref_id_01'
                ) subq_8
              ) subq_9
              INNER JOIN (
                -- Add column with generated UUID
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
                  , subq_11.metric_time__day
                  , subq_11.metric_time__week
                  , subq_11.metric_time__month
                  , subq_11.metric_time__quarter
                  , subq_11.metric_time__year
                  , subq_11.metric_time__extract_year
                  , subq_11.metric_time__extract_quarter
                  , subq_11.metric_time__extract_month
                  , subq_11.metric_time__extract_day
                  , subq_11.metric_time__extract_dow
                  , subq_11.metric_time__extract_doy
                  , subq_11.user
                  , subq_11.session_id
                  , subq_11.buy__user
                  , subq_11.buy__session_id
                  , subq_11.__buys
                  , subq_11.__buys_fill_nulls_with_0
                  , subq_11.__buys_fill_nulls_with_0_join_to_timespine
                  , CONCAT(CAST(RANDOM()*100000000 AS INT)::VARCHAR,CAST(RANDOM()*100000000 AS INT)::VARCHAR) AS mf_internal_uuid
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
                    , subq_10.ds_month__month
                    , subq_10.ds_month__quarter
                    , subq_10.ds_month__year
                    , subq_10.ds_month__extract_year
                    , subq_10.ds_month__extract_quarter
                    , subq_10.ds_month__extract_month
                    , subq_10.buy__ds__day
                    , subq_10.buy__ds__week
                    , subq_10.buy__ds__month
                    , subq_10.buy__ds__quarter
                    , subq_10.buy__ds__year
                    , subq_10.buy__ds__extract_year
                    , subq_10.buy__ds__extract_quarter
                    , subq_10.buy__ds__extract_month
                    , subq_10.buy__ds__extract_day
                    , subq_10.buy__ds__extract_dow
                    , subq_10.buy__ds__extract_doy
                    , subq_10.buy__ds_month__month
                    , subq_10.buy__ds_month__quarter
                    , subq_10.buy__ds_month__year
                    , subq_10.buy__ds_month__extract_year
                    , subq_10.buy__ds_month__extract_quarter
                    , subq_10.buy__ds_month__extract_month
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
                    , subq_10.session_id
                    , subq_10.buy__user
                    , subq_10.buy__session_id
                    , subq_10.__buys
                    , subq_10.__buys_fill_nulls_with_0
                    , subq_10.__buys_fill_nulls_with_0_join_to_timespine
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
                      , CASE WHEN EXTRACT(dow FROM buys_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM buys_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM buys_source_src_28000.ds) END AS ds__extract_dow
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
                      , CASE WHEN EXTRACT(dow FROM buys_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM buys_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM buys_source_src_28000.ds) END AS buy__ds__extract_dow
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
                  ) subq_10
                ) subq_11
              ) subq_12
              ON
                (
                  subq_9.user = subq_12.user
                ) AND (
                  (
                    subq_9.metric_time__day <= subq_12.metric_time__day
                  ) AND (
                    subq_9.metric_time__day > DATEADD(day, -7, subq_12.metric_time__day)
                  )
                )
            ) subq_13
          ) subq_14
        ) subq_15
      ) subq_16
    ) subq_17
  ) subq_18
) subq_19
