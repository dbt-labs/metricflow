test_name: test_conversion_metric_with_time_constraint
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric with a time constraint and categorical filter.
sql_engine: Redshift
---
-- Write to DataTable
SELECT
  subq_18.visit__referrer_id
  , subq_18.visit_buy_conversion_rate
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_17.visit__referrer_id
    , CAST(subq_17.__buys AS DOUBLE PRECISION) / CAST(NULLIF(subq_17.__visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_6.visit__referrer_id, subq_16.visit__referrer_id) AS visit__referrer_id
      , MAX(subq_6.__visits) AS __visits
      , MAX(subq_16.__buys) AS __buys
    FROM (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_5.visit__referrer_id
        , SUM(subq_5.__visits) AS __visits
      FROM (
        -- Pass Only Elements: ['__visits', 'visit__referrer_id']
        SELECT
          subq_4.visit__referrer_id
          , subq_4.__visits
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_3.visits AS __visits
            , subq_3.visits_fill_nulls_with_0_join_to_timespine AS __visits_fill_nulls_with_0_join_to_timespine
            , subq_3.referrer_id
            , subq_3.visit__referrer_id
            , subq_3.ds__day
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
            , subq_3.visit__ds__day
            , subq_3.visit__ds__week
            , subq_3.visit__ds__month
            , subq_3.visit__ds__quarter
            , subq_3.visit__ds__year
            , subq_3.visit__ds__extract_year
            , subq_3.visit__ds__extract_quarter
            , subq_3.visit__ds__extract_month
            , subq_3.visit__ds__extract_day
            , subq_3.visit__ds__extract_dow
            , subq_3.visit__ds__extract_doy
            , subq_3.metric_time__day
            , subq_3.metric_time__week
            , subq_3.metric_time__month
            , subq_3.metric_time__quarter
            , subq_3.metric_time__year
            , subq_3.metric_time__extract_year
            , subq_3.metric_time__extract_quarter
            , subq_3.metric_time__extract_month
            , subq_3.metric_time__extract_day
            , subq_3.metric_time__extract_dow
            , subq_3.metric_time__extract_doy
            , subq_3.user
            , subq_3.session
            , subq_3.visit__user
            , subq_3.visit__session
          FROM (
            -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
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
              , subq_2.__visits AS visits
              , subq_2.__visits_fill_nulls_with_0_join_to_timespine AS visits_fill_nulls_with_0_join_to_timespine
            FROM (
              -- Metric Time Dimension 'ds'
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
                , subq_1.ds__day AS metric_time__day
                , subq_1.ds__week AS metric_time__week
                , subq_1.ds__month AS metric_time__month
                , subq_1.ds__quarter AS metric_time__quarter
                , subq_1.ds__year AS metric_time__year
                , subq_1.ds__extract_year AS metric_time__extract_year
                , subq_1.ds__extract_quarter AS metric_time__extract_quarter
                , subq_1.ds__extract_month AS metric_time__extract_month
                , subq_1.ds__extract_day AS metric_time__extract_day
                , subq_1.ds__extract_dow AS metric_time__extract_dow
                , subq_1.ds__extract_doy AS metric_time__extract_doy
                , subq_1.user
                , subq_1.session
                , subq_1.visit__user
                , subq_1.visit__session
                , subq_1.referrer_id
                , subq_1.visit__referrer_id
                , subq_1.__visits
                , subq_1.__visits_fill_nulls_with_0_join_to_timespine
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
              ) subq_1
            ) subq_2
            WHERE subq_2.metric_time__day BETWEEN '2020-01-01' AND '2020-01-02'
          ) subq_3
          WHERE visit__referrer_id = 'ref_id_01'
        ) subq_4
      ) subq_5
      GROUP BY
        subq_5.visit__referrer_id
    ) subq_6
    FULL OUTER JOIN (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_15.visit__referrer_id
        , SUM(subq_15.__buys) AS __buys
      FROM (
        -- Pass Only Elements: ['__buys', 'visit__referrer_id']
        SELECT
          subq_14.visit__referrer_id
          , subq_14.__buys
        FROM (
          -- Find conversions for user within the range of INF
          SELECT
            subq_13.metric_time__day
            , subq_13.user
            , subq_13.visit__referrer_id
            , subq_13.__buys
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
              , subq_12.__buys AS __buys
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
                  , subq_7.visits_fill_nulls_with_0_join_to_timespine AS __visits_fill_nulls_with_0_join_to_timespine
                  , subq_7.referrer_id
                  , subq_7.visit__referrer_id
                  , subq_7.ds__day
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
                FROM (
                  -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
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
                    , subq_2.__visits AS visits
                    , subq_2.__visits_fill_nulls_with_0_join_to_timespine AS visits_fill_nulls_with_0_join_to_timespine
                  FROM (
                    -- Metric Time Dimension 'ds'
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
                      , subq_1.ds__day AS metric_time__day
                      , subq_1.ds__week AS metric_time__week
                      , subq_1.ds__month AS metric_time__month
                      , subq_1.ds__quarter AS metric_time__quarter
                      , subq_1.ds__year AS metric_time__year
                      , subq_1.ds__extract_year AS metric_time__extract_year
                      , subq_1.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_1.ds__extract_month AS metric_time__extract_month
                      , subq_1.ds__extract_day AS metric_time__extract_day
                      , subq_1.ds__extract_dow AS metric_time__extract_dow
                      , subq_1.ds__extract_doy AS metric_time__extract_doy
                      , subq_1.user
                      , subq_1.session
                      , subq_1.visit__user
                      , subq_1.visit__session
                      , subq_1.referrer_id
                      , subq_1.visit__referrer_id
                      , subq_1.__visits
                      , subq_1.__visits_fill_nulls_with_0_join_to_timespine
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
                    ) subq_1
                  ) subq_2
                  WHERE subq_2.metric_time__day BETWEEN '2020-01-01' AND '2020-01-02'
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
                (subq_9.metric_time__day <= subq_12.metric_time__day)
              )
          ) subq_13
        ) subq_14
      ) subq_15
      GROUP BY
        subq_15.visit__referrer_id
    ) subq_16
    ON
      subq_6.visit__referrer_id = subq_16.visit__referrer_id
    GROUP BY
      COALESCE(subq_6.visit__referrer_id, subq_16.visit__referrer_id)
  ) subq_17
) subq_18
