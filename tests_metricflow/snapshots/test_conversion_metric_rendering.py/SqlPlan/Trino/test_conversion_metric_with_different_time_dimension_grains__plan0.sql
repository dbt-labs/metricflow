test_name: test_conversion_metric_with_different_time_dimension_grains
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
sql_engine: Trino
---
-- Write to DataTable
SELECT
  subq_17.visit_buy_conversion_rate_with_monthly_conversion
FROM (
  -- Compute Metrics via Expressions
  SELECT
    CAST(subq_16.__buys_month AS DOUBLE) / CAST(NULLIF(subq_16.__visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_with_monthly_conversion
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      MAX(subq_4.__visits) AS __visits
      , MAX(subq_15.__buys_month) AS __buys_month
    FROM (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        SUM(subq_3.__visits) AS __visits
      FROM (
        -- Pass Only Elements: ['__visits']
        SELECT
          subq_2.__visits
        FROM (
          -- Pass Only Elements: ['__visits']
          SELECT
            subq_1.__visits
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
      ) subq_3
    ) subq_4
    CROSS JOIN (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        SUM(subq_14.__buys_month) AS __buys_month
      FROM (
        -- Pass Only Elements: ['__buys_month']
        SELECT
          subq_13.__buys_month
        FROM (
          -- Pass Only Elements: ['__buys_month']
          SELECT
            subq_12.__buys_month
          FROM (
            -- Find conversions for user within the range of 1 month
            SELECT
              subq_11.metric_time__month
              , subq_11.user
              , subq_11.__buys_month
              , subq_11.__visits
            FROM (
              -- Dedupe the fanout with mf_internal_uuid in the conversion data set
              SELECT DISTINCT
                FIRST_VALUE(subq_7.__visits) OVER (
                  PARTITION BY
                    subq_10.user
                    , subq_10.metric_time__month
                    , subq_10.mf_internal_uuid
                  ORDER BY subq_7.metric_time__month DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS __visits
                , FIRST_VALUE(subq_7.metric_time__month) OVER (
                  PARTITION BY
                    subq_10.user
                    , subq_10.metric_time__month
                    , subq_10.mf_internal_uuid
                  ORDER BY subq_7.metric_time__month DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS metric_time__month
                , FIRST_VALUE(subq_7.user) OVER (
                  PARTITION BY
                    subq_10.user
                    , subq_10.metric_time__month
                    , subq_10.mf_internal_uuid
                  ORDER BY subq_7.metric_time__month DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS user
                , subq_10.mf_internal_uuid AS mf_internal_uuid
                , subq_10.__buys_month AS __buys_month
              FROM (
                -- Pass Only Elements: ['__visits', 'metric_time__month', 'user']
                SELECT
                  subq_6.metric_time__month
                  , subq_6.user
                  , subq_6.__visits
                FROM (
                  -- Pass Only Elements: ['__visits', 'metric_time__month', 'user']
                  SELECT
                    subq_5.metric_time__month
                    , subq_5.user
                    , subq_5.__visits
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
                  ) subq_5
                ) subq_6
              ) subq_7
              INNER JOIN (
                -- Add column with generated UUID
                SELECT
                  subq_9.ds__day
                  , subq_9.ds__week
                  , subq_9.ds__month
                  , subq_9.ds__quarter
                  , subq_9.ds__year
                  , subq_9.ds__extract_year
                  , subq_9.ds__extract_quarter
                  , subq_9.ds__extract_month
                  , subq_9.ds__extract_day
                  , subq_9.ds__extract_dow
                  , subq_9.ds__extract_doy
                  , subq_9.ds_month__month
                  , subq_9.ds_month__quarter
                  , subq_9.ds_month__year
                  , subq_9.ds_month__extract_year
                  , subq_9.ds_month__extract_quarter
                  , subq_9.ds_month__extract_month
                  , subq_9.buy__ds__day
                  , subq_9.buy__ds__week
                  , subq_9.buy__ds__month
                  , subq_9.buy__ds__quarter
                  , subq_9.buy__ds__year
                  , subq_9.buy__ds__extract_year
                  , subq_9.buy__ds__extract_quarter
                  , subq_9.buy__ds__extract_month
                  , subq_9.buy__ds__extract_day
                  , subq_9.buy__ds__extract_dow
                  , subq_9.buy__ds__extract_doy
                  , subq_9.buy__ds_month__month
                  , subq_9.buy__ds_month__quarter
                  , subq_9.buy__ds_month__year
                  , subq_9.buy__ds_month__extract_year
                  , subq_9.buy__ds_month__extract_quarter
                  , subq_9.buy__ds_month__extract_month
                  , subq_9.metric_time__month
                  , subq_9.metric_time__quarter
                  , subq_9.metric_time__year
                  , subq_9.metric_time__extract_year
                  , subq_9.metric_time__extract_quarter
                  , subq_9.metric_time__extract_month
                  , subq_9.user
                  , subq_9.session_id
                  , subq_9.buy__user
                  , subq_9.buy__session_id
                  , subq_9.__buys_month
                  , uuid() AS mf_internal_uuid
                FROM (
                  -- Metric Time Dimension 'ds_month'
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
                    , subq_8.ds_month__month
                    , subq_8.ds_month__quarter
                    , subq_8.ds_month__year
                    , subq_8.ds_month__extract_year
                    , subq_8.ds_month__extract_quarter
                    , subq_8.ds_month__extract_month
                    , subq_8.buy__ds__day
                    , subq_8.buy__ds__week
                    , subq_8.buy__ds__month
                    , subq_8.buy__ds__quarter
                    , subq_8.buy__ds__year
                    , subq_8.buy__ds__extract_year
                    , subq_8.buy__ds__extract_quarter
                    , subq_8.buy__ds__extract_month
                    , subq_8.buy__ds__extract_day
                    , subq_8.buy__ds__extract_dow
                    , subq_8.buy__ds__extract_doy
                    , subq_8.buy__ds_month__month
                    , subq_8.buy__ds_month__quarter
                    , subq_8.buy__ds_month__year
                    , subq_8.buy__ds_month__extract_year
                    , subq_8.buy__ds_month__extract_quarter
                    , subq_8.buy__ds_month__extract_month
                    , subq_8.ds_month__month AS metric_time__month
                    , subq_8.ds_month__quarter AS metric_time__quarter
                    , subq_8.ds_month__year AS metric_time__year
                    , subq_8.ds_month__extract_year AS metric_time__extract_year
                    , subq_8.ds_month__extract_quarter AS metric_time__extract_quarter
                    , subq_8.ds_month__extract_month AS metric_time__extract_month
                    , subq_8.user
                    , subq_8.session_id
                    , subq_8.buy__user
                    , subq_8.buy__session_id
                    , subq_8.__buys_month
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
                      , EXTRACT(DAY_OF_WEEK FROM buys_source_src_28000.ds) AS ds__extract_dow
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
                      , EXTRACT(DAY_OF_WEEK FROM buys_source_src_28000.ds) AS buy__ds__extract_dow
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
                  ) subq_8
                ) subq_9
              ) subq_10
              ON
                (
                  subq_7.user = subq_10.user
                ) AND (
                  (
                    subq_7.metric_time__month <= subq_10.metric_time__month
                  ) AND (
                    subq_7.metric_time__month > DATE_ADD('month', -1, subq_10.metric_time__month)
                  )
                )
            ) subq_11
          ) subq_12
        ) subq_13
      ) subq_14
    ) subq_15
  ) subq_16
) subq_17
